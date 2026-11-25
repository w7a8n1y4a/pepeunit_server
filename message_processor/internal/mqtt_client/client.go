package mqtt_client

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"message_processor/internal/config"
	"message_processor/internal/logging"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/golang-jwt/jwt/v5"
)

const (
	subscribeTimeout      = 10 * time.Second
	sessionExpirySeconds  = 24 * 60 * 60
	defaultKeepAliveSecs  = 60
	reconnectBackoffDelay = 5 * time.Second
)

type MQTTClient struct {
	cfg            *config.Config
	cm             *autopaho.ConnectionManager
	router         *paho.StandardRouter
	messageHandler func(topic string, payload []byte)
	connMu         sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	// connGen increments on every successful broker connection.
	// Subscribe retries are bound to a generation so a stale disconnect
	// callback cannot abort resubscribe on the live connection.
	connGen     uint64
	connectOnce sync.Once
}

func New(cfg *config.Config, messageHandler func(topic string, payload []byte)) (*MQTTClient, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}

	if messageHandler == nil {
		return nil, errors.New("message handler cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &MQTTClient{
		cfg:            cfg,
		router:         paho.NewStandardRouter(),
		messageHandler: messageHandler,
		ctx:            ctx,
		cancel:         cancel,
	}

	return client, nil
}

func (c *MQTTClient) Connect(ctx context.Context) error {
	var connectErr error
	c.connectOnce.Do(func() {
		token, err := generateToken(c.cfg)
		if err != nil {
			connectErr = fmt.Errorf("failed to generate token: %w", err)
			return
		}

		mqttURL := fmt.Sprintf(
			"mqtt://%s:%d",
			c.cfg.MQTTHost,
			c.cfg.MQTTPort,
		)

		serverURL, err := url.Parse(mqttURL)
		if err != nil {
			connectErr = fmt.Errorf("failed to parse MQTT URL: %w", err)
			return
		}

		keepAlive := uint16(c.cfg.MQTTKeepAlive)
		if keepAlive == 0 {
			keepAlive = defaultKeepAliveSecs
		}

		cliCfg := autopaho.ClientConfig{
			ServerUrls:                    []*url.URL{serverURL},
			KeepAlive:                     keepAlive,
			CleanStartOnInitialConnection: true,
			// Keep the MQTT session on the broker across brief outages so
			// subscriptions survive even if a SUBSCRIBE after reconnect is delayed.
			SessionExpiryInterval: sessionExpirySeconds,
			ReconnectBackoff:      autopaho.NewConstantBackoff(reconnectBackoffDelay),
			ConnectTimeout:        subscribeTimeout,
			OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
				// Reconnection is owned by autopaho. This callback is the only
				// safe place to (re)subscribe: SessionPresent may be false after
				// a broker restart, so SUBSCRIBE is always sent.
				gen := c.markConnected()
				if connAck != nil && connAck.SessionPresent {
					logging.Infof("MQTT connection up at %s:%d (session present, generation %d)", c.cfg.MQTTHost, c.cfg.MQTTPort, gen)
				} else {
					logging.Infof("MQTT connection up at %s:%d (new session, generation %d)", c.cfg.MQTTHost, c.cfg.MQTTPort, gen)
				}

				// SUBSCRIBE runs in the background. A rejected or timed out
				// attempt closes this connection so the next one is clean.
				c.startSubscribeRetry(cm, gen)
			},
			OnConnectError: func(err error) {
				logging.Warningf("MQTT connection error: %v", err)
			},
			ClientConfig: paho.ClientConfig{
				ClientID: c.cfg.Domain,
				Router:   c.router,
				OnClientError: func(err error) {
					logging.Warningf("MQTT client error: %v", err)
				},
				OnServerDisconnect: func(disconnect *paho.Disconnect) {
					logging.Warningf("MQTT server disconnected: %v", disconnect)
				},
			},
			ConnectUsername: token,
		}

		c.router.RegisterHandler("#", func(p *paho.Publish) {
			c.messageHandler(p.Topic, p.Payload)
		})

		cm, err := autopaho.NewConnection(c.ctx, cliCfg)
		if err != nil {
			connectErr = fmt.Errorf("failed to create connection manager: %w", err)
			return
		}

		c.connMu.Lock()
		c.cm = cm
		c.connMu.Unlock()

		if err := cm.AwaitConnection(ctx); err != nil {
			connectErr = fmt.Errorf("failed to wait for connection: %w", err)
			return
		}
	})

	return connectErr
}

func (c *MQTTClient) markConnected() uint64 {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	c.connGen++
	return c.connGen
}

func (c *MQTTClient) currentConnGen() uint64 {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.connGen
}

func (c *MQTTClient) startSubscribeRetry(cm *autopaho.ConnectionManager, gen uint64) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.subscribeWithRetry(cm, gen)
	}()
}

func (c *MQTTClient) subscribeWithRetry(cm *autopaho.ConnectionManager, gen uint64) {
	if c.ctx.Err() != nil || c.currentConnGen() != gen {
		return
	}
	if err := c.subscribe(cm); err != nil && c.ctx.Err() == nil {
		logging.Warningf("Failed to subscribe, reconnecting MQTT in %s: %v", reconnectBackoffDelay, err)
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(reconnectBackoffDelay):
		}
		if c.currentConnGen() == gen {
			cm.TerminateConnectionForTest()
		}
	}
}

func (c *MQTTClient) subscribe(cm *autopaho.ConnectionManager) error {
	if cm == nil {
		return errors.New("mqtt connection manager is not initialized")
	}

	topic := c.cfg.Domain + "/+"
	ctx, cancel := context.WithTimeout(c.ctx, subscribeTimeout)
	defer cancel()

	_, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{
				Topic: topic,
				QoS:   1,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	logging.Infof("Success subscribed to topic pattern: %s", topic)
	return nil
}

func (c *MQTTClient) Publish(ctx context.Context, topic string, payload []byte, qos byte) error {
	c.connMu.RLock()
	cm := c.cm
	c.connMu.RUnlock()
	if cm == nil {
		return errors.New("MQTT publisher is unavailable")
	}
	_, err := cm.Publish(ctx, &paho.Publish{Topic: topic, Payload: payload, QoS: qos})
	return err
}

func (c *MQTTClient) Disconnect() {
	c.cancel()
	c.connMu.RLock()
	cm := c.cm
	c.connMu.RUnlock()
	if cm != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = cm.Disconnect(ctx)
	}
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
	}
}

func generateToken(cfg *config.Config) (string, error) {
	claims := jwt.MapClaims{
		"domain": cfg.Domain,
		"type":   "Backend",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	signedToken, err := token.SignedString([]byte(cfg.SecretKey))
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %v", err)
	}

	return signedToken, nil
}
