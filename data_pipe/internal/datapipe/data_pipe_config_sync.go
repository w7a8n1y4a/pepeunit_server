package datapipe

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"data_pipe/internal/config"
	"data_pipe/internal/database"
	"data_pipe/internal/logging"
	"data_pipe/internal/types"

	"github.com/google/uuid"
)

type storedNodeConfig struct {
	raw         string
	unitUUID    uuid.UUID
	creatorUUID uuid.UUID
	topicName   string
	unitName    string
}

// NodeTarget is who and where a data pipe alert belongs
type NodeTarget struct {
	UnitUUID    uuid.UUID
	CreatorUUID uuid.UUID
	TopicName   string
	UnitName    string
}

// DataPipeConfigs stores and manages DataPipe configurations for nodes
type DataPipeConfigs struct {
	mu      sync.RWMutex
	configs map[string]storedNodeConfig
	cfg     *config.Config
}

// NewDataPipeConfigs creates a new DataPipeConfigs instance
func NewDataPipeConfigs(cfg *config.Config) *DataPipeConfigs {
	return &DataPipeConfigs{
		configs: make(map[string]storedNodeConfig),
		cfg:     cfg,
	}
}

// Get retrieves a node's configuration together with the unit that owns the node
func (c *DataPipeConfigs) Get(ioUUID string) (types.DataPipeConfig, NodeTarget, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	stored, exists := c.configs[ioUUID]
	if !exists {
		return types.DataPipeConfig{}, NodeTarget{}, false
	}

	var config types.DataPipeConfig
	if err := json.Unmarshal([]byte(stored.raw), &config); err != nil {
		return types.DataPipeConfig{}, NodeTarget{}, false
	}
	return config, NodeTarget{
		UnitUUID:    stored.unitUUID,
		CreatorUUID: stored.creatorUUID,
		TopicName:   stored.topicName,
		UnitName:    stored.unitName,
	}, true
}

// LoadNodeConfigs loads active node configurations from PostgreSQL
func (c *DataPipeConfigs) LoadNodeConfigs(ctx context.Context, postgresDB *database.PostgresDB) error {
	nodes, err := postgresDB.GetActiveUnitIOs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active unit IO: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Create a map of existing configs for quick lookup
	existingConfigs := make(map[string]struct{})
	for uuid := range c.configs {
		existingConfigs[uuid] = struct{}{}
	}

	// Update configs and track which ones are still active
	activeConfigs := make(map[string]struct{})
	for _, node := range nodes {
		activeConfigs[node.UUID.String()] = struct{}{}
		if node.DataPipeYML != nil {
			c.configs[node.UUID.String()] = storedNodeConfig{
				raw:         *node.DataPipeYML,
				unitUUID:    node.UnitUUID,
				creatorUUID: node.CreatorUUID,
				topicName:   node.TopicName,
				unitName:    node.UnitName,
			}
		}
	}

	// Remove configs for nodes that are no longer active
	for uuid := range existingConfigs {
		if _, active := activeConfigs[uuid]; !active {
			delete(c.configs, uuid)
		}
	}

	return nil
}

const messageProcessorStream = "backend_message_processor"

func (c *DataPipeConfigs) StartConfigSync(ctx context.Context, postgresDB *database.PostgresDB, redisDB *database.RedisDB) {
	changes := make(chan int)
	go func() {
		defer close(changes)
		lastID := "$"
		for ctx.Err() == nil {
			messages, err := redisDB.ReadStream(ctx, messageProcessorStream, lastID)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logging.Errorf("Failed to read MessageProcessor reload signals: %v", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			if len(messages) == 0 {
				continue
			}
			lastID = messages[len(messages)-1].ID
			select {
			case <-ctx.Done():
				return
			case changes <- len(messages):
			}
		}
	}()

	go func() {
		periodic := time.NewTicker(time.Duration(c.cfg.ConfigSyncInterval) * time.Second)
		defer periodic.Stop()
		flush := time.NewTicker(time.Duration(c.cfg.BufferFlushInterval) * time.Second)
		defer flush.Stop()

		pending := 0
		for {
			select {
			case <-ctx.Done():
				return
			case count, ok := <-changes:
				if !ok {
					return
				}
				pending += count
				if pending < c.cfg.BufferMaxSize {
					continue
				}
			case <-flush.C:
				if pending == 0 {
					continue
				}
			case <-periodic.C:
			}

			if err := c.LoadNodeConfigs(ctx, postgresDB); err != nil {
				logging.Errorf("Failed to reload configurations: %v", err)
				continue
			}
			pending = 0
		}
	}()
}
