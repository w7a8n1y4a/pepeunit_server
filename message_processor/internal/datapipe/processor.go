package datapipe

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"message_processor/internal/apperr"
	"message_processor/internal/config"
	"message_processor/internal/database"
	"message_processor/internal/datapipe/active_period"
	"message_processor/internal/datapipe/alerts"
	"message_processor/internal/datapipe/cleanup"
	"message_processor/internal/datapipe/filters"
	"message_processor/internal/datapipe/processing_policy"
	"message_processor/internal/datapipe/transformations"
	"message_processor/internal/logging"

	"github.com/google/uuid"
)

// NodeState stores the state for a single node
type NodeState struct {
	LastMessageTime time.Time
	LastValue       string
	Alert           alerts.State
	mu              sync.RWMutex
}

// Processor handles message processing and configuration management
type Processor struct {
	clickhouseDB      *database.ClickHouseDB
	postgresDB        *database.PostgresDB
	domain            string
	repoCachePath     string
	configs           *DataPipeConfigs
	snapshot          atomic.Pointer[Snapshot]
	nodeStates        map[string]*NodeState
	statesMu          sync.RWMutex
	policy            *processing_policy.ProcessingPolicy
	alertBuffer       *alerts.Buffer
	cleanup           *cleanup.NRecordsCleanupService
	notifications     bool
	dataPipeEnabled   atomic.Bool
	firmwareWait      time.Duration
	unitLogExpiration time.Duration
	publish           publishMessage
	resources         *resourceBuffer
}

// NewProcessor creates a new Processor instance
func NewProcessor(
	clickhouseDB *database.ClickHouseDB, postgresDB *database.PostgresDB, cfg *config.Config, publish publishMessage,
) *Processor {
	flushInterval := time.Duration(cfg.BufferFlushInterval) * time.Second
	bufferFactory := processing_policy.NewBufferFactory(
		postgresDB,
		clickhouseDB,
		flushInterval,
		cfg.BufferMaxSize,
	)
	policy := processing_policy.NewProcessingPolicy(bufferFactory)
	alertBuffer := alerts.NewBuffer(postgresDB, flushInterval, cfg.BufferMaxSize)
	cleanup := cleanup.NewNRecordsCleanupService(clickhouseDB, cfg)

	processor := &Processor{
		clickhouseDB:      clickhouseDB,
		postgresDB:        postgresDB,
		domain:            cfg.Domain,
		repoCachePath:     cfg.RepoCachePath,
		configs:           NewDataPipeConfigs(cfg),
		nodeStates:        make(map[string]*NodeState),
		policy:            policy,
		alertBuffer:       alertBuffer,
		cleanup:           cleanup,
		notifications:     cfg.NotificationEnable,
		publish:           publish,
		firmwareWait:      2 * time.Duration(cfg.UnitStateSendInterval) * time.Second,
		unitLogExpiration: time.Duration(cfg.UnitLogExpiration) * time.Second,
	}
	processor.SetDataPipeEnabled(cfg.DatapipeEnable)
	processor.resources = newResourceBuffer(clickhouseDB, flushInterval, cfg.BufferMaxSize)
	clickhouseDB.SetDataPipeWriteRecorder(func(unitIOUUID uuid.UUID, at time.Time, size uint64) {
		processor.resources.add(at, database.UnitIOResourceWindow{UnitIOUUID: unitIOUUID, DataPipeBytes: size})
	})
	return processor
}

func (p *Processor) SetDataPipeEnabled(enabled bool) {
	p.dataPipeEnabled.Store(enabled)
}

// LoadNodeConfigs loads active node configurations from PostgreSQL
func (p *Processor) LoadNodeConfigs(ctx context.Context) error {
	return p.configs.LoadNodeConfigs(ctx, p.postgresDB)
}

// StartConfigSync starts background processes for configuration synchronization
func (p *Processor) StartConfigSync(ctx context.Context, redisDB *database.RedisDB) <-chan struct{} {
	requests, ready := p.startSnapshotSync(ctx)
	p.configs.StartConfigSync(ctx, p.postgresDB, redisDB, requests)
	return ready
}

// Start starts the processor
func (p *Processor) Start(ctx context.Context) {
	p.resources.start(ctx)
	p.policy.Start(ctx)
	if p.notifications {
		p.alertBuffer.Start(ctx)
	}
	p.cleanup.Start(ctx)
}

// Stop stops all background processes
func (p *Processor) Stop() {
	p.policy.Stop()
	p.alertBuffer.Stop()
	p.cleanup.Stop()
	p.resources.stop()
}

// getNodeState returns or creates a NodeState for the given node
func (p *Processor) getNodeState(ioUUID string) *NodeState {
	p.statesMu.Lock()
	defer p.statesMu.Unlock()

	state, exists := p.nodeStates[ioUUID]
	if !exists {
		state = &NodeState{}
		p.nodeStates[ioUUID] = state
	}
	return state
}

// ProcessMessage processes a message from a topic
func (p *Processor) ProcessMessage(ctx context.Context, topic string, payload []byte) error {
	domain, address, found := strings.Cut(topic, "/")
	ioUUID, err := uuid.Parse(address)
	if !found || domain != p.domain || err != nil || ioUUID.String() != address {
		return apperr.New(apperr.InvalidTopic, "invalid UnitIO topic: %s", topic)
	}
	return p.processUnitIOMessage(ctx, address, payload, time.Now())
}

func (p *Processor) processDataPipe(ctx context.Context, ioUUID string, payload []byte, raw bool) error {
	if !p.dataPipeEnabled.Load() {
		return nil
	}
	config, target, exists := p.configs.Get(ioUUID)
	if !exists {
		return nil
	}

	currentTime := time.Now()
	if !active_period.IsActive(&config.ActivePeriod, currentTime) {
		logging.Infof("Node %s is not active at %v", ioUUID, currentTime)
		return nil
	}

	message := string(payload)
	if raw {
		message = base64.StdEncoding.EncodeToString(payload)
	}
	nodeState := p.getNodeState(ioUUID)
	shouldProcess := filters.ApplyFilters(message, config.Filters, nodeState.LastMessageTime, nodeState.LastValue)
	if !shouldProcess {
		return nil
	}

	transformedValue := message
	if config.Transformations != nil {
		// Apply transformations
		var err error
		transformedValue, err = transformations.ApplyTransformations(message, config.Transformations, &config.Filters)
		if err != nil {
			logging.Errorf("Failed to apply transformations for node %s: %v", ioUUID, err)
			return nil
		}
	}

	// Apply processing policy
	uuid, err := uuid.Parse(ioUUID)
	if err != nil {
		return apperr.Wrap(apperr.InvalidNodeUUID, err, "invalid node UUID")
	}

	// Create a new context with the policy and filters configurations
	ctxWithConfig := context.WithValue(ctx, "policy", config.ProcessingPolicy)
	ctxWithConfig = context.WithValue(ctxWithConfig, "filters", config.Filters)

	if err := p.policy.ApplyProcessingPolicy(ctxWithConfig, uuid, transformedValue, currentTime, config.ProcessingPolicy); err != nil {
		logging.Errorf("Failed to apply processing policy for node %s: %v", ioUUID, err)
		return fmt.Errorf("failed to apply processing policy: %w", err)
	}

	// Update node state and evaluate the alert on the value that passed the pipe
	nodeState.mu.Lock()
	nodeState.LastMessageTime = currentTime
	nodeState.LastValue = transformedValue
	var violation alerts.Violation
	if p.notifications && config.Alerts != nil {
		violation, nodeState.Alert = alerts.Evaluate(
			transformedValue, config.Alerts, config.Filters.TypeInputValue, nodeState.Alert, currentTime,
		)
	}
	nodeState.mu.Unlock()

	if violation.Any() {
		row, err := alerts.Build(ioUUID, alerts.Target{
			UnitUUID:    target.UnitUUID.String(),
			CreatorUUID: target.CreatorUUID.String(),
			TopicName:   target.TopicName,
			UnitName:    target.UnitName,
		}, transformedValue, config.Alerts, violation)
		if err != nil {
			logging.Errorf("Failed to build data pipe alert for node %s: %v", ioUUID, err)
			return nil
		}
		if err := p.alertBuffer.Add(ctx, row); err != nil {
			logging.Errorf("Failed to buffer data pipe alert for node %s: %v", ioUUID, err)
		}
	}

	return nil
}
