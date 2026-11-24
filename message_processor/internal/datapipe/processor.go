package datapipe

import (
	"context"
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
	clickhouseDB  *database.ClickHouseDB
	postgresDB    *database.PostgresDB
	domain        string
	repoCachePath string
	configs       *DataPipeConfigs
	snapshot      atomic.Pointer[Snapshot]
	nodeStates    map[string]*NodeState
	statesMu      sync.RWMutex
	policy        *processing_policy.ProcessingPolicy
	alertBuffer   *alerts.Buffer
	cleanup       *cleanup.NRecordsCleanupService
	notifications bool
}

// NewProcessor creates a new Processor instance
func NewProcessor(clickhouseDB *database.ClickHouseDB, postgresDB *database.PostgresDB, cfg *config.Config) *Processor {
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

	return &Processor{
		clickhouseDB:  clickhouseDB,
		postgresDB:    postgresDB,
		domain:        cfg.Domain,
		repoCachePath: cfg.RepoCachePath,
		configs:       NewDataPipeConfigs(cfg),
		nodeStates:    make(map[string]*NodeState),
		policy:        policy,
		alertBuffer:   alertBuffer,
		cleanup:       cleanup,
		notifications: cfg.NotificationEnable,
	}
}

// LoadNodeConfigs loads active node configurations from PostgreSQL
func (p *Processor) LoadNodeConfigs(ctx context.Context) error {
	return p.configs.LoadNodeConfigs(ctx, p.postgresDB)
}

// StartConfigSync starts background processes for configuration synchronization
func (p *Processor) StartConfigSync(ctx context.Context, redisDB *database.RedisDB) {
	p.configs.StartConfigSync(ctx, p.postgresDB, redisDB, p.startSnapshotSync(ctx))
}

// Start starts the processor
func (p *Processor) Start(ctx context.Context) {
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
	if domain, ioUUID, ok := strings.Cut(topic, "/"); ok && !strings.Contains(ioUUID, "/") {
		if domain != p.domain || uuid.Validate(ioUUID) != nil {
			return apperr.New(apperr.InvalidTopic, "invalid UnitIO topic: %s", topic)
		}
		return p.validateMessage(ioUUID, payload)
	}

	// Extract the IO UUID from the topic
	ioUUID := extractIOUUID(topic)
	if ioUUID == "" {
		return apperr.New(apperr.InvalidTopic, "invalid topic format: %s", topic)
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

	nodeState := p.getNodeState(ioUUID)
	shouldProcess := filters.ApplyFilters(string(payload), config.Filters, nodeState.LastMessageTime, nodeState.LastValue)
	if !shouldProcess {
		return nil
	}

	transformedValue := string(payload)
	if config.Transformations != nil {
		// Apply transformations
		var err error
		transformedValue, err = transformations.ApplyTransformations(string(payload), config.Transformations, &config.Filters)
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

func (p *Processor) validateMessage(ioUUID string, payload []byte) error {
	snapshot := p.snapshot.Load()
	if snapshot == nil {
		return fmt.Errorf("MessageProcessor schemas have not been loaded")
	}
	node, exists := snapshot.unitIOs[ioUUID]
	if !exists {
		return fmt.Errorf("UnitIO %s has no loaded schema", ioUUID)
	}
	if _, err := decodePayload(node.definition, payload); err != nil {
		return fmt.Errorf("UnitIO %s: %w", ioUUID, err)
	}
	return nil
}

// extractIOUUID extracts the IO UUID from a topic
func extractIOUUID(topic string) string {
	// Topic format: pu_dp_domain/io_uuid
	parts := strings.Split(topic, "/")
	if len(parts) != 3 {
		return ""
	}
	return parts[1]
}
