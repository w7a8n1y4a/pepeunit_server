package datapipe

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"message_processor/internal/database"
	"message_processor/internal/logging"

	"github.com/google/uuid"
)

const (
	resourceWriteTimeout = 10 * time.Second
	resourceStopTimeout  = 2 * time.Second
)

type resourceWindowKey struct {
	unitIOUUID uuid.UUID
	start      time.Time
}

type resourceBatch struct {
	token   uuid.UUID
	windows []database.UnitIOResourceWindow
}

type resourceBuffer struct {
	db       *database.ClickHouseDB
	interval time.Duration
	maxSize  int
	mu       sync.Mutex
	pending  map[resourceWindowKey]database.UnitIOResourceWindow
	ready    chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
}

func newResourceBuffer(db *database.ClickHouseDB, interval time.Duration, maxSize int) *resourceBuffer {
	return &resourceBuffer{
		db:       db,
		interval: interval,
		maxSize:  maxSize,
		pending:  make(map[resourceWindowKey]database.UnitIOResourceWindow),
		ready:    make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

func (b *resourceBuffer) add(at time.Time, increment database.UnitIOResourceWindow) {
	key := resourceWindowKey{unitIOUUID: increment.UnitIOUUID, start: at.UTC().Truncate(time.Hour)}
	b.mu.Lock()
	window, exists := b.pending[key]
	window.UnitIOUUID, window.Start = key.unitIOUUID, key.start
	window.Requests += increment.Requests
	window.Bytes += increment.Bytes
	window.DataPipeBytes += increment.DataPipeBytes
	b.pending[key] = window
	if !exists {
		b.signalIfFull()
	}
	b.mu.Unlock()
}

func (b *resourceBuffer) signalIfFull() {
	if len(b.pending) >= b.maxSize {
		select {
		case b.ready <- struct{}{}:
		default:
		}
	}
}

func (b *resourceBuffer) start(ctx context.Context) {
	ctx, b.cancel = context.WithCancel(context.WithoutCancel(ctx))
	go b.run(ctx)
}

func (b *resourceBuffer) stop() {
	b.cancel()
	<-b.done
}

func (b *resourceBuffer) run(ctx context.Context) {
	defer close(b.done)
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	var batch resourceBatch

	for {
		select {
		case <-ctx.Done():
			finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), resourceStopTimeout)
			hadRetry := len(batch.windows) != 0
			if b.flush(finalCtx, &batch) && hadRetry {
				b.flush(finalCtx, &batch)
			}
			cancel()
			return
		case <-b.ready:
			if len(batch.windows) != 0 {
				continue
			}
		case <-ticker.C:
		}
		b.flush(ctx, &batch)
	}
}

func (b *resourceBuffer) flush(ctx context.Context, batch *resourceBatch) bool {
	if len(batch.windows) == 0 {
		b.mu.Lock()
		pending := b.pending
		if len(pending) == 0 {
			b.mu.Unlock()
			return true
		}
		b.pending = make(map[resourceWindowKey]database.UnitIOResourceWindow)
		b.mu.Unlock()
		batch.token = uuid.New()
		batch.windows = slices.Collect(maps.Values(pending))
	}

	writeCtx, cancel := context.WithTimeout(ctx, resourceWriteTimeout)
	defer cancel()
	if err := b.db.InsertUnitIOResources(writeCtx, batch.token, batch.windows); err != nil {
		if ctx.Err() == nil {
			logging.Warningf("Failed to flush UnitIO resources: %v", err)
		}
		return false
	}
	*batch = resourceBatch{}
	b.mu.Lock()
	b.signalIfFull()
	b.mu.Unlock()
	return true
}
