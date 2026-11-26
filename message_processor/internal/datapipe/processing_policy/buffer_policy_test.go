package processing_policy

import (
	"context"
	"testing"
	"time"

	"message_processor/internal/types"

	"github.com/google/uuid"
)

func TestBufferedRecordsKeepEachNodesLimit(t *testing.T) {
	buffer := NewNRecordsBuffer(nil, time.Second, 10)
	first, second := uuid.New(), uuid.New()
	for node, limit := range map[uuid.UUID]int{first: 2, second: 10} {
		ctx := context.WithValue(context.Background(), "policy", types.ProcessingPolicyConfig{NRecordsCount: &limit})
		ctx = context.WithValue(ctx, "filters", types.FiltersConfig{})
		if err := buffer.Add(ctx, node, "1", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if buffer.updates[first][0].MaxCount != 2 || buffer.updates[second][0].MaxCount != 10 {
		t.Fatalf("limits were not retained per record: %v", buffer.updates)
	}
}

func TestAggregationWindowsKeepEachNodesPolicy(t *testing.T) {
	buffer := NewAggregationBuffer(nil, time.Second, 10)
	first, second := uuid.New(), uuid.New()
	now := time.Now().Add(time.Hour)
	for node, function := range map[uuid.UUID]types.AggregationFunctions{first: types.AggregationFunctionsMin, second: types.AggregationFunctionsMax} {
		windowSize := 60
		if node == second {
			windowSize = 300
		}
		ctx := context.WithValue(context.Background(), "policy", types.ProcessingPolicyConfig{
			AggregationFunctions: &function,
			TimeWindowSize:       &windowSize,
		})
		if err := buffer.Add(ctx, node, "1", now); err != nil {
			t.Fatal(err)
		}
	}
	firstWindow := buffer.updates[first][getWindowKey(now, 60)]
	secondWindow := buffer.updates[second][getWindowKey(now, 300)]
	if firstWindow == nil || firstWindow.Function != types.AggregationFunctionsMin || firstWindow.WindowSize != 60 {
		t.Fatalf("wrong policy for first node: %+v", firstWindow)
	}
	if secondWindow == nil || secondWindow.Function != types.AggregationFunctionsMax || secondWindow.WindowSize != 300 {
		t.Fatalf("wrong policy for second node: %+v", secondWindow)
	}
}

func TestLastValueRejectsInvalidTextBeforeItEntersBatch(t *testing.T) {
	buffer := NewMessageBuffer(nil, time.Second, 10)
	ctx := context.Background()
	now := time.Now()
	if err := buffer.Add(ctx, uuid.New(), "bad\x00value", now); err == nil {
		t.Fatal("PostgreSQL text with NUL was accepted")
	}
	healthy := uuid.New()
	if err := buffer.Add(ctx, healthy, "ok", now); err != nil {
		t.Fatal(err)
	}
	if len(buffer.updates) != 1 || buffer.updates[healthy].State != "ok" {
		t.Fatalf("invalid text entered the batch: %v", buffer.updates)
	}
}
