package datapipe

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"message_processor/internal/database"
)

func TestAnyNumberUsesTheInputUnit(t *testing.T) {
	snapshot := Snapshot{conversions: map[unitPair]affineConversion{
		{"s", "s"}: {scale: big.NewRat(1, 1), offset: big.NewRat(0, 1)},
	}}
	input := ioField{
		"type": "int", "qos": json.Number("1"), "codec": codecJSON, "unit": "s",
		"min": json.Number("0"), "max": json.Number("3"),
	}
	payload, qos, err := snapshot.forwardPayload(json.Number("2"), ioField{"type": "any"}, input)
	if err != nil || string(payload) != "2" || qos != 1 {
		t.Fatalf("expected payload 2 at QoS 1, got %q at QoS %d: %v", payload, qos, err)
	}
}

func TestRepeatedMessagesFillTheResourceBuffer(t *testing.T) {
	buffer := newResourceBuffer(nil, time.Second, 3)
	window := database.UnitIOResourceWindow{UnitIOUUID: uuid.New(), Requests: 1}
	now := time.Now()
	buffer.add(now, window)
	buffer.add(now, window)
	select {
	case <-buffer.ready:
		t.Fatal("buffer flushed below message threshold")
	default:
	}
	buffer.add(now, window)
	select {
	case <-buffer.ready:
	default:
		t.Fatal("buffer ignored messages for the same UnitIO")
	}
	if len(buffer.pending) != 1 {
		t.Fatalf("expected one grouped window, got %d", len(buffer.pending))
	}
}

func TestLoadManifestsReusesOnlyRequestedVersions(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	first := manifestVersion{repoUUID: uuid.New(), commit: "commit"}
	second := manifestVersion{repoUUID: uuid.New(), commit: first.commit}
	stale := manifestVersion{repoUUID: first.repoUUID, commit: "older-commit"}
	previous := &Snapshot{manifests: map[manifestVersion]manifestIO{
		first:  {Input: ioDeclarations{"first": {"type": "int"}}},
		second: {Input: ioDeclarations{"second": {"type": "string"}}},
		stale:  {Input: ioDeclarations{"stale": {"type": "bool"}}},
	}}
	var processor Processor
	processor.snapshot.Store(previous)

	units := []database.UnitVersionSelection{
		{UnitUUID: uuid.New(), RepoUUID: first.repoUUID},
		{UnitUUID: uuid.New(), RepoUUID: first.repoUUID},
		{UnitUUID: uuid.New(), RepoUUID: second.repoUUID},
	}
	commits := map[uuid.UUID]string{
		units[0].UnitUUID: first.commit,
		units[1].UnitUUID: first.commit,
		units[2].UnitUUID: second.commit,
	}
	manifests, err := processor.loadManifests(context.Background(), units, commits)
	if err != nil {
		t.Fatalf("cached manifests should load without Git or Python: %v", err)
	}
	if len(manifests) != 2 || manifests[first].Input["first"] == nil || manifests[second].Input["second"] == nil {
		t.Fatalf("expected both requested repo/commit pairs, got %#v", manifests)
	}
	if len(previous.manifests) != 3 {
		t.Fatal("loading manifests changed the previous snapshot")
	}
}

func TestLoadManifestsRequiresValidationForNewCommit(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	unit := database.UnitVersionSelection{UnitUUID: uuid.New(), RepoUUID: uuid.New()}
	previous := &Snapshot{manifests: map[manifestVersion]manifestIO{
		{repoUUID: unit.RepoUUID, commit: "old-commit"}: {},
	}}
	var processor Processor
	processor.snapshot.Store(previous)

	commits := map[uuid.UUID]string{unit.UnitUUID: "new-commit"}
	manifests, err := processor.loadManifests(context.Background(), []database.UnitVersionSelection{unit}, commits)
	if err == nil || manifests != nil {
		t.Fatalf("expected a cache miss and a Git error, got %#v, %v", manifests, err)
	}
	if processor.snapshot.Load() != previous || len(previous.manifests) != 1 {
		t.Fatal("failed manifest loading changed the previous snapshot")
	}
}
