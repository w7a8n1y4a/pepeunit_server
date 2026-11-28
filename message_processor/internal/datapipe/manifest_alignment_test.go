package datapipe

import (
	"context"
	"encoding/json"
	"math/big"
	"os/exec"
	"path/filepath"
	"strings"
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

func TestSnapshotLoadsHealthyUnitWhenAnotherCommitHasNoManifest(t *testing.T) {
	ctx := context.Background()
	healthy := database.UnitVersionSelection{UnitUUID: uuid.New(), RepoUUID: uuid.New()}
	broken := database.UnitVersionSelection{UnitUUID: uuid.New(), RepoUUID: uuid.New()}
	repoCache := t.TempDir()
	brokenRepo := filepath.Join(repoCache, broken.RepoUUID.String())
	if output, err := exec.Command("git", "init", brokenRepo).CombinedOutput(); err != nil {
		t.Fatalf("init repository: %v: %s", err, output)
	}
	if output, err := exec.Command("git", "-C", brokenRepo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "Commit without manifest").CombinedOutput(); err != nil {
		t.Fatalf("create commit: %v: %s", err, output)
	}
	output, err := exec.Command("git", "-C", brokenRepo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	brokenCommit := strings.TrimSpace(string(output))
	healthyVersion := manifestVersion{repoUUID: healthy.RepoUUID, commit: "healthy-commit"}
	commits := map[uuid.UUID]string{healthy.UnitUUID: healthyVersion.commit, broken.UnitUUID: brokenCommit}
	processor := Processor{repoCachePath: repoCache}
	processor.snapshot.Store(&Snapshot{manifests: map[manifestVersion]manifestIO{
		healthyVersion: {
			Input:  ioDeclarations{"counter": {"type": "int"}},
			Output: ioDeclarations{"state": {"type": "object"}},
		},
	}})
	state := database.ProcessorUnitIO{UUID: uuid.New(), UnitUUID: healthy.UnitUUID, Direction: outputDirection, TopicName: "state"}
	counter := database.ProcessorUnitIO{UUID: uuid.New(), UnitUUID: healthy.UnitUUID, Direction: inputDirection, TopicName: "counter"}
	brokenInput := database.ProcessorUnitIO{UUID: uuid.New(), UnitUUID: broken.UnitUUID, Direction: inputDirection, TopicName: "counter"}
	rows := database.ProcessorRows{
		Units:   []database.UnitVersionSelection{broken, healthy},
		UnitIOs: []database.ProcessorUnitIO{brokenInput, state, counter},
		Links: []database.ProcessorLink{
			{OutputUUID: state.UUID, InputUUID: brokenInput.UUID},
			{OutputUUID: state.UUID, InputUUID: counter.UUID},
		},
	}
	manifests, err := processor.loadManifests(ctx, rows.Units, commits)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := buildSnapshot(rows, commits, manifests)
	if len(snapshot.selectedCommits) != 1 || snapshot.selectedCommits[healthy.UnitUUID] != healthyVersion.commit {
		t.Fatalf("expected only the healthy Unit, got %v", snapshot.selectedCommits)
	}
	if len(snapshot.unitIOs) != 2 || snapshot.unitIOs[state.UUID.String()].TopicName != "state" {
		t.Fatalf("expected the healthy Unit's MQTT topics, got %v", snapshot.unitIOs)
	}
	links := snapshot.links[state.UUID.String()]
	if _, found := links[counter.UUID.String()]; !found || len(links) != 1 {
		t.Fatalf("expected only the link between healthy topics, got %v", links)
	}
}

func TestSnapshotIsolatesUnitsWithMismatchedIO(t *testing.T) {
	healthy := database.UnitVersionSelection{UnitUUID: uuid.New(), RepoUUID: uuid.New()}
	invalid := database.UnitVersionSelection{UnitUUID: uuid.New(), RepoUUID: healthy.RepoUUID}
	incomplete := database.UnitVersionSelection{UnitUUID: uuid.New(), RepoUUID: healthy.RepoUUID}
	commits := map[uuid.UUID]string{healthy.UnitUUID: "commit", invalid.UnitUUID: "commit", incomplete.UnitUUID: "commit"}
	manifests := map[manifestVersion]manifestIO{
		{repoUUID: healthy.RepoUUID, commit: "commit"}: {
			Input:  ioDeclarations{"counter": {"type": "int"}},
			Output: ioDeclarations{"state": {"type": "object"}},
		},
	}
	state := database.ProcessorUnitIO{UUID: uuid.New(), UnitUUID: healthy.UnitUUID, Direction: outputDirection, TopicName: "state"}
	counter := database.ProcessorUnitIO{UUID: uuid.New(), UnitUUID: healthy.UnitUUID, Direction: inputDirection, TopicName: "counter"}
	invalidState := database.ProcessorUnitIO{UUID: uuid.New(), UnitUUID: invalid.UnitUUID, Direction: outputDirection, TopicName: "state"}
	rows := database.ProcessorRows{
		Units: []database.UnitVersionSelection{healthy, invalid, incomplete},
		UnitIOs: []database.ProcessorUnitIO{
			state, counter, invalidState,
			{UUID: uuid.New(), UnitUUID: invalid.UnitUUID, Direction: inputDirection, TopicName: "old-counter"},
			{UUID: uuid.New(), UnitUUID: incomplete.UnitUUID, Direction: outputDirection, TopicName: "state"},
		},
		Links: []database.ProcessorLink{
			{OutputUUID: state.UUID, InputUUID: counter.UUID},
			{OutputUUID: counter.UUID, InputUUID: state.UUID},
			{OutputUUID: invalidState.UUID, InputUUID: counter.UUID},
		},
	}
	snapshot := buildSnapshot(rows, commits, manifests)
	if len(snapshot.selectedCommits) != 1 || len(snapshot.unitIOs) != 2 || len(snapshot.links) != 1 {
		t.Fatalf("expected only the healthy Unit and its valid link, got %+v", snapshot)
	}
	if _, found := snapshot.links[state.UUID.String()][counter.UUID.String()]; !found {
		t.Fatal("valid link was not loaded")
	}
}
