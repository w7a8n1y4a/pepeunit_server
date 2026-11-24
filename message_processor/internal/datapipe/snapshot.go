package datapipe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"

	"message_processor/internal/database"
	"message_processor/internal/logging"

	"github.com/google/uuid"
)

const (
	inputDirection  = "Input"
	outputDirection = "Output"
)

type manifestVersion struct {
	repoUUID uuid.UUID
	commit   string
}

type snapshotUnitIO struct {
	database.ProcessorUnitIO
	definition json.RawMessage
}

type Snapshot struct {
	unitIOs map[string]snapshotUnitIO
	links   map[string]map[string]struct{}
}

func (p *Processor) startSnapshotSync(ctx context.Context) chan<- struct{} {
	requests := make(chan struct{}, 1)
	requests <- struct{}{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-requests:
				if err := p.LoadSnapshot(ctx); err != nil && ctx.Err() == nil {
					logging.Errorf("Failed to reload MessageProcessor schemas: %v", err)
				}
			}
		}
	}()
	return requests
}

func (p *Processor) LoadSnapshot(ctx context.Context) error {
	rows, err := p.postgresDB.ReadProcessorRows(ctx)
	if err != nil {
		return fmt.Errorf("read MessageProcessor configuration: %w", err)
	}
	commits, err := selectCommits(ctx, rows.Units)
	if err != nil {
		return err
	}

	manifests := make(map[manifestVersion]manifestIO)
	dictionaries := make(map[string]*manifestDictionary)
	units := make(map[uuid.UUID]manifestIO, len(rows.Units))
	for _, unit := range rows.Units {
		version := manifestVersion{repoUUID: unit.RepoUUID, commit: commits[unit.UnitUUID]}
		definitions, exists := manifests[version]
		if !exists {
			definitions, err = loadManifestIO(ctx, p.repoCachePath, version, dictionaries)
			if err != nil {
				return fmt.Errorf("Unit %s, repo %s at %s: %w", unit.UnitUUID, unit.RepoUUID, version.commit, err)
			}
			manifests[version] = definitions
		}
		units[unit.UnitUUID] = definitions
	}

	snapshot, err := buildSnapshot(rows, units)
	if err != nil {
		return err
	}
	p.snapshot.Store(snapshot)
	return nil
}

func selectCommits(ctx context.Context, units []database.UnitVersionSelection) (map[uuid.UUID]string, error) {
	if len(units) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(units)
	if err != nil {
		return nil, fmt.Errorf("encode Unit versions: %w", err)
	}

	cmd := exec.CommandContext(ctx, "python", "-m", "app.services.message_processor_service")
	cmd.Stdin = bytes.NewReader(body)
	result, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
			return nil, fmt.Errorf("select Unit commits: %w: %s", err, bytes.TrimSpace(exitErr.Stderr))
		}
		return nil, fmt.Errorf("select Unit commits: %w", err)
	}

	var commits map[uuid.UUID]string
	if err := json.Unmarshal(result, &commits); err != nil {
		return nil, fmt.Errorf("decode selected commits: %w", err)
	}
	if len(commits) != len(units) {
		return nil, fmt.Errorf("expected %d selected commits, received %d", len(units), len(commits))
	}
	for _, unit := range units {
		if !gitCommitPattern.MatchString(commits[unit.UnitUUID]) {
			return nil, fmt.Errorf("missing or invalid selected commit for Unit %s", unit.UnitUUID)
		}
	}
	return commits, nil
}

func buildSnapshot(rows database.ProcessorRows, units map[uuid.UUID]manifestIO) (*Snapshot, error) {
	snapshot := &Snapshot{
		unitIOs: make(map[string]snapshotUnitIO, len(rows.UnitIOs)),
		links:   make(map[string]map[string]struct{}),
	}
	counts := make(map[uuid.UUID]int, len(units))
	for _, node := range rows.UnitIOs {
		definitions := units[node.UnitUUID]
		var definition json.RawMessage
		switch node.Direction {
		case inputDirection:
			definition = definitions.input[node.TopicName]
		case outputDirection:
			definition = definitions.output[node.TopicName]
		}
		if definition == nil {
			return nil, fmt.Errorf("Unit %s has no schema for %s %q (%s)", node.UnitUUID, node.Direction, node.TopicName, node.UUID)
		}
		snapshot.unitIOs[node.UUID.String()] = snapshotUnitIO{ProcessorUnitIO: node, definition: definition}
		counts[node.UnitUUID]++
	}
	for unitUUID, definitions := range units {
		if counts[unitUUID] != len(definitions.input)+len(definitions.output) {
			return nil, fmt.Errorf("incomplete IO set for Unit %s", unitUUID)
		}
	}
	for _, link := range rows.Links {
		output := link.OutputUUID.String()
		input := link.InputUUID.String()
		if snapshot.unitIOs[output].Direction != outputDirection || snapshot.unitIOs[input].Direction != inputDirection {
			return nil, fmt.Errorf("link %s -> %s is missing its Output or Input", output, input)
		}
		if snapshot.links[output] == nil {
			snapshot.links[output] = make(map[string]struct{})
		}
		snapshot.links[output][input] = struct{}{}
	}
	return snapshot, nil
}
