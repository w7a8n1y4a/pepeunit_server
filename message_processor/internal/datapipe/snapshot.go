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
	definition ioField
}

type Snapshot struct {
	manifests       map[manifestVersion]manifestIO
	unitIOs         map[string]snapshotUnitIO
	links           map[string]map[string]struct{}
	conversions     map[unitPair]affineConversion
	selectedCommits map[uuid.UUID]string
}

func (p *Processor) startSnapshotSync(ctx context.Context) (chan<- struct{}, <-chan struct{}) {
	requests := make(chan struct{}, 1)
	ready := make(chan struct{})
	requests <- struct{}{}
	go func() {
		loaded := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-requests:
				if err := p.LoadSnapshot(ctx); err != nil {
					if ctx.Err() == nil {
						logging.Errorf("Failed to reload MessageProcessor snapshot: %v", err)
					}
					continue
				}
				if !loaded {
					close(ready)
					loaded = true
				}
			}
		}
	}()
	return requests, ready
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

	manifests, err := p.loadManifests(ctx, rows.Units, commits)
	if err != nil {
		return err
	}

	snapshot := buildSnapshot(rows, commits, manifests)
	snapshot.conversions, err = loadUnitConversions(ctx, snapshot)
	if err != nil {
		return fmt.Errorf("load MessageProcessor UCUM conversions: %w", err)
	}
	p.snapshot.Store(snapshot)
	return nil
}

func (p *Processor) loadManifests(
	ctx context.Context, units []database.UnitVersionSelection, commits map[uuid.UUID]string,
) (map[manifestVersion]manifestIO, error) {
	var cached map[manifestVersion]manifestIO
	if previous := p.snapshot.Load(); previous != nil {
		cached = previous.manifests
	}

	manifests := make(map[manifestVersion]manifestIO)
	seen := make(map[manifestVersion]struct{})
	var sources []manifestSource
	for _, unit := range units {
		commit := commits[unit.UnitUUID]
		if commit == "" {
			continue
		}
		version := manifestVersion{repoUUID: unit.RepoUUID, commit: commit}
		if _, exists := seen[version]; exists {
			continue
		}
		seen[version] = struct{}{}
		if definition, exists := cached[version]; exists {
			manifests[version] = definition
			continue
		}
		content, err := readGitManifest(ctx, p.repoCachePath, version)
		if err != nil {
			var gitError *exec.ExitError
			badVersion := errors.Is(err, errManifestTooLarge) || (errors.As(err, &gitError) && gitError.ExitCode() >= 0)
			if ctx.Err() != nil || !badVersion {
				return nil, fmt.Errorf("Unit %s, repo %s at %s: %w", unit.UnitUUID, version.repoUUID, version.commit, err)
			}
			logging.Warningf("Skipping manifest for repo %s at %s: %v", version.repoUUID, version.commit, err)
			continue
		}
		sources = append(sources, manifestSource{RepoUUID: version.repoUUID, Commit: version.commit, Content: content})
	}
	if len(sources) == 0 {
		return manifests, nil
	}

	var raw []json.RawMessage
	if err := runPythonJSON(ctx, "app.services.message_processor_service", sources, &raw, "--manifests"); err != nil {
		return nil, fmt.Errorf("validate Unit manifests: %w", err)
	}
	if len(raw) != len(sources) {
		return nil, fmt.Errorf("expected %d validated manifests, received %d", len(sources), len(raw))
	}

	for index, manifest := range raw {
		source := sources[index]
		var result struct {
			Manifest *manifestIO `json:"manifest"`
			Error    string      `json:"error"`
		}
		if err := decodeJSON(manifest, &result); err != nil {
			return nil, fmt.Errorf("decode manifest for repo %s at %s: %w", source.RepoUUID, source.Commit, err)
		}
		if result.Error != "" {
			logging.Warningf("Skipping manifest for repo %s at %s: %s", source.RepoUUID, source.Commit, result.Error)
			continue
		}
		if result.Manifest == nil {
			return nil, fmt.Errorf("missing result for manifest in repo %s at %s", source.RepoUUID, source.Commit)
		}
		manifests[manifestVersion{repoUUID: source.RepoUUID, commit: source.Commit}] = *result.Manifest
	}
	return manifests, nil
}

func selectCommits(ctx context.Context, units []database.UnitVersionSelection) (map[uuid.UUID]string, error) {
	if len(units) == 0 {
		return nil, nil
	}
	var results map[uuid.UUID]struct {
		Commit string `json:"commit"`
		Error  string `json:"error"`
	}
	if err := runPythonJSON(ctx, "app.services.message_processor_service", units, &results); err != nil {
		return nil, fmt.Errorf("select Unit commits: %w", err)
	}
	if len(results) != len(units) {
		return nil, fmt.Errorf("expected %d commit results, received %d", len(units), len(results))
	}
	commits := make(map[uuid.UUID]string, len(units))
	for _, unit := range units {
		result := results[unit.UnitUUID]
		if result.Error != "" {
			logging.Warningf("Skipping Unit %s in repo %s: %s", unit.UnitUUID, unit.RepoUUID, result.Error)
			continue
		}
		if !gitCommitPattern.MatchString(result.Commit) {
			return nil, fmt.Errorf("missing or invalid selected commit for Unit %s", unit.UnitUUID)
		}
		commits[unit.UnitUUID] = result.Commit
	}
	return commits, nil
}

func runPythonJSON(ctx context.Context, module string, input, output any, args ...string) error {
	body, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode %s input: %w", module, err)
	}
	cmd := exec.CommandContext(ctx, "python", append([]string{"-m", module}, args...)...)
	cmd.Stdin = bytes.NewReader(body)
	result, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
			return fmt.Errorf("run %s: %w: %s", module, err, bytes.TrimSpace(exitErr.Stderr))
		}
		return fmt.Errorf("run %s: %w", module, err)
	}
	if err := json.Unmarshal(result, output); err != nil {
		return fmt.Errorf("decode %s output: %w", module, err)
	}
	return nil
}

func buildSnapshot(
	rows database.ProcessorRows, commits map[uuid.UUID]string, manifests map[manifestVersion]manifestIO,
) *Snapshot {
	units := make(map[uuid.UUID]manifestIO, len(rows.Units))
	for _, unit := range rows.Units {
		version := manifestVersion{repoUUID: unit.RepoUUID, commit: commits[unit.UnitUUID]}
		definition, exists := manifests[version]
		if !exists {
			continue
		}
		units[unit.UnitUUID] = definition
	}
	snapshot := &Snapshot{
		manifests:       manifests,
		unitIOs:         make(map[string]snapshotUnitIO, len(rows.UnitIOs)),
		links:           make(map[string]map[string]struct{}),
		selectedCommits: make(map[uuid.UUID]string, len(units)),
	}
	counts := make(map[uuid.UUID]int, len(units))
	for _, node := range rows.UnitIOs {
		definitions, exists := units[node.UnitUUID]
		if !exists {
			continue
		}
		var definition ioField
		switch node.Direction {
		case inputDirection:
			definition = definitions.Input[node.TopicName]
		case outputDirection:
			definition = definitions.Output[node.TopicName]
		}
		if definition == nil {
			logging.Warningf("Skipping Unit %s: no manifest definition for %s %q (%s)", node.UnitUUID, node.Direction, node.TopicName, node.UUID)
			delete(units, node.UnitUUID)
			continue
		}
		snapshot.unitIOs[node.UUID.String()] = snapshotUnitIO{ProcessorUnitIO: node, definition: definition}
		counts[node.UnitUUID]++
	}
	for unitUUID, definitions := range units {
		if counts[unitUUID] != len(definitions.Input)+len(definitions.Output) {
			logging.Warningf("Skipping Unit %s: incomplete IO set", unitUUID)
			continue
		}
		snapshot.selectedCommits[unitUUID] = commits[unitUUID]
	}
	for id, node := range snapshot.unitIOs {
		if _, valid := snapshot.selectedCommits[node.UnitUUID]; !valid {
			delete(snapshot.unitIOs, id)
		}
	}
	for _, link := range rows.Links {
		output := link.OutputUUID.String()
		input := link.InputUUID.String()
		source, foundOutput := snapshot.unitIOs[output]
		target, foundInput := snapshot.unitIOs[input]
		if !foundOutput || !foundInput {
			continue
		}
		if source.Direction != outputDirection || target.Direction != inputDirection {
			logging.Warningf("Skipping invalid link %s -> %s: expected Output and Input", output, input)
			continue
		}
		if snapshot.links[output] == nil {
			snapshot.links[output] = make(map[string]struct{})
		}
		snapshot.links[output][input] = struct{}{}
	}
	return snapshot
}
