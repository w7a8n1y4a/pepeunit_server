package datapipe

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"message_processor/internal/database"
	"message_processor/internal/logging"

	"github.com/google/uuid"
)

const (
	stateOutput     = "state"
	logOutput       = "log"
	deviceCommitKey = "pu_commit_version"
	logTimeKey      = "create_datetime"
	logLevelKey     = "level"
	logTextKey      = "text"
)

func (p *Processor) processStandardOutput(
	ctx context.Context, snapshot *Snapshot, source snapshotUnitIO, value any, payload []byte,
) error {
	var err error
	switch source.TopicName {
	case stateOutput:
		state, _ := value.(map[string]any)
		deviceCommit, _ := state[deviceCommitKey].(string)
		if deviceCommit == "" {
			return p.rejectMessage(ctx, source, errors.New("state requires a non-empty pu_commit_version"))
		}
		selectedCommit := snapshot.selectedCommits[source.UnitUUID]
		if selectedCommit == "" {
			err = errors.New("selected commit is unavailable")
			break
		}
		err = p.postgresDB.UpdateUnitState(
			ctx, source.UnitUUID, string(payload), deviceCommit, deviceCommit == selectedCommit, time.Now().UTC(), p.firmwareWait,
		)
	case logOutput:
		now := time.Now().UTC()
		entries, parseErr := prepareUnitLogs(value, source.UnitUUID, now, now.Add(p.unitLogExpiration))
		if parseErr != nil {
			return p.rejectMessage(ctx, source, parseErr)
		}
		err = p.clickhouseDB.InsertUnitLogs(ctx, entries)
		if err == nil {
			err = p.postgresDB.TouchUnit(ctx, source.UnitUUID, now)
		}
	}
	if err != nil {
		err = fmt.Errorf("store %s for Unit %s: %w", source.TopicName, source.UnitUUID, err)
		logging.Errorf("%v", err)
	}
	return err
}

func prepareUnitLogs(value any, unitUUID uuid.UUID, now, expiration time.Time) ([]database.UnitLog, error) {
	batch, ok := value.([]any)
	if !ok || len(batch) == 0 {
		return nil, errors.New("log requires a non-empty array")
	}
	entries := make([]database.UnitLog, 0, len(batch))
	for index, value := range batch {
		item, _ := value.(map[string]any)
		level, levelOK := item[logLevelKey].(string)
		text, textOK := item[logTextKey].(string)
		if !levelOK || !textOK {
			return nil, fmt.Errorf("log item %d requires level and text", index)
		}
		createdAt := now.Add(time.Duration(index) * time.Second)
		if timestamp, _ := item[logTimeKey].(string); timestamp != "" {
			parsed, err := parseLogTime(timestamp)
			if err != nil {
				return nil, fmt.Errorf("log item %d: invalid create_datetime", index)
			}
			createdAt = parsed.UTC()
		}
		entries = append(entries, database.UnitLog{
			UUID: uuid.New(), UnitUUID: unitUUID, Level: level, Text: text,
			CreateDateTime: createdAt, ExpirationDateTime: expiration,
		})
	}
	return entries, nil
}

func parseLogTime(value string) (time.Time, error) {
	value = strings.Replace(value, " ", "T", 1)
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err == nil {
		return parsed, nil
	}
	return time.Parse("2006-01-02T15:04:05", value)
}
