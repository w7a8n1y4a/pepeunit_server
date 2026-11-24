package database

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

type UnitIOResourceWindow struct {
	UnitIOUUID    uuid.UUID
	Start         time.Time
	Requests      uint64
	Bytes         uint64
	DataPipeBytes uint64
}

func (db *ClickHouseDB) InsertUnitIOResources(ctx context.Context, token uuid.UUID, windows []UnitIOResourceWindow) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplicate":         1,
		"insert_deduplication_token": token.String(),
	}))
	batch, err := db.conn.PrepareBatch(ctx, `
		INSERT INTO unit_io_resources_aggregation
		(unit_io_uuid, start_window_datetime, total_requests, total_bytes, total_datapipe_bytes_increment)`)
	if err != nil {
		return err
	}
	defer batch.Close()
	for _, window := range windows {
		if err := batch.Append(window.UnitIOUUID, window.Start, window.Requests, window.Bytes, window.DataPipeBytes); err != nil {
			return err
		}
	}
	return batch.Send()
}
