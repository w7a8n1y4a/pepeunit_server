package database

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type UnitVersionSelection struct {
	UnitUUID      uuid.UUID `json:"unit_uuid"`
	RepoUUID      uuid.UUID `json:"repo_uuid"`
	Branch        string    `json:"target_branch"`
	TargetCommit  *string   `json:"target_commit"`
	AutoUpdate    bool      `json:"is_auto_update_unit"`
	Compilable    bool      `json:"is_compilable_unit"`
	OnlyTagUpdate bool      `json:"is_only_tag_update"`
}

type ProcessorUnitIO struct {
	UUID        uuid.UUID
	UnitUUID    uuid.UUID
	CreatorUUID uuid.UUID
	Direction   string
	TopicName   string
}

type ProcessorLink struct {
	OutputUUID uuid.UUID
	InputUUID  uuid.UUID
}

type ProcessorRows struct {
	Units   []UnitVersionSelection
	UnitIOs []ProcessorUnitIO
	Links   []ProcessorLink
}

func (db *PostgresDB) ReadProcessorRows(ctx context.Context) (ProcessorRows, error) {
	tx, err := db.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return ProcessorRows{}, err
	}
	defer tx.Rollback(ctx)

	units, err := processorRows[UnitVersionSelection](ctx, tx, `
		SELECT uuid, repo_uuid, target_branch, target_commit, is_auto_update_unit,
			is_compilable_unit, is_only_tag_update
		FROM units`)
	if err != nil {
		return ProcessorRows{}, fmt.Errorf("read Unit versions: %w", err)
	}
	unitIOs, err := processorRows[ProcessorUnitIO](ctx, tx, `
		SELECT uuid, unit_uuid, creator_uuid, type, topic_name FROM units_io
		WHERE unit_uuid IS NOT NULL`)
	if err != nil {
		return ProcessorRows{}, fmt.Errorf("read UnitIOs: %w", err)
	}
	links, err := processorRows[ProcessorLink](ctx, tx, `
		SELECT output_uuid, input_uuid FROM links`)
	if err != nil {
		return ProcessorRows{}, fmt.Errorf("read links: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ProcessorRows{}, err
	}
	return ProcessorRows{Units: units, UnitIOs: unitIOs, Links: links}, nil
}

func processorRows[T any](ctx context.Context, tx pgx.Tx, query string) ([]T, error) {
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[T])
}

func (db *PostgresDB) SetLastMessageError(ctx context.Context, ioUUID uuid.UUID, reason string) error {
	return db.Exec(ctx, `
		UPDATE units_io SET last_message_error = $2
		WHERE uuid = $1 AND last_message_error IS DISTINCT FROM $2`, ioUUID, reason)
}

func (db *PostgresDB) UpdateUnitState(
	ctx context.Context, unitUUID uuid.UUID, state, deviceCommit string, updateConfirmed bool, now time.Time, firmwareWait time.Duration,
) error {
	return db.Exec(ctx, `
		UPDATE units SET
			unit_state_dict = $2,
			current_commit_version = $3,
			last_update_datetime = GREATEST(last_update_datetime, $5),
			(firmware_update_status, firmware_update_error, last_firmware_update_datetime) = (
				SELECT COALESCE(status, firmware_update_status),
					CASE status WHEN 'Success' THEN NULL WHEN 'Error' THEN $7::text ELSE firmware_update_error END,
					CASE WHEN status IS NULL THEN last_firmware_update_datetime END
				FROM (SELECT CASE
					WHEN firmware_update_status IS DISTINCT FROM 'RequestSent' THEN NULL
					WHEN $4::boolean THEN 'Success'
					WHEN last_firmware_update_datetime < $6 THEN 'Error'
				END AS status) AS firmware
			)
		WHERE uuid = $1`, unitUUID, state, deviceCommit, updateConfirmed, now, now.Add(-firmwareWait),
		fmt.Sprintf("Firmware update was not confirmed within %s", firmwareWait))
}

func (db *PostgresDB) TouchUnit(ctx context.Context, unitUUID uuid.UUID, now time.Time) error {
	return db.Exec(ctx, `
		UPDATE units SET last_update_datetime = GREATEST(last_update_datetime, $2)
		WHERE uuid = $1`, unitUUID, now)
}
