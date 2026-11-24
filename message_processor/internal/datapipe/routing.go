package datapipe

import (
	"context"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"strings"

	"message_processor/internal/logging"
)

const broadcastRouting = "broadcast"

type publishMessage func(context.Context, string, []byte, byte) error

func (p *Processor) processUnitIOMessage(ctx context.Context, ioUUID string, payload []byte) error {
	snapshot := p.snapshot.Load()
	if snapshot == nil {
		return errors.New("MessageProcessor schemas have not been loaded")
	}
	source, exists := snapshot.unitIOs[ioUUID]
	if !exists {
		return fmt.Errorf("UnitIO %s has no loaded schema", ioUUID)
	}
	value, err := decodePayload(source.definition, payload)
	if err != nil {
		return p.rejectMessage(ctx, source, err)
	}
	if source.Direction != outputDirection || source.definition["routing"] != broadcastRouting {
		return nil
	}

	inputs := snapshot.links[ioUUID]
	if len(inputs) != 0 && p.publish == nil {
		return errors.New("MQTT publisher is unavailable")
	}
	var failures []error
	for inputUUID := range inputs {
		target := snapshot.unitIOs[inputUUID]
		encoded, qos, err := snapshot.forwardPayload(value, source.definition, target.definition)
		if err != nil {
			failures = append(failures, p.rejectMessage(ctx, target, err))
			continue
		}
		topic := p.domain + "/" + inputUUID
		if err := p.publish(ctx, topic, encoded, qos); err != nil {
			err = fmt.Errorf("publish to UnitIO %s: %w", inputUUID, err)
			logging.Warningf("%v", err)
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (snapshot *Snapshot) forwardPayload(value any, source, target ioField) ([]byte, byte, error) {
	converted, err := snapshot.convertValue(value, source, target)
	if err != nil {
		return nil, 0, err
	}
	if err := validatePayloadValue(target, converted); err != nil {
		return nil, 0, err
	}
	var qos byte
	switch target["qos"] {
	case json.Number("0"):
	case json.Number("1"):
		qos = 1
	default:
		return nil, 0, errors.New("invalid Input QoS")
	}
	payload, err := encodePayload(target, converted)
	return payload, qos, err
}

func encodePayload(field ioField, value any) ([]byte, error) {
	switch field["codec"] {
	case codecJSON:
		return jsonv2.Marshal(value)
	case codecUTF8:
		text, ok := value.(string)
		if !ok || strings.IndexByte(text, 0) >= 0 {
			return nil, errors.New("payload must be UTF-8 text without NUL")
		}
		return []byte(text), nil
	case codecRaw:
		if raw, ok := value.([]byte); ok {
			return raw, nil
		}
		return nil, errors.New("raw payload requires bytes")
	default:
		return nil, errors.New("invalid payload codec in schema")
	}
}

func (p *Processor) rejectMessage(ctx context.Context, node snapshotUnitIO, reason error) error {
	if p.postgresDB != nil {
		if err := p.postgresDB.SetLastMessageError(ctx, node.UUID, reason.Error()); err != nil {
			logging.Errorf("Failed to store rejection for UnitIO %s: %v", node.UUID, err)
		}
	}
	return fmt.Errorf("UnitIO %s: %w", node.UUID, reason)
}
