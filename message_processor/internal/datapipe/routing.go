package datapipe

import (
	"context"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"strings"

	"message_processor/internal/logging"

	"github.com/google/uuid"
)

const (
	broadcastRouting    = "broadcast"
	selectedLinkRouting = "selected-link"
	selectedInputKey    = "input_uuid"
	selectedDataKey     = "data"
)

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
	var routingError error
	if source.Direction == outputDirection {
		if err := p.processStandardOutput(ctx, snapshot, source, value, payload); err != nil {
			return err
		}
		switch source.definition["routing"] {
		case broadcastRouting:
			var failures []error
			for inputUUID := range snapshot.links[ioUUID] {
				if err := p.forwardMessage(ctx, snapshot, snapshot.unitIOs[inputUUID], source.definition, value); err != nil {
					failures = append(failures, err)
				}
			}
			routingError = errors.Join(failures...)
		case selectedLinkRouting:
			envelope, _ := value.(map[string]any)
			rawInput, _ := envelope[selectedInputKey].(string)
			input, err := uuid.Parse(rawInput)
			if err != nil {
				return p.rejectMessage(ctx, source, errors.New("selected-link requires a valid input_uuid"))
			}
			inputUUID := input.String()
			if _, linked := snapshot.links[ioUUID][inputUUID]; !linked {
				return p.rejectMessage(ctx, source, fmt.Errorf("Input %s is not linked to this Output", inputUUID))
			}
			data, exists := envelope[selectedDataKey]
			action, _ := envelope[variantAction].(string)
			declaration := selectedDataField(source.definition, action)
			if !exists || declaration == nil {
				return p.rejectMessage(ctx, source, errors.New("selected-link requires declared data"))
			}
			routingError = p.forwardMessage(ctx, snapshot, snapshot.unitIOs[inputUUID], declaration, data)
		}
	}
	return errors.Join(routingError, p.processDataPipe(ctx, ioUUID, payload, source.definition["codec"] == codecRaw))
}

func selectedDataField(field ioField, action string) ioField {
	data, _ := objectFields(field, action)[selectedDataKey].(ioField)
	return data
}

func (p *Processor) forwardMessage(
	ctx context.Context, snapshot *Snapshot, target snapshotUnitIO, source ioField, value any,
) error {
	encoded, qos, err := snapshot.forwardPayload(value, source, target.definition)
	if err != nil {
		return p.rejectMessage(ctx, target, err)
	}
	if p.publish == nil {
		return errors.New("MQTT publisher is unavailable")
	}
	if err := p.publish(ctx, p.domain+"/"+target.UUID.String(), encoded, qos); err != nil {
		err = fmt.Errorf("publish to UnitIO %s: %w", target.UUID, err)
		logging.Warningf("%v", err)
		return err
	}
	return nil
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
