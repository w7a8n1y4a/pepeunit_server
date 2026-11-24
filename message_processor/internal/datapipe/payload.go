package datapipe

import (
	"bytes"
	"cmp"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	codecJSON     = "json"
	codecUTF8     = "utf8"
	codecRaw      = "raw"
	variantAction = "action"
)

var errNonFiniteNumber = errors.New("JSON numbers must be finite")

var jsonNumberDecoder = jsonv2.WithUnmarshalers(jsonv2.UnmarshalFromFunc(readJSONNumber))

var jsonFloatEncoder = jsonv2.WithMarshalers(jsonv2.MarshalFunc(func(value float64) ([]byte, error) {
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return nil, errNonFiniteNumber
	}
	return []byte(formatJSONFloat(value)), nil
}))

func readJSONNumber(decoder *jsontext.Decoder, value *any) error {
	if decoder.PeekKind() != jsontext.KindNumber {
		return errors.ErrUnsupported
	}
	token, err := decoder.ReadToken()
	if err != nil {
		return err
	}
	number := token.String()
	if strings.ContainsAny(number, ".eE") {
		parsed, err := strconv.ParseFloat(number, 64)
		if err != nil {
			return errNonFiniteNumber
		}
		number = formatJSONFloat(parsed)
	} else if number == "-0" {
		number = "0"
	}
	*value = json.Number(number)
	return nil
}

func formatJSONFloat(value float64) string {
	magnitude := math.Abs(value)
	if magnitude >= 1e16 || (magnitude != 0 && magnitude < 1e-4) {
		return strconv.FormatFloat(value, 'e', -1, 64)
	}
	number := strconv.FormatFloat(value, 'f', -1, 64)
	if !strings.Contains(number, ".") {
		number += ".0"
	}
	return number
}

func decodeJSON(payload []byte, value any) error {
	return jsonv2.Unmarshal(
		payload, value, jsonNumberDecoder,
		jsontext.AllowDuplicateNames(true), json.MergeWithLegacySemantics(true),
	)
}

func decodePayload(field ioField, payload []byte) (any, error) {
	var value any
	switch field["codec"] {
	case codecJSON:
		if err := decodeJSON(payload, &value); err != nil {
			if errors.Is(err, errNonFiniteNumber) {
				return nil, errNonFiniteNumber
			}
			var syntax *jsontext.SyntacticError
			if errors.As(err, &syntax) {
				return nil, fmt.Errorf("invalid JSON payload at byte %d", syntax.ByteOffset)
			}
			return nil, errors.New("invalid JSON payload")
		}
	case codecUTF8:
		if !utf8.Valid(payload) || bytes.IndexByte(payload, 0) >= 0 {
			return nil, errors.New("payload must be UTF-8 text without NUL")
		}
		value = string(payload)
	case codecRaw:
		value = payload
	default:
		return nil, errors.New("invalid payload codec in schema")
	}
	if err := validatePayloadValue(field, value); err != nil {
		return nil, err
	}
	return value, nil
}

func validatePayloadValue(field ioField, value any) error {
	kind, _ := field["type"].(string)
	if value == nil && kind != "any" {
		if field["nullable"] == true {
			return nil
		}
		return errors.New("null requires nullable: true")
	}

	switch kind {
	case "int":
		return validateNumber(field, value, json.Number.Int64)
	case "float":
		return validateNumber(field, value, json.Number.Float64)
	case "bool":
		if _, ok := value.(bool); !ok {
			return typeMismatch(kind)
		}
	case "string":
		text, ok := value.(string)
		if !ok {
			return typeMismatch(kind)
		}
		if field["format"] == "uuid" && uuid.Validate(text) != nil {
			return errors.New("value must be a UUID")
		}
		if err := validateByteLimit(field, len(text)); err != nil {
			return err
		}
	case "bytes":
		data, ok := value.([]byte)
		if !ok {
			return typeMismatch(kind)
		}
		return validateByteLimit(field, len(data))
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return typeMismatch(kind)
		}
		return validateObject(field, object)
	case "array":
		items, ok := value.([]any)
		if !ok {
			return typeMismatch(kind)
		}
		return validateArray(field, items)
	case "any":
		if _, limited := field["max_bytes"]; !limited {
			return nil
		}
		encoded, err := jsonv2.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode JSON value for max_bytes: %w", err)
		}
		return validateByteLimit(field, len(encoded))
	default:
		return fmt.Errorf("unsupported schema type %q", kind)
	}

	if enum, exists := field["enum"]; exists {
		allowed, ok := enum.([]any)
		if !ok || !slices.Contains(allowed, value) {
			return errors.New("value must belong to enum")
		}
	}
	return nil
}

func validateObject(field ioField, object map[string]any) error {
	action, isString := object[variantAction].(string)
	fields := objectFields(field, action)
	_, hasVariants := field["variants"]
	if hasVariants && (!isString || fields == nil) {
		return fmt.Errorf("field %q must select a declared variant", variantAction)
	}
	for name, declaration := range fields {
		child, ok := declaration.(ioField)
		if !ok {
			return fmt.Errorf("invalid schema for field %q", name)
		}
		value, exists := object[name]
		if !exists {
			if child["optional"] != true {
				return fmt.Errorf("required field %q is missing", name)
			}
			continue
		}
		if err := validatePayloadValue(child, value); err != nil {
			return fmt.Errorf("field %q: %w", name, err)
		}
	}
	if !hasVariants && field["extra"] == true {
		return nil
	}
	for name := range object {
		if hasVariants && name == variantAction {
			continue
		}
		if _, exists := fields[name]; !exists {
			return fmt.Errorf("unexpected field %q", name)
		}
	}
	return nil
}

func objectFields(field ioField, action string) map[string]any {
	if variants, ok := field["variants"].(map[string]any); ok {
		fields, _ := variants[action].(map[string]any)
		return fields
	}
	fields, _ := field["fields"].(map[string]any)
	return fields
}

func validateArray(field ioField, items []any) error {
	count := uint64(len(items))
	for _, key := range []string{"min_items", "max_items"} {
		raw, exists := field[key]
		if !exists {
			continue
		}
		limit, err := parseSizeLimit(key, raw)
		if err != nil {
			return err
		}
		if (key == "min_items" && count < limit) || (key == "max_items" && count > limit) {
			return fmt.Errorf("array length violates %s", key)
		}
	}
	itemSchema, ok := field["items"].(ioField)
	if !ok {
		return errors.New("invalid array items schema")
	}
	for index, item := range items {
		if err := validatePayloadValue(itemSchema, item); err != nil {
			return fmt.Errorf("item %d: %w", index, err)
		}
	}
	return nil
}

func validateNumber[T int64 | float64](
	field ioField, value any, parse func(json.Number) (T, error),
) error {
	kind, _ := field["type"].(string)
	number, ok := value.(json.Number)
	if !ok {
		return typeMismatch(kind)
	}
	actual, err := parse(number)
	if err != nil {
		return fmt.Errorf("value must be a representable %s", kind)
	}

	for _, key := range []string{"min", "max"} {
		bound, ok := field[key].(json.Number)
		if !ok {
			return fmt.Errorf("invalid %s in numeric schema", key)
		}
		limit, err := parse(bound)
		if err != nil {
			return fmt.Errorf("invalid %s in numeric schema", key)
		}
		order := compareNumbers(number, bound, actual, limit)
		if (key == "min" && order < 0) || (key == "max" && order > 0) {
			return errors.New("value is outside numeric limits")
		}
	}

	enum, exists := field["enum"]
	if !exists {
		return nil
	}
	allowed, _ := enum.([]any)
	for _, entry := range allowed {
		candidate, ok := entry.(json.Number)
		if !ok {
			continue
		}
		parsed, err := parse(candidate)
		if err == nil && compareNumbers(number, candidate, actual, parsed) == 0 {
			return nil
		}
	}
	return errors.New("value must belong to enum")
}

func compareNumbers[T int64 | float64](left, right json.Number, actual, expected T) int {
	order := cmp.Compare(actual, expected)
	rounded, isFloat := any(actual).(float64)
	if order != 0 || !isFloat || math.Abs(rounded) < 1<<53 {
		return order
	}
	return bigNumber(left, rounded).Cmp(bigNumber(right, rounded))
}

func bigNumber(number json.Number, rounded float64) *big.Float {
	if !strings.ContainsAny(string(number), ".eE") {
		if integer, ok := new(big.Int).SetString(string(number), 10); ok {
			return new(big.Float).SetInt(integer)
		}
	}
	return big.NewFloat(rounded)
}

func validateByteLimit(field ioField, size int) error {
	limit, exists := field["max_bytes"]
	if !exists {
		return nil
	}
	maxBytes, err := parseSizeLimit("max_bytes", limit)
	if err != nil {
		return err
	}
	if uint64(size) > maxBytes {
		return errors.New("value exceeds max_bytes")
	}
	return nil
}

func parseSizeLimit(name string, value any) (uint64, error) {
	if number, ok := value.(json.Number); ok {
		limit, err := strconv.ParseUint(string(number), 10, 64)
		if err == nil || errors.Is(err, strconv.ErrRange) {
			return limit, nil
		}
	}
	return 0, fmt.Errorf("invalid %s in schema", name)
}

func typeMismatch(kind string) error {
	return fmt.Errorf("value must have type %s", kind)
}
