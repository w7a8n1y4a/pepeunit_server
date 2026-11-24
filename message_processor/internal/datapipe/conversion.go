package datapipe

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/big"
	"strconv"
	"strings"
)

func (snapshot *Snapshot) convertValue(value any, source, target ioField) (any, error) {
	if value == nil {
		return nil, nil
	}
	from, to := fieldKind(source), fieldKind(target)
	if from == "" || from == "any" {
		from = valueKind(value)
	}
	if to == "any" && from != "bytes" {
		return value, nil
	}

	switch to {
	case "int", "float":
		if from == "bool" || isNumeric(from) {
			return snapshot.convertNumber(value, source, target, from, to)
		}
	case "bool":
		if isNumeric(from) {
			number, err := rationalValue(value)
			if err != nil {
				return nil, err
			}
			return number.Sign() != 0, nil
		}
	case "string":
		if isNumeric(from) {
			number := value.(json.Number)
			if strings.ContainsAny(string(number), "eE") {
				parsed, err := number.Float64()
				if err != nil {
					return nil, fmt.Errorf("number overflows float64")
				}
				return strconv.FormatFloat(parsed, 'f', -1, 64), nil
			}
			return string(number), nil
		}
	case "object":
		if from == to {
			return snapshot.convertObject(value.(map[string]any), source, target)
		}
	case "array":
		if from == to {
			return snapshot.convertArray(value.([]any), source, target)
		}
	}
	if from == to {
		return value, nil
	}
	return nil, fmt.Errorf("cannot convert %s to %s", from, to)
}

func (snapshot *Snapshot) convertObject(value map[string]any, source, target ioField) (map[string]any, error) {
	action, _ := value[variantAction].(string)
	sourceFields := objectFields(source, action)
	converted := maps.Clone(value)
	for name, declaration := range objectFields(target, action) {
		child, exists := value[name]
		if !exists {
			continue
		}
		sourceField, _ := sourceFields[name].(ioField)
		targetField, ok := declaration.(ioField)
		if !ok {
			return nil, fmt.Errorf("invalid schema for field %q", name)
		}
		result, err := snapshot.convertValue(child, sourceField, targetField)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", name, err)
		}
		converted[name] = result
	}
	return converted, nil
}

func (snapshot *Snapshot) convertArray(value []any, source, target ioField) ([]any, error) {
	sourceItem, _ := source["items"].(ioField)
	targetItem, ok := target["items"].(ioField)
	if !ok {
		return nil, fmt.Errorf("invalid array items schema")
	}
	converted := make([]any, len(value))
	for index, item := range value {
		result, err := snapshot.convertValue(item, sourceItem, targetItem)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", index, err)
		}
		converted[index] = result
	}
	return converted, nil
}

func (snapshot *Snapshot) convertNumber(value any, source, target ioField, from, to string) (json.Number, error) {
	number, err := rationalValue(value)
	if err != nil {
		return "", err
	}
	if err := snapshot.convertUnits(number, numericUnitPair(source, target, from == "bool")); err != nil {
		return "", err
	}
	if to == "int" {
		remainder := new(big.Int)
		rounded, _ := new(big.Int).QuoRem(number.Num(), number.Denom(), remainder)
		if remainder.Lsh(remainder.Abs(remainder), 1).Cmp(number.Denom()) >= 0 {
			rounded.Add(rounded, big.NewInt(int64(number.Sign())))
		}
		if !rounded.IsInt64() {
			return "", fmt.Errorf("number overflows int64")
		}
		return json.Number(rounded.String()), nil
	}
	converted, _ := number.Float64()
	if math.IsInf(converted, 0) {
		return "", fmt.Errorf("number overflows float64")
	}
	return json.Number(formatJSONFloat(converted)), nil
}

func rationalValue(value any) (*big.Rat, error) {
	switch number := value.(type) {
	case bool:
		if number {
			return big.NewRat(1, 1), nil
		}
		return big.NewRat(0, 1), nil
	case json.Number:
		if rational, ok := new(big.Rat).SetString(string(number)); ok {
			return rational, nil
		}
	}
	return nil, fmt.Errorf("invalid numeric value")
}

func valueKind(value any) string {
	switch typed := value.(type) {
	case bool:
		return "bool"
	case json.Number:
		if strings.ContainsAny(string(typed), ".eE") {
			return "float"
		}
		return "int"
	case string:
		return "string"
	case []byte:
		return "bytes"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "unknown"
	}
}
