package datapipe

import (
	"context"
	"fmt"
	"maps"
	"math/big"
	"slices"
)

type unitPair [2]string

type affineConversion struct {
	scale  *big.Rat
	offset *big.Rat
}

func loadUnitConversions(ctx context.Context, snapshot *Snapshot) (map[unitPair]affineConversion, error) {
	pairs := make(map[unitPair]struct{})
	for output, inputs := range snapshot.links {
		source := snapshot.unitIOs[output].definition
		for input := range inputs {
			target := snapshot.unitIOs[input].definition
			switch source["routing"] {
			case broadcastRouting:
				collectUnitPairs(source, target, pairs)
			case selectedLinkRouting:
				if variants, ok := source["variants"].(map[string]any); ok {
					for action := range variants {
						collectUnitPairs(selectedDataField(source, action), target, pairs)
					}
				} else {
					collectUnitPairs(selectedDataField(source, ""), target, pairs)
				}
			}
		}
	}
	if len(pairs) == 0 {
		return nil, nil
	}

	request := slices.Collect(maps.Keys(pairs))
	var response []*[2]*big.Rat
	if err := runPythonJSON(ctx, "app.services.ucum_conversion", request, &response); err != nil {
		return nil, err
	}
	if len(response) != len(request) {
		return nil, fmt.Errorf("expected %d UCUM conversions, received %d", len(request), len(response))
	}
	conversions := make(map[unitPair]affineConversion, len(response))
	for index, coefficients := range response {
		if coefficients == nil {
			continue
		}
		scale, offset := coefficients[0], coefficients[1]
		if scale == nil || scale.Sign() == 0 || offset == nil {
			return nil, fmt.Errorf("invalid UCUM coefficients for %q → %q", request[index][0], request[index][1])
		}
		conversions[request[index]] = affineConversion{scale: scale, offset: offset}
	}
	return conversions, nil
}

func collectUnitPairs(source, target ioField, pairs map[unitPair]struct{}) {
	from, to := fieldKind(source), fieldKind(target)
	dynamic := from == "" || from == "any"
	if isNumeric(to) {
		if !dynamic && from != "bool" && !isNumeric(from) {
			return
		}
		pair := numericUnitPair(source, target, dynamic || from == "bool")
		if pair[0] != "" && pair[1] != "" {
			pairs[pair] = struct{}{}
		}
		return
	}
	if !dynamic && from != to {
		return
	}
	switch to {
	case "array":
		sourceItem, _ := source["items"].(ioField)
		targetItem, _ := target["items"].(ioField)
		collectUnitPairs(sourceItem, targetItem, pairs)
	case "object":
		sourceVariants, _ := source["variants"].(map[string]any)
		targetVariants, _ := target["variants"].(map[string]any)
		switch {
		case len(sourceVariants) != 0:
			for action := range sourceVariants {
				collectFieldUnitPairs(objectFields(source, action), objectFields(target, action), pairs)
			}
		case len(targetVariants) != 0:
			for action := range targetVariants {
				collectFieldUnitPairs(objectFields(source, action), objectFields(target, action), pairs)
			}
		default:
			collectFieldUnitPairs(objectFields(source, ""), objectFields(target, ""), pairs)
		}
	}
}

func collectFieldUnitPairs(source, target map[string]any, pairs map[unitPair]struct{}) {
	for name, declaration := range target {
		from, _ := source[name].(ioField)
		to, _ := declaration.(ioField)
		collectUnitPairs(from, to, pairs)
	}
}

func numericUnitPair(source, target ioField, useTargetUnit bool) unitPair {
	from, _ := source["unit"].(string)
	to, _ := target["unit"].(string)
	if useTargetUnit {
		from = to
	}
	return unitPair{from, to}
}

func fieldKind(field ioField) string {
	kind, _ := field["type"].(string)
	return kind
}

func isNumeric(kind string) bool {
	return kind == "int" || kind == "float"
}

func (snapshot *Snapshot) convertUnits(value *big.Rat, pair unitPair) error {
	if pair == (unitPair{}) {
		return nil
	}
	conversion, exists := snapshot.conversions[pair]
	if !exists {
		return fmt.Errorf("cannot convert UCUM units %q to %q", pair[0], pair[1])
	}
	value.Mul(value, conversion.scale).Add(value, conversion.offset)
	return nil
}
