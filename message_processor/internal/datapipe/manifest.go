package datapipe

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"

	"go.yaml.in/yaml/v3"
)

const maxManifestBytes = 1024 * 1024

var (
	gitCommitPattern       = regexp.MustCompile("^[0-9a-f]{40}([0-9a-f]{24})?$")
	contractVersionPattern = regexp.MustCompile("^v[0-9]+(\\.[0-9]+)?$")
)

type ioField = map[string]any

type ioDeclarations map[string]ioField

type unitManifest struct {
	ContractVersion string         `yaml:"contract_version"`
	Input           ioDeclarations `yaml:"input"`
	Output          ioDeclarations `yaml:"output"`
}

type manifestDictionary struct {
	unitManifest `yaml:",inline"`
	Types        []string           `yaml:"types"`
	Routing      []string           `yaml:"routing"`
	Defaults     ioField            `yaml:"defaults"`
	TypeLimits   map[string]ioField `yaml:"type_limits"`
	NamePatterns struct {
		IO string `yaml:"io"`
	} `yaml:"name_patterns"`
	ioName *regexp.Regexp
}

type manifestIO struct {
	input  ioDeclarations
	output ioDeclarations
}

func loadManifestIO(
	ctx context.Context, repoCache string, version manifestVersion, dictionaries map[string]*manifestDictionary,
) (manifestIO, error) {
	content, err := readGitManifest(ctx, repoCache, version)
	if err != nil {
		return manifestIO{}, err
	}
	var manifest unitManifest
	if err := decodeManifestYAML(content, &manifest); err != nil {
		return manifestIO{}, fmt.Errorf("decode unit.yml: %w", err)
	}
	contract := manifest.ContractVersion
	if !contractVersionPattern.MatchString(contract) {
		return manifestIO{}, fmt.Errorf("invalid unit.yml contract_version %q", contract)
	}
	dictionary := dictionaries[contract]
	if dictionary == nil {
		dictionary, err = loadManifestDictionary(contract)
		if err != nil {
			return manifestIO{}, fmt.Errorf("load contract %s dictionary: %w", contract, err)
		}
		dictionaries[contract] = dictionary
	}
	input, err := dictionary.resolveIO(manifest.Input, inputDirection, dictionary.Input, dictionary.Output)
	if err != nil {
		return manifestIO{}, err
	}
	output, err := dictionary.resolveIO(manifest.Output, outputDirection, dictionary.Output, dictionary.Input)
	if err != nil {
		return manifestIO{}, err
	}
	for name := range output {
		if _, exists := input[name]; exists {
			return manifestIO{}, fmt.Errorf("IO name %q is declared as Input and Output", name)
		}
	}
	return manifestIO{input: input, output: output}, nil
}

func readGitManifest(ctx context.Context, repoCache string, version manifestVersion) ([]byte, error) {
	repoPath := filepath.Join(repoCache, version.repoUUID.String())
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "cat-file", "blob", version.commit+":unit.yml")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	content, readErr := io.ReadAll(io.LimitReader(stdout, maxManifestBytes+1))
	if readErr != nil || len(content) > maxManifestBytes {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if readErr != nil {
			return nil, readErr
		}
		return nil, fmt.Errorf("unit.yml exceeds %d bytes", maxManifestBytes)
	}
	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("read unit.yml from local Git: %w: %s", err, bytes.TrimSpace(stderr.Bytes()))
	}
	return content, nil
}

func decodeManifestYAML(content []byte, model any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	if err := decoder.Decode(model); err != nil {
		return err
	}
	if err := decoder.Decode(new(yaml.Node)); err != io.EOF {
		return fmt.Errorf("expected a single YAML document")
	}
	return nil
}

func loadManifestDictionary(contract string) (*manifestDictionary, error) {
	content, err := os.ReadFile(filepath.Join("app", "schemas", "unit_manifest", contract, "dictionary.yml"))
	if err != nil {
		return nil, err
	}
	var dictionary manifestDictionary
	if err := decodeManifestYAML(content, &dictionary); err != nil {
		return nil, err
	}
	if dictionary.ContractVersion != contract || dictionary.NamePatterns.IO == "" {
		return nil, fmt.Errorf("invalid dictionary header or IO name pattern")
	}
	dictionary.ioName, err = regexp.Compile("^(?:" + dictionary.NamePatterns.IO + ")$")
	if err != nil {
		return nil, err
	}
	return &dictionary, nil
}

func (d *manifestDictionary) resolveIO(
	fields ioDeclarations, direction string, std, opposite ioDeclarations,
) (ioDeclarations, error) {
	for name, field := range fields {
		_, reserved := std[name]
		if _, wrongDirection := opposite[name]; wrongDirection || reserved && (field == nil || len(field) != 0) {
			return nil, fmt.Errorf("invalid declaration of reserved %s %q", direction, name)
		}
	}
	if fields == nil {
		fields = make(ioDeclarations)
	}
	maps.Copy(fields, std)
	resolved := make(ioDeclarations, len(fields))
	for name, field := range fields {
		if !d.ioName.MatchString(name) {
			return nil, fmt.Errorf("invalid %s name %q", direction, name)
		}
		if err := d.resolveField(field, direction); err != nil {
			return nil, fmt.Errorf("%s %q: %w", direction, name, err)
		}
		encoded, err := json.Marshal(field, jsonFloatEncoder)
		if err != nil {
			return nil, fmt.Errorf("encode %s %q: %w", direction, name, err)
		}
		var definition ioField
		if err := decodeJSON(encoded, &definition); err != nil {
			return nil, fmt.Errorf("decode %s %q: %w", direction, name, err)
		}
		resolved[name] = definition
	}
	return resolved, nil
}

func (d *manifestDictionary) resolveField(field ioField, direction string) error {
	kind, _ := field["type"].(string)
	if !slices.Contains(d.Types, kind) || (direction == "" && kind == "bytes") {
		return fmt.Errorf("invalid field type %q", kind)
	}
	for _, key := range []string{"optional", "nullable"} {
		setFieldDefault(field, key, d.Defaults[key])
		if _, ok := field[key].(bool); !ok {
			return fmt.Errorf("%s must be a boolean", key)
		}
	}
	for key, bound := range d.TypeLimits[kind] {
		setFieldDefault(field, key, bound)
	}
	if err := d.resolveTransport(field, kind, direction); err != nil {
		return err
	}
	switch kind {
	case "array":
		items, ok := field["items"].(ioField)
		if !ok {
			return fmt.Errorf("array items must be a field declaration")
		}
		return d.resolveField(items, "")
	case "object":
		setFieldDefault(field, "extra", d.Defaults["extra"])
		extra, ok := field["extra"].(bool)
		if !ok {
			return fmt.Errorf("extra must be a boolean")
		}
		fields, hasFields := field["fields"]
		variants, hasVariants := field["variants"]
		if hasVariants {
			choices, ok := variants.(map[string]any)
			if !ok || len(choices) == 0 || hasFields || extra {
				return fmt.Errorf("variants require a nonempty mapping and forbid fields and extra")
			}
			for name, declarations := range choices {
				variantFields, _ := declarations.(map[string]any)
				if _, exists := variantFields[variantAction]; exists {
					return fmt.Errorf("variant %q: field %q is defined by variants", name, variantAction)
				}
				if err := d.resolveObjectFields(declarations); err != nil {
					return fmt.Errorf("variant %q: %w", name, err)
				}
			}
		} else if hasFields {
			return d.resolveObjectFields(fields)
		}
	}
	return nil
}

func (d *manifestDictionary) resolveObjectFields(value any) error {
	fields, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("object fields must be a mapping")
	}
	for name, value := range fields {
		child, ok := value.(ioField)
		if !ok {
			return fmt.Errorf("field %q must be a declaration", name)
		}
		if err := d.resolveField(child, ""); err != nil {
			return fmt.Errorf("field %q: %w", name, err)
		}
	}
	return nil
}

func (d *manifestDictionary) resolveTransport(field ioField, kind, direction string) error {
	if direction == "" {
		for _, key := range []string{"codec", "qos", "routing"} {
			if _, exists := field[key]; exists {
				return fmt.Errorf("%s is only allowed on a top-level IO", key)
			}
		}
		return nil
	}
	codecs, ok := d.Defaults["codec"].(map[string]any)
	if !ok {
		return fmt.Errorf("dictionary defaults.codec must map types to codecs")
	}
	defaultCodec, exists := codecs[kind]
	if !exists || (kind == "string" && field["nullable"] == true) {
		defaultCodec = codecs["default"]
	}
	setFieldDefault(field, "codec", defaultCodec)
	setFieldDefault(field, "qos", d.Defaults["qos"])
	codec, _ := field["codec"].(string)
	codecValid := codec == codecJSON && kind != "bytes" ||
		codec == codecUTF8 && kind == "string" || codec == codecRaw && kind == "bytes"
	if !codecValid || (codec != codecJSON && field["nullable"] == true) {
		return fmt.Errorf("codec %q does not support this type or nullability", codec)
	}
	qos, ok := field["qos"].(int)
	if !ok || (qos != 0 && qos != 1) {
		return fmt.Errorf("qos must be 0 or 1")
	}
	if direction == outputDirection {
		setFieldDefault(field, "routing", d.Defaults["routing"])
		routing, _ := field["routing"].(string)
		if !slices.Contains(d.Routing, routing) || (routing == "selected-link" && kind != "object") {
			return fmt.Errorf("invalid routing %q for type %s", routing, kind)
		}
	} else if _, exists := field["routing"]; exists {
		return fmt.Errorf("routing is Output-only")
	}
	return nil
}

func setFieldDefault(field ioField, key string, value any) {
	if _, exists := field[key]; !exists {
		field[key] = value
	}
}
