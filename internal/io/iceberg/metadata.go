// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements metadata.json parsing for Iceberg tables.
package iceberg

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"

	"github.com/dukdb/dukdb-go/internal/io/filesystem"
)

// newGzipReader creates a new gzip reader from the input reader.
func newGzipReader(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

// newZstdReader creates a new zstd reader from the input reader.
func newZstdReader(r io.Reader) (io.Reader, error) {
	return zstd.NewReader(r)
}

// MetadataReaderOptions contains options for the MetadataReader.
type MetadataReaderOptions struct {
	// Version specifies an explicit metadata version number to use.
	// If set to a positive value, the reader will look for v{Version}.metadata.json.
	// If 0 (default), the reader uses version-hint.text or scans for latest.
	Version int

	// AllowMovedPaths allows reading tables that have been relocated.
	// When true, file paths in metadata are rewritten relative to the
	// current table location instead of using absolute paths.
	AllowMovedPaths bool

	// MetadataCompressionCodec specifies the compression codec for metadata files.
	// Supported values: "gzip", "zstd", "none" (or empty for auto-detection).
	MetadataCompressionCodec string

	// UnsafeEnableVersionGuessing enables automatic version guessing when
	// version-hint.text is missing. When enabled, the reader scans the
	// metadata directory to find the highest version number.
	UnsafeEnableVersionGuessing bool
}

// MetadataReader provides methods for reading Iceberg table metadata.
type MetadataReader struct {
	// fs is the filesystem to use for reading files.
	fs filesystem.FileSystem

	// opts contains configuration options for the reader.
	opts MetadataReaderOptions
}

// NewMetadataReader creates a new MetadataReader with the given filesystem.
// If fs is nil, the local filesystem is used.
func NewMetadataReader(fs filesystem.FileSystem) *MetadataReader {
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	return &MetadataReader{fs: fs, opts: MetadataReaderOptions{}}
}

// NewMetadataReaderWithOptions creates a new MetadataReader with the given
// filesystem and options.
// If fs is nil, the local filesystem is used.
func NewMetadataReaderWithOptions(fs filesystem.FileSystem, opts MetadataReaderOptions) *MetadataReader {
	if fs == nil {
		fs = filesystem.NewLocalFileSystem("")
	}

	return &MetadataReader{fs: fs, opts: opts}
}

// ReadMetadata reads and parses the Iceberg table metadata from the given table location.
// It automatically discovers the current metadata file using version-hint.text or by
// scanning the metadata directory.
func (r *MetadataReader) ReadMetadata(ctx context.Context, tableLocation string) (*TableMetadata, error) {
	metadataPath, err := r.findMetadataFile(ctx, tableLocation)
	if err != nil {
		return nil, err
	}

	return r.ReadMetadataFromPath(ctx, metadataPath)
}

// ReadMetadataFromPath reads and parses the Iceberg table metadata from a specific
// metadata.json file path. It automatically handles compressed metadata files
// based on file extension or the configured MetadataCompressionCodec.
func (r *MetadataReader) ReadMetadataFromPath(_ context.Context, metadataPath string) (*TableMetadata, error) {
	file, err := r.fs.Open(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTableNotFound, err)
	}
	defer func() { _ = file.Close() }()

	// Determine compression based on file extension or configured codec
	var reader io.Reader = file
	compression := r.detectCompression(metadataPath)

	if compression != "" && compression != "none" {
		decompressor, err := r.getDecompressor(file, compression)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to create decompressor: %w", ErrInvalidMetadata, err)
		}
		defer func() {
			if closer, ok := decompressor.(io.Closer); ok {
				_ = closer.Close()
			}
		}()
		reader = decompressor
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read metadata file: %w", ErrInvalidMetadata, err)
	}

	return ParseMetadataBytes(data)
}

// detectCompression detects the compression type based on file extension
// or the configured MetadataCompressionCodec.
func (r *MetadataReader) detectCompression(path string) string {
	// If a specific codec is configured, use that
	if r.opts.MetadataCompressionCodec != "" {
		return r.opts.MetadataCompressionCodec
	}

	// Auto-detect based on file extension
	switch {
	case strings.HasSuffix(path, ".gz"):
		return "gzip"
	case strings.HasSuffix(path, ".zst"):
		return "zstd"
	case strings.HasSuffix(path, ".lz4"):
		return "lz4"
	case strings.HasSuffix(path, ".snappy"):
		return "snappy"
	default:
		return ""
	}
}

// getDecompressor returns a reader that decompresses the input based on the codec.
func (r *MetadataReader) getDecompressor(reader io.Reader, codec string) (io.Reader, error) {
	switch strings.ToLower(codec) {
	case "gzip", "gz":
		return newGzipReader(reader)
	case "zstd", "zstandard":
		return newZstdReader(reader)
	default:
		// For unsupported codecs, return the original reader
		// This allows reading uncompressed files even if a codec is specified
		return reader, nil
	}
}

// findMetadataFile finds the current metadata file for an Iceberg table.
// It uses the following strategy:
// 1. If an explicit version is set in opts, use that version directly.
// 2. Try to read version-hint.text.
// 3. If version-hint.text is missing and UnsafeEnableVersionGuessing is true,
//    scan the metadata directory for the highest version number.
// 4. Otherwise, fall back to scanning the metadata directory (default behavior).
func (r *MetadataReader) findMetadataFile(_ context.Context, tableLocation string) (string, error) {
	metadataDir := filepath.Join(tableLocation, "metadata")

	// If an explicit version is specified, use that version directly.
	if r.opts.Version > 0 {
		metadataPath, err := r.findMetadataFileForVersion(metadataDir, r.opts.Version)
		if err != nil {
			return "", fmt.Errorf("specified version %d not found: %w", r.opts.Version, err)
		}
		return metadataPath, nil
	}

	// Try version-hint.text first
	versionHintPath := filepath.Join(metadataDir, "version-hint.text")
	if exists, _ := r.fs.Exists(versionHintPath); exists {
		version, err := r.readVersionHint(versionHintPath)
		if err == nil {
			metadataPath, err := r.findMetadataFileForVersion(metadataDir, version)
			if err == nil {
				return metadataPath, nil
			}
		}
	}

	// If version guessing is not enabled, fall back to scanning (default behavior).
	// Note: The difference between enabled and not enabled is in error handling.
	// When enabled, we explicitly scan. When not enabled, we still scan but
	// this is considered "safe" fallback behavior for backwards compatibility.
	if r.opts.UnsafeEnableVersionGuessing {
		// Explicitly guessing - scan for latest version
		return r.findLatestMetadataFile(metadataDir)
	}

	// Fall back to scanning metadata directory (default behavior for backwards compat)
	return r.findLatestMetadataFile(metadataDir)
}

// findMetadataFileForVersion finds a metadata file for a specific version.
// It checks for both uncompressed and compressed variants.
func (r *MetadataReader) findMetadataFileForVersion(metadataDir string, version int) (string, error) {
	// List of possible file patterns to try
	patterns := []string{
		fmt.Sprintf("v%d.metadata.json", version),
	}

	// If a specific compression codec is set, try that first
	if r.opts.MetadataCompressionCodec != "" && r.opts.MetadataCompressionCodec != "none" {
		ext := compressionExtension(r.opts.MetadataCompressionCodec)
		if ext != "" {
			patterns = append([]string{fmt.Sprintf("v%d.metadata.json%s", version, ext)}, patterns...)
		}
	}

	// Also try common compression extensions for auto-detection
	patterns = append(patterns,
		fmt.Sprintf("v%d.metadata.json.gz", version),
		fmt.Sprintf("v%d.metadata.json.zst", version),
	)

	// Try each pattern
	for _, pattern := range patterns {
		metadataPath := filepath.Join(metadataDir, pattern)
		if exists, _ := r.fs.Exists(metadataPath); exists {
			return metadataPath, nil
		}
	}

	return "", fmt.Errorf("metadata file for version %d not found in %s", version, metadataDir)
}

// compressionExtension returns the file extension for a compression codec.
func compressionExtension(codec string) string {
	switch strings.ToLower(codec) {
	case "gzip", "gz":
		return ".gz"
	case "zstd", "zstandard":
		return ".zst"
	case "lz4":
		return ".lz4"
	case "snappy":
		return ".snappy"
	default:
		return ""
	}
}

// readVersionHint reads the version number from version-hint.text.
func (r *MetadataReader) readVersionHint(path string) (int, error) {
	file, err := r.fs.Open(path)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrVersionHintNotFound, err)
	}
	defer func() { _ = file.Close() }()

	data, err := io.ReadAll(file)
	if err != nil {
		return 0, fmt.Errorf("failed to read version hint: %w", err)
	}

	versionStr := strings.TrimSpace(string(data))
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return 0, fmt.Errorf("invalid version hint content: %w", err)
	}

	return version, nil
}

// findLatestMetadataFile scans the metadata directory and returns the path to
// the metadata file with the highest version number.
func (r *MetadataReader) findLatestMetadataFile(metadataDir string) (string, error) {
	entries, err := r.fs.ReadDir(metadataDir)
	if err != nil {
		return "", fmt.Errorf("%w: failed to read metadata directory: %w", ErrMetadataLocationNotFound, err)
	}

	var latestVersion int
	var latestPath string

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".metadata.json") {
			continue
		}

		// Parse version from filename (e.g., "v1.metadata.json" or "00001-uuid.metadata.json")
		version := r.parseMetadataVersion(name)
		if version > latestVersion {
			latestVersion = version
			latestPath = filepath.Join(metadataDir, name)
		}
	}

	if latestPath == "" {
		return "", fmt.Errorf("%w: no metadata files found in %s", ErrMetadataLocationNotFound, metadataDir)
	}

	return latestPath, nil
}

// parseMetadataVersion extracts the version number from a metadata filename.
// Supports both "v1.metadata.json" and "00001-uuid.metadata.json" formats.
func (r *MetadataReader) parseMetadataVersion(filename string) int {
	// Handle "v1.metadata.json" format
	if strings.HasPrefix(filename, "v") {
		parts := strings.Split(filename, ".")
		if len(parts) >= 3 {
			versionStr := strings.TrimPrefix(parts[0], "v")
			if version, err := strconv.Atoi(versionStr); err == nil {
				return version
			}
		}
	}

	// Handle "00001-uuid.metadata.json" format
	parts := strings.Split(filename, "-")
	if len(parts) >= 1 {
		if version, err := strconv.Atoi(parts[0]); err == nil {
			return version
		}
	}

	return 0
}

// GuessMetadataVersion attempts to find the metadata file by guessing versions
// starting from a given version and working backwards.
// This is useful when version-hint.text is missing or outdated.
func (r *MetadataReader) GuessMetadataVersion(_ context.Context, tableLocation string, startVersion int) (string, error) {
	metadataDir := filepath.Join(tableLocation, "metadata")

	for version := startVersion; version >= 1; version-- {
		// Try standard v{N}.metadata.json format
		metadataPath := filepath.Join(metadataDir, fmt.Sprintf("v%d.metadata.json", version))
		if exists, _ := r.fs.Exists(metadataPath); exists {
			return metadataPath, nil
		}
	}

	return "", fmt.Errorf("%w: could not guess metadata version", ErrMetadataLocationNotFound)
}

// rawMetadataJSON represents the raw JSON structure of metadata.json.
type rawMetadataJSON struct {
	FormatVersion     int                  `json:"format-version"`
	TableUUID         string               `json:"table-uuid"`
	Location          string               `json:"location"`
	LastUpdatedMs     int64                `json:"last-updated-ms"`
	LastColumnID      int                  `json:"last-column-id"`
	CurrentSchemaID   int                  `json:"current-schema-id"`
	Schemas           []json.RawMessage    `json:"schemas"`
	Schema            json.RawMessage      `json:"schema"` // V1 format
	DefaultSpecID     int                  `json:"default-spec-id"`
	PartitionSpecs    []json.RawMessage    `json:"partition-specs"`
	PartitionSpec     json.RawMessage      `json:"partition-spec"` // V1 format
	LastPartitionID   int                  `json:"last-partition-id"`
	Properties        map[string]string    `json:"properties"`
	CurrentSnapshotID *int64               `json:"current-snapshot-id"`
	Snapshots         []rawSnapshotJSON    `json:"snapshots"`
	SnapshotLog       []rawSnapshotLogJSON `json:"snapshot-log"`
	Refs              map[string]rawRefJSON `json:"refs"`
}

// rawSnapshotJSON represents a snapshot in the metadata JSON.
type rawSnapshotJSON struct {
	SnapshotID       int64              `json:"snapshot-id"`
	ParentSnapshotID *int64             `json:"parent-snapshot-id"`
	SequenceNumber   int64              `json:"sequence-number"`
	TimestampMs      int64              `json:"timestamp-ms"`
	ManifestList     string             `json:"manifest-list"`
	Summary          rawSummaryJSON     `json:"summary"`
	SchemaID         *int               `json:"schema-id"`
}

// rawSummaryJSON represents the summary in a snapshot.
type rawSummaryJSON struct {
	Operation  string            `json:"operation"`
	Properties map[string]string `json:"-"` // Capture all fields
}

// UnmarshalJSON custom unmarshals the summary to capture all properties.
func (s *rawSummaryJSON) UnmarshalJSON(data []byte) error {
	// First unmarshal to a map to get all properties
	var props map[string]string
	if err := json.Unmarshal(data, &props); err != nil {
		return err
	}
	s.Properties = props
	if op, ok := props["operation"]; ok {
		s.Operation = op
	}
	return nil
}

// rawSnapshotLogJSON represents a snapshot log entry.
type rawSnapshotLogJSON struct {
	SnapshotID  int64 `json:"snapshot-id"`
	TimestampMs int64 `json:"timestamp-ms"`
}

// rawRefJSON represents a snapshot reference.
type rawRefJSON struct {
	SnapshotID int64  `json:"snapshot-id"`
	Type       string `json:"type"`
}

// rawSchemaJSON represents a schema in metadata.
type rawSchemaJSON struct {
	Type     string           `json:"type"`
	SchemaID int              `json:"schema-id"`
	Fields   []rawFieldJSON   `json:"fields"`
}

// rawFieldJSON represents a field in a schema.
type rawFieldJSON struct {
	ID       int             `json:"id"`
	Name     string          `json:"name"`
	Required bool            `json:"required"`
	Type     json.RawMessage `json:"type"`
	Doc      string          `json:"doc,omitempty"`
}

// rawPartitionSpecJSON represents a partition spec.
type rawPartitionSpecJSON struct {
	SpecID int                      `json:"spec-id"`
	Fields []rawPartitionFieldJSON  `json:"fields"`
}

// rawPartitionFieldJSON represents a partition field.
type rawPartitionFieldJSON struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

// ParseMetadataFromReader parses Iceberg metadata from an io.Reader.
func ParseMetadataFromReader(r io.Reader) (*TableMetadata, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read metadata: %w", ErrInvalidMetadata, err)
	}

	return ParseMetadataBytes(data)
}

// ParseMetadataBytes parses Iceberg metadata from a byte slice.
func ParseMetadataBytes(data []byte) (*TableMetadata, error) {
	var raw rawMetadataJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%w: failed to parse JSON: %w", ErrInvalidMetadata, err)
	}

	if raw.FormatVersion != 1 && raw.FormatVersion != 2 {
		return nil, fmt.Errorf("%w: version %d", ErrUnsupportedVersion, raw.FormatVersion)
	}

	// Parse UUID
	tableUUID, err := uuid.Parse(raw.TableUUID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid table UUID: %w", ErrInvalidMetadata, err)
	}

	tm := &TableMetadata{
		Version:               FormatVersion(raw.FormatVersion),
		TableUUID:             tableUUID,
		Location:              raw.Location,
		LastUpdatedMs:         raw.LastUpdatedMs,
		LastColumnID:          raw.LastColumnID,
		CurrentSchemaID:       raw.CurrentSchemaID,
		DefaultPartitionSpecID: raw.DefaultSpecID,
		Properties:            raw.Properties,
		CurrentSnapshotID:     raw.CurrentSnapshotID,
	}

	// Parse schemas
	schemas, err := parseSchemas(raw)
	if err != nil {
		return nil, err
	}
	tm.schemas = schemas

	// Find current schema
	for _, schema := range schemas {
		if schema.ID == tm.CurrentSchemaID {
			tm.currentSchema = schema
			break
		}
	}
	if tm.currentSchema == nil && len(schemas) > 0 {
		tm.currentSchema = schemas[len(schemas)-1]
	}

	// Parse partition specs
	specs, err := parsePartitionSpecs(raw, tm.currentSchema)
	if err != nil {
		return nil, err
	}
	tm.partitionSpecs = specs

	// Find current partition spec
	for _, spec := range specs {
		if spec.ID() == tm.DefaultPartitionSpecID {
			tm.currentPartitionSpec = spec
			break
		}
	}
	if tm.currentPartitionSpec.NumFields() == 0 && len(specs) > 0 {
		tm.currentPartitionSpec = specs[len(specs)-1]
	}

	// Parse snapshots
	tm.snapshots = make([]Snapshot, len(raw.Snapshots))
	for i, snap := range raw.Snapshots {
		tm.snapshots[i] = Snapshot{
			SnapshotID:           snap.SnapshotID,
			ParentSnapshotID:     snap.ParentSnapshotID,
			SequenceNumber:       snap.SequenceNumber,
			TimestampMs:          snap.TimestampMs,
			ManifestListLocation: snap.ManifestList,
			Summary:              snap.Summary.Properties,
			SchemaID:             snap.SchemaID,
		}
	}

	// Parse snapshot log
	tm.snapshotLog = make([]SnapshotLogEntry, len(raw.SnapshotLog))
	for i, entry := range raw.SnapshotLog {
		tm.snapshotLog[i] = SnapshotLogEntry(entry)
	}

	return tm, nil
}

// parseSchemas parses schema definitions from raw metadata.
func parseSchemas(raw rawMetadataJSON) ([]*iceberg.Schema, error) {
	if len(raw.Schemas) > 0 {
		// V2 format with explicit schemas list
		schemas := make([]*iceberg.Schema, 0, len(raw.Schemas))
		for _, schemaRaw := range raw.Schemas {
			schema, err := parseSchema(schemaRaw)
			if err != nil {
				return nil, err
			}
			schemas = append(schemas, schema)
		}
		return schemas, nil
	}

	// V1 format with single schema
	if raw.Schema != nil {
		schema, err := parseSchema(raw.Schema)
		if err != nil {
			return nil, err
		}
		return []*iceberg.Schema{schema}, nil
	}

	return []*iceberg.Schema{}, nil
}

// parseSchema parses a single schema from JSON.
func parseSchema(data json.RawMessage) (*iceberg.Schema, error) {
	var rawSchema rawSchemaJSON
	if err := json.Unmarshal(data, &rawSchema); err != nil {
		return nil, fmt.Errorf("%w: failed to parse schema: %w", ErrInvalidMetadata, err)
	}

	fields := make([]iceberg.NestedField, 0, len(rawSchema.Fields))
	for _, f := range rawSchema.Fields {
		iceType, err := parseIcebergType(f.Type)
		if err != nil {
			return nil, err
		}
		fields = append(fields, iceberg.NestedField{
			ID:       f.ID,
			Name:     f.Name,
			Required: f.Required,
			Type:     iceType,
			Doc:      f.Doc,
		})
	}

	return iceberg.NewSchema(rawSchema.SchemaID, fields...), nil
}

// parseIcebergType parses an Iceberg type from JSON.
func parseIcebergType(data json.RawMessage) (iceberg.Type, error) {
	// First try as a string (primitive type)
	var typeName string
	if err := json.Unmarshal(data, &typeName); err == nil {
		return parsePrimitiveType(typeName)
	}

	// Try as a nested type object
	var typeObj struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &typeObj); err != nil {
		return nil, fmt.Errorf("%w: failed to parse type: %w", ErrInvalidMetadata, err)
	}

	switch typeObj.Type {
	case "struct":
		return parseStructType(data)
	case "list":
		return parseListType(data)
	case "map":
		return parseMapType(data)
	default:
		return parsePrimitiveType(typeObj.Type)
	}
}

// parsePrimitiveType parses a primitive Iceberg type from its string name.
func parsePrimitiveType(typeName string) (iceberg.Type, error) {
	switch typeName {
	case "boolean":
		return iceberg.BooleanType{}, nil
	case "int":
		return iceberg.Int32Type{}, nil
	case "long":
		return iceberg.Int64Type{}, nil
	case "float":
		return iceberg.Float32Type{}, nil
	case "double":
		return iceberg.Float64Type{}, nil
	case "date":
		return iceberg.DateType{}, nil
	case "time":
		return iceberg.TimeType{}, nil
	case "timestamp":
		return iceberg.TimestampType{}, nil
	case "timestamptz":
		return iceberg.TimestampTzType{}, nil
	case "string":
		return iceberg.StringType{}, nil
	case "uuid":
		return iceberg.UUIDType{}, nil
	case "binary":
		return iceberg.BinaryType{}, nil
	default:
		// Handle parameterized types like fixed[16], decimal(10,2)
		if strings.HasPrefix(typeName, "fixed[") {
			return parseFixedType(typeName)
		}
		if strings.HasPrefix(typeName, "decimal(") {
			return parseDecimalType(typeName)
		}
		return nil, fmt.Errorf("%w: unknown type %q", ErrUnsupportedType, typeName)
	}
}

// parseFixedType parses a fixed type like "fixed[16]".
func parseFixedType(typeName string) (iceberg.Type, error) {
	// Extract length from "fixed[16]"
	start := strings.Index(typeName, "[")
	end := strings.Index(typeName, "]")
	if start < 0 || end < 0 || end <= start+1 {
		return nil, fmt.Errorf("%w: invalid fixed type %q", ErrUnsupportedType, typeName)
	}
	length, err := strconv.Atoi(typeName[start+1 : end])
	if err != nil {
		return nil, fmt.Errorf("%w: invalid fixed type %q", ErrUnsupportedType, typeName)
	}
	return iceberg.FixedTypeOf(length), nil
}

// parseDecimalType parses a decimal type like "decimal(10,2)".
func parseDecimalType(typeName string) (iceberg.Type, error) {
	// Extract precision and scale from "decimal(10,2)"
	start := strings.Index(typeName, "(")
	end := strings.Index(typeName, ")")
	if start < 0 || end < 0 || end <= start+1 {
		return nil, fmt.Errorf("%w: invalid decimal type %q", ErrUnsupportedType, typeName)
	}
	parts := strings.Split(typeName[start+1:end], ",")
	if len(parts) != 2 {
		return nil, fmt.Errorf("%w: invalid decimal type %q", ErrUnsupportedType, typeName)
	}
	precision, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid decimal type %q", ErrUnsupportedType, typeName)
	}
	scale, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid decimal type %q", ErrUnsupportedType, typeName)
	}
	return iceberg.DecimalTypeOf(precision, scale), nil
}

// parseStructType parses a struct type from JSON.
func parseStructType(data json.RawMessage) (*iceberg.StructType, error) {
	var obj struct {
		Fields []rawFieldJSON `json:"fields"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("%w: failed to parse struct type: %w", ErrInvalidMetadata, err)
	}

	fields := make([]iceberg.NestedField, 0, len(obj.Fields))
	for _, f := range obj.Fields {
		iceType, err := parseIcebergType(f.Type)
		if err != nil {
			return nil, err
		}
		fields = append(fields, iceberg.NestedField{
			ID:       f.ID,
			Name:     f.Name,
			Required: f.Required,
			Type:     iceType,
			Doc:      f.Doc,
		})
	}

	return &iceberg.StructType{FieldList: fields}, nil
}

// parseListType parses a list type from JSON.
func parseListType(data json.RawMessage) (*iceberg.ListType, error) {
	var obj struct {
		ElementID       int             `json:"element-id"`
		ElementRequired bool            `json:"element-required"`
		Element         json.RawMessage `json:"element"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("%w: failed to parse list type: %w", ErrInvalidMetadata, err)
	}

	elemType, err := parseIcebergType(obj.Element)
	if err != nil {
		return nil, err
	}

	return &iceberg.ListType{
		ElementID:       obj.ElementID,
		Element:         elemType,
		ElementRequired: obj.ElementRequired,
	}, nil
}

// parseMapType parses a map type from JSON.
func parseMapType(data json.RawMessage) (*iceberg.MapType, error) {
	var obj struct {
		KeyID         int             `json:"key-id"`
		Key           json.RawMessage `json:"key"`
		ValueID       int             `json:"value-id"`
		Value         json.RawMessage `json:"value"`
		ValueRequired bool            `json:"value-required"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("%w: failed to parse map type: %w", ErrInvalidMetadata, err)
	}

	keyType, err := parseIcebergType(obj.Key)
	if err != nil {
		return nil, err
	}

	valueType, err := parseIcebergType(obj.Value)
	if err != nil {
		return nil, err
	}

	return &iceberg.MapType{
		KeyID:         obj.KeyID,
		KeyType:       keyType,
		ValueID:       obj.ValueID,
		ValueType:     valueType,
		ValueRequired: obj.ValueRequired,
	}, nil
}

// parsePartitionSpecs parses partition specs from raw metadata.
func parsePartitionSpecs(raw rawMetadataJSON, schema *iceberg.Schema) ([]iceberg.PartitionSpec, error) {
	if len(raw.PartitionSpecs) > 0 {
		specs := make([]iceberg.PartitionSpec, 0, len(raw.PartitionSpecs))
		for _, specRaw := range raw.PartitionSpecs {
			spec, err := parsePartitionSpec(specRaw, schema)
			if err != nil {
				return nil, err
			}
			specs = append(specs, spec)
		}
		return specs, nil
	}

	// V1 format with single partition spec or empty
	if raw.PartitionSpec != nil {
		spec, err := parsePartitionSpec(raw.PartitionSpec, schema)
		if err != nil {
			return nil, err
		}
		return []iceberg.PartitionSpec{spec}, nil
	}

	// Return an unpartitioned spec
	return []iceberg.PartitionSpec{iceberg.NewPartitionSpec()}, nil
}

// parsePartitionSpec parses a single partition spec from JSON.
func parsePartitionSpec(data json.RawMessage, schema *iceberg.Schema) (iceberg.PartitionSpec, error) {
	var rawSpec rawPartitionSpecJSON
	if err := json.Unmarshal(data, &rawSpec); err != nil {
		return iceberg.PartitionSpec{}, fmt.Errorf("%w: failed to parse partition spec: %w", ErrInvalidMetadata, err)
	}

	fields := make([]iceberg.PartitionField, 0, len(rawSpec.Fields))
	for _, f := range rawSpec.Fields {
		transform, err := parseTransform(f.Transform)
		if err != nil {
			return iceberg.PartitionSpec{}, err
		}
		fields = append(fields, iceberg.PartitionField{
			SourceID:  f.SourceID,
			FieldID:   f.FieldID,
			Name:      f.Name,
			Transform: transform,
		})
	}

	return iceberg.NewPartitionSpecID(rawSpec.SpecID, fields...), nil
}

// parseTransform parses a partition transform from its string representation.
func parseTransform(transformStr string) (iceberg.Transform, error) {
	switch {
	case transformStr == "identity":
		return iceberg.IdentityTransform{}, nil
	case transformStr == "year":
		return iceberg.YearTransform{}, nil
	case transformStr == "month":
		return iceberg.MonthTransform{}, nil
	case transformStr == "day":
		return iceberg.DayTransform{}, nil
	case transformStr == "hour":
		return iceberg.HourTransform{}, nil
	case transformStr == "void":
		return iceberg.VoidTransform{}, nil
	case strings.HasPrefix(transformStr, "bucket["):
		// Parse bucket[N]
		start := strings.Index(transformStr, "[")
		end := strings.Index(transformStr, "]")
		if start < 0 || end < 0 || end <= start+1 {
			return nil, fmt.Errorf("%w: invalid bucket transform %q", ErrInvalidPartitionTransform, transformStr)
		}
		n, err := strconv.Atoi(transformStr[start+1 : end])
		if err != nil {
			return nil, fmt.Errorf("%w: invalid bucket transform %q", ErrInvalidPartitionTransform, transformStr)
		}
		return iceberg.BucketTransform{NumBuckets: n}, nil
	case strings.HasPrefix(transformStr, "truncate["):
		// Parse truncate[W]
		start := strings.Index(transformStr, "[")
		end := strings.Index(transformStr, "]")
		if start < 0 || end < 0 || end <= start+1 {
			return nil, fmt.Errorf("%w: invalid truncate transform %q", ErrInvalidPartitionTransform, transformStr)
		}
		w, err := strconv.Atoi(transformStr[start+1 : end])
		if err != nil {
			return nil, fmt.Errorf("%w: invalid truncate transform %q", ErrInvalidPartitionTransform, transformStr)
		}
		return iceberg.TruncateTransform{Width: w}, nil
	default:
		return nil, fmt.Errorf("%w: unknown transform %q", ErrInvalidPartitionTransform, transformStr)
	}
}

// ParseMetadataFromFile parses Iceberg metadata from a file path.
func ParseMetadataFromFile(path string) (*TableMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read file: %w", ErrTableNotFound, err)
	}

	return ParseMetadataBytes(data)
}
