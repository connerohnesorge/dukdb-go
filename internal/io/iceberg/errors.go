// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file defines Iceberg-specific error types.
package iceberg

import "errors"

var (
	// ErrTableNotFound indicates that no Iceberg table was found at the specified location.
	ErrTableNotFound = errors.New("iceberg: table not found")

	// ErrSnapshotNotFound indicates that the requested snapshot ID does not exist.
	ErrSnapshotNotFound = errors.New("iceberg: snapshot not found")

	// ErrNoSnapshotAtTimestamp indicates that no snapshot exists at or before the given timestamp.
	ErrNoSnapshotAtTimestamp = errors.New("iceberg: no snapshot at or before timestamp")

	// ErrInvalidMetadata indicates that the metadata.json file is invalid or corrupted.
	ErrInvalidMetadata = errors.New("iceberg: invalid metadata")

	// ErrUnsupportedVersion indicates that the Iceberg format version is not supported.
	ErrUnsupportedVersion = errors.New("iceberg: unsupported format version")

	// ErrUnsupportedType indicates that an Iceberg type cannot be mapped to a DuckDB type.
	ErrUnsupportedType = errors.New("iceberg: unsupported type")

	// ErrManifestReadFailed indicates that a manifest file could not be read.
	ErrManifestReadFailed = errors.New("iceberg: failed to read manifest")

	// ErrManifestListReadFailed indicates that a manifest list file could not be read.
	ErrManifestListReadFailed = errors.New("iceberg: failed to read manifest list")

	// ErrNoCurrentSnapshot indicates that the table has no current snapshot.
	ErrNoCurrentSnapshot = errors.New("iceberg: no current snapshot")

	// ErrSchemaNotFound indicates that the requested schema ID does not exist.
	ErrSchemaNotFound = errors.New("iceberg: schema not found")

	// ErrPartitionSpecNotFound indicates that the requested partition spec ID does not exist.
	ErrPartitionSpecNotFound = errors.New("iceberg: partition spec not found")

	// ErrInvalidPartitionTransform indicates an unsupported or invalid partition transform.
	ErrInvalidPartitionTransform = errors.New("iceberg: invalid partition transform")

	// ErrVersionHintNotFound indicates that version-hint.text file was not found.
	ErrVersionHintNotFound = errors.New("iceberg: version-hint.text not found")

	// ErrMetadataLocationNotFound indicates that the metadata file location could not be determined.
	ErrMetadataLocationNotFound = errors.New("iceberg: metadata location not found")

	// ErrDeleteFileNotSupported indicates that delete files are not yet supported.
	ErrDeleteFileNotSupported = errors.New("iceberg: delete files not yet supported")
)
