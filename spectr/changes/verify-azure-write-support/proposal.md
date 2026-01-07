# Change: Verify and Document Azure Write Support

## Why

DuckDB v1.4.3 added Azure Blob Storage write support (previously read-only). This proposal is to verify that dukdb-go's existing Azure cloud storage implementation includes write capability and document the correct usage patterns.

Azure Blob Storage is a critical cloud storage option for Azure-based workflows. The gap is whether:
1. dukdb-go's existing Azure implementation supports writes
2. The COPY TO syntax works correctly with Azure blob storage
3. Secret management works for Azure credentials

## What Changes

- **VERIFIED**: Azure Blob Storage write support in existing implementation
- **ADDED**: Documentation for Azure write operations
- **ADDED**: Integration tests for Azure write operations
- **ADDED**: Example configurations for Azure authentication
- **FIXED**: Any issues discovered during verification

## Impact

- Affected specs: `specs/cloud-storage/spec.md`
- Affected code:
  - `internal/io/cloud/` - Azure blob storage I/O
  - `internal/secrets/` - Azure credential management
  - Documentation and tests
- Breaking changes: None
- Dependencies: Existing Azure SDK for Go

## Priority

**MEDIUM** - Azure write support is a DuckDB v1.4.3 feature that needs verification. If missing, it's a compatibility gap. If present, we need documentation.
