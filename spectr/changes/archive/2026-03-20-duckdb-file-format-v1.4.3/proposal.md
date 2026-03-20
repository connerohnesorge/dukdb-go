# DuckDB Native File Format Support (v1.4.3)

**Change ID:** `duckdb-file-format-v1.4.3`

## Overview

This proposal outlines the implementation of native DuckDB file format support in dukdb-go to achieve full compatibility with DuckDB v1.4.3. The implementation will enable dukdb-go to read and write DuckDB database files directly, ensuring seamless interoperability with the official DuckDB implementation while maintaining our pure Go architecture.

## Motivation

Currently, dukdb-go operates as an in-memory database engine without persistent storage capabilities. Implementing native file format support will:

1. **Enable Persistence**: Allow databases to survive process restarts
2. **Ensure Compatibility**: Read/write files compatible with DuckDB v1.4.3
3. **Support Migration**: Facilitate data migration between dukdb-go and DuckDB
4. **Enable Advanced Features**: Support for larger-than-memory datasets through MMAP
5. **Maintain Purity**: Achieve this without CGO dependencies

## Goals

1. **Full v1.4.3 Compatibility**: Read and write files compatible with DuckDB v1.4.3
2. **Performance**: Achieve comparable read/write performance to native DuckDB
3. **Memory Efficiency**: Support MMAP for large files, efficient caching strategies
4. **Durability**: ACID compliance with WAL integration
5. **Version Management**: Handle format evolution and migration

## Non-Goals

1. **Backward Compatibility**: Focus on v1.4.3, not older versions
2. **Encryption**: File encryption is out of scope for initial implementation
3. **Network Storage**: Focus on local file system operations

## Key Components

### 1. File Format Parser (`specs/format-reading/spec.md`)
- Block-based file format parsing
- Metadata extraction and validation
- Data chunk deserialization
- Index reconstruction
- Compression handling

### 2. File Format Writer (`specs/format-writing/spec.md`)
- Block allocation and management
- Metadata serialization
- Data chunk serialization
- Index persistence
- Checksum calculation

### 3. Version Compatibility (`specs/version-compatibility/spec.md`)
- Version detection and validation
- Migration strategies
- Feature flags handling
- Compatibility matrices

### 4. Metadata Storage (`specs/metadata-storage/spec.md`)
- Catalog serialization
- Statistics persistence
- Schema evolution
- Transaction metadata

## Technical Approach

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│                  SQL Interface                              │
├─────────────────────────────────────────────────────────────┤
│              Storage Manager                                │
│  ┌─────────────┬──────────────┬─────────────┬──────────┐  │
│  │Block Manager│Buffer Manager│File Manager │MMAP Layer│  │
│  └─────────────┴──────────────┴─────────────┴──────────┘  │
├─────────────────────────────────────────────────────────────┤
│              DuckDB Format Layer                            │
│  ┌─────────────┬──────────────┬─────────────┬──────────┐  │
│  │Format Parser│Format Writer │Version Mgr  │Checksum  │  │
│  └─────────────┴──────────────┴─────────────┴──────────┘  │
├─────────────────────────────────────────────────────────────┤
│              Metadata Layer                                 │
│  ┌─────────────┬──────────────┬─────────────┬──────────┐  │
│  │Catalog Store│Statistics    │Schema       │Index     │  │
│  │             │Manager       │Manager      │Manager   │  │
│  └─────────────┴──────────────┴─────────────┴──────────┘  │
├─────────────────────────────────────────────────────────────┤
│              Data Layer                                     │
│  ┌─────────────┬──────────────┬─────────────┬──────────┐  │
│  │DataChunk    │Vector        │ValidityMask │Selection │  │
│  │Serializer   │Serializer    │Serializer   │Vector    │  │
│  └─────────────┴──────────────┴─────────────┴──────────┘  │
├─────────────────────────────────────────────────────────────┤
│              WAL Integration                                │
│  ┌─────────────┬──────────────┬─────────────┬──────────┐  │
│  │WAL Manager  │Recovery      │Checkpoint   │Truncate  │  │
│  │             │Manager       │Manager      │Manager   │  │
│  └─────────────┴──────────────┴─────────────┴──────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### File Format Structure

DuckDB files are organized as a collection of blocks:

```
┌─────────────────────────────────────────────────────────────┐
│                    File Header                              │
│  ┌─────────────┬──────────────┬─────────────┬──────────┐  │
│  │Magic Number │Version Info  │Block Size   │Checksum  │  │
│  └─────────────┴──────────────┴─────────────┴──────────┘  │
├─────────────────────────────────────────────────────────────┤
│                  Block 1 (Metadata)                         │
│  ┌─────────────┬──────────────┬─────────────┬──────────┐  │
│  │Block Type   │Size          │Data         │Checksum  │  │
│  └─────────────┴──────────────┴─────────────┴──────────┘  │
├─────────────────────────────────────────────────────────────┤
│                  Block 2 (Data)                             │
│  ┌─────────────┬──────────────┬─────────────┬──────────┐  │
│  │Block Type   │Size          │Data         │Checksum  │  │
│  └─────────────┴──────────────┴─────────────┴──────────┘  │
├─────────────────────────────────────────────────────────────┤
│                      ...                                    │
├─────────────────────────────────────────────────────────────┤
│                  Block N (Free Space)                       │
│  ┌─────────────┬──────────────┬─────────────┬──────────┐  │
│  │Block Type   │Size          │Free Space   │Checksum  │  │
│  └─────────────┴──────────────┴─────────────┴──────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Block Types

- **Header Block**: File metadata, version info
- **Catalog Block**: Schema definitions, table metadata
- **Data Block**: Actual table data as compressed chunks
- **Index Block**: Index structures (hash, ART)
- **Statistics Block**: Column statistics for optimization
- **WAL Block**: Write-ahead log entries
- **Free Block**: Available space tracking

## Implementation Phases

### Phase 1: Core Infrastructure
- File format parser foundation
- Block management system
- Basic metadata handling
- Version detection

### Phase 2: Reading Support
- Catalog deserialization
- Data chunk reading
- Index reconstruction
- Statistics loading

### Phase 3: Writing Support
- Block allocation
- Data serialization
- Checksum calculation
- Atomic writes

### Phase 4: Advanced Features
- MMAP support
- Compression
- Parallel I/O
- Caching strategies

### Phase 5: Integration & Testing
- WAL integration
- Recovery procedures
- Performance optimization
- Compatibility testing

## Success Criteria

1. **Functional**: Successfully read/write DuckDB v1.4.3 files
2. **Compatible**: Pass DuckDB's compatibility test suite
3. **Performance**: Within 20% of native DuckDB I/O performance
4. **Reliable**: Zero data corruption in failure scenarios
5. **Maintainable**: Clean, well-documented Go code

## Risks and Mitigation

### Risk 1: Format Complexity
- **Mitigation**: Incremental implementation, comprehensive testing

### Risk 2: Performance
- **Mitigation**: Profiling, optimization, parallel I/O

### Risk 3: Version Drift
- **Mitigation**: Version compatibility layer, migration tools

### Risk 4: Memory Usage
- **Mitigation**: MMAP, streaming, intelligent caching

## Testing Strategy

1. **Unit Tests**: Individual component testing
2. **Integration Tests**: End-to-end file operations
3. **Compatibility Tests**: Cross-validation with DuckDB
4. **Performance Tests**: Benchmark I/O operations
5. **Stress Tests**: Concurrent access, large files
6. **Recovery Tests**: Crash recovery scenarios

## Future Work

1. **Encryption**: File-level encryption support
2. **Compression**: Advanced compression algorithms
3. **Network**: Remote storage support
4. **Replication**: Multi-node synchronization

## Conclusion

Implementing native DuckDB file format support is a critical step toward making dukdb-go a complete, production-ready database engine. This proposal provides a comprehensive roadmap for achieving full compatibility with DuckDB v1.4.3 while maintaining our commitment to pure Go implementation.