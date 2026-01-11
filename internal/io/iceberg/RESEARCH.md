# Iceberg Table Support - Library Research

This document evaluates pure Go libraries needed for Apache Iceberg table format support in dukdb-go.

## Requirements

- **Zero CGO dependencies** (required for TinyGo/WASM compatibility)
- **License compatibility** with MIT/Apache 2.0
- **Support for Iceberg spec v1 and v2**
- **AVRO support** for manifest lists and manifest files
- **Parquet support** for data files (already have `github.com/parquet-go/parquet-go`)

## Library Evaluations

### 1. Apache Iceberg Go Library

#### github.com/apache/iceberg-go

| Attribute | Value |
|-----------|-------|
| Stars | 374 |
| License | Apache-2.0 |
| Created | August 2023 |
| Last Updated | January 2026 |
| Go Version | 1.24+ |
| Maintainer | Apache Software Foundation |

**Features Supported:**

| Feature | Status |
|---------|--------|
| Metadata parsing (metadata.json) | Yes |
| Snapshot discovery | Yes |
| Manifest list parsing | Yes |
| Manifest file parsing | Yes |
| Partition specs | Yes |
| Schema evolution | Yes |
| Time travel queries | Yes |
| REST catalog | Yes |
| Glue catalog | Yes |
| SQL catalog | Yes |
| Hive catalog | No |
| S3 filesystem | Yes |
| GCS filesystem | Yes |
| Azure Blob | Yes |
| Local filesystem | Yes |
| Scan planning | Yes |
| Arrow data reading | Yes |
| Append writes | Yes |
| Delete files (pos/eq) | Partial |

**Dependencies:**

```
github.com/apache/arrow-go/v18  - Arrow for data reading
github.com/hamba/avro/v2        - AVRO codec for manifests
cloud.google.com/go/storage     - GCS support
github.com/aws/aws-sdk-go-v2    - S3 support
github.com/Azure/azure-sdk-for-go - Azure support
```

**Pros:**
- Official Apache implementation - will track spec changes
- Comprehensive feature support including REST/Glue/SQL catalogs
- Already uses hamba/avro for AVRO encoding
- Active development with recent commits
- Supports both Iceberg v1 and v2 specs
- Read and write support

**Cons:**
- Heavy dependency footprint (Arrow, cloud SDKs)
- Requires Go 1.24+
- Arrow dependency may have CGO-optional optimizations
- Not yet tested for TinyGo/WASM compatibility
- No explicit CGO-free guarantee

**CGO Analysis:**
- The library itself is pure Go
- Arrow-go has optional SIMD optimizations via c2goasm (can be disabled with `noasm` build tag)
- Cloud SDK dependencies are pure Go
- Overall: **CGO-free with `noasm` tag**

**TinyGo/WASM Compatibility:**
- Unknown - no explicit testing or support documented
- Arrow-go may have issues with WASM due to memory model
- Cloud SDKs likely work but untested
- Recommendation: Requires testing before adoption

---

### 2. AVRO Libraries

Iceberg uses AVRO format for manifest lists and manifest files. The AVRO schema is embedded in the Iceberg spec and defines structures for:
- Manifest file entries (data_file, status, snapshot_id, etc.)
- Manifest list entries (manifest_path, partition_spec_id, etc.)

#### github.com/hamba/avro (v2)

| Attribute | Value |
|-----------|-------|
| Stars | 503 |
| License | MIT |
| Created | February 2019 |
| Last Updated | January 2026 |
| Go Version | 1.24+ |
| Status | **No longer maintained** (see warning below) |

**WARNING:** The maintainer announced in early 2025 that this project is no longer actively maintained. PRs may be reviewed but no new features are being developed.

**Features:**
- Full AVRO spec support (primitives, complex types, logical types)
- Schema evolution support
- Union type handling
- Object Container File (OCF) support
- Snappy and deflate compression
- High performance (faster than goavro)

**Dependencies:**
```
github.com/golang/snappy         - Snappy compression
github.com/klauspost/compress    - Deflate compression
github.com/json-iterator/go      - JSON parsing
github.com/ettle/strcase         - String case conversion
```

**Pros:**
- MIT license (compatible)
- Pure Go, no CGO
- High performance (3-4x faster than goavro in benchmarks)
- Schema-based marshal/unmarshal with struct tags
- Active until recently, mature codebase
- Used by apache/iceberg-go

**Cons:**
- **No longer maintained** - critical concern for long-term support
- May not receive security patches
- No TinyGo/WASM testing

**CGO Status:** Pure Go, no CGO dependencies

**Usage Example:**
```go
import "github.com/hamba/avro/v2"

type ManifestFile struct {
    ManifestPath  string `avro:"manifest_path"`
    ManifestLength int64 `avro:"manifest_length"`
    PartitionSpecID int32 `avro:"partition_spec_id"`
    // ...
}

schema, _ := avro.Parse(`{
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
        {"name": "partition_spec_id", "type": "int"}
    ]
}`)

data, _ := avro.Marshal(schema, manifestFile)
```

---

#### github.com/linkedin/goavro (v2)

| Attribute | Value |
|-----------|-------|
| Stars | 1050 |
| License | Apache-2.0 |
| Created | February 2015 |
| Last Updated | January 2026 |
| Go Version | 1.12+ |
| Status | **Not actively developed** (recommends hamba/avro) |

**IMPORTANT:** LinkedIn has internally migrated to hamba/avro for better performance. The README states: "Internally, most of LinkedIn has moved over to use https://github.com/hamba/avro for Avro serialization/deserialization needs as we found it to be significantly more performant."

**Features:**
- AVRO binary and JSON encoding/decoding
- Schema namespace support
- Record field default values
- Stateless, goroutine-safe Codec

**Dependencies:**
```
github.com/golang/snappy   - Snappy compression
```

**Pros:**
- Apache-2.0 license (compatible)
- Pure Go, no CGO
- Most starred Go AVRO library
- Minimal dependencies
- Mature, battle-tested codebase

**Cons:**
- **Not actively developed** - recommends hamba/avro
- Lower performance than hamba/avro (3-4x slower)
- Uses native Go maps instead of typed structs (less type-safe)
- No schema-based struct marshaling

**CGO Status:** Pure Go, no CGO dependencies

**Usage Example:**
```go
import "github.com/linkedin/goavro/v2"

codec, _ := goavro.NewCodec(`{
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"}
    ]
}`)

// Encode
native := map[string]interface{}{
    "manifest_path": "/path/to/manifest.avro",
    "manifest_length": int64(1024),
}
binary, _ := codec.BinaryFromNative(nil, native)

// Decode
native, _, _ := codec.NativeFromBinary(binary)
```

---

#### github.com/actgardner/gogen-avro (v10)

| Attribute | Value |
|-----------|-------|
| Stars | 374 |
| License | MIT |
| Created | March 2016 |
| Last Updated | December 2025 |
| Status | **On break** (possibly permanent) |

**Note:** This is a code generation tool, not a runtime library. It generates Go structs and serialization code from AVRO schemas.

**Features:**
- Generates type-safe Go code from AVRO schemas
- Schema evolution support
- Generic data deserialization (beta)
- Object Container File support

**Pros:**
- Type-safe generated code
- Better compile-time error detection
- MIT license

**Cons:**
- **On possibly permanent break** - maintainer unavailable
- Requires build-time code generation
- Generated code may need updates for new schemas
- More complex build process

**Recommendation:** Not suitable for dynamic Iceberg schema handling where schemas are discovered at runtime.

---

### 3. Parquet Library (Already in Use)

#### github.com/parquet-go/parquet-go

| Attribute | Value |
|-----------|-------|
| Stars | 642 |
| License | Apache-2.0 |
| In Project | Yes (go.mod) |
| Go Version | 1.24+ |

**Status:** Already integrated in dukdb-go for Parquet file reading/writing.

**Features Used:**
- GenericReader/GenericWriter
- Column projection
- Row group iteration
- All Parquet compressions (Snappy, GZIP, ZSTD, LZ4, Brotli)

**CGO Status:** Pure Go, no CGO dependencies

**Integration Notes:**
- Works well for reading Iceberg data files
- Supports all Parquet types needed by Iceberg
- Schema can be constructed from Iceberg schema definitions

---

### 4. Other Iceberg Libraries Considered

#### github.com/tabular-io/iceberg-go
- **Status:** Does not exist (404 Not Found)

#### github.com/transferia/iceberg
- **Stars:** 5
- **Status:** Too new/immature for production use

#### github.com/eatonphil/otf
- **Stars:** 43
- **Status:** Delta Lake inspired, not full Iceberg implementation

---

## Decision Matrix

### AVRO Library Selection

| Criteria | hamba/avro | linkedin/goavro | gogen-avro |
|----------|------------|-----------------|------------|
| License | MIT | Apache-2.0 | MIT |
| CGO-free | Yes | Yes | Yes |
| Performance | High (3-4x) | Baseline | N/A (generated) |
| Maintenance | Abandoned | Abandoned | Abandoned |
| Type Safety | Struct tags | Maps | Generated |
| TinyGo/WASM | Unknown | Unknown | Unknown |
| Used by iceberg-go | Yes | No | No |

**Recommendation: github.com/hamba/avro/v2**

Rationale:
1. Already used by apache/iceberg-go (proven integration)
2. Higher performance than goavro
3. Type-safe struct marshaling
4. MIT license compatible with project
5. Mature codebase despite maintenance status

Mitigation for maintenance status:
- Fork if critical bugs found
- AVRO spec is stable, unlikely to need updates
- Consider contributing to community maintenance efforts

---

### Iceberg Library Selection

| Option | Pros | Cons |
|--------|------|------|
| **A: Use apache/iceberg-go** | Official, comprehensive | Heavy deps, Arrow required |
| **B: Build custom parser** | Minimal deps, full control | Significant effort |
| **C: Hybrid approach** | Balance of features/deps | Integration complexity |

**Recommendation: Option C - Hybrid Approach**

Use apache/iceberg-go's metadata parsing packages selectively:
1. Import only `github.com/apache/iceberg-go/table` for metadata
2. Use existing parquet-go for data file reading
3. Avoid Arrow dependency for data representation
4. Convert to dukdb-go's DataChunk format directly

This approach:
- Leverages official Iceberg parsing (spec compliance)
- Avoids heavy Arrow dependency
- Reuses existing Parquet infrastructure
- Maintains pure Go requirement

---

## Implementation Recommendations

### Phase 1: Core Metadata Support

```go
// Use iceberg-go for metadata parsing
import (
    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog"
    "github.com/apache/iceberg-go/table"
)

// Use existing parquet-go for data
import "github.com/parquet-go/parquet-go"

// Use hamba/avro for manifest parsing
import "github.com/hamba/avro/v2"
```

### Phase 2: Catalog Integration

Implement catalog adapters:
- Local filesystem catalog (directory-based)
- REST catalog (for Tabular, Nessie, etc.)
- Optional: Glue catalog, SQL catalog

### Phase 3: Data Reading Pipeline

```
Iceberg Table Location
        |
        v
metadata.json (JSON parsing - stdlib)
        |
        v
manifest-list.avro (hamba/avro)
        |
        v
manifest-*.avro (hamba/avro)
        |
        v
data-*.parquet (parquet-go)
        |
        v
DataChunk (dukdb-go storage)
```

---

## TinyGo/WASM Compatibility Notes

### Known Issues

1. **Apache Arrow-Go**
   - Uses `unsafe` package extensively
   - Has optional SIMD via c2goasm
   - Memory model may conflict with WASM
   - Recommendation: Test with `noasm` tag, consider alternatives

2. **Cloud SDKs**
   - AWS/GCP/Azure SDKs are large
   - May have network/HTTP issues in WASM
   - Recommendation: Make cloud storage optional via build tags

3. **hamba/avro**
   - Uses reflection heavily
   - TinyGo reflection support is limited
   - Recommendation: Test critical paths in TinyGo

### Build Tag Strategy

```go
// +build !tinygo,!wasm

import "github.com/apache/iceberg-go" // Full feature set

// +build tinygo wasm

// Minimal implementation for constrained environments
```

---

## Dependency Summary

### Required Dependencies

| Package | Purpose | License | CGO |
|---------|---------|---------|-----|
| github.com/hamba/avro/v2 | Manifest parsing | MIT | No |
| github.com/parquet-go/parquet-go | Data file reading | Apache-2.0 | No |
| github.com/apache/iceberg-go | Metadata parsing | Apache-2.0 | No* |

*With `noasm` build tag for Arrow

### Optional Dependencies (Cloud Storage)

| Package | Purpose | License | CGO |
|---------|---------|---------|-----|
| github.com/aws/aws-sdk-go-v2 | S3 access | Apache-2.0 | No |
| cloud.google.com/go/storage | GCS access | Apache-2.0 | No |
| github.com/Azure/azure-sdk-for-go | Azure access | MIT | No |

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| hamba/avro unmaintained | High | Medium | Fork if needed, AVRO spec is stable |
| iceberg-go breaking changes | Medium | Medium | Pin version, contribute upstream |
| TinyGo incompatibility | Medium | High | Test early, provide fallback impl |
| WASM memory limits | Low | High | Streaming API, chunk-based reading |
| Iceberg spec changes | Low | Medium | Track apache/iceberg-go releases |

---

## References

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Apache Iceberg Go](https://github.com/apache/iceberg-go)
- [hamba/avro](https://github.com/hamba/avro)
- [linkedin/goavro](https://github.com/linkedin/goavro)
- [parquet-go](https://github.com/parquet-go/parquet-go)
- [DuckDB Iceberg Extension](https://github.com/duckdb/duckdb-iceberg)
- [Iceberg Format Version 2](https://iceberg.apache.org/spec/#version-2)

---

## Conclusion

For implementing Iceberg table support in dukdb-go:

1. **Use `github.com/apache/iceberg-go`** for metadata parsing and catalog integration
2. **Use `github.com/hamba/avro/v2`** for manifest file parsing (already a dependency of iceberg-go)
3. **Use existing `github.com/parquet-go/parquet-go`** for data file reading
4. **Build with `noasm` tag** when targeting TinyGo/WASM
5. **Make cloud storage optional** via build tags for minimal builds

This approach provides:
- Official Iceberg spec compliance
- Minimal additional dependencies
- Pure Go (CGO-free) implementation
- Reuse of existing Parquet infrastructure
- Path to TinyGo/WASM support with testing
