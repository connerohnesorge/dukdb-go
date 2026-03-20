# Serialization Specification

## Requirements

### Requirement: DuckDB 1.4.3 Complex Type Format

The system SHALL serialize and deserialize complex vectors in DuckDB 1.4.3 row group format.

#### Scenario: JSONVector serialization
- GIVEN JSONVector with 100 JSON document strings
- WHEN converting to DuckDBColumnSegment
- THEN segment.LogicalTypeID equals TYPE_JSON (37)
- AND segment.Data contains FSST-compressed JSON strings
- AND segment.Validity contains NULL bitmap
- AND segment.SegmentMetadata.CompressionType is CompressionFSST

#### Scenario: JSONVector deserialization
- GIVEN DuckDBColumnSegment with JSON data in DuckDB format
- WHEN converting to JSONVector
- THEN JSONVector populated with string values
- AND validity bitmap restored
- AND JSON parsing deferred until access

#### Scenario: MapVector serialization
- GIVEN MapVector(VARCHAR keys, INTEGER values) with 50 entries
- WHEN converting to DuckDBColumnSegment
- THEN parent segment.LogicalTypeID equals TYPE_MAP (102)
- AND parent segment has 2 children (key segment, value segment)
- AND child segments properly typed
- AND child offsets stored in parent

#### Scenario: MapVector deserialization
- GIVEN DuckDBColumnSegment with 2 children for MAP
- WHEN converting to MapVector
- THEN MapVector created with key and value vectors
- AND children populated from segments
- AND offsets reconstructed

#### Scenario: StructVector serialization
- GIVEN StructVector with fields [id INT, name VARCHAR, age INT]
- WHEN converting to DuckDBColumnSegment
- THEN segment.LogicalTypeID equals TYPE_STRUCT (100)
- AND segment has 3 children (one per field)
- AND child order matches field order
- AND field names stored in metadata

#### Scenario: StructVector deserialization
- GIVEN DuckDBColumnSegment with STRUCT format
- WHEN converting to StructVector
- THEN StructVector created with all fields
- AND field names extracted from metadata
- AND field order preserved
- AND vectors fully populated

#### Scenario: UnionVector serialization
- GIVEN UnionVector with members [value INT, error VARCHAR]
- WHEN converting to DuckDBColumnSegment
- THEN segment.LogicalTypeID equals TYPE_UNION (107)
- AND segment has 3 children (indices, value segment, error segment)
- AND child 0 contains active member indices
- AND metadata stores member names and types

#### Scenario: UnionVector deserialization
- GIVEN DuckDBColumnSegment with UNION format
- WHEN converting to UnionVector
- THEN UnionVector created with all members
- AND member names extracted
- AND active indices reconstructed
- AND member vectors populated

### Requirement: Child Vector Serialization

The system SHALL recursively serialize nested complex types.

#### Scenario: StructVector with complex field types
- GIVEN StructVector with field "data" of type MAP
- WHEN serializing
- THEN field segment created with MAP child segments
- AND full type hierarchy persisted

#### Scenario: Nested StructVector serialization
- GIVEN StructVector with field containing another STRUCT
- WHEN serializing
- THEN nested struct appears as child segment
- AND recursion handled correctly

#### Scenario: Deep nesting serialization
- GIVEN multiple levels of nesting (STRUCT → STRUCT → MAP)
- WHEN serializing
- THEN all levels persisted in segment tree
- AND no truncation or loss

#### Scenario: Circular reference prevention
- GIVEN complex vector structure (validated as tree)
- WHEN serializing
- THEN no infinite loops
- AND all references serialized exactly once

### Requirement: Compression Selection for Complex Types

The system SHALL select appropriate compression algorithms for complex type storage.

#### Scenario: JSON compression uses FSST
- GIVEN JSONVector with diverse JSON documents
- WHEN compressing
- THEN compression algorithm is FSST
- AND symbol table trained on sample
- AND compression ratio >50% expected

#### Scenario: MAP key compression depends on key type
- GIVEN MapVector with INTEGER keys
- WHEN compressing keys
- THEN key segment uses BitPacking compression
- AND value segment compression independent

#### Scenario: MAP value compression depends on value type
- GIVEN MapVector with VARCHAR values
- WHEN compressing values
- THEN value segment uses FSST compression

#### Scenario: STRUCT field compression independent
- GIVEN StructVector with fields [int_col INT, str_col VARCHAR, float_col DOUBLE]
- WHEN compressing
- THEN each field compressed with appropriate algorithm
- AND int_col → BitPacking, str_col → FSST, float_col → Chimp

#### Scenario: UNION indices compressed with RLE
- GIVEN UnionVector where most rows use same member
- WHEN compressing indices
- THEN indices column uses RLE compression
- AND good compression ratio when dominated by single member

#### Scenario: Validity bitmap compression
- GIVEN complex vectors with sparse NULLs
- WHEN compressing
- THEN validity bitmaps included in segment
- AND RLE applied if beneficial

### Requirement: Metadata Preservation

The system SHALL preserve all type metadata in serialization.

#### Scenario: Field names preserved in StructVector
- GIVEN StructVector with fields ["user_id", "user_name", "user_age"]
- WHEN serializing and deserializing
- THEN field names exactly match original

#### Scenario: Member names preserved in UnionVector
- GIVEN UnionVector with members ["success", "error", "pending"]
- WHEN serializing and deserializing
- THEN member names exactly match original

#### Scenario: Field order preserved in StructVector
- GIVEN StructVector with fields in specific order
- WHEN deserializing
- THEN field order matches original (not alphabetical)

#### Scenario: Type information exact match
- GIVEN StructVector with fields of specific types
- WHEN serializing and deserializing
- THEN each field type exactly matches (no type promotion)

#### Scenario: Map type parameters preserved
- GIVEN MapVector(VARCHAR, DECIMAL(18,4))
- WHEN serializing and deserializing
- THEN key type VARCHAR, value type DECIMAL(18,4)
- AND decimal precision preserved

### Requirement: Round-Trip Format Validation

The system SHALL ensure complex vectors survive serialization round-trip.

#### Scenario: JSON round-trip
- GIVEN JSONVector with diverse JSON values including NULLs
- WHEN serializing to segment and deserializing
- THEN values and NULLs match exactly

#### Scenario: MAP round-trip
- GIVEN MapVector with various key-value pairs including NULLs
- WHEN serializing and deserializing
- THEN map structure and values match
- AND NULLs at all levels preserved

#### Scenario: STRUCT round-trip
- GIVEN StructVector with mixed valid/NULL fields
- WHEN serializing and deserializing
- THEN field values match
- AND field nullability preserved
- AND parent struct nullability preserved

#### Scenario: UNION round-trip
- GIVEN UnionVector with various active members
- WHEN serializing and deserializing
- THEN active members match original
- AND member values match
- AND NULLs preserved

#### Scenario: Complex value fidelity
- GIVEN JSON value '{"nested": {"array": [1, 2, 3]}}'
- WHEN serializing to bytes and deserializing
- THEN JSON string exactly matches original (byte-level)

### Requirement: Compatibility with Existing Serialization Code

The system SHALL integrate with existing DuckDB row group serialization.

#### Scenario: Complex type segments integrate with row group
- GIVEN RowGroup with mixed primitive and complex columns
- WHEN serializing to DuckDB format
- THEN all columns serialized correctly
- AND format remains valid for DuckDB consumption

#### Scenario: Primitive column changes not affected
- GIVEN existing primitive column serialization code
- WHEN adding complex type support
- THEN primitive serialization unchanged
- AND backward compatible

#### Scenario: Type map integration
- GIVEN TypeInfo for complex type
- WHEN converting to LogicalTypeID
- THEN correct ID used (MAP→102, STRUCT→100, etc.)
- AND integration with mapTypeToLogicalTypeID()

#### Scenario: Deserialization routing
- GIVEN DuckDBColumnSegment with TYPE_MAP
- WHEN deserializing
- THEN routed to MapVector deserializer
- AND correct vector type instantiated

### Requirement: Error Handling in Serialization

The system SHALL provide clear errors for serialization failures.

#### Scenario: Unsupported compression for complex type
- GIVEN complex type with compression mismatch
- WHEN attempting serialization
- THEN error indicates unsupported compression
- AND suggests compatible alternative

#### Scenario: Invalid metadata format
- GIVEN segment with malformed metadata
- WHEN deserializing
- THEN error explains metadata issue
- AND points to recovery strategy

#### Scenario: Child segment mismatch
- GIVEN STRUCT segment with field count mismatch
- WHEN deserializing
- THEN error indicates mismatch
- AND helps identify corrupt segment

#### Scenario: Type mismatch detection
- GIVEN segment claiming to be STRUCT but with UNION format
- WHEN deserializing
- THEN error detected
- AND mismatch clearly reported

