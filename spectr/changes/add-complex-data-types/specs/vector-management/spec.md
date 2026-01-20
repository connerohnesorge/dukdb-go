# Vector Management Specification

## ADDED Requirements

### Requirement: Child Vector Lifecycle Management

The system SHALL manage child vector allocation, cleanup, and recycling for complex types.

#### Scenario: Child vectors allocated on complex vector creation
- GIVEN StructVector creation with 3 fields
- WHEN constructor executes
- THEN 3 child vectors allocated with parent capacity
- AND each child vector fully initialized

#### Scenario: Child vectors deallocated on complex vector release
- GIVEN StructVector in vector pool
- WHEN releasing to pool
- THEN all child vectors recursively released to pool
- AND child validity bitmasks cleared
- AND memory available for reuse

#### Scenario: Recursive allocation for nested types
- GIVEN StructVector with field type LIST (future phase)
- WHEN creating StructVector
- THEN LIST child vector allocated
- AND LIST child's element vector allocated
- AND full hierarchy exists

#### Scenario: Child vector count consistency
- GIVEN MapVector
- WHEN calling GetChildCount()
- THEN returns 2 (key and value vectors)
- AND matches actual children

#### Scenario: Reuse child vectors across rows
- GIVEN StructVector processing 10,000 rows in 5 batches
- WHEN each batch completes and returns to pool
- THEN child vectors reused from pool
- AND no allocation thrashing

### Requirement: Validity Bitmap Parent-Child Coordination

The system SHALL coordinate validity bitmaps between parent and child vectors.

#### Scenario: Parent NULL prevents child access
- GIVEN StructVector with struct-level NULL at index 5
- WHEN accessing field vector data at index 5
- THEN child field returns NULL regardless of field's validity
- AND short-circuits at parent check

#### Scenario: Child NULL independent from parent
- GIVEN StructVector with valid parent at index 7
- AND field vector has NULL at index 7
- WHEN accessing field
- THEN returns NULL (field-level NULL)
- AND valid parent doesn't override

#### Scenario: Setting parent NULL cascades
- GIVEN StructVector with multiple fields
- WHEN calling SetNull on parent at index 3
- THEN all field vectors at index 3 marked NULL
- AND validity propagates to all children

#### Scenario: Setting child NULL doesn't affect parent
- GIVEN StructVector with valid parent
- WHEN setting field NULL at index 3
- THEN only field marked NULL
- AND parent remains valid

#### Scenario: Count valid across hierarchies
- GIVEN StructVector with 1000 rows where 50 parents are NULL
- AND within valid parents, field has 100 additional NULLs
- WHEN calling field vector CountValid()
- THEN returns (1000 - 50 - 100) = 850

#### Scenario: Batch validity operations on complex vectors
- GIVEN MapVector with 2048 rows
- WHEN calling SetAllValid()
- THEN parent and all child vectors marked fully valid
- AND validity bitmasks correctly reflect state

#### Scenario: Validity check doesn't allocate
- GIVEN complex vector with validity bitmap
- WHEN calling IsValid(i) repeatedly
- THEN no allocations occur
- AND operation is O(1)

### Requirement: Vector State Coherence

The system SHALL maintain consistent state between parent and children across all operations.

#### Scenario: Resize maintains child state
- GIVEN StructVector with capacity 1024
- WHEN resizing to 2048
- THEN parent row count increases
- AND all child vectors resized to 2048
- AND existing data preserved
- AND new slots available

#### Scenario: Flatten operation recurses into children
- GIVEN StructVector with selection vector
- WHEN calling Flatten()
- THEN parent data reordered
- AND all child vectors flattened in same order
- AND selection vector applied consistently

#### Scenario: Clone operation deep-copies children
- GIVEN StructVector cloned
- WHEN modifying clone's field vectors
- THEN original field vectors unaffected
- AND modifications isolated

#### Scenario: Column count consistency
- GIVEN MapVector
- WHEN querying child count at different points
- THEN always returns consistent child count (2)
- AND no implicit children added/removed

### Requirement: Vector Pool Integration

The system SHALL integrate complex vectors with existing vector pool for memory recycling.

#### Scenario: Complex vectors acquired from pool
- GIVEN vector pool with available StructVector
- WHEN calling pool.Acquire(TYPE_STRUCT, capacity)
- THEN returns reusable StructVector
- AND validity bitmap cleared
- AND ready for new data

#### Scenario: Pool handles child allocation
- GIVEN pool.Acquire for StructVector with 3 fields
- THEN pool allocates parent + 3 child vectors
- AND returns fully initialized vector

#### Scenario: Pool cleans up children on release
- GIVEN StructVector released to pool
- WHEN calling pool.Release()
- THEN parent validity cleared
- AND all child vectors released to pool
- AND child validity cleared
- AND memory returned to pool

#### Scenario: Pool prevents child vector leaks
- GIVEN application creating many StructVectors
- WHEN cycling through pool
- THEN child vectors properly recycled
- AND no memory accumulation
- AND pprof shows stable memory

#### Scenario: Pool capacity enforces limits
- GIVEN vector pool with max cached vectors
- WHEN releasing excess vectors
- THEN excess not cached (returned to GC)
- AND active vectors in use stay live

### Requirement: Complex Vector Mutability Safety

The system SHALL protect complex vector invariants during mutations.

#### Scenario: Cannot modify field structure during iteration
- GIVEN StructVector being iterated
- WHEN attempting AddField()
- THEN operation blocked or deferred
- AND error returned

#### Scenario: Capacity changes don't corrupt data
- GIVEN StructVector with data
- WHEN calling SetCapacity with different size
- THEN data integrity maintained
- AND no corruption/loss

#### Scenario: Type mismatch on field assignment
- GIVEN StructVector with field "age" TYPE_INTEGER
- WHEN assigning float value directly to field vector
- THEN type check catches error
- AND value rejected

#### Scenario: Out-of-bounds index protection
- GIVEN StructVector with 100 rows
- WHEN accessing index 150
- THEN error returned
- AND bounds checked before access

### Requirement: Vector Relationship Validation

The system SHALL validate parent-child vector relationships.

#### Scenario: Child vector capacity matches parent
- GIVEN StructVector
- WHEN querying any field vector capacity
- THEN equals parent capacity
- AND synchronized

#### Scenario: Orphaned child detection
- GIVEN complex vector reference
- WHEN checking child vectors
- THEN all children properly linked to parent
- AND no orphaned references

#### Scenario: Circular reference prevention
- GIVEN complex vector structure
- WHEN validating relationships
- THEN no cycles detected
- AND tree structure maintained

#### Scenario: Validity mask size consistency
- GIVEN complex vector with 2048 rows
- WHEN checking validity bitmask size
- THEN bitmap size matches (2048 + 63) / 64 = 32 uint64 entries
- AND matches all child vectors
