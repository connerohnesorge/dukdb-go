# ART Range Scan Interface Design

This document specifies the design of the range scan interface for the Adaptive Radix Tree (ART)
index, enabling support for `<`, `>`, `<=`, `>=`, and `BETWEEN` predicates.

## 1. Overview

The ART index currently supports only point lookups via `Lookup()`. Range scans are essential
for efficiently executing range predicates without falling back to full table scans. This
design introduces an iterator-based interface for traversing keys in sorted order within a
specified range.

### Current State

```go
// Existing ART interface (internal/storage/index/art.go)
type ART struct {
    root    *ARTNode
    keyType dukdb.Type
}

func (a *ART) Insert(key []byte, value any) error
func (a *ART) Lookup(key []byte) (any, bool)
func (a *ART) Serialize() ([]byte, error)
```

### Target State

```go
// Extended ART interface
func (a *ART) RangeScan(lower, upper []byte, opts RangeScanOptions) *ARTIterator
func (a *ART) ScanFrom(lower []byte, inclusive bool) *ARTIterator
func (a *ART) ScanTo(upper []byte, inclusive bool) *ARTIterator
func (a *ART) ScanAll() *ARTIterator
```

## 2. ARTIterator Struct Design

### 2.1 Core Structure

```go
// ARTIterator provides ordered traversal of keys within a range.
// It uses a stack-based approach for depth-first traversal of the ART.
type ARTIterator struct {
    // art is a reference to the ART being iterated.
    art *ART

    // stack holds the traversal state for depth-first traversal.
    // Each entry represents a node and the current child index being explored.
    stack []iteratorStackEntry

    // currentKey accumulates the key bytes as we traverse down the tree.
    // This represents the full key to the current position.
    currentKey []byte

    // lowerBound is the encoded lower bound key (nil means -infinity).
    lowerBound []byte

    // upperBound is the encoded upper bound key (nil means +infinity).
    upperBound []byte

    // lowerInclusive indicates whether the lower bound is inclusive.
    lowerInclusive bool

    // upperInclusive indicates whether the upper bound is inclusive.
    upperInclusive bool

    // exhausted indicates the iterator has reached the end.
    exhausted bool

    // initialized indicates whether the iterator has been positioned.
    initialized bool

    // lastKey holds the last returned key for duplicate detection.
    lastKey []byte

    // lastValue holds the last returned value.
    lastValue any
}

// iteratorStackEntry tracks the state at each level of traversal.
type iteratorStackEntry struct {
    // node is the current node at this level.
    node *ARTNode

    // childIndex is the current child position being explored.
    // For Node4/Node16: index into keys/children arrays (0-3 or 0-15)
    // For Node48: the key byte (0-255), with actual index looked up via keys array
    // For Node256: the key byte (0-255), which is also the child index
    childIndex int

    // prefixConsumed tracks how many prefix bytes have been added to currentKey.
    // Used for proper cleanup when popping this entry from the stack.
    prefixConsumed int
}
```

### 2.2 Field Purposes

| Field | Purpose | Memory Impact |
|-------|---------|---------------|
| `art` | Reference to source ART | 8 bytes (pointer) |
| `stack` | Traversal state | ~O(key_length) entries |
| `currentKey` | Accumulated key bytes | O(key_length) bytes |
| `lowerBound` | Lower range bound | O(key_length) bytes |
| `upperBound` | Upper range bound | O(key_length) bytes |
| `lowerInclusive` | Lower bound type | 1 byte |
| `upperInclusive` | Upper bound type | 1 byte |
| `exhausted` | End-of-range flag | 1 byte |
| `initialized` | First-call flag | 1 byte |

### 2.3 Stack Entry Size Analysis

```
Per-entry memory: ~32 bytes
- node pointer: 8 bytes
- childIndex: 8 bytes (int)
- prefixConsumed: 8 bytes (int)
- slice overhead: ~8 bytes

Max stack depth: O(key_length)
- For 8-byte integer key: max ~8 entries = ~256 bytes
- For 256-byte string key: max ~256 entries = ~8 KB

Total iterator memory (typical case): ~1-2 KB
```

## 3. RangeScan() Method Signature

### 3.1 Primary Interface

```go
// RangeScanOptions configures the behavior of a range scan.
type RangeScanOptions struct {
    // LowerInclusive determines if lower bound is inclusive (>=) or exclusive (>).
    // Default: true (inclusive).
    LowerInclusive bool

    // UpperInclusive determines if upper bound is inclusive (<=) or exclusive (<).
    // Default: false (exclusive), matching typical database BETWEEN semantics.
    UpperInclusive bool

    // Reverse scans from upper to lower bound (descending order).
    // Default: false (ascending order).
    Reverse bool

    // MaxResults limits the number of results returned.
    // Default: 0 (unlimited).
    MaxResults int
}

// DefaultRangeScanOptions returns sensible defaults for range scans.
func DefaultRangeScanOptions() RangeScanOptions {
    return RangeScanOptions{
        LowerInclusive: true,
        UpperInclusive: false,
        Reverse:        false,
        MaxResults:     0,
    }
}

// RangeScan creates an iterator for keys in the range [lower, upper).
//
// Parameters:
//   - lower: The lower bound key (nil for unbounded, i.e., -infinity)
//   - upper: The upper bound key (nil for unbounded, i.e., +infinity)
//   - opts: Options controlling inclusivity, direction, and limits
//
// Returns:
//   - *ARTIterator: An iterator positioned before the first matching key.
//                   Call Next() to advance to the first result.
//
// The iterator remains valid until the ART is modified. Modifications during
// iteration result in undefined behavior.
//
// Example usage:
//
//     // WHERE x >= 10 AND x < 100
//     it := art.RangeScan(encodeKey(10), encodeKey(100), DefaultRangeScanOptions())
//     for it.HasNext() {
//         key, value, ok := it.Next()
//         if !ok {
//             break
//         }
//         // Process key, value
//     }
//     it.Close()
//
func (a *ART) RangeScan(lower, upper []byte, opts RangeScanOptions) *ARTIterator {
    return &ARTIterator{
        art:            a,
        stack:          make([]iteratorStackEntry, 0, 16), // Pre-allocate for typical depth
        currentKey:     make([]byte, 0, 64),               // Pre-allocate for typical key size
        lowerBound:     lower,
        upperBound:     upper,
        lowerInclusive: opts.LowerInclusive,
        upperInclusive: opts.UpperInclusive,
        exhausted:      false,
        initialized:    false,
    }
}
```

### 3.2 Convenience Methods

```go
// ScanFrom creates an iterator starting at the given lower bound.
// Equivalent to RangeScan(lower, nil, opts) - no upper bound.
func (a *ART) ScanFrom(lower []byte, inclusive bool) *ARTIterator {
    return a.RangeScan(lower, nil, RangeScanOptions{
        LowerInclusive: inclusive,
        UpperInclusive: false,
    })
}

// ScanTo creates an iterator up to the given upper bound.
// Equivalent to RangeScan(nil, upper, opts) - no lower bound.
func (a *ART) ScanTo(upper []byte, inclusive bool) *ARTIterator {
    return a.RangeScan(nil, upper, RangeScanOptions{
        LowerInclusive: true,
        UpperInclusive: inclusive,
    })
}

// ScanAll creates an iterator over all keys in the ART.
// Equivalent to RangeScan(nil, nil, opts) - full scan.
func (a *ART) ScanAll() *ARTIterator {
    return a.RangeScan(nil, nil, DefaultRangeScanOptions())
}
```

## 4. Iterator Interface Methods

### 4.1 Next() Method

```go
// Next advances the iterator to the next key-value pair in range.
//
// Returns:
//   - key: The encoded key bytes (valid until next call to Next or Close)
//   - value: The associated value (typically a RowID as uint64)
//   - ok: false if no more entries exist or an error occurred
//
// The returned key slice is owned by the iterator and must not be modified.
// If the caller needs to retain the key, they should copy it.
//
// Thread safety: Not thread-safe. External synchronization required for
// concurrent access.
func (it *ARTIterator) Next() (key []byte, value any, ok bool) {
    if it.exhausted {
        return nil, nil, false
    }

    if !it.initialized {
        // Position at or after lower bound
        if !it.seekToLowerBound() {
            it.exhausted = true
            return nil, nil, false
        }
        it.initialized = true
    } else {
        // Advance to next leaf
        if !it.advanceToNextLeaf() {
            it.exhausted = true
            return nil, nil, false
        }
    }

    // Check if we've exceeded the upper bound
    if it.upperBound != nil && it.exceedsUpperBound() {
        it.exhausted = true
        return nil, nil, false
    }

    // Return current key and value
    it.lastKey = make([]byte, len(it.currentKey))
    copy(it.lastKey, it.currentKey)
    it.lastValue = it.getCurrentValue()

    return it.lastKey, it.lastValue, true
}
```

### 4.2 HasNext() Method

```go
// HasNext returns true if there are more entries to iterate.
//
// Note: HasNext() peeks ahead without consuming the entry. It is implemented
// by attempting to advance and caching the result, then returning that
// cached result on the next Next() call.
//
// For most use cases, simply checking the `ok` return value of Next() is
// preferred over calling HasNext() first.
func (it *ARTIterator) HasNext() bool {
    if it.exhausted {
        return false
    }

    // If we have a cached next entry, return true
    if it.lastKey != nil && it.initialized {
        return true
    }

    // Try to peek at the next entry
    key, value, ok := it.peekNext()
    if !ok {
        it.exhausted = true
        return false
    }

    // Cache the result for the next Next() call
    it.lastKey = key
    it.lastValue = value
    return true
}
```

### 4.3 Close() Method

```go
// Close releases resources held by the iterator.
//
// After Close() is called, the iterator must not be used. Subsequent calls
// to Next() or HasNext() will return false/nil.
//
// It is safe to call Close() multiple times.
//
// Note: The iterator does not hold locks or file handles, so Close() is
// primarily for memory cleanup and to mark the iterator as unusable.
func (it *ARTIterator) Close() {
    it.stack = nil
    it.currentKey = nil
    it.lowerBound = nil
    it.upperBound = nil
    it.lastKey = nil
    it.lastValue = nil
    it.exhausted = true
}
```

### 4.4 Additional Utility Methods

```go
// Key returns the current key without advancing the iterator.
// Returns nil if the iterator is not positioned on a valid entry.
func (it *ARTIterator) Key() []byte {
    if it.exhausted || !it.initialized {
        return nil
    }
    return it.lastKey
}

// Value returns the current value without advancing the iterator.
// Returns nil if the iterator is not positioned on a valid entry.
func (it *ARTIterator) Value() any {
    if it.exhausted || !it.initialized {
        return nil
    }
    return it.lastValue
}

// Seek repositions the iterator to the first key >= the given key.
// Returns true if a matching key was found, false otherwise.
func (it *ARTIterator) Seek(key []byte) bool {
    // Reset iterator state
    it.stack = it.stack[:0]
    it.currentKey = it.currentKey[:0]
    it.exhausted = false
    it.initialized = false
    it.lastKey = nil
    it.lastValue = nil

    // Update lower bound to seek target
    it.lowerBound = key
    it.lowerInclusive = true

    // Advance to position
    _, _, ok := it.Next()
    return ok
}
```

## 5. Inclusive vs Exclusive Bounds

### 5.1 Bound Handling Strategy

The key insight is that inclusive/exclusive handling must be applied during:
1. Initial positioning (lower bound)
2. Termination check (upper bound)

```go
// seekToLowerBound positions the iterator at or after the lower bound.
func (it *ARTIterator) seekToLowerBound() bool {
    if it.art.root == nil {
        return false
    }

    if it.lowerBound == nil {
        // No lower bound: start at minimum key
        return it.findMinimum(it.art.root)
    }

    // Find position >= lower bound
    found := it.lowerBoundSearch(it.art.root, 0)
    if !found {
        return false
    }

    // Handle exclusive lower bound
    if !it.lowerInclusive && bytes.Equal(it.currentKey, it.lowerBound) {
        // Skip the exact match
        return it.advanceToNextLeaf()
    }

    return true
}

// exceedsUpperBound checks if current key exceeds the upper bound.
func (it *ARTIterator) exceedsUpperBound() bool {
    if it.upperBound == nil {
        return false // No upper bound
    }

    cmp := bytes.Compare(it.currentKey, it.upperBound)
    if it.upperInclusive {
        return cmp > 0 // Stop when current > upper
    }
    return cmp >= 0 // Stop when current >= upper
}
```

### 5.2 Predicate to Bound Mapping

| SQL Predicate | Lower Bound | Lower Inclusive | Upper Bound | Upper Inclusive |
|---------------|-------------|-----------------|-------------|-----------------|
| `x = 5` | encode(5) | true | encode(5) | true |
| `x > 5` | encode(5) | false | nil | - |
| `x >= 5` | encode(5) | true | nil | - |
| `x < 5` | nil | - | encode(5) | false |
| `x <= 5` | nil | - | encode(5) | true |
| `x BETWEEN 5 AND 10` | encode(5) | true | encode(10) | true |
| `x > 5 AND x < 10` | encode(5) | false | encode(10) | false |
| `x >= 5 AND x <= 10` | encode(5) | true | encode(10) | true |

### 5.3 Combining Multiple Predicates

```go
// Example: WHERE x > 5 AND x <= 10 AND x <> 7
// Split into:
//   Range: (5, 10] via RangeScan
//   Filter: x <> 7 as residual predicate applied by executor

// RangeBounds extracts bounds from multiple predicates on the same column.
type RangeBounds struct {
    Lower          []byte
    Upper          []byte
    LowerInclusive bool
    UpperInclusive bool
}

// MergeRangeBounds combines multiple range predicates into optimal bounds.
// Returns nil if the ranges are contradictory (empty result set).
func MergeRangeBounds(predicates []RangePredicate) *RangeBounds {
    var lower, upper []byte
    lowerInc := true
    upperInc := true

    for _, pred := range predicates {
        switch pred.Op {
        case OpGreaterThan:
            if lower == nil || bytes.Compare(pred.Value, lower) > 0 ||
               (bytes.Equal(pred.Value, lower) && lowerInc) {
                lower = pred.Value
                lowerInc = false
            }
        case OpGreaterThanOrEqual:
            if lower == nil || bytes.Compare(pred.Value, lower) > 0 {
                lower = pred.Value
                lowerInc = true
            }
        case OpLessThan:
            if upper == nil || bytes.Compare(pred.Value, upper) < 0 ||
               (bytes.Equal(pred.Value, upper) && upperInc) {
                upper = pred.Value
                upperInc = false
            }
        case OpLessThanOrEqual:
            if upper == nil || bytes.Compare(pred.Value, upper) < 0 {
                upper = pred.Value
                upperInc = true
            }
        }
    }

    // Check for contradictory bounds
    if lower != nil && upper != nil {
        cmp := bytes.Compare(lower, upper)
        if cmp > 0 {
            return nil // Empty range
        }
        if cmp == 0 && (!lowerInc || !upperInc) {
            return nil // Empty range
        }
    }

    return &RangeBounds{
        Lower:          lower,
        Upper:          upper,
        LowerInclusive: lowerInc,
        UpperInclusive: upperInc,
    }
}
```

## 6. Composite Key Range Scans

### 6.1 Composite Key Encoding

Composite keys are encoded by concatenating individual encoded key components:

```go
// EncodeCompositeKey encodes multiple values into a single comparable key.
// The encoding preserves lexicographic ordering for the entire composite.
func EncodeCompositeKey(values []any, types []dukdb.Type) ([]byte, error) {
    var result []byte
    for i, v := range values {
        encoded, err := encodeValue(v, types[i])
        if err != nil {
            return nil, err
        }
        result = append(result, encoded...)
    }
    return result, nil
}

// Example:
// Index on (a INT, b VARCHAR)
// Query: WHERE a = 5 AND b >= 'foo'
// Lower bound: encode(5) + encode('foo')
// Upper bound: encode(5) + encode(maximum string value)
```

### 6.2 Prefix Range Scans

For composite indexes, often only a prefix of columns has predicates:

```go
// CompositeRangeScan handles partial key specifications.
//
// Example: Index on (a, b, c), Query: WHERE a = 5 AND b >= 10
//
// We need to scan all keys that start with encode(5) + encode(10...).
// This is achieved by:
//   Lower: encode(5) + encode(10)
//   Upper: encode(5) + encode(MAX_INT) or encode(6) - 1 byte
func (a *ART) CompositeRangeScan(
    prefix []byte,           // Fixed prefix (columns with = predicates)
    rangeLower []byte,       // Lower bound for first range column
    rangeUpper []byte,       // Upper bound for first range column
    opts RangeScanOptions,
) *ARTIterator {
    var lower, upper []byte

    if rangeLower != nil {
        lower = append(prefix, rangeLower...)
    } else {
        lower = prefix
    }

    if rangeUpper != nil {
        upper = append(prefix, rangeUpper...)
    } else {
        // Upper bound is first key > prefix
        // This requires computing prefix + 1 at the last byte
        upper = computePrefixUpperBound(prefix)
    }

    return a.RangeScan(lower, upper, opts)
}

// computePrefixUpperBound returns the first key lexicographically > prefix.
// This handles the case where we want all keys starting with prefix.
func computePrefixUpperBound(prefix []byte) []byte {
    if len(prefix) == 0 {
        return nil // No prefix means scan to end
    }

    // Find the rightmost byte that can be incremented
    upper := make([]byte, len(prefix))
    copy(upper, prefix)

    for i := len(upper) - 1; i >= 0; i-- {
        if upper[i] < 0xFF {
            upper[i]++
            return upper[:i+1]
        }
    }

    // All bytes are 0xFF, no upper bound possible (scan to end)
    return nil
}
```

### 6.3 Composite Key Range Examples

| Query | Index | Lower Bound | Upper Bound |
|-------|-------|-------------|-------------|
| `a=1 AND b>10` | `(a,b)` | `[1,10]` (excl) | `[1,MAX]` (incl) or `[2]` (excl) |
| `a=1 AND b>=10 AND b<20` | `(a,b)` | `[1,10]` (incl) | `[1,20]` (excl) |
| `a BETWEEN 1 AND 5` | `(a,b)` | `[1]` (incl) | `[6]` (excl) |
| `(a,b) > (1,10)` | `(a,b)` | `[1,10]` (excl) | nil |
| `a=1` | `(a,b,c)` | `[1]` (incl) | `[2]` (excl) |

## 7. Stack-Based Traversal Algorithm

### 7.1 Algorithm Overview

The traversal maintains a stack of (node, childIndex) pairs representing the path from root to current position. This enables efficient forward iteration without recursion.

```
Algorithm: In-Order ART Traversal

1. SEEK_TO_LOWER_BOUND:
   - Start at root
   - At each inner node, find smallest child >= lower bound
   - Push (node, childIndex) onto stack
   - Descend to child, repeat until leaf reached
   - At leaf: position established

2. ADVANCE_TO_NEXT:
   - Pop current leaf from consideration
   - At parent: try next child (childIndex++)
   - If no more children: pop parent, go to its parent's next child
   - When valid child found: descend to its minimum leaf
   - Repeat until leaf found or stack empty

3. CHECK_UPPER_BOUND:
   - Before returning, compare currentKey with upperBound
   - If exceeded: mark exhausted, return false
```

### 7.2 Detailed Pseudocode

```go
// lowerBoundSearch finds the first key >= lower bound.
func (it *ARTIterator) lowerBoundSearch(node *ARTNode, depth int) bool {
    for node != nil {
        // Add prefix bytes to current key
        it.currentKey = append(it.currentKey, node.prefix...)

        if node.nodeType == NodeTypeLeaf {
            // Found a leaf - check if it's >= lower bound
            cmp := bytes.Compare(it.currentKey, it.lowerBound)
            if cmp >= 0 {
                // Record this leaf's position
                return true
            }
            // This leaf is too small, need to advance
            return it.advanceFromLeaf()
        }

        // Inner node: find appropriate child
        childIdx, child := it.findChildForLowerBound(node, depth)
        if child == nil {
            // No suitable child, need to advance from this node
            return it.advanceFromNode()
        }

        // Push this node onto stack and descend
        it.stack = append(it.stack, iteratorStackEntry{
            node:           node,
            childIndex:     childIdx,
            prefixConsumed: len(node.prefix),
        })
        it.currentKey = append(it.currentKey, it.getChildKey(node, childIdx))
        node = child
        depth++
    }
    return false
}

// advanceToNextLeaf moves to the next leaf in order.
func (it *ARTIterator) advanceToNextLeaf() bool {
    for len(it.stack) > 0 {
        // Get top of stack
        top := &it.stack[len(it.stack)-1]

        // Try next child at this level
        nextIdx, nextChild := it.findNextChild(top.node, top.childIndex)
        if nextChild != nil {
            // Update stack entry
            it.currentKey = it.currentKey[:len(it.currentKey)-1] // Remove old child byte
            it.currentKey = append(it.currentKey, it.getChildKey(top.node, nextIdx))
            top.childIndex = nextIdx

            // Descend to minimum leaf in this subtree
            return it.findMinimum(nextChild)
        }

        // No more children at this level, pop and try parent
        it.stack = it.stack[:len(it.stack)-1]
        // Remove prefix bytes and child byte from current key
        it.currentKey = it.currentKey[:len(it.currentKey)-top.prefixConsumed-1]
    }

    // Stack empty, no more entries
    return false
}

// findMinimum descends to the leftmost leaf starting from node.
func (it *ARTIterator) findMinimum(node *ARTNode) bool {
    for node != nil {
        // Add prefix bytes
        it.currentKey = append(it.currentKey, node.prefix...)

        if node.nodeType == NodeTypeLeaf {
            return true
        }

        // Find first (smallest) child
        firstIdx, firstChild := it.findFirstChild(node)
        if firstChild == nil {
            // Empty inner node (shouldn't happen in valid ART)
            return false
        }

        // Push onto stack and descend
        it.stack = append(it.stack, iteratorStackEntry{
            node:           node,
            childIndex:     firstIdx,
            prefixConsumed: len(node.prefix),
        })
        it.currentKey = append(it.currentKey, it.getChildKey(node, firstIdx))
        node = firstChild
    }
    return false
}
```

### 7.3 Node-Specific Child Operations

```go
// findChildForLowerBound finds the smallest child >= lower bound key byte at depth.
func (it *ARTIterator) findChildForLowerBound(node *ARTNode, depth int) (int, *ARTNode) {
    targetByte := byte(0)
    if depth < len(it.lowerBound) {
        targetByte = it.lowerBound[depth]
    }

    switch node.nodeType {
    case NodeType4, NodeType16:
        // Linear search through sorted keys
        for i, key := range node.keys {
            if key >= targetByte && i < len(node.children) {
                return i, node.children[i]
            }
        }
        return -1, nil

    case NodeType48:
        // Search through key byte mapping
        for b := int(targetByte); b <= 255; b++ {
            idx := node.keys[b]
            if idx != 0xFF && int(idx) < len(node.children) {
                return b, node.children[idx]
            }
        }
        return -1, nil

    case NodeType256:
        // Direct array lookup
        for b := int(targetByte); b <= 255; b++ {
            if node.children[b] != nil {
                return b, node.children[b]
            }
        }
        return -1, nil
    }

    return -1, nil
}

// findNextChild finds the next child after childIdx.
func (it *ARTIterator) findNextChild(node *ARTNode, childIdx int) (int, *ARTNode) {
    switch node.nodeType {
    case NodeType4, NodeType16:
        // Next index in sorted array
        for i := childIdx + 1; i < len(node.children); i++ {
            if node.children[i] != nil {
                return i, node.children[i]
            }
        }
        return -1, nil

    case NodeType48:
        // Search from next byte value
        for b := childIdx + 1; b <= 255; b++ {
            idx := node.keys[b]
            if idx != 0xFF && int(idx) < len(node.children) {
                return b, node.children[idx]
            }
        }
        return -1, nil

    case NodeType256:
        // Direct array lookup from next index
        for b := childIdx + 1; b <= 255; b++ {
            if node.children[b] != nil {
                return b, node.children[b]
            }
        }
        return -1, nil
    }

    return -1, nil
}

// findFirstChild finds the smallest child in a node.
func (it *ARTIterator) findFirstChild(node *ARTNode) (int, *ARTNode) {
    switch node.nodeType {
    case NodeType4, NodeType16:
        if len(node.children) > 0 {
            return 0, node.children[0]
        }
        return -1, nil

    case NodeType48:
        for b := 0; b <= 255; b++ {
            idx := node.keys[b]
            if idx != 0xFF && int(idx) < len(node.children) {
                return b, node.children[idx]
            }
        }
        return -1, nil

    case NodeType256:
        for b := 0; b <= 255; b++ {
            if node.children[b] != nil {
                return b, node.children[b]
            }
        }
        return -1, nil
    }

    return -1, nil
}

// getChildKey returns the key byte used to reach child at given index.
func (it *ARTIterator) getChildKey(node *ARTNode, childIdx int) byte {
    switch node.nodeType {
    case NodeType4, NodeType16:
        return node.keys[childIdx]
    case NodeType48, NodeType256:
        return byte(childIdx) // childIdx IS the key byte
    }
    return 0
}
```

## 8. Edge Cases

### 8.1 Empty Range

When lower > upper or bounds exclude all values:

```go
// Query: WHERE x > 10 AND x < 5 (impossible)
// RangeScan(encode(10), encode(5), {lower: false, upper: false})

// Detection in RangeScan:
func (a *ART) RangeScan(lower, upper []byte, opts RangeScanOptions) *ARTIterator {
    // Check for empty range
    if lower != nil && upper != nil {
        cmp := bytes.Compare(lower, upper)
        if cmp > 0 {
            // lower > upper: empty range
            return &ARTIterator{exhausted: true}
        }
        if cmp == 0 {
            if !opts.LowerInclusive || !opts.UpperInclusive {
                // Bounds equal but not both inclusive: empty range
                return &ARTIterator{exhausted: true}
            }
        }
    }
    // ... normal iterator creation
}
```

### 8.2 Unbounded Ranges

```go
// Query: WHERE x > 10 (no upper bound)
// RangeScan(encode(10), nil, {lower: false})
//
// Handled naturally: upperBound = nil means exceedsUpperBound() always returns false

// Query: WHERE x < 100 (no lower bound)
// RangeScan(nil, encode(100), {upper: false})
//
// Handled naturally: seekToLowerBound() with lowerBound = nil finds minimum

// Full table scan through index (no bounds)
// RangeScan(nil, nil, {})
//
// Scans entire ART from minimum to maximum key
```

### 8.3 Single-Element Range

```go
// Query: WHERE x = 5 (point lookup via range scan)
// RangeScan(encode(5), encode(5), {lower: true, upper: true})
//
// This works correctly:
// 1. Seek finds first key >= 5
// 2. If found key != 5, iterator exhausts (exceeds upper bound)
// 3. If found key == 5, returns it, then exhausts on next iteration

// Note: For single-value lookups, Lookup() is more efficient than RangeScan()
```

### 8.4 Empty ART

```go
func (it *ARTIterator) seekToLowerBound() bool {
    if it.art.root == nil {
        return false // Empty tree, nothing to iterate
    }
    // ... normal seek
}
```

### 8.5 Keys at Type Boundaries

```go
// Example: Index on TINYINT, query WHERE x >= -128
// Encoded -128 is 0x00 (after sign flip)
// This should match all possible values

// Example: Index on TINYINT, query WHERE x <= 127
// Encoded 127 is 0xFF (after sign flip)
// Upper bound 0xFF inclusive should match maximum value

// The encoding already handles these correctly:
// - Signed types flip sign bit to preserve order
// - Minimum value encodes to 0x00...
// - Maximum value encodes to 0xFF...
```

### 8.6 Prefix Mismatch During Search

```go
// When descending, the target key may diverge from the actual prefix:
func (it *ARTIterator) lowerBoundSearch(node *ARTNode, depth int) bool {
    // At node with prefix, compare with target
    prefixLen := len(node.prefix)
    for i := 0; i < prefixLen; i++ {
        if depth+i >= len(it.lowerBound) {
            // Lower bound is shorter than prefix
            // All keys in this subtree are >= lower bound
            return it.findMinimum(node)
        }

        prefixByte := node.prefix[i]
        targetByte := it.lowerBound[depth+i]

        if prefixByte > targetByte {
            // Prefix is greater: all keys here are >= lower bound
            return it.findMinimum(node)
        }
        if prefixByte < targetByte {
            // Prefix is less: no keys here can match
            // Need to advance to next subtree
            return false // Will cause backtracking
        }
        // Equal: continue comparing
    }
    // Prefix matches, continue descent
    // ...
}
```

## 9. Performance Considerations

### 9.1 Memory Usage

| Component | Memory | Notes |
|-----------|--------|-------|
| Iterator struct | ~100 bytes | Fixed overhead |
| Stack (typical) | ~256 bytes | 8 entries * 32 bytes |
| Stack (worst case) | ~8 KB | 256 entries for long string keys |
| currentKey | ~64 bytes | Pre-allocated, grows as needed |
| Bounds | ~128 bytes | Copies of bound keys |
| **Total (typical)** | **~500 bytes** | For integer keys |
| **Total (worst case)** | **~16 KB** | For very long string keys |

### 9.2 Traversal Cost

| Operation | Cost | Notes |
|-----------|------|-------|
| Seek to lower bound | O(k) | k = key length |
| Advance to next | O(1) amortized | May backtrack up tree |
| Check upper bound | O(k) | Key comparison |
| Total for N results | O(k + N) | Initial seek + N advances |

### 9.3 Comparison vs Hash Index

| Operation | ART Range Scan | Hash Index |
|-----------|---------------|------------|
| Point lookup | O(k) | O(1) |
| Range scan N results | O(k + N) | O(table_size) * |
| Memory per iterator | ~500 bytes | N/A |

\* Hash index cannot do range scans efficiently; requires full scan.

### 9.4 Optimization Opportunities

1. **Pre-allocation**: Stack and currentKey are pre-allocated to typical sizes
2. **Key reuse**: currentKey is modified in-place, not reallocated
3. **Early termination**: Upper bound check avoids unnecessary traversal
4. **No copying**: Returned keys point into iterator's buffer (caller copies if needed)

### 9.5 Concurrent Access

The iterator is NOT thread-safe. For concurrent access:

```go
// Option 1: External locking
mu.RLock()
it := art.RangeScan(lower, upper, opts)
for key, value, ok := it.Next(); ok; key, value, ok = it.Next() {
    // Process while holding lock
}
it.Close()
mu.RUnlock()

// Option 2: Snapshot (future work)
// Take a snapshot of the ART and iterate the snapshot
snapshot := art.Snapshot()
it := snapshot.RangeScan(lower, upper, opts)
// Can release lock after snapshot
```

## 10. Implementation Checklist

### Phase 1: Core Iterator (Week 1)

- [ ] Define `ARTIterator` struct in `internal/storage/index/art_iterator.go`
- [ ] Define `iteratorStackEntry` struct
- [ ] Define `RangeScanOptions` struct
- [ ] Implement `ART.RangeScan()` constructor
- [ ] Implement `ARTIterator.Close()`
- [ ] Add unit tests for iterator construction

### Phase 2: Traversal Logic (Week 2)

- [ ] Implement `findMinimum()` - descend to leftmost leaf
- [ ] Implement `findFirstChild()` for each node type
- [ ] Implement `findNextChild()` for each node type
- [ ] Implement `getChildKey()` for each node type
- [ ] Implement `advanceToNextLeaf()` - stack-based advance
- [ ] Add unit tests for basic traversal

### Phase 3: Bound Handling (Week 3)

- [ ] Implement `seekToLowerBound()` - position at lower bound
- [ ] Implement `findChildForLowerBound()` for each node type
- [ ] Implement `exceedsUpperBound()` - upper bound check
- [ ] Implement `ARTIterator.Next()` - full iteration
- [ ] Implement `ARTIterator.HasNext()` - peek support
- [ ] Add unit tests for bound handling

### Phase 4: Edge Cases & Optimization (Week 4)

- [ ] Handle empty ART
- [ ] Handle empty range (lower > upper)
- [ ] Handle unbounded ranges (nil bounds)
- [ ] Handle single-element ranges
- [ ] Implement prefix comparison for lower bound search
- [ ] Add integration tests with executor
- [ ] Performance benchmarks

### Phase 5: Composite Keys (Week 4)

- [ ] Implement `CompositeRangeScan()`
- [ ] Implement `computePrefixUpperBound()`
- [ ] Add tests for composite key ranges
- [ ] Document composite key limitations

## 11. Integration with Executor

### 11.1 Executor Changes

The `PhysicalIndexScanOperator` needs to support range scans:

```go
// In internal/executor/index_scan.go

type PhysicalIndexScanOperator struct {
    // ... existing fields ...

    // Range scan fields (new)
    isRangeScan    bool
    lowerBound     []byte
    upperBound     []byte
    lowerInclusive bool
    upperInclusive bool
    artIterator    *index.ARTIterator
}

func (op *PhysicalIndexScanOperator) executeRangeScan() error {
    artIndex, ok := op.index.(*index.ART)
    if !ok {
        return fmt.Errorf("range scan requires ART index, got %T", op.index)
    }

    op.artIterator = artIndex.RangeScan(
        op.lowerBound,
        op.upperBound,
        index.RangeScanOptions{
            LowerInclusive: op.lowerInclusive,
            UpperInclusive: op.upperInclusive,
        },
    )
    return nil
}

func (op *PhysicalIndexScanOperator) nextRangeScanRow() (*DataChunk, error) {
    key, value, ok := op.artIterator.Next()
    if !ok {
        return nil, nil // End of range
    }

    rowID, ok := value.(uint64)
    if !ok {
        return nil, fmt.Errorf("expected uint64 row ID, got %T", value)
    }

    return op.fetchRowByID(rowID)
}
```

### 11.2 Access Hint Extension

```go
// In internal/optimizer/index_matcher.go

type AccessHint struct {
    // ... existing fields ...

    // Range scan fields (new)
    IsRangeScan    bool   // True for range predicates, false for equality
    LowerBound     any    // Lower bound value (nil for unbounded)
    UpperBound     any    // Upper bound value (nil for unbounded)
    LowerInclusive bool   // Include lower bound
    UpperInclusive bool   // Include upper bound
}
```

## 12. References

- DuckDB ART Iterator: `references/duckdb/src/execution/index/art/iterator.cpp`
- DuckDB ART Iterator Header: `references/duckdb/src/include/duckdb/execution/index/art/iterator.hpp`
- Current ART Implementation: `internal/storage/index/art.go`
- PhysicalIndexScan Design: `spectr/changes/fix-index-usage/PHYSICAL_INDEX_SCAN_DESIGN.md`
- Hints Passing Design: `spectr/changes/fix-index-usage/HINTS_PASSING_DESIGN.md`
