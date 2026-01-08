// Package index provides index structures for the native Go DuckDB implementation.
//
// This file defines the ARTIterator for range scans over ART indexes.
// The iterator provides ordered traversal of keys within a specified range,
// enabling support for <, >, <=, >=, and BETWEEN predicates.
package index

// ARTIterator provides ordered traversal of keys within a range in an Adaptive Radix Tree.
//
// The iterator uses a stack-based approach for depth-first traversal of the ART,
// allowing efficient forward iteration without recursion. Keys are returned in
// sorted order (lexicographic order of the encoded keys).
//
// Range bounds can be specified to limit iteration to a subset of keys:
//   - Lower bound: Start iteration at the first key >= lower bound
//   - Upper bound: Stop iteration when a key exceeds the upper bound
//   - Bounds can be inclusive or exclusive
//   - Either bound can be nil for unbounded ranges
//
// The iterator remains valid until the ART is modified. Modifications during
// iteration result in undefined behavior.
//
// Thread safety: The iterator is NOT thread-safe. External synchronization is
// required for concurrent access.
//
// Example usage:
//
//	// WHERE x >= 10 AND x < 100
//	it := art.RangeScan(encodeKey(10), encodeKey(100), DefaultRangeScanOptions())
//	for {
//	    key, value, ok := it.Next()
//	    if !ok {
//	        break
//	    }
//	    // Process key, value
//	}
//	it.Close()
type ARTIterator struct {
	// art is a reference to the ART being iterated.
	// This pointer is used to access the tree structure during traversal.
	art *ART

	// stack holds the traversal state for depth-first traversal.
	// Each entry represents a node and the current child index being explored.
	// The stack grows as we descend into the tree and shrinks as we backtrack.
	// Maximum depth is O(key_length) - typically 8 entries for integer keys,
	// up to 256 entries for very long string keys.
	stack []iteratorStackEntry

	// currentKey accumulates the key bytes as we traverse down the tree.
	// This represents the full key to the current position, built by appending
	// prefix bytes and child key bytes during descent.
	currentKey []byte

	// lowerBound is the encoded lower bound key.
	// If nil, iteration starts from the minimum key in the tree (-infinity).
	// The bound is compared lexicographically with encoded keys.
	lowerBound []byte

	// upperBound is the encoded upper bound key.
	// If nil, iteration continues to the maximum key in the tree (+infinity).
	// The bound is compared lexicographically with encoded keys.
	upperBound []byte

	// lowerInclusive indicates whether the lower bound is inclusive.
	// If true, keys equal to lowerBound are included (>= semantics).
	// If false, keys equal to lowerBound are excluded (> semantics).
	lowerInclusive bool

	// upperInclusive indicates whether the upper bound is inclusive.
	// If true, keys equal to upperBound are included (<= semantics).
	// If false, keys equal to upperBound are excluded (< semantics).
	upperInclusive bool

	// exhausted indicates the iterator has reached the end of the range.
	// Once set to true, all subsequent calls to Next() will return false.
	// This flag is set when:
	//   - The current key exceeds the upper bound
	//   - There are no more keys in the tree to iterate
	//   - Close() has been called
	exhausted bool

	// initialized indicates whether the iterator has been positioned.
	// The iterator is lazy - it does not seek to the first position until
	// the first call to Next(). This allows creating iterators cheaply
	// without immediately performing tree traversal.
	initialized bool

	// lastKey holds the last returned key for the current position.
	// This is a copy of currentKey at the time Next() was called.
	// The slice is owned by the iterator and valid until the next Next() call.
	lastKey []byte

	// lastValue holds the last returned value from the current leaf node.
	// Typically this is a uint64 row ID for index lookups.
	lastValue any
}

// iteratorStackEntry tracks the state at each level of traversal.
//
// During depth-first traversal, we push an entry onto the stack when descending
// into a node, and pop it when backtracking. Each entry records:
//   - Which node we are at
//   - Which child we are currently exploring (or will explore next)
//   - The key length at the time this entry was created
//
// The childIndex interpretation varies by node type:
//   - Node4/Node16: Index into the sorted keys/children arrays (0-3 or 0-15)
//   - Node48: The key byte (0-255), with actual child index looked up via keys array
//   - Node256: The key byte (0-255), which directly indexes the children array
type iteratorStackEntry struct {
	// node is the current node at this level of the tree.
	// This is an inner node (Node4, Node16, Node48, or Node256) - leaf nodes
	// are not pushed onto the stack since they have no children to explore.
	node *ARTNode

	// childIndex is the current child position being explored.
	// The interpretation depends on node type:
	//   - For Node4/Node16: Direct index into keys/children arrays
	//   - For Node48: Key byte value (0-255), maps via keys array to child index
	//   - For Node256: Key byte value (0-255), directly indexes children array
	// After visiting a subtree, we increment this to move to the next child.
	childIndex int

	// keyLenAtEntry stores the length of currentKey BEFORE we descended
	// into this node's child. This allows us to restore currentKey correctly
	// when backtracking - we simply truncate to this length and add the
	// new child's key byte.
	keyLenAtEntry int
}

// RangeScanOptions configures the behavior of a range scan operation.
//
// These options control how range bounds are interpreted and can be used
// to implement various SQL predicates:
//
//	WHERE x > 5        : LowerInclusive=false, UpperInclusive=N/A (no upper)
//	WHERE x >= 5       : LowerInclusive=true,  UpperInclusive=N/A (no upper)
//	WHERE x < 10       : LowerInclusive=N/A,   UpperInclusive=false
//	WHERE x <= 10      : LowerInclusive=N/A,   UpperInclusive=true
//	WHERE x BETWEEN 5 AND 10: LowerInclusive=true, UpperInclusive=true
type RangeScanOptions struct {
	// LowerInclusive determines if lower bound is inclusive (>=) or exclusive (>).
	// When true, keys equal to the lower bound are included in the result.
	// When false, keys equal to the lower bound are skipped.
	// Default: true (inclusive), matching SQL BETWEEN semantics.
	LowerInclusive bool

	// UpperInclusive determines if upper bound is inclusive (<=) or exclusive (<).
	// When true, keys equal to the upper bound are included in the result.
	// When false, keys equal to the upper bound are excluded.
	// Default: false (exclusive), which is common for range iteration.
	UpperInclusive bool

	// Reverse indicates whether to scan in descending order (from upper to lower).
	// When false (default), keys are returned in ascending order.
	// When true, keys are returned in descending order.
	// Note: Reverse scanning requires different traversal logic and is not
	// yet implemented in the initial version.
	Reverse bool

	// MaxResults limits the maximum number of results to return.
	// When 0 (default), there is no limit - all matching keys are returned.
	// When > 0, iteration stops after returning this many key-value pairs.
	// This is useful for implementing LIMIT clauses efficiently.
	MaxResults int
}

// DefaultRangeScanOptions returns sensible defaults for range scans.
//
// The defaults are:
//   - LowerInclusive: true (matches SQL BETWEEN semantics)
//   - UpperInclusive: false (common for [lower, upper) ranges)
//   - Reverse: false (ascending order)
//   - MaxResults: 0 (unlimited)
//
// These defaults mean RangeScan(lower, upper, DefaultRangeScanOptions())
// will return all keys k where lower <= k < upper.
func DefaultRangeScanOptions() RangeScanOptions {
	return RangeScanOptions{
		LowerInclusive: true,
		UpperInclusive: false,
		Reverse:        false,
		MaxResults:     0,
	}
}

// RangeBounds represents the computed bounds for a range scan.
//
// This struct is used by the optimizer to extract and merge range predicates
// from SQL WHERE clauses. Multiple predicates on the same column can be
// combined into a single RangeBounds for efficient index scanning.
//
// Examples:
//   - WHERE x > 5:              Lower=[5], LowerInclusive=false, Upper=nil
//   - WHERE x BETWEEN 5 AND 10: Lower=[5], LowerInclusive=true, Upper=[10], UpperInclusive=true
//   - WHERE x > 5 AND x <= 10:  Lower=[5], LowerInclusive=false, Upper=[10], UpperInclusive=true
type RangeBounds struct {
	// Lower is the encoded lower bound key.
	// Nil means no lower bound (start from minimum key).
	Lower []byte

	// Upper is the encoded upper bound key.
	// Nil means no upper bound (scan to maximum key).
	Upper []byte

	// LowerInclusive indicates whether Lower is inclusive.
	LowerInclusive bool

	// UpperInclusive indicates whether Upper is inclusive.
	UpperInclusive bool
}

// RangePredicateOp represents the operation type for a range predicate.
type RangePredicateOp int

const (
	// OpGreaterThan represents the > operator.
	OpGreaterThan RangePredicateOp = iota
	// OpGreaterThanOrEqual represents the >= operator.
	OpGreaterThanOrEqual
	// OpLessThan represents the < operator.
	OpLessThan
	// OpLessThanOrEqual represents the <= operator.
	OpLessThanOrEqual
)

// RangePredicate represents a single range predicate extracted from a WHERE clause.
//
// Multiple RangePredicates on the same column can be merged into a RangeBounds
// using MergeRangeBounds() to determine the tightest valid range.
type RangePredicate struct {
	// Op is the comparison operator.
	Op RangePredicateOp

	// Value is the encoded bound value to compare against.
	Value []byte
}

// Pre-allocation sizes for iterator internal buffers.
// These are tuned for typical use cases to minimize allocations.
const (
	// defaultStackCapacity is the initial capacity for the traversal stack.
	// 16 entries handles keys up to ~16 bytes without reallocation.
	// Most integer keys are 8 bytes, so this provides headroom.
	defaultStackCapacity = 16

	// defaultKeyCapacity is the initial capacity for the currentKey buffer.
	// 64 bytes handles most keys without reallocation.
	defaultKeyCapacity = 64

	// maxKeyByte is the maximum value for a single byte (used in iteration).
	maxKeyByte = 255
)

// RangeScan creates an iterator for keys in the specified range.
//
// Parameters:
//   - lower: The lower bound key (nil for unbounded, i.e., -infinity)
//   - upper: The upper bound key (nil for unbounded, i.e., +infinity)
//   - opts: Options controlling inclusivity, direction, and limits
//
// Returns:
//   - *ARTIterator: An iterator positioned before the first matching key.
//     Call Next() to advance to the first result.
//
// The iterator remains valid until the ART is modified. Modifications during
// iteration result in undefined behavior.
//
// Edge cases:
//   - Empty ART: Returns an exhausted iterator (Next() returns false immediately)
//   - Invalid range (lower > upper): Returns an exhausted iterator
//   - Bounds equal but not both inclusive: Returns an exhausted iterator
//
// Example usage:
//
//	// WHERE x >= 10 AND x < 100
//	it := art.RangeScan(encodeKey(10), encodeKey(100), DefaultRangeScanOptions())
//	for {
//	    key, value, ok := it.Next()
//	    if !ok {
//	        break
//	    }
//	    // Process key, value
//	}
//	it.Close()
//
//	// WHERE x > 5 (unbounded upper)
//	it := art.RangeScan(encodeKey(5), nil, RangeScanOptions{LowerInclusive: false})
//
//	// Full index scan
//	it := art.RangeScan(nil, nil, DefaultRangeScanOptions())
func (a *ART) RangeScan(lower, upper []byte, opts RangeScanOptions) *ARTIterator {
	// Handle empty ART - return exhausted iterator
	if a.root == nil {
		return &ARTIterator{
			art:       a,
			exhausted: true,
		}
	}

	// Check for invalid range (lower > upper)
	if lower != nil && upper != nil {
		cmp := compareKeys(lower, upper)
		if cmp > 0 {
			// lower > upper: empty range
			return &ARTIterator{
				art:       a,
				exhausted: true,
			}
		}
		if cmp == 0 {
			// Bounds are equal - only valid if both are inclusive
			if !opts.LowerInclusive || !opts.UpperInclusive {
				// Bounds equal but not both inclusive: empty range
				return &ARTIterator{
					art:       a,
					exhausted: true,
				}
			}
		}
	}

	// Create the iterator with pre-allocated buffers
	return &ARTIterator{
		art:            a,
		stack:          make([]iteratorStackEntry, 0, defaultStackCapacity),
		currentKey:     make([]byte, 0, defaultKeyCapacity),
		lowerBound:     lower,
		upperBound:     upper,
		lowerInclusive: opts.LowerInclusive,
		upperInclusive: opts.UpperInclusive,
		exhausted:      false,
		initialized:    false,
	}
}

// ScanFrom creates an iterator starting at the given lower bound with no upper bound.
//
// This is a convenience method equivalent to:
//
//	art.RangeScan(lower, nil, RangeScanOptions{LowerInclusive: inclusive})
//
// Parameters:
//   - lower: The lower bound key to start scanning from
//   - inclusive: If true, include keys equal to lower (>=); if false, exclude (>)
//
// Example usage:
//
//	// WHERE x >= 10 (scan from 10 to end)
//	it := art.ScanFrom(encodeKey(10), true)
//
//	// WHERE x > 10 (scan after 10 to end)
//	it := art.ScanFrom(encodeKey(10), false)
func (a *ART) ScanFrom(lower []byte, inclusive bool) *ARTIterator {
	return a.RangeScan(lower, nil, RangeScanOptions{
		LowerInclusive: inclusive,
		UpperInclusive: false, // Not used since upper is nil
	})
}

// ScanTo creates an iterator up to the given upper bound with no lower bound.
//
// This is a convenience method equivalent to:
//
//	art.RangeScan(nil, upper, RangeScanOptions{LowerInclusive: true, UpperInclusive: inclusive})
//
// Parameters:
//   - upper: The upper bound key to stop scanning at
//   - inclusive: If true, include keys equal to upper (<=); if false, exclude (<)
//
// Example usage:
//
//	// WHERE x <= 100 (scan from start to 100, including 100)
//	it := art.ScanTo(encodeKey(100), true)
//
//	// WHERE x < 100 (scan from start to 100, excluding 100)
//	it := art.ScanTo(encodeKey(100), false)
func (a *ART) ScanTo(upper []byte, inclusive bool) *ARTIterator {
	return a.RangeScan(nil, upper, RangeScanOptions{
		LowerInclusive: true, // Not used since lower is nil
		UpperInclusive: inclusive,
	})
}

// ScanAll creates an iterator over all keys in the ART in sorted order.
//
// This is a convenience method equivalent to:
//
//	art.RangeScan(nil, nil, DefaultRangeScanOptions())
//
// The iterator will traverse every key in the index from minimum to maximum
// in lexicographic order of the encoded keys.
//
// Example usage:
//
//	// Iterate over entire index
//	it := art.ScanAll()
//	for {
//	    key, value, ok := it.Next()
//	    if !ok {
//	        break
//	    }
//	    fmt.Printf("key=%v, value=%v\n", key, value)
//	}
//	it.Close()
func (a *ART) ScanAll() *ARTIterator {
	return a.RangeScan(nil, nil, DefaultRangeScanOptions())
}

// Close releases resources held by the iterator.
//
// After Close() is called, the iterator must not be used. Subsequent calls
// to Next() will return false.
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

// IsExhausted returns true if the iterator has no more entries to return.
//
// This can happen when:
//   - The iterator has traversed all keys in the range
//   - The iterator was created with an invalid range
//   - The ART is empty
//   - Close() was called
func (it *ARTIterator) IsExhausted() bool {
	return it.exhausted
}

// compareKeys compares two byte slices lexicographically.
// Returns:
//
//	-1 if a < b
//	 0 if a == b
//	+1 if a > b
func compareKeys(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}

	// Common prefix is equal, shorter key is smaller
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// emptyMarker is used to indicate empty slots in Node48 index array.
const emptyMarker = 0xFF

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
	} else if !it.advanceToNextLeaf() {
		// Advance to next leaf
		it.exhausted = true
		return nil, nil, false
	}

	// Check if we've exceeded the upper bound
	if it.exceedsUpperBound() {
		it.exhausted = true
		return nil, nil, false
	}

	// Return current key and value
	it.lastKey = make([]byte, len(it.currentKey))
	copy(it.lastKey, it.currentKey)
	it.lastValue = it.getCurrentValue()

	return it.lastKey, it.lastValue, true
}

// seekToLowerBound positions the iterator at or after the lower bound.
// Returns true if a valid position was found, false otherwise.
func (it *ARTIterator) seekToLowerBound() bool {
	if it.art.root == nil {
		return false
	}

	// Reset state
	it.stack = it.stack[:0]
	it.currentKey = it.currentKey[:0]

	if it.lowerBound == nil {
		// No lower bound: start at minimum key
		return it.findMinimum(it.art.root)
	}

	// Find position >= lower bound using lower bound search
	found := it.lowerBoundSearch(it.art.root, 0)
	if !found {
		return false
	}

	// Handle exclusive lower bound
	if !it.lowerInclusive && compareKeys(it.currentKey, it.lowerBound) == 0 {
		// Skip the exact match - advance to next leaf
		return it.advanceToNextLeaf()
	}

	return true
}

// lowerBoundSearch finds the first key >= lower bound starting from node.
// depth indicates the current depth in terms of key bytes consumed.
// Returns true if a valid leaf was found, false otherwise.
//
//nolint:gocognit // Complex traversal logic requires multiple conditions
func (it *ARTIterator) lowerBoundSearch(node *ARTNode, depth int) bool {
	for node != nil {
		// Add prefix bytes to current key
		prefixLen := len(node.prefix)
		it.currentKey = append(it.currentKey, node.prefix...)

		if node.nodeType == NodeTypeLeaf {
			// Found a leaf - check if it's >= lower bound
			cmp := compareKeys(it.currentKey, it.lowerBound)
			if cmp >= 0 {
				// This leaf is >= lower bound
				return true
			}
			// This leaf is too small, need to backtrack and find next
			// Trim the prefix we just added
			it.currentKey = it.currentKey[:len(it.currentKey)-prefixLen]
			return false
		}

		// Check prefix against lower bound
		prefixCmp := it.comparePrefixWithBound(node.prefix, depth)
		if prefixCmp > 0 {
			// Prefix is greater than bound - all keys here are valid
			// Just find the minimum in this subtree
			// But first, we've already added the prefix, so remove it and call findMinimum
			// Actually, findMinimum will add the prefix again, so let's be careful
			it.currentKey = it.currentKey[:len(it.currentKey)-prefixLen]
			return it.findMinimum(node)
		}
		if prefixCmp < 0 {
			// Prefix is less than bound - no keys here can match
			// Need to backtrack
			it.currentKey = it.currentKey[:len(it.currentKey)-prefixLen]
			return false
		}
		// Prefix matches - need to continue descent

		// Find appropriate child based on lower bound
		childIdx, child := it.findChildForLowerBound(node, depth+prefixLen)
		if child == nil {
			// No suitable child found - need to backtrack
			it.currentKey = it.currentKey[:len(it.currentKey)-prefixLen]
			return false
		}

		// Push this node onto stack - record key length AFTER adding prefix, BEFORE adding child byte
		keyLenAfterPrefix := len(it.currentKey)
		it.currentKey = append(it.currentKey, it.getChildKey(node, childIdx))

		it.stack = append(it.stack, iteratorStackEntry{
			node:          node,
			childIndex:    childIdx,
			keyLenAtEntry: keyLenAfterPrefix,
		})
		node = child
		depth += prefixLen + 1
	}
	return false
}

// comparePrefixWithBound compares a node prefix with the lower bound at the given depth.
// Returns:
//
//	-1 if prefix < bound (at this depth)
//	 0 if prefix matches bound (at this depth)
//	+1 if prefix > bound (at this depth)
func (it *ARTIterator) comparePrefixWithBound(prefix []byte, depth int) int {
	for i, b := range prefix {
		boundIdx := depth + i
		if boundIdx >= len(it.lowerBound) {
			// Lower bound is shorter - prefix is greater
			return 1
		}
		if b < it.lowerBound[boundIdx] {
			return -1
		}
		if b > it.lowerBound[boundIdx] {
			return 1
		}
	}
	return 0
}

// findChildForLowerBound finds the smallest child >= lower bound key byte at depth.
// Returns (childIdx, child) where childIdx is interpreted based on node type.
// Note: NodeTypeLeaf is intentionally not handled - leaves have no children.
//
//nolint:gocognit,exhaustive // Complex traversal logic; leaf case not applicable
func (it *ARTIterator) findChildForLowerBound(node *ARTNode, depth int) (int, *ARTNode) {
	targetByte := byte(0)
	if depth < len(it.lowerBound) {
		targetByte = it.lowerBound[depth]
	}

	switch node.nodeType {
	case NodeType4, NodeType16:
		// Linear/binary search through sorted keys
		for i, key := range node.keys {
			if i >= len(node.children) {
				break
			}
			if key >= targetByte {
				if node.children[i] != nil {
					return i, node.children[i]
				}
			}
		}
		return -1, nil

	case NodeType48:
		// Search through key byte mapping starting from targetByte
		for b := int(targetByte); b <= maxKeyByte; b++ {
			idx := node.keys[b]
			if idx != emptyMarker && int(idx) < len(node.children) && node.children[idx] != nil {
				return b, node.children[idx]
			}
		}
		return -1, nil

	case NodeType256:
		// Direct array lookup starting from targetByte
		for b := int(targetByte); b <= maxKeyByte; b++ {
			if node.children[b] != nil {
				return b, node.children[b]
			}
		}
		return -1, nil
	}

	return -1, nil
}

// advanceToNextLeaf moves to the next leaf in order.
// Returns true if a valid leaf was found, false if exhausted.
func (it *ARTIterator) advanceToNextLeaf() bool {
	for len(it.stack) > 0 {
		// Get top of stack
		top := &it.stack[len(it.stack)-1]

		// Try next child at this level
		nextIdx, nextChild := it.findNextChild(top.node, top.childIndex)
		if nextChild != nil {
			// Found a next child - restore key to entry point and add new child byte
			it.currentKey = it.currentKey[:top.keyLenAtEntry]
			it.currentKey = append(it.currentKey, it.getChildKey(top.node, nextIdx))
			top.childIndex = nextIdx

			// Descend to minimum leaf in this subtree
			return it.findMinimum(nextChild)
		}

		// No more children at this level, pop and try parent
		it.stack = it.stack[:len(it.stack)-1]
		// Restore currentKey to the length it was at entry (before child byte was added)
		// This effectively removes the child byte and everything that was added below it
		it.currentKey = it.currentKey[:top.keyLenAtEntry]
	}

	// Stack empty, no more entries
	return false
}

// findNextChild finds the next child after childIdx in sorted order.
// Returns (nextIdx, nextChild) or (-1, nil) if no more children.
// Note: NodeTypeLeaf is intentionally not handled - leaves have no children.
//
//nolint:exhaustive // leaf case not applicable
func (*ARTIterator) findNextChild(node *ARTNode, childIdx int) (int, *ARTNode) {
	switch node.nodeType {
	case NodeType4, NodeType16:
		// Next index in array
		for i := childIdx + 1; i < len(node.children); i++ {
			if node.children[i] != nil {
				return i, node.children[i]
			}
		}
		return -1, nil

	case NodeType48:
		// Search from next byte value
		// childIdx is the key byte, not the array index
		for b := childIdx + 1; b <= maxKeyByte; b++ {
			idx := node.keys[b]
			if idx != emptyMarker && int(idx) < len(node.children) && node.children[idx] != nil {
				return b, node.children[idx]
			}
		}
		return -1, nil

	case NodeType256:
		// Direct array lookup from next index
		for b := childIdx + 1; b <= maxKeyByte; b++ {
			if node.children[b] != nil {
				return b, node.children[b]
			}
		}
		return -1, nil
	}

	return -1, nil
}

// findMinimum descends to the leftmost (minimum) leaf starting from node.
// Pushes all intermediate nodes onto the stack and updates currentKey.
// Returns true if a leaf was found, false otherwise.
func (it *ARTIterator) findMinimum(node *ARTNode) bool {
	for node != nil {
		// Add prefix bytes
		prefixLen := len(node.prefix)
		it.currentKey = append(it.currentKey, node.prefix...)

		if node.nodeType == NodeTypeLeaf {
			// Found a leaf - we're done
			return true
		}

		// Find first (smallest) child
		firstIdx, firstChild := it.findFirstChild(node)
		if firstChild == nil {
			// Empty inner node (shouldn't happen in valid ART)
			// Remove prefix and return false
			it.currentKey = it.currentKey[:len(it.currentKey)-prefixLen]
			return false
		}

		// Record key length AFTER adding prefix but BEFORE adding child byte
		keyLenAfterPrefix := len(it.currentKey)

		// Add child byte
		it.currentKey = append(it.currentKey, it.getChildKey(node, firstIdx))

		// Push onto stack
		it.stack = append(it.stack, iteratorStackEntry{
			node:          node,
			childIndex:    firstIdx,
			keyLenAtEntry: keyLenAfterPrefix,
		})
		node = firstChild
	}
	return false
}

// findFirstChild finds the smallest child in a node.
// Returns (childIdx, child) or (-1, nil) if no children.
// Note: NodeTypeLeaf is intentionally not handled - leaves have no children.
//
//nolint:exhaustive // leaf case not applicable
func (*ARTIterator) findFirstChild(node *ARTNode) (int, *ARTNode) {
	switch node.nodeType {
	case NodeType4, NodeType16:
		// First entry in sorted array
		for i := 0; i < len(node.children); i++ {
			if node.children[i] != nil {
				return i, node.children[i]
			}
		}
		return -1, nil

	case NodeType48:
		// Search from byte 0
		for b := 0; b <= maxKeyByte; b++ {
			idx := node.keys[b]
			if idx != emptyMarker && int(idx) < len(node.children) && node.children[idx] != nil {
				return b, node.children[idx]
			}
		}
		return -1, nil

	case NodeType256:
		// Direct array lookup from 0
		for b := 0; b <= maxKeyByte; b++ {
			if node.children[b] != nil {
				return b, node.children[b]
			}
		}
		return -1, nil
	}

	return -1, nil
}

// getChildKey returns the key byte used to reach child at given index.
// The interpretation of childIdx depends on node type:
//   - Node4/Node16: index into keys array -> keys[childIdx]
//   - Node48/Node256: childIdx IS the key byte
// Note: NodeTypeLeaf is intentionally not handled - leaves have no children.
//
//nolint:exhaustive // leaf case not applicable
func (*ARTIterator) getChildKey(node *ARTNode, childIdx int) byte {
	switch node.nodeType {
	case NodeType4, NodeType16:
		if childIdx < len(node.keys) {
			return node.keys[childIdx]
		}
		return 0
	case NodeType48, NodeType256:
		return byte(childIdx) // childIdx IS the key byte
	}
	return 0
}

// exceedsUpperBound checks if current key exceeds the upper bound.
// Returns true if we should stop iteration.
func (it *ARTIterator) exceedsUpperBound() bool {
	if it.upperBound == nil {
		return false // No upper bound
	}

	cmp := compareKeys(it.currentKey, it.upperBound)
	if it.upperInclusive {
		return cmp > 0 // Stop when current > upper
	}
	return cmp >= 0 // Stop when current >= upper
}

// getCurrentValue returns the value from the current leaf node.
// This walks the tree from root following currentKey to find the leaf.
// Returns nil if no leaf is found at the current position.
func (it *ARTIterator) getCurrentValue() any {
	if it.art.root == nil {
		return nil
	}

	// If the root is a leaf and matches our key, return its value
	if it.art.root.nodeType == NodeTypeLeaf {
		if compareKeys(it.currentKey, it.art.root.prefix) == 0 {
			return it.art.root.value
		}
		return nil
	}

	// Walk down from root following the key bytes
	node := it.art.root
	depth := 0

	for node != nil {
		// Check and consume prefix
		prefixLen := len(node.prefix)
		if depth+prefixLen > len(it.currentKey) {
			return nil
		}
		// Verify prefix matches
		for i := 0; i < prefixLen; i++ {
			if it.currentKey[depth+i] != node.prefix[i] {
				return nil
			}
		}
		depth += prefixLen

		if node.nodeType == NodeTypeLeaf {
			// Found the leaf
			return node.value
		}

		// Need to find child
		if depth >= len(it.currentKey) {
			return nil
		}
		keyByte := it.currentKey[depth]
		child := it.getChild(node, keyByte)
		if child == nil {
			return nil
		}
		node = child
		depth++
	}

	return nil
}

// getChild finds the child node for a given key byte.
// Note: NodeTypeLeaf is intentionally not handled - leaves have no children.
//
//nolint:exhaustive // leaf case not applicable
func (*ARTIterator) getChild(node *ARTNode, keyByte byte) *ARTNode {
	switch node.nodeType {
	case NodeType4, NodeType16:
		for i, k := range node.keys {
			if i >= len(node.children) {
				break
			}
			if k == keyByte && node.children[i] != nil {
				return node.children[i]
			}
		}
		return nil

	case NodeType48:
		idx := node.keys[keyByte]
		if idx != emptyMarker && int(idx) < len(node.children) {
			return node.children[idx]
		}
		return nil

	case NodeType256:
		return node.children[keyByte]
	}

	return nil
}

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
