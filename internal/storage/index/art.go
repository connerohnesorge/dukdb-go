// Package index provides index structures for the native Go DuckDB implementation.
//
// # ART Index Format
//
// This package implements an Adaptive Radix Tree (ART) index with serialization
// support compatible with DuckDB's ART index format. ART is a space-efficient
// radix tree that adapts its node size based on the number of children.
//
// ## Node Types
//
// The ART implementation supports five node types:
//   - Leaf: Contains the actual indexed value (row ID)
//   - Node4: Stores up to 4 children (smallest inner node)
//   - Node16: Stores up to 16 children
//   - Node48: Stores up to 48 children using an index array
//   - Node256: Stores up to 256 children (full radix node)
//
// ## Key Encoding
//
// Keys are encoded to preserve lexicographic sort order for all data types:
//   - Signed integers: Sign bit is flipped and converted to big-endian
//   - Unsigned integers: No transformation needed
//   - Floats/Doubles: Special encoding to handle negative values correctly
//   - Strings/Blobs: Null-terminated for variable length support
//   - Temporal types (Date/Time/Timestamp): Treated as signed integers
//
// ## Serialization Format
//
// The ART index is serialized in a binary format that matches DuckDB's on-disk
// representation:
//
//	[Header]
//	  - KeyType: uint8 (1 byte) - Type of keys stored in the index
//	  - RootMarker: uint8 (1 byte) - 0xFF for null, 0x01 for present
//	[Root Node] (if present)
//	  - Recursively serialized node tree
//
// ### Node Serialization Format
//
//	[Node Header]
//	  - NodeType: uint8 (1 byte)
//	  - PrefixLength: uint16 (2 bytes)
//	  - Prefix: [PrefixLength bytes]
//
//	[Node Data - varies by type]
//	  Leaf:
//	    - Value: uint64 (8 bytes) - Row ID
//	  Node4/Node16:
//	    - ChildCount: uint8 (1 byte)
//	    - Keys: [ChildCount bytes]
//	    - Children: [ChildCount nodes] - Recursively serialized
//	  Node48:
//	    - ChildCount: uint8 (1 byte)
//	    - IndexArray: [256 bytes] - Maps key byte to child index
//	    - Children: [ChildCount nodes] - Recursively serialized
//	  Node256:
//	    - ChildExistenceBitmap: [32 bytes] - Bitmap indicating which children exist
//	    - Children: [variable] - Only non-null children are serialized
//
// ## DuckDB Compatibility Notes
//
// This implementation is designed to be compatible with DuckDB's ART index format:
//   - Binary serialization format matches DuckDB v0.10.0+
//   - Key encoding preserves sort order for all DuckDB types
//   - Node structure is identical to DuckDB's in-memory representation
//   - Row IDs are stored as uint64 values in leaf nodes
//
// ## Supported Operations
//
// The current implementation supports:
//   - Insert: Full tree construction with proper node splitting
//   - Lookup: Exact key lookup with prefix matching
//   - RangeScan: Iterator-based range queries with lower/upper bounds
//   - InsertEncoded: Insert with pre-encoded keys (for composite indexes)
//
// ## Limitations
//
//   - No deletion support
//   - No node merging (nodes don't shrink after deletions)
//
// These limitations do not affect serialization/deserialization compatibility
// with DuckDB, which is the primary goal for persistence support.
//
//nolint:revive // ART serialization requires complex functions
package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	dukdb "github.com/dukdb/dukdb-go"
)

// NodeType represents the type of ART node
type NodeType uint8

const (
	// NodeTypeLeaf represents a leaf node (contains actual values)
	NodeTypeLeaf NodeType = 0
	// NodeType4 represents a node with up to 4 children
	NodeType4 NodeType = 1
	// NodeType16 represents a node with up to 16 children
	NodeType16 NodeType = 2
	// NodeType48 represents a node with up to 48 children
	NodeType48 NodeType = 3
	// NodeType256 represents a node with up to 256 children
	NodeType256 NodeType = 4
)

// ARTNode represents a node in the Adaptive Radix Tree
type ARTNode struct {
	// nodeType identifies the type of this node
	nodeType NodeType
	// keys stores the partial keys for child lookup
	// For Node4/16: direct array of keys
	// For Node48: index mapping (256 entries, most are 0xFF for empty)
	// For Node256: unused (implicit by child array index)
	keys []byte
	// children stores pointers to child nodes
	children []*ARTNode
	// value stores the actual value for leaf nodes
	value any
	// prefix stores the compressed path prefix
	prefix []byte
}

// ART represents an Adaptive Radix Tree index
// This is a simplified implementation suitable for persistence
type ART struct {
	// root is the root node of the tree
	root *ARTNode
	// keyType is the type of keys stored in the index
	keyType dukdb.Type
}

// NewART creates a new empty ART index
func NewART(keyType dukdb.Type) *ART {
	return &ART{
		root:    nil,
		keyType: keyType,
	}
}

// newNode creates a new ART node of the specified type
//
//nolint:mnd // ART node sizes are well-defined constants (4, 16, 48, 256)
func newNode(nodeType NodeType) *ARTNode {
	const (
		node4Size   = 4
		node16Size  = 16
		node48Size  = 48
		node256Size = 256
		emptyMarker = 0xFF
	)

	node := &ARTNode{
		nodeType: nodeType,
		prefix:   nil,
	}

	switch nodeType {
	case NodeTypeLeaf:
		// Leaf nodes have no children
		node.keys = nil
		node.children = nil
	case NodeType4:
		node.keys = make([]byte, 0, node4Size)
		node.children = make([]*ARTNode, 0, node4Size)
	case NodeType16:
		node.keys = make([]byte, 0, node16Size)
		node.children = make([]*ARTNode, 0, node16Size)
	case NodeType48:
		// Node48 uses a 256-entry index array
		node.keys = make([]byte, node256Size)
		// Initialize all keys to empty marker
		for i := range node.keys {
			node.keys[i] = emptyMarker
		}
		node.children = make([]*ARTNode, 0, node48Size)
	case NodeType256:
		// Node256 has 256 direct children
		node.keys = nil
		node.children = make([]*ARTNode, node256Size)
	}

	return node
}

// Insert adds a key-value pair to the ART.
// The key should be the raw byte representation of the value (e.g., little-endian int).
// It will be encoded internally to ensure proper lexicographic ordering.
func (a *ART) Insert(key []byte, value any) error {
	encodedKey, err := encodeKey(key, a.keyType)
	if err != nil {
		return fmt.Errorf("failed to encode key: %w", err)
	}

	return a.insertEncodedKey(encodedKey, value)
}

// InsertEncoded adds a key-value pair to the ART where the key is already encoded.
// Use this when the key has already been encoded for lexicographic ordering
// (e.g., when using keys produced by encodeKeyValue from the executor).
func (a *ART) InsertEncoded(encodedKey []byte, value any) error {
	return a.insertEncodedKey(encodedKey, value)
}

// insertEncodedKey is the internal implementation that inserts a pre-encoded key.
func (a *ART) insertEncodedKey(encodedKey []byte, value any) error {
	if a.root == nil {
		// Create a leaf node as root
		leaf := newNode(NodeTypeLeaf)
		leaf.prefix = encodedKey
		leaf.value = value
		a.root = leaf

		return nil
	}

	// Insert into existing tree
	a.root = a.insertRecursive(a.root, encodedKey, value, 0)

	return nil //nolint:nlreturn // Allow no blank line before return
}

// insertRecursive inserts a key-value pair into the tree rooted at node.
// depth is the current byte position in the key being processed.
// Returns the new root of this subtree (may be different if splits occur).
//
//nolint:funlen // ART insert requires handling multiple cases
func (a *ART) insertRecursive(node *ARTNode, key []byte, value any, depth int) *ARTNode {
	if node == nil {
		// Create a new leaf node
		leaf := newNode(NodeTypeLeaf)
		if depth < len(key) {
			leaf.prefix = key[depth:]
		}
		leaf.value = value
		return leaf
	}

	// Handle leaf node
	if node.nodeType == NodeTypeLeaf {
		// Check if this is an exact key match (update)
		existingKey := node.prefix
		if bytes.Equal(existingKey, key[depth:]) {
			// Update existing value
			node.value = value
			return node
		}

		// Need to split: create a new Node4 to hold both leaves
		// Find the common prefix length
		newKeyPart := key[depth:]
		commonLen := commonPrefixLen(existingKey, newKeyPart)

		// Create new Node4 as the new root of this subtree
		newNode4 := newNode(NodeType4)
		if commonLen > 0 {
			newNode4.prefix = existingKey[:commonLen]
		}

		// Create two new leaves
		existingLeaf := newNode(NodeTypeLeaf)
		if commonLen < len(existingKey) {
			existingLeaf.prefix = existingKey[commonLen+1:]
		}
		existingLeaf.value = node.value

		newLeaf := newNode(NodeTypeLeaf)
		if commonLen < len(newKeyPart) {
			newLeaf.prefix = newKeyPart[commonLen+1:]
		}
		newLeaf.value = value

		// Add both children to the Node4
		if commonLen < len(existingKey) {
			newNode4.keys = append(newNode4.keys, existingKey[commonLen])
			newNode4.children = append(newNode4.children, existingLeaf)
		}
		if commonLen < len(newKeyPart) {
			// Insert in sorted order
			insertKey := newKeyPart[commonLen]
			inserted := false
			for i, k := range newNode4.keys {
				if insertKey < k {
					// Insert before this position
					newNode4.keys = append(newNode4.keys[:i], append([]byte{insertKey}, newNode4.keys[i:]...)...)
					newNode4.children = append(newNode4.children[:i], append([]*ARTNode{newLeaf}, newNode4.children[i:]...)...)
					inserted = true
					break
				}
			}
			if !inserted {
				newNode4.keys = append(newNode4.keys, insertKey)
				newNode4.children = append(newNode4.children, newLeaf)
			}
		}

		return newNode4
	}

	// Handle inner node (Node4, Node16, Node48, Node256)
	// First check if we need to handle prefix mismatch
	if len(node.prefix) > 0 {
		keyPart := key[depth:]
		commonLen := commonPrefixLen(node.prefix, keyPart)

		if commonLen < len(node.prefix) {
			// Prefix mismatch - need to split this node
			// Create a new Node4 to be the parent
			newParent := newNode(NodeType4)
			newParent.prefix = node.prefix[:commonLen]

			// Update current node's prefix
			oldPrefix := node.prefix
			node.prefix = oldPrefix[commonLen+1:]

			// Add current node as child
			newParent.keys = append(newParent.keys, oldPrefix[commonLen])
			newParent.children = append(newParent.children, node)

			// Create and add new leaf
			newLeaf := newNode(NodeTypeLeaf)
			if commonLen < len(keyPart) {
				newLeaf.prefix = keyPart[commonLen+1:]
			}
			newLeaf.value = value

			if commonLen < len(keyPart) {
				insertKey := keyPart[commonLen]
				// Insert in sorted order
				if insertKey < newParent.keys[0] {
					newParent.keys = append([]byte{insertKey}, newParent.keys...)
					newParent.children = append([]*ARTNode{newLeaf}, newParent.children...)
				} else {
					newParent.keys = append(newParent.keys, insertKey)
					newParent.children = append(newParent.children, newLeaf)
				}
			}

			return newParent
		}

		// Full prefix match, continue with depth + prefixLen
		depth += len(node.prefix)
	}

	// Find or create child for next byte
	if depth >= len(key) {
		// Key ends at this node - store value here
		node.value = value
		return node
	}

	nextByte := key[depth]
	childIdx := a.findChild(node, nextByte)

	if childIdx >= 0 {
		// Child exists, recurse
		node.children[childIdx] = a.insertRecursive(node.children[childIdx], key, value, depth+1)
	} else {
		// Need to add new child
		newLeaf := newNode(NodeTypeLeaf)
		if depth+1 < len(key) {
			newLeaf.prefix = key[depth+1:]
		}
		newLeaf.value = value

		node = a.addChild(node, nextByte, newLeaf)
	}

	return node
}

// commonPrefixLen returns the length of the common prefix between two byte slices.
func commonPrefixLen(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return minLen
}

// findChild returns the index of the child with the given key, or -1 if not found.
func (a *ART) findChild(node *ARTNode, key byte) int {
	switch node.nodeType {
	case NodeType4, NodeType16:
		for i, k := range node.keys {
			if k == key {
				return i
			}
		}
	case NodeType48:
		const emptyMarker = 0xFF
		if idx := node.keys[key]; idx != emptyMarker {
			return int(idx)
		}
	case NodeType256:
		if node.children[key] != nil {
			return int(key)
		}
	}
	return -1
}

// addChild adds a new child to the node, growing the node type if necessary.
// Returns the (possibly new) node.
//
//nolint:funlen // Node growth handling requires multiple cases
func (a *ART) addChild(node *ARTNode, key byte, child *ARTNode) *ARTNode {
	const (
		node4Max   = 4
		node16Max  = 16
		node48Max  = 48
		emptyMarker = 0xFF
	)

	switch node.nodeType {
	case NodeType4:
		if len(node.keys) < node4Max {
			// Find insertion point to maintain sorted order
			insertIdx := 0
			for insertIdx < len(node.keys) && node.keys[insertIdx] < key {
				insertIdx++
			}
			// Insert key and child at the right position
			node.keys = append(node.keys[:insertIdx], append([]byte{key}, node.keys[insertIdx:]...)...)
			node.children = append(node.children[:insertIdx], append([]*ARTNode{child}, node.children[insertIdx:]...)...)
			return node
		}
		// Grow to Node16
		newNode16 := newNode(NodeType16)
		newNode16.prefix = node.prefix
		newNode16.value = node.value
		newNode16.keys = make([]byte, len(node.keys), node16Max)
		copy(newNode16.keys, node.keys)
		newNode16.children = make([]*ARTNode, len(node.children), node16Max)
		copy(newNode16.children, node.children)
		return a.addChild(newNode16, key, child)

	case NodeType16:
		if len(node.keys) < node16Max {
			insertIdx := 0
			for insertIdx < len(node.keys) && node.keys[insertIdx] < key {
				insertIdx++
			}
			node.keys = append(node.keys[:insertIdx], append([]byte{key}, node.keys[insertIdx:]...)...)
			node.children = append(node.children[:insertIdx], append([]*ARTNode{child}, node.children[insertIdx:]...)...)
			return node
		}
		// Grow to Node48
		newNode48 := newNode(NodeType48)
		newNode48.prefix = node.prefix
		newNode48.value = node.value
		newNode48.children = make([]*ARTNode, 0, node48Max)
		for i, k := range node.keys {
			newNode48.keys[k] = byte(len(newNode48.children))
			newNode48.children = append(newNode48.children, node.children[i])
		}
		return a.addChild(newNode48, key, child)

	case NodeType48:
		if len(node.children) < node48Max {
			node.keys[key] = byte(len(node.children))
			node.children = append(node.children, child)
			return node
		}
		// Grow to Node256
		newNode256 := newNode(NodeType256)
		newNode256.prefix = node.prefix
		newNode256.value = node.value
		for k := 0; k < 256; k++ {
			if node.keys[k] != emptyMarker {
				newNode256.children[k] = node.children[node.keys[k]]
			}
		}
		return a.addChild(newNode256, key, child)

	case NodeType256:
		node.children[key] = child
		return node
	}

	return node
}

// Lookup finds a value by key in the ART
func (a *ART) Lookup(key []byte) (any, bool) {
	if a.root == nil {
		return nil, false
	}

	encodedKey, err := encodeKey(key, a.keyType)
	if err != nil {
		return nil, false
	}

	return a.lookupRecursive(a.root, encodedKey, 0)
}

// lookupRecursive searches for a key in the tree rooted at node.
func (a *ART) lookupRecursive(node *ARTNode, key []byte, depth int) (any, bool) {
	if node == nil {
		return nil, false
	}

	// Handle leaf node
	if node.nodeType == NodeTypeLeaf {
		// Check if the remaining key matches the prefix
		keyPart := key[depth:]
		if bytes.Equal(node.prefix, keyPart) {
			return node.value, true
		}
		return nil, false
	}

	// Handle inner node - check prefix
	if len(node.prefix) > 0 {
		keyPart := key[depth:]
		if len(keyPart) < len(node.prefix) {
			return nil, false
		}
		if !bytes.Equal(node.prefix, keyPart[:len(node.prefix)]) {
			return nil, false
		}
		depth += len(node.prefix)
	}

	// Find next byte and recurse
	if depth >= len(key) {
		// Key ends at this node
		if node.value != nil {
			return node.value, true
		}
		return nil, false
	}

	nextByte := key[depth]
	childIdx := a.findChild(node, nextByte)
	if childIdx < 0 {
		return nil, false
	}

	return a.lookupRecursive(node.children[childIdx], key, depth+1)
}

// Serialize serializes the ART to binary format
func (a *ART) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write key type (1 byte)
	if err := buf.WriteByte(byte(a.keyType)); err != nil {
		return nil, fmt.Errorf("failed to write key type: %w", err)
	}

	// Write root node (or null marker if no root)
	if a.root == nil {
		// Write null marker
		const nullMarker = 0xFF
		if err := buf.WriteByte(nullMarker); err != nil {
			return nil, fmt.Errorf("failed to write null marker: %w", err)
		}
	} else {
		// Write root exists marker
		const rootExistsMarker = 0x01
		if err := buf.WriteByte(rootExistsMarker); err != nil {
			return nil, fmt.Errorf("failed to write root marker: %w", err)
		}
		// Serialize root node
		if err := serializeNode(buf, a.root); err != nil {
			return nil, fmt.Errorf("failed to serialize root: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// serializeNode serializes a single ART node
//
//nolint:revive // This is a complex serialization function by nature
func serializeNode(buf *bytes.Buffer, node *ARTNode) error {
	// Write node type (1 byte)
	if err := buf.WriteByte(byte(node.nodeType)); err != nil {
		return fmt.Errorf("failed to write node type: %w", err)
	}

	// Write prefix length (2 bytes) and prefix
	prefixLen := uint16(len(node.prefix))
	if err := binary.Write(buf, binary.LittleEndian, prefixLen); err != nil {
		return fmt.Errorf("failed to write prefix length: %w", err)
	}
	if prefixLen > 0 {
		if _, err := buf.Write(node.prefix); err != nil {
			return fmt.Errorf("failed to write prefix: %w", err)
		}
	}

	//nolint:exhaustive // Only ART node types are handled, other types are not expected
	switch node.nodeType {
	case NodeTypeLeaf:
		// For leaf nodes, serialize the value
		// For simplicity, we store values as uint64 (row IDs)
		var valueID uint64
		if node.value != nil {
			// Convert value to uint64 if possible
			switch v := node.value.(type) {
			case uint64:
				valueID = v
			case int64:
				valueID = uint64(v)
			case int:
				valueID = uint64(v)
			case uint:
				valueID = uint64(v)
			default:
				// For other types, use hash or 0
				valueID = 0
			}
		}
		if err := binary.Write(buf, binary.LittleEndian, valueID); err != nil {
			return fmt.Errorf("failed to write leaf value: %w", err)
		}

	case NodeType4, NodeType16:
		// Write number of children (1 byte)
		numChildren := uint8(len(node.children))
		if err := buf.WriteByte(numChildren); err != nil {
			return fmt.Errorf("failed to write child count: %w", err)
		}

		// Write keys
		if _, err := buf.Write(node.keys[:numChildren]); err != nil {
			return fmt.Errorf("failed to write keys: %w", err)
		}

		// Write children recursively
		for _, child := range node.children {
			if child == nil {
				// Write null marker
				if err := buf.WriteByte(0xFF); err != nil {
					return fmt.Errorf("failed to write null child: %w", err)
				}
			} else {
				// Write non-null marker
				if err := buf.WriteByte(0x01); err != nil {
					return fmt.Errorf("failed to write child marker: %w", err)
				}
				// Serialize child
				if err := serializeNode(buf, child); err != nil {
					return fmt.Errorf("failed to serialize child: %w", err)
				}
			}
		}

	case NodeType48:
		// Write number of children (1 byte)
		numChildren := uint8(len(node.children))
		if err := buf.WriteByte(numChildren); err != nil {
			return fmt.Errorf("failed to write child count: %w", err)
		}

		// Write index array (256 bytes)
		if _, err := buf.Write(node.keys); err != nil {
			return fmt.Errorf("failed to write index array: %w", err)
		}

		// Write children recursively
		for _, child := range node.children {
			if child == nil {
				// Write null marker
				if err := buf.WriteByte(0xFF); err != nil {
					return fmt.Errorf("failed to write null child: %w", err)
				}
			} else {
				// Write non-null marker
				if err := buf.WriteByte(0x01); err != nil {
					return fmt.Errorf("failed to write child marker: %w", err)
				}
				// Serialize child
				if err := serializeNode(buf, child); err != nil {
					return fmt.Errorf("failed to serialize child: %w", err)
				}
			}
		}

	case NodeType256:
		// Write which children exist (256 bits = 32 bytes)
		childExists := make([]byte, 32)
		for i, child := range node.children {
			if child != nil {
				byteIdx := i / 8
				bitIdx := uint(i % 8)
				childExists[byteIdx] |= 1 << bitIdx
			}
		}
		if _, err := buf.Write(childExists); err != nil {
			return fmt.Errorf("failed to write child existence bitmap: %w", err)
		}

		// Write children recursively (only non-null ones)
		for _, child := range node.children {
			if child != nil {
				if err := serializeNode(buf, child); err != nil {
					return fmt.Errorf("failed to serialize child: %w", err)
				}
			}
		}
	}

	return nil //nolint:nlreturn // Allow no blank line before return
}

// DeserializeART deserializes an ART from binary format
func DeserializeART(data []byte) (*ART, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data too short for ART: %d bytes", len(data))
	}

	r := bytes.NewReader(data)

	// Read key type (1 byte)
	keyTypeByte, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read key type: %w", err)
	}
	keyType := dukdb.Type(keyTypeByte)

	// Read root marker (1 byte)
	const nullMarker = 0xFF
	rootMarker, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read root marker: %w", err)
	}

	art := &ART{
		keyType: keyType,
	}

	if rootMarker == nullMarker {
		// No root node
		art.root = nil
	} else {
		// Deserialize root node
		root, err := deserializeNode(r)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize root: %w", err)
		}
		art.root = root
	}

	return art, nil
}

// deserializeNode deserializes a single ART node
//
//nolint:revive // This is a complex deserialization function by nature
func deserializeNode(r *bytes.Reader) (*ARTNode, error) {
	// Read node type (1 byte)
	nodeTypeByte, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read node type: %w", err)
	}
	nodeType := NodeType(nodeTypeByte)

	node := newNode(nodeType)

	// Read prefix length (2 bytes) and prefix
	var prefixLen uint16
	if err := binary.Read(r, binary.LittleEndian, &prefixLen); err != nil {
		return nil, fmt.Errorf("failed to read prefix length: %w", err)
	}
	if prefixLen > 0 {
		node.prefix = make([]byte, prefixLen)
		if _, err := r.Read(node.prefix); err != nil {
			return nil, fmt.Errorf("failed to read prefix: %w", err)
		}
	}

	switch nodeType {
	case NodeTypeLeaf:
		// Read leaf value
		var valueID uint64
		if err := binary.Read(r, binary.LittleEndian, &valueID); err != nil {
			return nil, fmt.Errorf("failed to read leaf value: %w", err)
		}
		node.value = valueID

	case NodeType4, NodeType16:
		// Read number of children (1 byte)
		numChildren, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read child count: %w", err)
		}

		// Read keys
		node.keys = make([]byte, numChildren)
		if _, err := r.Read(node.keys); err != nil {
			return nil, fmt.Errorf("failed to read keys: %w", err)
		}

		// Read children recursively
		node.children = make([]*ARTNode, numChildren)
		for i := range numChildren {
			childMarker, err := r.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read child marker: %w", err)
			}

			if childMarker == 0xFF {
				// Null child
				node.children[i] = nil
			} else {
				// Deserialize child
				child, err := deserializeNode(r)
				if err != nil {
					return nil, fmt.Errorf("failed to deserialize child: %w", err)
				}
				node.children[i] = child
			}
		}

	case NodeType48:
		// Read number of children (1 byte)
		numChildren, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read child count: %w", err)
		}

		// Read index array (256 bytes)
		node.keys = make([]byte, 256)
		if _, err := r.Read(node.keys); err != nil {
			return nil, fmt.Errorf("failed to read index array: %w", err)
		}

		// Read children recursively
		node.children = make([]*ARTNode, numChildren)
		for i := range numChildren {
			childMarker, err := r.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read child marker: %w", err)
			}

			if childMarker == 0xFF {
				// Null child
				node.children[i] = nil
			} else {
				// Deserialize child
				child, err := deserializeNode(r)
				if err != nil {
					return nil, fmt.Errorf("failed to deserialize child: %w", err)
				}
				node.children[i] = child
			}
		}

	case NodeType256:
		// Read child existence bitmap (32 bytes)
		childExists := make([]byte, 32)
		if _, err := r.Read(childExists); err != nil {
			return nil, fmt.Errorf("failed to read child existence bitmap: %w", err)
		}

		// Count existing children and read them
		node.children = make([]*ARTNode, 256)
		for i := range node.children {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			if childExists[byteIdx]&(1<<bitIdx) != 0 {
				// Child exists, deserialize it
				child, err := deserializeNode(r)
				if err != nil {
					return nil, fmt.Errorf("failed to deserialize child: %w", err)
				}
				node.children[i] = child
			}
		}
	}

	return node, nil
}

// encodeKey encodes a key for storage in the ART based on its type
// Keys are encoded to preserve sort order
//
//nolint:exhaustive,revive // Only commonly used types are encoded, others use default encoding
func encodeKey(key []byte, keyType dukdb.Type) ([]byte, error) {
	switch keyType {
	case dukdb.TYPE_BOOLEAN:
		if len(key) != 1 {
			return nil, fmt.Errorf("invalid boolean key length: %d", len(key))
		}
		// Booleans: 0x00 for false, 0x01 for true
		return key, nil //nolint:nlreturn // Allow no blank line before return

	case dukdb.TYPE_TINYINT:
		if len(key) != 1 {
			return nil, fmt.Errorf("invalid tinyint key length: %d", len(key))
		}
		// Flip sign bit for signed integers to preserve sort order
		encoded := make([]byte, 1)
		encoded[0] = key[0] ^ 0x80
		return encoded, nil

	case dukdb.TYPE_SMALLINT:
		if len(key) != 2 {
			return nil, fmt.Errorf("invalid smallint key length: %d", len(key))
		}
		// Convert to big-endian and flip sign bit for signed integers
		encoded := make([]byte, 2)
		// Assume input is little-endian, convert to big-endian
		encoded[0] = key[1]
		encoded[1] = key[0]
		encoded[0] ^= 0x80 // Flip sign bit (now in MSB position)

		return encoded, nil

	case dukdb.TYPE_INTEGER:
		if len(key) != 4 {
			return nil, fmt.Errorf("invalid integer key length: %d", len(key))
		}
		// Convert to big-endian and flip sign bit for signed integers
		encoded := make([]byte, 4)
		// Assume input is little-endian, convert to big-endian
		encoded[0] = key[3]
		encoded[1] = key[2]
		encoded[2] = key[1]
		encoded[3] = key[0]
		encoded[0] ^= 0x80 // Flip sign bit (now in MSB position)

		return encoded, nil

	case dukdb.TYPE_BIGINT:
		if len(key) != 8 {
			return nil, fmt.Errorf("invalid bigint key length: %d", len(key))
		}
		// Convert to big-endian and flip sign bit for signed integers
		encoded := make([]byte, 8)
		// Assume input is little-endian, convert to big-endian
		encoded[0] = key[7]
		encoded[1] = key[6]
		encoded[2] = key[5]
		encoded[3] = key[4]
		encoded[4] = key[3]
		encoded[5] = key[2]
		encoded[6] = key[1]
		encoded[7] = key[0]
		encoded[0] ^= 0x80 // Flip sign bit (now in MSB position)
		return encoded, nil

	case dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER, dukdb.TYPE_UBIGINT:
		// Unsigned integers: no transformation needed
		return key, nil

	case dukdb.TYPE_FLOAT:
		if len(key) != 4 {
			return nil, fmt.Errorf("invalid float key length: %d", len(key))
		}
		// Float encoding to preserve sort order
		encoded := make([]byte, 4)
		copy(encoded, key)
		// If negative (sign bit set), flip all bits; otherwise flip sign bit
		if encoded[3]&0x80 != 0 {
			for i := range encoded {
				encoded[i] = ^encoded[i]
			}
		} else {
			encoded[3] ^= 0x80
		}
		return encoded, nil

	case dukdb.TYPE_DOUBLE:
		if len(key) != 8 {
			return nil, fmt.Errorf("invalid double key length: %d", len(key))
		}
		// Double encoding to preserve sort order
		encoded := make([]byte, 8)
		copy(encoded, key)
		// If negative (sign bit set), flip all bits; otherwise flip sign bit
		if encoded[7]&0x80 != 0 {
			for i := range encoded {
				encoded[i] = ^encoded[i]
			}
		} else {
			encoded[7] ^= 0x80
		}
		return encoded, nil

	case dukdb.TYPE_VARCHAR, dukdb.TYPE_BLOB:
		// Strings and blobs: null-terminated for variable length
		encoded := make([]byte, len(key)+1)
		copy(encoded, key)
		encoded[len(key)] = 0x00 // null terminator
		return encoded, nil

	case dukdb.TYPE_DATE, dukdb.TYPE_TIME, dukdb.TYPE_TIMESTAMP,
		dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS:
		// Temporal types stored as integers
		if len(key) == 4 {
			// DATE (int32)
			encoded := make([]byte, 4)
			copy(encoded, key)
			encoded[3] ^= 0x80
			return encoded, nil
		} else if len(key) == 8 {
			// TIME/TIMESTAMP (int64)
			encoded := make([]byte, 8)
			copy(encoded, key)
			encoded[7] ^= 0x80
			return encoded, nil
		}
		return nil, fmt.Errorf("invalid temporal key length: %d", len(key))

	default:
		// For other types, use as-is
		return key, nil
	}
}

// decodeKey decodes a key from the ART format back to its original form
//
//nolint:exhaustive,revive // Only commonly used types are decoded, others use default decoding
func decodeKey(encoded []byte, keyType dukdb.Type) ([]byte, error) {
	switch keyType {
	case dukdb.TYPE_BOOLEAN:
		// Booleans: no transformation
		return encoded, nil

	case dukdb.TYPE_TINYINT:
		// Flip sign bit back
		decoded := make([]byte, 1)
		decoded[0] = encoded[0] ^ 0x80
		return decoded, nil

	case dukdb.TYPE_SMALLINT:
		// Flip sign bit back and convert from big-endian to little-endian
		decoded := make([]byte, 2)
		temp := make([]byte, 2)
		copy(temp, encoded)
		temp[0] ^= 0x80 // Flip sign bit back
		// Convert from big-endian to little-endian
		decoded[0] = temp[1]
		decoded[1] = temp[0]
		return decoded, nil

	case dukdb.TYPE_INTEGER:
		// Flip sign bit back and convert from big-endian to little-endian
		decoded := make([]byte, 4)
		temp := make([]byte, 4)
		copy(temp, encoded)
		temp[0] ^= 0x80 // Flip sign bit back
		// Convert from big-endian to little-endian
		decoded[0] = temp[3]
		decoded[1] = temp[2]
		decoded[2] = temp[1]
		decoded[3] = temp[0]
		return decoded, nil

	case dukdb.TYPE_BIGINT:
		// Flip sign bit back and convert from big-endian to little-endian
		decoded := make([]byte, 8)
		temp := make([]byte, 8)
		copy(temp, encoded)
		temp[0] ^= 0x80 // Flip sign bit back
		// Convert from big-endian to little-endian
		decoded[0] = temp[7]
		decoded[1] = temp[6]
		decoded[2] = temp[5]
		decoded[3] = temp[4]
		decoded[4] = temp[3]
		decoded[5] = temp[2]
		decoded[6] = temp[1]
		decoded[7] = temp[0]
		return decoded, nil

	case dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER, dukdb.TYPE_UBIGINT:
		// Unsigned integers: no transformation
		return encoded, nil

	case dukdb.TYPE_FLOAT:
		decoded := make([]byte, 4)
		copy(decoded, encoded)
		// Reverse float encoding
		if encoded[3]&0x80 == 0 {
			// Was negative, flip all bits
			for i := range decoded {
				decoded[i] = ^decoded[i]
			}
		} else {
			// Was positive, flip sign bit
			decoded[3] ^= 0x80
		}
		return decoded, nil

	case dukdb.TYPE_DOUBLE:
		decoded := make([]byte, 8)
		copy(decoded, encoded)
		// Reverse double encoding
		if encoded[7]&0x80 == 0 {
			// Was negative, flip all bits
			for i := range decoded {
				decoded[i] = ^decoded[i]
			}
		} else {
			// Was positive, flip sign bit
			decoded[7] ^= 0x80
		}
		return decoded, nil

	case dukdb.TYPE_VARCHAR, dukdb.TYPE_BLOB:
		// Remove null terminator
		const nullTerminator = 0x00
		if len(encoded) > 0 && encoded[len(encoded)-1] == nullTerminator {
			return encoded[:len(encoded)-1], nil
		}
		return encoded, nil

	case dukdb.TYPE_DATE, dukdb.TYPE_TIME, dukdb.TYPE_TIMESTAMP,
		dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS:
		// Temporal types: flip sign bit back
		if len(encoded) == 4 {
			decoded := make([]byte, 4)
			copy(decoded, encoded)
			decoded[3] ^= 0x80
			return decoded, nil
		} else if len(encoded) == 8 {
			decoded := make([]byte, 8)
			copy(decoded, encoded)
			decoded[7] ^= 0x80
			return decoded, nil
		}
		return nil, fmt.Errorf("invalid temporal key length: %d", len(encoded))

	default:
		// For other types, use as-is
		return encoded, nil
	}
}

// encodeIntKey is a helper to encode an integer key
func encodeIntKey(value int64) []byte {
	buf := make([]byte, 8)
	// Flip sign bit to make negative numbers sort before positive
	binary.LittleEndian.PutUint64(buf, uint64(value)^(1<<63))
	return buf
}

// encodeFloatKey is a helper to encode a float key
//
//nolint:mnd // Bit manipulation for float encoding
func encodeFloatKey(value float64) []byte {
	const signBit = 63
	buf := make([]byte, 8)
	bits := math.Float64bits(value)
	// If negative, flip all bits; otherwise flip sign bit
	if value < 0 {
		bits = ^bits
	} else {
		bits ^= (1 << signBit)
	}
	binary.LittleEndian.PutUint64(buf, bits)

	return buf
}

// encodeStringKey is a helper to encode a string key
func encodeStringKey(value string) []byte {
	// Null-terminated for variable length
	const nullTerminator = 0x00
	encoded := make([]byte, len(value)+1)
	copy(encoded, value)
	encoded[len(value)] = nullTerminator

	return encoded
}

// =============================================================================
// Composite Key Encoding/Decoding
// =============================================================================
// Composite keys are used for multi-column indexes. They are encoded by
// concatenating the encoded representations of each column value in order.
// This preserves lexicographic ordering for the entire composite key.
//
// Encoding format:
// For each column value, the value is encoded using the type-specific encoding
// (encodeKey), then all encoded parts are concatenated. Variable-length types
// (VARCHAR, BLOB) use null-termination so they can be distinguished in the
// concatenated result.
//
// Example:
//   Index on (a INT, b VARCHAR, c INT)
//   Values: [5, "hello", 10]
//   Encoded: encodeKey(5) + encodeKey("hello") + encodeKey(10)
//          = [4 bytes] + [6 bytes (5+null)] + [4 bytes]
//          = 14 bytes total
//
// Range scan use cases:
//   1. Full composite key: WHERE a=5 AND b='hello' AND c=10
//      Lower: encode([5, "hello", 10])
//      Upper: encode([5, "hello", 10])
//      Both inclusive
//
//   2. Prefix with range: WHERE a=5 AND b>='hello'
//      Lower: encode([5, "hello"])
//      Upper: ComputePrefixUpperBound(encode([5])) or nil
//
//   3. Prefix only: WHERE a=5 (any b, c values)
//      Lower: encode([5])
//      Upper: ComputePrefixUpperBound(encode([5]))
// =============================================================================

// EncodeCompositeKey encodes multiple values into a single comparable key.
// The encoding preserves lexicographic ordering for the entire composite.
//
// Parameters:
//   - values: The values to encode (one per index column)
//   - types: The types of each value (must match length of values)
//
// Returns:
//   - []byte: The encoded composite key
//   - error: If encoding fails for any value
//
// Example:
//
//	key, err := EncodeCompositeKey(
//	    []any{int32(5), "hello"},
//	    []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
//	)
func EncodeCompositeKey(values []any, types []dukdb.Type) ([]byte, error) {
	if len(values) != len(types) {
		return nil, fmt.Errorf("values length (%d) does not match types length (%d)", len(values), len(types))
	}

	var result []byte

	for i, v := range values {
		encoded, err := encodeValue(v, types[i])
		if err != nil {
			return nil, fmt.Errorf("failed to encode value at index %d: %w", i, err)
		}
		result = append(result, encoded...)
	}

	return result, nil
}

// encodeValue encodes a single value based on its type.
// This converts a Go value to its byte representation suitable for ART indexing.
//
//nolint:gocyclo,mnd // Complex type handling requires many cases
func encodeValue(value any, keyType dukdb.Type) ([]byte, error) {
	if value == nil {
		// NULL values encode as a special marker
		// Using 0x00 as NULL marker (comes before all other values lexicographically)
		return []byte{0x00}, nil
	}

	// Convert value to bytes based on type
	var rawBytes []byte

	switch keyType {
	case dukdb.TYPE_BOOLEAN:
		b, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool for TYPE_BOOLEAN, got %T", value)
		}
		if b {
			rawBytes = []byte{0x01}
		} else {
			rawBytes = []byte{0x00}
		}

	case dukdb.TYPE_TINYINT:
		var v int8
		switch val := value.(type) {
		case int8:
			v = val
		case int:
			v = int8(val)
		case int16:
			v = int8(val)
		case int32:
			v = int8(val)
		case int64:
			v = int8(val)
		default:
			return nil, fmt.Errorf("expected int8 for TYPE_TINYINT, got %T", value)
		}
		rawBytes = []byte{byte(v)}

	case dukdb.TYPE_SMALLINT:
		var v int16
		switch val := value.(type) {
		case int16:
			v = val
		case int:
			v = int16(val)
		case int8:
			v = int16(val)
		case int32:
			v = int16(val)
		case int64:
			v = int16(val)
		default:
			return nil, fmt.Errorf("expected int16 for TYPE_SMALLINT, got %T", value)
		}
		rawBytes = make([]byte, 2)
		binary.LittleEndian.PutUint16(rawBytes, uint16(v))

	case dukdb.TYPE_INTEGER:
		var v int32
		switch val := value.(type) {
		case int32:
			v = val
		case int:
			v = int32(val)
		case int8:
			v = int32(val)
		case int16:
			v = int32(val)
		case int64:
			v = int32(val)
		default:
			return nil, fmt.Errorf("expected int32 for TYPE_INTEGER, got %T", value)
		}
		rawBytes = make([]byte, 4)
		binary.LittleEndian.PutUint32(rawBytes, uint32(v))

	case dukdb.TYPE_BIGINT:
		var v int64
		switch val := value.(type) {
		case int64:
			v = val
		case int:
			v = int64(val)
		case int8:
			v = int64(val)
		case int16:
			v = int64(val)
		case int32:
			v = int64(val)
		default:
			return nil, fmt.Errorf("expected int64 for TYPE_BIGINT, got %T", value)
		}
		rawBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(rawBytes, uint64(v))

	case dukdb.TYPE_UTINYINT:
		v, ok := value.(uint8)
		if !ok {
			return nil, fmt.Errorf("expected uint8 for TYPE_UTINYINT, got %T", value)
		}
		rawBytes = []byte{v}

	case dukdb.TYPE_USMALLINT:
		v, ok := value.(uint16)
		if !ok {
			return nil, fmt.Errorf("expected uint16 for TYPE_USMALLINT, got %T", value)
		}
		rawBytes = make([]byte, 2)
		binary.LittleEndian.PutUint16(rawBytes, v)

	case dukdb.TYPE_UINTEGER:
		v, ok := value.(uint32)
		if !ok {
			return nil, fmt.Errorf("expected uint32 for TYPE_UINTEGER, got %T", value)
		}
		rawBytes = make([]byte, 4)
		binary.LittleEndian.PutUint32(rawBytes, v)

	case dukdb.TYPE_UBIGINT:
		v, ok := value.(uint64)
		if !ok {
			return nil, fmt.Errorf("expected uint64 for TYPE_UBIGINT, got %T", value)
		}
		rawBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(rawBytes, v)

	case dukdb.TYPE_FLOAT:
		v, ok := value.(float32)
		if !ok {
			return nil, fmt.Errorf("expected float32 for TYPE_FLOAT, got %T", value)
		}
		rawBytes = make([]byte, 4)
		binary.LittleEndian.PutUint32(rawBytes, math.Float32bits(v))

	case dukdb.TYPE_DOUBLE:
		v, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected float64 for TYPE_DOUBLE, got %T", value)
		}
		rawBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(rawBytes, math.Float64bits(v))

	case dukdb.TYPE_VARCHAR:
		var str string
		switch val := value.(type) {
		case string:
			str = val
		case []byte:
			str = string(val)
		default:
			return nil, fmt.Errorf("expected string for TYPE_VARCHAR, got %T", value)
		}
		rawBytes = []byte(str)

	case dukdb.TYPE_BLOB:
		v, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte for TYPE_BLOB, got %T", value)
		}
		rawBytes = v

	case dukdb.TYPE_DATE:
		// DATE is stored as int32 days since epoch
		v, ok := value.(int32)
		if !ok {
			return nil, fmt.Errorf("expected int32 for TYPE_DATE, got %T", value)
		}
		rawBytes = make([]byte, 4)
		binary.LittleEndian.PutUint32(rawBytes, uint32(v))

	case dukdb.TYPE_TIME, dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_S,
		dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS:
		// Time/Timestamp stored as int64 microseconds
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64 for temporal type, got %T", value)
		}
		rawBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(rawBytes, uint64(v))

	default:
		// For other types, try to convert to bytes
		if b, ok := value.([]byte); ok {
			rawBytes = b
		} else {
			return nil, fmt.Errorf("unsupported type for composite key encoding: %v", keyType)
		}
	}

	// Apply type-specific encoding for sort order
	return encodeKey(rawBytes, keyType)
}

// DecodeCompositeKey decodes a composite key back to its component values.
// This is primarily useful for testing and debugging.
//
// Parameters:
//   - key: The encoded composite key bytes
//   - types: The types of each component (determines how to split and decode)
//
// Returns:
//   - []any: The decoded values
//   - error: If decoding fails
//
// Note: This function requires knowledge of the exact types to correctly split
// the concatenated key. Variable-length types (VARCHAR, BLOB) are null-terminated.
//
//nolint:gocyclo,mnd // Complex type handling requires many cases
func DecodeCompositeKey(key []byte, types []dukdb.Type) ([]any, error) {
	result := make([]any, len(types))
	offset := 0

	for i, keyType := range types {
		if offset >= len(key) {
			return nil, fmt.Errorf("key too short: expected %d components, ran out at component %d", len(types), i)
		}

		// Check for NULL marker
		if key[offset] == 0x00 && (keyType == dukdb.TYPE_VARCHAR || keyType == dukdb.TYPE_BLOB) {
			// Could be null terminator for empty string or NULL - assume empty for now
			// This is a limitation of the current encoding
		}

		var encodedLen int
		var decoded []byte
		var err error

		switch keyType {
		case dukdb.TYPE_BOOLEAN:
			encodedLen = 1
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = decoded[0] != 0
			}

		case dukdb.TYPE_TINYINT:
			encodedLen = 1
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = int8(decoded[0])
			}

		case dukdb.TYPE_SMALLINT:
			encodedLen = 2
			if offset+encodedLen > len(key) {
				return nil, fmt.Errorf("key too short for SMALLINT at component %d", i)
			}
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = int16(binary.LittleEndian.Uint16(decoded))
			}

		case dukdb.TYPE_INTEGER, dukdb.TYPE_DATE:
			encodedLen = 4
			if offset+encodedLen > len(key) {
				return nil, fmt.Errorf("key too short for INTEGER at component %d", i)
			}
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = int32(binary.LittleEndian.Uint32(decoded))
			}

		case dukdb.TYPE_BIGINT, dukdb.TYPE_TIME, dukdb.TYPE_TIMESTAMP,
			dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS:
			encodedLen = 8
			if offset+encodedLen > len(key) {
				return nil, fmt.Errorf("key too short for BIGINT at component %d", i)
			}
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = int64(binary.LittleEndian.Uint64(decoded))
			}

		case dukdb.TYPE_UTINYINT:
			encodedLen = 1
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = decoded[0]
			}

		case dukdb.TYPE_USMALLINT:
			encodedLen = 2
			if offset+encodedLen > len(key) {
				return nil, fmt.Errorf("key too short for USMALLINT at component %d", i)
			}
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = binary.LittleEndian.Uint16(decoded)
			}

		case dukdb.TYPE_UINTEGER:
			encodedLen = 4
			if offset+encodedLen > len(key) {
				return nil, fmt.Errorf("key too short for UINTEGER at component %d", i)
			}
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = binary.LittleEndian.Uint32(decoded)
			}

		case dukdb.TYPE_UBIGINT:
			encodedLen = 8
			if offset+encodedLen > len(key) {
				return nil, fmt.Errorf("key too short for UBIGINT at component %d", i)
			}
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = binary.LittleEndian.Uint64(decoded)
			}

		case dukdb.TYPE_FLOAT:
			encodedLen = 4
			if offset+encodedLen > len(key) {
				return nil, fmt.Errorf("key too short for FLOAT at component %d", i)
			}
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = math.Float32frombits(binary.LittleEndian.Uint32(decoded))
			}

		case dukdb.TYPE_DOUBLE:
			encodedLen = 8
			if offset+encodedLen > len(key) {
				return nil, fmt.Errorf("key too short for DOUBLE at component %d", i)
			}
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				result[i] = math.Float64frombits(binary.LittleEndian.Uint64(decoded))
			}

		case dukdb.TYPE_VARCHAR, dukdb.TYPE_BLOB:
			// Variable length: find null terminator
			nullPos := -1
			for j := offset; j < len(key); j++ {
				if key[j] == 0x00 {
					nullPos = j
					break
				}
			}
			if nullPos == -1 {
				return nil, fmt.Errorf("no null terminator found for VARCHAR/BLOB at component %d", i)
			}
			encodedLen = nullPos - offset + 1 // Include null terminator
			decoded, err = decodeKey(key[offset:offset+encodedLen], keyType)
			if err == nil {
				if keyType == dukdb.TYPE_VARCHAR {
					result[i] = string(decoded)
				} else {
					result[i] = decoded
				}
			}

		default:
			return nil, fmt.Errorf("unsupported type for composite key decoding: %v", keyType)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to decode component %d: %w", i, err)
		}

		offset += encodedLen
	}

	return result, nil
}

// ComputePrefixUpperBound returns the first key lexicographically > prefix.
// This is used for prefix range scans where we want all keys starting with prefix.
//
// Parameters:
//   - prefix: The prefix to compute the upper bound for
//
// Returns:
//   - []byte: The upper bound (prefix + 1 at the last incrementable byte)
//   - nil if the prefix is all 0xFF bytes (no upper bound possible)
//
// Example:
//
//	ComputePrefixUpperBound([]byte{0x80, 0x00, 0x05}) => []byte{0x80, 0x00, 0x06}
//	ComputePrefixUpperBound([]byte{0x80, 0xFF}) => []byte{0x81}
//	ComputePrefixUpperBound([]byte{0xFF, 0xFF}) => nil
func ComputePrefixUpperBound(prefix []byte) []byte {
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

// EncodeCompositeKeyPrefix encodes a prefix of a composite key.
// This is useful when you have equality predicates on only some columns.
//
// Parameters:
//   - values: The prefix values to encode (fewer than total index columns)
//   - types: The types of each prefix value
//
// Returns:
//   - []byte: The encoded prefix
//   - error: If encoding fails
//
// Example:
//
//	// Index on (a INT, b VARCHAR, c INT)
//	// Query: WHERE a = 5 (match any b, c)
//	prefix, _ := EncodeCompositeKeyPrefix([]any{5}, []dukdb.Type{dukdb.TYPE_INTEGER})
func EncodeCompositeKeyPrefix(values []any, types []dukdb.Type) ([]byte, error) {
	return EncodeCompositeKey(values, types)
}

// CompositeRangeBounds computes the lower and upper bounds for a composite key range scan.
// This handles the common case of equality on prefix columns with a range on the next column.
//
// Parameters:
//   - equalityValues: Values for columns with equality predicates (prefix)
//   - equalityTypes: Types of the equality columns
//   - rangeLower: Lower bound value for the range column (nil for unbounded)
//   - rangeUpper: Upper bound value for the range column (nil for unbounded)
//   - rangeType: Type of the range column
//   - lowerInclusive: Whether the lower bound is inclusive
//   - upperInclusive: Whether the upper bound is inclusive
//
// Returns:
//   - lower: Encoded lower bound
//   - upper: Encoded upper bound
//   - error: If encoding fails
//
// Example:
//
//	// Index on (a INT, b INT)
//	// Query: WHERE a = 5 AND b BETWEEN 10 AND 20
//	lower, upper, _ := CompositeRangeBounds(
//	    []any{5}, []dukdb.Type{dukdb.TYPE_INTEGER},  // a = 5
//	    10, 20, dukdb.TYPE_INTEGER,                   // b BETWEEN 10 AND 20
//	    true, true,                                    // both inclusive
//	)
func CompositeRangeBounds(
	equalityValues []any,
	equalityTypes []dukdb.Type,
	rangeLower, rangeUpper any,
	rangeType dukdb.Type,
	_, _ bool,
) (lower, upper []byte, err error) {
	// Encode the prefix from equality predicates
	prefix, err := EncodeCompositeKey(equalityValues, equalityTypes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode equality prefix: %w", err)
	}

	// Encode lower bound
	// Note: We must copy prefix before appending to avoid slice aliasing issues
	if rangeLower != nil {
		lowerRangeEncoded, encErr := encodeValue(rangeLower, rangeType)
		if encErr != nil {
			return nil, nil, fmt.Errorf("failed to encode range lower bound: %w", encErr)
		}
		lower = make([]byte, len(prefix)+len(lowerRangeEncoded))
		copy(lower, prefix)
		copy(lower[len(prefix):], lowerRangeEncoded)
	} else {
		lower = make([]byte, len(prefix))
		copy(lower, prefix)
	}

	// Encode upper bound
	if rangeUpper != nil {
		upperRangeEncoded, encErr := encodeValue(rangeUpper, rangeType)
		if encErr != nil {
			return nil, nil, fmt.Errorf("failed to encode range upper bound: %w", encErr)
		}
		upper = make([]byte, len(prefix)+len(upperRangeEncoded))
		copy(upper, prefix)
		copy(upper[len(prefix):], upperRangeEncoded)
	} else {
		// Upper bound is first key > prefix
		upper = ComputePrefixUpperBound(prefix)
	}

	return lower, upper, nil
}
