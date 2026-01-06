// Package engine provides the core execution engine for the native Go DuckDB implementation.
package engine

import (
	"errors"
	"fmt"
	"time"
)

// Savepoint represents a named point within a transaction that can be rolled back to.
// Savepoints allow partial rollback of transaction operations without ending the transaction.
type Savepoint struct {
	// Name is the identifier for this savepoint.
	Name string

	// UndoIndex is the position in the transaction's undo log when this savepoint was created.
	// Rolling back to this savepoint will undo all operations with index >= UndoIndex.
	UndoIndex int

	// CreatedAt is the timestamp when this savepoint was created.
	// This field supports deterministic testing via clock injection.
	CreatedAt time.Time
}

// SavepointStack manages a stack of savepoints within a transaction.
// It maintains both ordering (via stack) and O(1) lookup by name (via map).
// Savepoints are ordered with the newest at the end of the stack.
type SavepointStack struct {
	// stack holds savepoints in creation order (oldest first, newest last).
	stack []*Savepoint

	// byName provides O(1) lookup of savepoints by name.
	byName map[string]*Savepoint
}

// NewSavepointStack creates a new empty SavepointStack.
func NewSavepointStack() *SavepointStack {
	return &SavepointStack{
		stack:  make([]*Savepoint, 0),
		byName: make(map[string]*Savepoint),
	}
}

// Push adds a savepoint to the stack.
// If a savepoint with the same name already exists, it is replaced (PostgreSQL behavior).
// This does NOT return an error for duplicate names - the existing savepoint is removed
// and the new one is added at the current position.
func (s *SavepointStack) Push(sp *Savepoint) error {
	if sp == nil {
		return errors.New("savepoint cannot be nil")
	}
	if sp.Name == "" {
		return errors.New("savepoint name cannot be empty")
	}

	// If a savepoint with this name already exists, remove it first.
	// This follows PostgreSQL behavior where duplicate savepoint names replace existing ones.
	if existing, exists := s.byName[sp.Name]; exists {
		// Find and remove the existing savepoint from the stack
		for i, candidate := range s.stack {
			if candidate == existing {
				s.stack = append(s.stack[:i], s.stack[i+1:]...)

				break
			}
		}
		delete(s.byName, sp.Name)
	}

	// Add the new savepoint to the end of the stack
	s.stack = append(s.stack, sp)
	s.byName[sp.Name] = sp

	return nil
}

// Get looks up a savepoint by name.
// Returns the savepoint and true if found, nil and false otherwise.
func (s *SavepointStack) Get(name string) (*Savepoint, bool) {
	sp, exists := s.byName[name]

	return sp, exists
}

// Release removes a savepoint and all nested (newer) savepoints.
// Returns an error if the savepoint does not exist.
// All savepoints created after the named savepoint are also released.
func (s *SavepointStack) Release(name string) error {
	sp, exists := s.byName[name]
	if !exists {
		return fmt.Errorf("savepoint %q does not exist", name)
	}

	// Find the index of this savepoint in the stack
	idx := -1
	for i, candidate := range s.stack {
		if candidate == sp {
			idx = i

			break
		}
	}

	if idx < 0 {
		// This should never happen if byName and stack are in sync
		return fmt.Errorf("internal error: savepoint %q in map but not in stack", name)
	}

	// Remove this savepoint and all newer ones (indices >= idx)
	for i := idx; i < len(s.stack); i++ {
		delete(s.byName, s.stack[i].Name)
	}
	s.stack = s.stack[:idx]

	return nil
}

// RollbackTo finds a savepoint by name and prepares for rollback.
// It releases the savepoint and all nested (newer) savepoints, returning the savepoint
// so the caller can use its UndoIndex to perform the actual rollback.
// Returns an error if the savepoint does not exist.
func (s *SavepointStack) RollbackTo(name string) (*Savepoint, error) {
	sp, exists := s.byName[name]
	if !exists {
		return nil, fmt.Errorf("savepoint %q does not exist", name)
	}

	// Make a copy of the savepoint before releasing it
	savepointCopy := &Savepoint{
		Name:      sp.Name,
		UndoIndex: sp.UndoIndex,
		CreatedAt: sp.CreatedAt,
	}

	// Release this savepoint and all nested ones
	if err := s.Release(name); err != nil {
		return nil, err
	}

	return savepointCopy, nil
}

// Clear removes all savepoints from the stack.
// This is called when the transaction commits or rolls back completely.
func (s *SavepointStack) Clear() {
	s.stack = make([]*Savepoint, 0)
	s.byName = make(map[string]*Savepoint)
}

// Len returns the number of savepoints in the stack.
func (s *SavepointStack) Len() int {
	return len(s.stack)
}

// IsEmpty returns true if the stack contains no savepoints.
func (s *SavepointStack) IsEmpty() bool {
	return len(s.stack) == 0
}
