package catalog

import (
	"fmt"
	"math"
	"sync"
)

// SequenceDef represents a sequence definition in the catalog.
type SequenceDef struct {
	// Name is the sequence name.
	Name string

	// Schema is the schema name.
	Schema string

	// CurrentVal is the next value to be returned by NextVal.
	CurrentVal int64

	// LastVal is the last value returned by NextVal (for CurrVal).
	LastVal int64

	// Called indicates whether NextVal has been called at least once.
	Called bool

	// StartWith is the initial value of the sequence.
	StartWith int64

	// IncrementBy is the increment step.
	IncrementBy int64

	// MinValue is the minimum value allowed.
	MinValue int64

	// MaxValue is the maximum value allowed.
	MaxValue int64

	// IsCycle indicates whether the sequence wraps around when reaching limits.
	IsCycle bool

	// mu protects concurrent access to CurrentVal.
	mu sync.Mutex
}

// NewSequenceDef creates a new SequenceDef instance with default values.
func NewSequenceDef(name, schema string) *SequenceDef {
	return &SequenceDef{
		Name:        name,
		Schema:      schema,
		CurrentVal:  1,
		StartWith:   1,
		IncrementBy: 1,
		MinValue:    math.MinInt64,
		MaxValue:    math.MaxInt64,
		IsCycle:     false,
	}
}

// NextVal returns the current value and then increments for the next call.
// This follows standard SQL sequence behavior where the first call returns
// the START WITH value.
// This method is thread-safe.
func (s *SequenceDef) NextVal() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we've already exceeded bounds (from a previous call that hit the limit)
	if s.IncrementBy > 0 && s.CurrentVal > s.MaxValue {
		return 0, fmt.Errorf("sequence %s.%s reached max value %d", s.Schema, s.Name, s.MaxValue)
	} else if s.IncrementBy < 0 && s.CurrentVal < s.MinValue {
		return 0, fmt.Errorf("sequence %s.%s reached min value %d", s.Schema, s.Name, s.MinValue)
	}

	// Return the current value first
	result := s.CurrentVal

	// Calculate the next value for subsequent calls
	nextVal := s.CurrentVal + s.IncrementBy

	// Check bounds for the next value and handle cycling
	if s.IncrementBy > 0 && nextVal > s.MaxValue {
		if s.IsCycle {
			nextVal = s.StartWith
		}
		// If not cycling, nextVal will exceed MaxValue and the next call will error
	} else if s.IncrementBy < 0 && nextVal < s.MinValue {
		if s.IsCycle {
			nextVal = s.StartWith
		}
		// If not cycling, nextVal will be below MinValue and the next call will error
	}

	// Update current value for next call
	s.CurrentVal = nextVal
	// Store the last returned value for CurrVal
	s.LastVal = result
	s.Called = true
	return result, nil
}

// CurrVal returns the last value returned by NextVal.
// Returns an error if NextVal has not been called yet in this session.
// This method is thread-safe.
func (s *SequenceDef) CurrVal() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.Called {
		return 0, fmt.Errorf("currval of sequence '%s.%s' is not yet defined in this session", s.Schema, s.Name)
	}
	return s.LastVal, nil
}

// Clone creates a deep copy of the sequence definition.
func (s *SequenceDef) Clone() *SequenceDef {
	s.mu.Lock()
	defer s.mu.Unlock()

	return &SequenceDef{
		Name:        s.Name,
		Schema:      s.Schema,
		CurrentVal:  s.CurrentVal,
		LastVal:     s.LastVal,
		Called:      s.Called,
		StartWith:   s.StartWith,
		IncrementBy: s.IncrementBy,
		MinValue:    s.MinValue,
		MaxValue:    s.MaxValue,
		IsCycle:     s.IsCycle,
	}
}

// SetCurrentVal sets the current value of the sequence.
// This is used during WAL recovery to restore sequence state.
// This method is thread-safe.
func (s *SequenceDef) SetCurrentVal(val int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CurrentVal = val
	// If the value has been set, mark as called and set LastVal
	// to the value before CurrentVal (what would have been returned)
	if val != s.StartWith {
		s.Called = true
		s.LastVal = val - s.IncrementBy
	}
}

// GetCurrentVal returns the current value of the sequence (the next value to be returned).
// This method is thread-safe.
func (s *SequenceDef) GetCurrentVal() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.CurrentVal
}
