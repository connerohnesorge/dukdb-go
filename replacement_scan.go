package dukdb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/coder/quartz"
)

// ReplacementScanCallback is called when a table reference is encountered during query binding.
// It returns:
// - functionName: the name of the function to replace the table reference with (empty means no replacement)
// - params: parameters to pass to the replacement function (only string, int64, []string supported)
// - error: an error if the replacement scan fails
type ReplacementScanCallback func(tableName string) (string, []any, error)

// ReplacementScanContext provides context and clock for replacement scan execution.
// The clock is used for deterministic deadline checking in tests.
type ReplacementScanContext struct {
	ctx   context.Context
	clock quartz.Clock
}

// NewReplacementScanContext creates a new ReplacementScanContext.
// If clock is nil, the real system clock is used.
func NewReplacementScanContext(ctx context.Context, clock quartz.Clock) *ReplacementScanContext {
	if clock == nil {
		clock = quartz.NewReal()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &ReplacementScanContext{
		ctx:   ctx,
		clock: clock,
	}
}

// WithClock returns a new ReplacementScanContext with the given clock.
func (c *ReplacementScanContext) WithClock(clock quartz.Clock) *ReplacementScanContext {
	return &ReplacementScanContext{
		ctx:   c.ctx,
		clock: clock,
	}
}

// Context returns the underlying context.
func (c *ReplacementScanContext) Context() context.Context {
	return c.ctx
}

// Clock returns the clock used for timing.
func (c *ReplacementScanContext) Clock() quartz.Clock {
	return c.clock
}

// executeCallback executes the replacement scan callback with deadline checking.
// If the context has a deadline and it has passed (according to the clock), returns context.DeadlineExceeded.
func (c *ReplacementScanContext) executeCallback(
	callback ReplacementScanCallback,
	tableName string,
) (string, []any, error) {
	if deadline, ok := c.ctx.Deadline(); ok {
		if c.clock.Until(deadline) <= 0 {
			return "", nil, context.DeadlineExceeded
		}
	}
	return callback(tableName)
}

// replacementScanRegistry holds registered replacement scan callbacks for a connector.
type replacementScanRegistry struct {
	mu       sync.RWMutex
	callback ReplacementScanCallback
}

// newReplacementScanRegistry creates a new replacement scan registry.
func newReplacementScanRegistry() *replacementScanRegistry {
	return &replacementScanRegistry{}
}

// register registers a replacement scan callback, replacing any existing one.
func (r *replacementScanRegistry) register(callback ReplacementScanCallback) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.callback = callback
}

// get returns the registered callback, or nil if none is registered.
func (r *replacementScanRegistry) get() ReplacementScanCallback {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.callback
}

// RegisterReplacementScan registers a replacement scan callback for the connector.
// The callback is called during query binding when a table reference is encountered.
// Only one callback can be registered per connector; subsequent calls replace the previous callback.
func RegisterReplacementScan(c *Connector, callback ReplacementScanCallback) {
	if c.replacementScans == nil {
		c.replacementScans = newReplacementScanRegistry()
	}
	c.replacementScans.register(callback)
}

// Error variables for replacement scan operations.
var (
	errReplacementScanUnsupportedType = errors.New("replacement scan: unsupported parameter type")
	errReplacementScanFailed          = errors.New("replacement scan callback failed")
)

// validateReplacementParams validates that all parameters are of supported types.
// Supported types: string, int64, []string
func validateReplacementParams(params []any) error {
	for i, param := range params {
		switch param.(type) {
		case string, int64, []string:
			// Supported types
		default:
			return fmt.Errorf("%w: parameter %d has type %T", errReplacementScanUnsupportedType, i, param)
		}
	}
	return nil
}

// ReplacementScanResult holds the result of a replacement scan attempt.
type ReplacementScanResult struct {
	// FunctionName is the name of the function to replace the table with.
	// Empty means no replacement was made.
	FunctionName string
	// Params are the parameters to pass to the replacement function.
	Params []any
	// Replaced indicates whether a replacement was made.
	Replaced bool
	// Error holds any error that occurred during the scan.
	Error error
}

// TryReplacementScan attempts to replace a table reference using the connector's
// replacement scan callback. This is called by the binder during table resolution.
func TryReplacementScan(c *Connector, tableName string) ReplacementScanResult {
	return TryReplacementScanWithContext(c, tableName, nil)
}

// TryReplacementScanWithContext attempts to replace a table reference using the
// connector's replacement scan callback with a context for timeout checking.
func TryReplacementScanWithContext(c *Connector, tableName string, ctx *ReplacementScanContext) ReplacementScanResult {
	if c == nil || c.replacementScans == nil {
		return ReplacementScanResult{Replaced: false}
	}

	callback := c.replacementScans.get()
	if callback == nil {
		return ReplacementScanResult{Replaced: false}
	}

	var funcName string
	var params []any
	var err error

	if ctx != nil {
		funcName, params, err = ctx.executeCallback(callback, tableName)
	} else {
		funcName, params, err = callback(tableName)
	}

	// Handle callback error
	if err != nil {
		return ReplacementScanResult{
			Error: fmt.Errorf("%w: %v", errReplacementScanFailed, err),
		}
	}

	// Handle nil/empty function name (no replacement)
	if funcName == "" {
		return ReplacementScanResult{Replaced: false}
	}

	// Validate parameter types
	if len(params) > 0 {
		if err := validateReplacementParams(params); err != nil {
			return ReplacementScanResult{Error: err}
		}
	}

	return ReplacementScanResult{
		FunctionName: funcName,
		Params:       params,
		Replaced:     true,
	}
}

// ConvertParamsToFunctionArgs converts replacement scan parameters to
// function call arguments that can be used by the binder/executor.
func ConvertParamsToFunctionArgs(params []any) []FunctionArg {
	if len(params) == 0 {
		return nil
	}

	args := make([]FunctionArg, len(params))
	for i, param := range params {
		args[i] = FunctionArg{
			Position: i,
			Value:    param,
		}
	}
	return args
}

// FunctionArg represents a function argument for replacement scans.
type FunctionArg struct {
	Position int
	Value    any
}
