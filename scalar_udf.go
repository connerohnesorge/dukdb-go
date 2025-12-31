package dukdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/coder/quartz"
)

// ScalarFuncConfig contains the fields to configure a user-defined scalar function.
type ScalarFuncConfig struct {
	// InputTypeInfos contains Type information for each input parameter of the scalar function.
	InputTypeInfos []TypeInfo
	// ResultTypeInfo holds the Type information of the scalar function's result type.
	ResultTypeInfo TypeInfo

	// VariadicTypeInfo configures the number of input parameters.
	// If this field is nil, then the input parameters match InputTypeInfos.
	// Otherwise, the scalar function's input parameters are set to variadic, allowing any number of input parameters.
	// The Type of the first len(InputTypeInfos) parameters is configured by InputTypeInfos, and all
	// remaining parameters must match the variadic Type. To configure different variadic parameter types,
	// you must set the VariadicTypeInfo's Type to TYPE_ANY.
	VariadicTypeInfo TypeInfo
	// Volatile sets the stability of the scalar function to volatile, if true.
	// Volatile scalar functions might create a different result per row.
	// E.g., random() is a volatile scalar function.
	Volatile bool
	// SpecialNullHandling disables the default NULL handling of scalar functions, if true.
	// The default NULL handling is: NULL in, NULL out. I.e., if any input parameter is NULL, then the result is NULL.
	SpecialNullHandling bool
}

// ScalarUDFArg contains scalar UDF argument metadata and the optional argument.
type ScalarUDFArg struct {
	// Foldable is true, if the argument was folded into a value, else false.
	Foldable bool
	// Value is the folded argument value, or nil, if the argument is not foldable.
	Value driver.Value
}

type (
	// RowExecutorFn is the type for any row-based execution function.
	// It takes the row values and returns the row execution result, or error.
	RowExecutorFn func(values []driver.Value) (any, error)
	// RowContextExecutorFn accepts a row-based execution function using a context.
	// It takes a context and the row values, and returns the row execution result, or error.
	RowContextExecutorFn func(ctx context.Context, values []driver.Value) (any, error)
	// ScalarBinderFn takes a context and the scalar function's arguments.
	// It returns the updated context, which can now contain arbitrary data available during execution.
	ScalarBinderFn func(ctx context.Context, args []ScalarUDFArg) (context.Context, error)
)

// ScalarFuncExecutor contains the functions to execute a user-defined scalar function.
// It invokes its first non-nil member.
type ScalarFuncExecutor struct {
	// RowExecutor accepts a row-based execution function of type RowExecutorFn.
	RowExecutor RowExecutorFn
	// RowContextExecutor accepts a row-based execution function of type RowContextExecutorFn.
	RowContextExecutor RowContextExecutorFn
	// ScalarBinder accepts a bind function of type ScalarBinderFn.
	ScalarBinder ScalarBinderFn
}

// ScalarFunc is the user-defined scalar function interface.
// Any scalar function must implement a Config function, and an Executor function.
type ScalarFunc interface {
	// Config returns ScalarFuncConfig to configure the scalar function.
	Config() ScalarFuncConfig
	// Executor returns ScalarFuncExecutor to execute the scalar function.
	Executor() ScalarFuncExecutor
}

// ScalarFuncContext provides context for UDF execution with deterministic time access.
type ScalarFuncContext struct {
	ctx   context.Context
	clock quartz.Clock // Injected clock for timeouts
}

// NewScalarFuncContext creates a new ScalarFuncContext with the given context and clock.
func NewScalarFuncContext(ctx context.Context, clock quartz.Clock) *ScalarFuncContext {
	if clock == nil {
		clock = quartz.NewReal()
	}

	return &ScalarFuncContext{
		ctx:   ctx,
		clock: clock,
	}
}

// WithClock returns a new ScalarFuncContext with the given clock.
func (c *ScalarFuncContext) WithClock(clock quartz.Clock) *ScalarFuncContext {
	return &ScalarFuncContext{
		ctx:   c.ctx,
		clock: clock,
	}
}

// Context returns the underlying context.
func (c *ScalarFuncContext) Context() context.Context {
	return c.ctx
}

// Clock returns the clock used for timeout checking.
func (c *ScalarFuncContext) Clock() quartz.Clock {
	return c.clock
}

// checkTimeout checks if the context deadline has been exceeded using the injected clock.
func (c *ScalarFuncContext) checkTimeout() error {
	if deadline, ok := c.ctx.Deadline(); ok {
		if c.clock.Until(deadline) <= 0 {
			return context.DeadlineExceeded
		}
	}

	return nil
}

// registeredScalarFunc holds a registered scalar function with its metadata.
type registeredScalarFunc struct {
	name     string
	config   ScalarFuncConfig
	executor ScalarFuncExecutor
	bindCtx  context.Context // Context from bind phase
}

// scalarFuncRegistry holds registered scalar functions per connection.
type scalarFuncRegistry struct {
	mu        sync.RWMutex
	functions map[string][]registeredScalarFunc
}

// newScalarFuncRegistry creates a new scalar function registry.
func newScalarFuncRegistry() *scalarFuncRegistry {
	return &scalarFuncRegistry{
		functions: make(map[string][]registeredScalarFunc),
	}
}

// register adds a scalar function to the registry.
func (r *scalarFuncRegistry) register(name string, f ScalarFunc) error {
	if name == "" {
		return fmt.Errorf("scalar UDF name cannot be empty")
	}
	if f == nil {
		return fmt.Errorf("scalar UDF cannot be nil")
	}

	config := f.Config()
	executor := f.Executor()

	// Validate executor.
	if executor.RowExecutor == nil && executor.RowContextExecutor == nil {
		return fmt.Errorf("scalar UDF must have either RowExecutor or RowContextExecutor")
	}

	// Validate result type.
	if config.ResultTypeInfo == nil {
		return fmt.Errorf("scalar UDF result type cannot be nil")
	}
	if config.ResultTypeInfo.InternalType() == TYPE_ANY {
		return fmt.Errorf("scalar UDF result type cannot be TYPE_ANY")
	}

	// Validate input types.
	for i, info := range config.InputTypeInfos {
		if info == nil {
			return fmt.Errorf("scalar UDF input type at index %d cannot be nil", i)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	registered := registeredScalarFunc{
		name:     name,
		config:   config,
		executor: executor,
		bindCtx:  context.Background(),
	}

	r.functions[name] = append(r.functions[name], registered)

	return nil
}

// lookup finds a scalar function by name and argument types.
func (r *scalarFuncRegistry) lookup(name string, argTypes []Type) *registeredScalarFunc {
	r.mu.RLock()
	defer r.mu.RUnlock()

	funcs, ok := r.functions[name]
	if !ok {
		return nil
	}

	for i := range funcs {
		if funcs[i].matchesTypes(argTypes) {
			return &funcs[i]
		}
	}

	return nil
}

// LookupScalarUDF implements binder.ScalarUDFResolver interface.
// It looks up a scalar UDF by name and argument types.
func (r *scalarFuncRegistry) LookupScalarUDF(name string, argTypes []Type) (udfInfo any, resultType Type, found bool) {
	udf := r.lookup(name, argTypes)
	if udf == nil {
		return nil, TYPE_INVALID, false
	}

	return udf, udf.config.ResultTypeInfo.InternalType(), true
}

// BindScalarUDF implements binder.ScalarUDFResolver interface.
// It calls the ScalarBinder callback if present and returns the bind context.
// For volatile functions, this does not cache results to ensure each call is fresh.
func (r *scalarFuncRegistry) BindScalarUDF(udfInfo any, args []ScalarUDFArg) (bindCtx any, err error) {
	udf, ok := udfInfo.(*registeredScalarFunc)
	if !ok {
		return nil, fmt.Errorf("invalid UDF info type")
	}

	// Volatile functions should not have their results cached during constant folding.
	// Skip the ScalarBinder callback for volatile functions to prevent caching.
	if udf.config.Volatile {
		return nil, nil
	}

	// If no ScalarBinder is defined, return nil context
	if udf.executor.ScalarBinder == nil {
		return nil, nil
	}

	// Call the ScalarBinder callback
	ctx, err := udf.executor.ScalarBinder(context.Background(), args)
	if err != nil {
		return nil, err
	}

	// Store the bind context for execution time
	udf.bindCtx = ctx

	return ctx, nil
}

// IsVolatile implements binder.ScalarUDFResolver interface.
// It returns true if the UDF is marked as volatile.
func (r *scalarFuncRegistry) IsVolatile(udfInfo any) bool {
	udf, ok := udfInfo.(*registeredScalarFunc)
	if !ok {
		return false
	}

	return udf.config.Volatile
}

// matchesTypes checks if the function matches the given argument types.
func (f *registeredScalarFunc) matchesTypes(argTypes []Type) bool {
	config := f.config

	// Check non-variadic parameters.
	for i, expected := range config.InputTypeInfos {
		if i >= len(argTypes) {
			return false
		}
		if !typesCompatible(expected.InternalType(), argTypes[i]) {
			return false
		}
	}

	// Check if we have exactly the right number of args (non-variadic case).
	if config.VariadicTypeInfo == nil {
		return len(argTypes) == len(config.InputTypeInfos)
	}

	// Check variadic parameters.
	variadicType := config.VariadicTypeInfo.InternalType()
	for i := len(config.InputTypeInfos); i < len(argTypes); i++ {
		if variadicType != TYPE_ANY && !typesCompatible(variadicType, argTypes[i]) {
			return false
		}
	}

	return true
}

// typesCompatible checks if two types are compatible for function matching.
func typesCompatible(expected, actual Type) bool {
	if expected == TYPE_ANY {
		return true
	}

	return expected == actual
}

// executeScalarUDF executes a scalar UDF on a DataChunk and writes results to output vector.
func executeScalarUDF(
	ctx *ScalarFuncContext,
	udf *registeredScalarFunc,
	input *DataChunk,
	output *vector,
) error {
	executor := udf.executor
	size := input.GetSize()
	numCols := input.GetColumnCount()
	values := make([]driver.Value, numCols)

	// Get the execution function.
	var execFn func(values []driver.Value) (any, error)
	if executor.RowExecutor != nil {
		execFn = executor.RowExecutor
	} else {
		// Wrap context executor.
		execCtx := ctx.ctx
		if udf.bindCtx != nil {
			execCtx = udf.bindCtx
		}
		execFn = func(values []driver.Value) (any, error) {
			return executor.RowContextExecutor(execCtx, values)
		}
	}

	nullInNullOut := !udf.config.SpecialNullHandling

	for rowIdx := range size {
		// Check timeout periodically.
		if ctx != nil {
			if err := ctx.checkTimeout(); err != nil {
				return fmt.Errorf("scalar UDF '%s' timeout at row %d: %w", udf.name, rowIdx, err)
			}
		}

		// Gather input values.
		nullRow := false
		for colIdx := range numCols {
			val, err := input.GetValue(colIdx, rowIdx)
			if err != nil {
				return fmt.Errorf("scalar UDF '%s' failed to get input at row %d, col %d: %w", udf.name, rowIdx, colIdx, err)
			}
			values[colIdx] = val

			// NULL handling.
			if nullInNullOut && val == nil {
				if err := output.setFn(output, rowIdx, nil); err != nil {
					return fmt.Errorf("scalar UDF '%s' failed to set NULL output at row %d: %w", udf.name, rowIdx, err)
				}
				nullRow = true

				break
			}
		}

		if nullRow {
			continue
		}

		// Execute the user function with panic recovery.
		result, err := safeExecute(execFn, values)
		if err != nil {
			return fmt.Errorf("scalar UDF '%s' execution error at row %d: %w", udf.name, rowIdx, err)
		}

		// Write result to output.
		if err := output.setFn(output, rowIdx, result); err != nil {
			return fmt.Errorf("scalar UDF '%s' failed to set output at row %d: %w", udf.name, rowIdx, err)
		}
	}

	return nil
}

// safeExecute wraps user function execution with panic recovery.
func safeExecute(fn func([]driver.Value) (any, error), values []driver.Value) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in scalar UDF: %v", r)
		}
	}()

	return fn(values)
}

// RegisterScalarUDF registers a user-defined scalar function.
// c is the SQL connection on which to register the scalar function.
// name is the function name, and f is the scalar function's interface ScalarFunc.
func RegisterScalarUDF(c *sql.Conn, name string, f ScalarFunc) error {
	return c.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("invalid connection type: expected *Conn, got %T", driverConn)
		}

		if conn.scalarFuncs == nil {
			conn.scalarFuncs = newScalarFuncRegistry()
		}

		return conn.scalarFuncs.register(name, f)
	})
}

// RegisterScalarUDFSet registers a set of user-defined scalar functions with the same name.
// This enables overloading of scalar functions.
// c is the SQL connection on which to register the scalar function set.
// name is the function name of each function in the set.
// functions contains all ScalarFunc functions of the scalar function set.
func RegisterScalarUDFSet(c *sql.Conn, name string, functions ...ScalarFunc) error {
	return c.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("invalid connection type: expected *Conn, got %T", driverConn)
		}

		if conn.scalarFuncs == nil {
			conn.scalarFuncs = newScalarFuncRegistry()
		}

		for i, f := range functions {
			if err := conn.scalarFuncs.register(name, f); err != nil {
				return fmt.Errorf("failed to register function %d in set '%s': %w", i, name, err)
			}
		}

		return nil
	})
}
