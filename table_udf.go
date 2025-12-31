package dukdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/coder/quartz"
)

// ColumnInfo contains the metadata of a column for table functions.
type ColumnInfo struct {
	// Name is the column name.
	Name string
	// T is the column type.
	T TypeInfo
}

// CardinalityInfo contains the cardinality of a (table) function.
// If it is impossible or difficult to determine the exact cardinality, an approximate cardinality may be used.
type CardinalityInfo struct {
	// Cardinality is the absolute cardinality.
	Cardinality uint
	// Exact indicates whether the cardinality is exact.
	Exact bool
}

// TableFunctionConfig contains any information passed to DuckDB when registering the table function.
type TableFunctionConfig struct {
	// Arguments of the table function.
	Arguments []TypeInfo
	// NamedArguments of the table function.
	NamedArguments map[string]TypeInfo
}

// ParallelTableSourceInfo contains information for initializing a parallelism-aware table source.
type ParallelTableSourceInfo struct {
	// MaxThreads is the maximum number of threads on which to run the table source function.
	// If set to 0, it uses the default thread configuration.
	MaxThreads int
}

// tableSource is the base interface for all table sources.
type tableSource interface {
	// ColumnInfos returns column information for each column of the table function.
	ColumnInfos() []ColumnInfo
	// Cardinality returns the cardinality information of the table function.
	// Optionally, if no cardinality exists, it may return nil.
	Cardinality() *CardinalityInfo
}

// sequentialTableSource is the interface for sequential table sources.
type sequentialTableSource interface {
	tableSource
	// Init initializes the table source.
	Init()
}

// parallelTableSource is the interface for parallel table sources.
type parallelTableSource interface {
	tableSource
	// Init initializes the table source.
	// Additionally, it returns information for the parallelism-aware table source.
	Init() ParallelTableSourceInfo
	// NewLocalState returns a thread-local execution state.
	// It must return a pointer or a reference type for correct state updates.
	NewLocalState() any
}

// RowTableSource represents anything that produces rows in a non-vectorised way.
// The cardinality is requested before function initialization.
// After initializing the RowTableSource, the engine requests the rows.
// It sequentially calls the FillRow method with a single thread.
type RowTableSource interface {
	sequentialTableSource
	// FillRow takes a Row and fills it with values.
	// Returns true if there are more rows to fill.
	FillRow(Row) (bool, error)
}

// ParallelRowTableSource represents anything that produces rows in a non-vectorised way.
// The cardinality is requested before function initialization.
// After initializing the ParallelRowTableSource, the engine requests the rows.
// It simultaneously calls the FillRow method with multiple threads.
// If ParallelTableSourceInfo.MaxThreads is greater than one, FillRow must use synchronisation
// primitives to avoid race conditions.
type ParallelRowTableSource interface {
	parallelTableSource
	// FillRow takes a Row and fills it with values.
	// The localState parameter is passed first (thread-local state from NewLocalState).
	// Returns true if there are more rows to fill.
	FillRow(localState any, row Row) (bool, error)
}

// ChunkTableSource represents anything that produces rows in a vectorised way.
// The cardinality is requested before function initialization.
// After initializing the ChunkTableSource, the engine requests the rows.
// It sequentially calls the FillChunk method with a single thread.
type ChunkTableSource interface {
	sequentialTableSource
	// FillChunk takes a DataChunk and fills it with values.
	// Set the chunk size to 0 to end the function.
	// Note: Unlike duckdb-go which uses CGO value semantics, we use pointer
	// for proper Go semantics.
	FillChunk(*DataChunk) error
}

// ParallelChunkTableSource represents anything that produces rows in a vectorised way.
// The cardinality is requested before function initialization.
// After initializing the ParallelChunkTableSource, the engine requests the rows.
// It simultaneously calls the FillChunk method with multiple threads.
// If ParallelTableSourceInfo.MaxThreads is greater than one, FillChunk must use synchronization
// primitives to avoid race conditions.
type ParallelChunkTableSource interface {
	parallelTableSource
	// FillChunk takes a DataChunk and fills it with values.
	// The localState parameter is passed first (thread-local state from NewLocalState).
	// Set the chunk size to 0 to end the function.
	FillChunk(
		localState any,
		chunk *DataChunk,
	) error
}

// tableFunction is the generic table function type.
type tableFunction[T tableSource] struct {
	// Config returns the table function configuration, including the function arguments.
	Config TableFunctionConfig
	// BindArguments binds the arguments and returns a TableSource.
	BindArguments func(named map[string]any, args ...any) (T, error)
	// BindArgumentsContext binds the arguments with context and returns a TableSource.
	BindArgumentsContext func(ctx context.Context, named map[string]any, args ...any) (T, error)
}

// RowTableFunction is a type which can be bound to return a RowTableSource.
type RowTableFunction = tableFunction[RowTableSource]

// ParallelRowTableFunction is a type which can be bound to return a ParallelRowTableSource.
type ParallelRowTableFunction = tableFunction[ParallelRowTableSource]

// ChunkTableFunction is a type which can be bound to return a ChunkTableSource.
type ChunkTableFunction = tableFunction[ChunkTableSource]

// ParallelChunkTableFunction is a type which can be bound to return a ParallelChunkTableSource.
type ParallelChunkTableFunction = tableFunction[ParallelChunkTableSource]

// TableFunction is the union of all table function types.
type TableFunction interface {
	RowTableFunction | ParallelRowTableFunction | ChunkTableFunction | ParallelChunkTableFunction
}

// parallelRowTSWrapper wraps a synchronous RowTableSource for a parallel context with nthreads=1.
type parallelRowTSWrapper struct {
	s RowTableSource
}

func (w parallelRowTSWrapper) ColumnInfos() []ColumnInfo {
	return w.s.ColumnInfos()
}

func (w parallelRowTSWrapper) Cardinality() *CardinalityInfo {
	return w.s.Cardinality()
}

func (w parallelRowTSWrapper) Init() ParallelTableSourceInfo {
	w.s.Init()

	return ParallelTableSourceInfo{MaxThreads: 1}
}

func (w parallelRowTSWrapper) NewLocalState() any {
	return struct{}{}
}

func (w parallelRowTSWrapper) FillRow(
	localState any,
	row Row,
) (bool, error) {
	return w.s.FillRow(row)
}

// parallelChunkTSWrapper wraps a synchronous ChunkTableSource for a parallel context with nthreads=1.
type parallelChunkTSWrapper struct {
	s ChunkTableSource
}

func (w parallelChunkTSWrapper) ColumnInfos() []ColumnInfo {
	return w.s.ColumnInfos()
}

func (w parallelChunkTSWrapper) Cardinality() *CardinalityInfo {
	return w.s.Cardinality()
}

func (w parallelChunkTSWrapper) Init() ParallelTableSourceInfo {
	w.s.Init()

	return ParallelTableSourceInfo{MaxThreads: 1}
}

func (w parallelChunkTSWrapper) NewLocalState() any {
	return struct{}{}
}

func (w parallelChunkTSWrapper) FillChunk(
	localState any,
	chunk *DataChunk,
) error {
	return w.s.FillChunk(chunk)
}

// wrapRowTF wraps a RowTableFunction to a ParallelRowTableFunction.
func wrapRowTF(
	f RowTableFunction,
) ParallelRowTableFunction {
	tf := ParallelRowTableFunction{
		Config: f.Config,
	}

	if f.BindArguments != nil {
		tf.BindArguments = func(named map[string]any, args ...any) (ParallelRowTableSource, error) {
			rts, err := f.BindArguments(
				named,
				args...)
			if err != nil {
				return nil, err
			}

			return parallelRowTSWrapper{
				s: rts,
			}, nil
		}
	}

	if f.BindArgumentsContext != nil {
		tf.BindArgumentsContext = func(ctx context.Context, named map[string]any, args ...any) (ParallelRowTableSource, error) {
			rts, err := f.BindArgumentsContext(
				ctx,
				named,
				args...)
			if err != nil {
				return nil, err
			}

			return parallelRowTSWrapper{
				s: rts,
			}, nil
		}
	}

	return tf
}

// wrapChunkTF wraps a ChunkTableFunction to a ParallelChunkTableFunction.
func wrapChunkTF(
	f ChunkTableFunction,
) ParallelChunkTableFunction {
	tf := ParallelChunkTableFunction{
		Config: f.Config,
	}

	if f.BindArguments != nil {
		tf.BindArguments = func(named map[string]any, args ...any) (ParallelChunkTableSource, error) {
			rts, err := f.BindArguments(
				named,
				args...)
			if err != nil {
				return nil, err
			}

			return parallelChunkTSWrapper{
				s: rts,
			}, nil
		}
	}

	if f.BindArgumentsContext != nil {
		tf.BindArgumentsContext = func(ctx context.Context, named map[string]any, args ...any) (ParallelChunkTableSource, error) {
			rts, err := f.BindArgumentsContext(
				ctx,
				named,
				args...)
			if err != nil {
				return nil, err
			}

			return parallelChunkTSWrapper{
				s: rts,
			}, nil
		}
	}

	return tf
}

// Error variables for table UDF operations.
var (
	errTableUDFNoName = errors.New(
		"table UDF name cannot be empty",
	)
	errTableUDFMissingBindArgs = errors.New(
		"table UDF requires BindArguments or BindArgumentsContext",
	)
	errTableUDFArgumentIsNil = errors.New(
		"table UDF argument type cannot be nil",
	)
	errTableUDFColumnTypeIsNil = errors.New(
		"table UDF column type cannot be nil",
	)
)

// TableFunctionContext provides context and clock for table function execution.
// Used for deterministic testing with mock clocks.
type TableFunctionContext struct {
	ctx   context.Context
	clock quartz.Clock
}

// NewTableFunctionContext creates a TableFunctionContext with the given context and clock.
// If clock is nil, the real system clock is used.
func NewTableFunctionContext(
	ctx context.Context,
	clock quartz.Clock,
) *TableFunctionContext {
	if clock == nil {
		clock = quartz.NewReal()
	}

	return &TableFunctionContext{
		ctx:   ctx,
		clock: clock,
	}
}

// WithClock returns a new TableFunctionContext with the given clock.
func (c *TableFunctionContext) WithClock(
	clock quartz.Clock,
) *TableFunctionContext {
	return &TableFunctionContext{
		ctx:   c.ctx,
		clock: clock,
	}
}

// Context returns the underlying context.
func (c *TableFunctionContext) Context() context.Context {
	return c.ctx
}

// Clock returns the clock used for timeout checking.
func (c *TableFunctionContext) Clock() quartz.Clock {
	return c.clock
}

// tableFunctionRegistry holds registered table functions per connection.
type tableFunctionRegistry struct {
	mu        sync.RWMutex
	functions map[string]any // name -> tableFunction or wrapper
}

func newTableFunctionRegistry() *tableFunctionRegistry {
	return &tableFunctionRegistry{
		functions: make(map[string]any),
	}
}

func (r *tableFunctionRegistry) register(
	name string,
	f any,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.functions[name]; exists {
		return fmt.Errorf(
			"table function %q already registered",
			name,
		)
	}
	r.functions[name] = f

	return nil
}

// Get looks up a registered table function by name.
// Returns the function and true if found, nil and false otherwise.
// Used by query executor to resolve table function calls.
func (r *tableFunctionRegistry) Get(
	name string,
) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.functions[name]

	return f, ok
}

// RegisterTableUDF registers a user-defined table function.
// Projection pushdown is enabled by default.
func RegisterTableUDF[TFT TableFunction](
	conn *sql.Conn,
	name string,
	f TFT,
) error {
	if name == "" {
		return errTableUDFNoName
	}

	// Normalize the function to parallel variant.
	var x any = f
	switch tableFunc := x.(type) {
	case RowTableFunction:
		return registerParallelTableUDF(conn, name, wrapRowTF(tableFunc))
	case ChunkTableFunction:
		return registerParallelTableUDF(conn, name, wrapChunkTF(tableFunc))
	case ParallelRowTableFunction:
		return registerParallelTableUDF(conn, name, tableFunc)
	case ParallelChunkTableFunction:
		return registerParallelTableUDF(conn, name, tableFunc)
	default:
		return errors.New("unknown table function type")
	}
}

// registerParallelTableUDF registers a parallel table function.
func registerParallelTableUDF[T tableSource](
	conn *sql.Conn,
	name string,
	f tableFunction[T],
) error {
	// Validate that either BindArguments or BindArgumentsContext is set.
	if f.BindArguments == nil &&
		f.BindArgumentsContext == nil {
		return errTableUDFMissingBindArgs
	}

	// Validate argument types.
	for _, t := range f.Config.Arguments {
		if t == nil {
			return errTableUDFArgumentIsNil
		}
	}

	for _, t := range f.Config.NamedArguments {
		if t == nil {
			return errTableUDFArgumentIsNil
		}
	}

	// Register the function on the underlying driver connection.
	return conn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return errors.New(
				"invalid driver connection type",
			)
		}

		// Initialize registry if needed.
		if c.tableUDFs == nil {
			c.tableUDFs = newTableFunctionRegistry()
		}

		// Store the function in the registry.
		return c.tableUDFs.register(name, f)
	})
}

// TableSourceExecutor executes table sources and collects results.
type TableSourceExecutor struct {
	projection []int
	clock      quartz.Clock // For deterministic testing
}

// NewTableSourceExecutor creates a new table source executor.
func NewTableSourceExecutor() *TableSourceExecutor {
	return &TableSourceExecutor{
		clock: quartz.NewReal(),
	}
}

// WithProjection sets the column projection for the executor.
// Columns not in the projection list will not be populated.
func (e *TableSourceExecutor) WithProjection(
	projection []int,
) *TableSourceExecutor {
	e.projection = projection

	return e
}

// WithClock sets the clock for the executor to enable deterministic testing.
func (e *TableSourceExecutor) WithClock(
	clock quartz.Clock,
) *TableSourceExecutor {
	e.clock = clock

	return e
}

// ExecuteRowSource executes a RowTableSource and collects all rows into DataChunks.
func (e *TableSourceExecutor) ExecuteRowSource(
	source RowTableSource,
) ([]*DataChunk, error) {
	// Get column types
	colInfos := source.ColumnInfos()
	types := make([]TypeInfo, len(colInfos))
	for i, col := range colInfos {
		if col.T == nil {
			return nil, errTableUDFColumnTypeIsNil
		}
		types[i] = col.T
	}

	// Initialize the source
	source.Init()

	// Collect results
	var chunks []*DataChunk
	var currentChunk *DataChunk
	var err error

	// Create initial chunk
	if e.projection != nil {
		currentChunk, err = NewDataChunkWithProjection(
			types,
			e.projection,
		)
	} else {
		currentChunk, err = NewDataChunk(types)
	}
	if err != nil {
		return nil, err
	}

	rowIdx := 0
	for {
		row := NewRow(currentChunk, rowIdx)
		hasMore, fillErr := source.FillRow(row)
		if fillErr != nil {
			return nil, fillErr
		}

		if !hasMore {
			// Set final size and we're done
			if rowIdx > 0 {
				if err := currentChunk.SetSize(rowIdx); err != nil {
					return nil, err
				}
				chunks = append(
					chunks,
					currentChunk,
				)
			}

			break
		}

		rowIdx++
		if rowIdx >= GetDataChunkCapacity() {
			// Chunk is full, save it and create a new one
			if err := currentChunk.SetSize(rowIdx); err != nil {
				return nil, err
			}
			chunks = append(chunks, currentChunk)

			// Create new chunk
			if e.projection != nil {
				currentChunk, err = NewDataChunkWithProjection(
					types,
					e.projection,
				)
			} else {
				currentChunk, err = NewDataChunk(types)
			}
			if err != nil {
				return nil, err
			}
			rowIdx = 0
		}
	}

	return chunks, nil
}

// ExecuteChunkSource executes a ChunkTableSource and collects all chunks.
func (e *TableSourceExecutor) ExecuteChunkSource(
	source ChunkTableSource,
) ([]*DataChunk, error) {
	// Get column types
	colInfos := source.ColumnInfos()
	types := make([]TypeInfo, len(colInfos))
	for i, col := range colInfos {
		if col.T == nil {
			return nil, errTableUDFColumnTypeIsNil
		}
		types[i] = col.T
	}

	// Initialize the source
	source.Init()

	// Collect results
	var chunks []*DataChunk

	for {
		var chunk *DataChunk
		var err error

		if e.projection != nil {
			chunk, err = NewDataChunkWithProjection(
				types,
				e.projection,
			)
		} else {
			chunk, err = NewDataChunk(types)
		}
		if err != nil {
			return nil, err
		}

		// Reset size to capacity so FillChunk can fill it
		if err := chunk.SetSize(GetDataChunkCapacity()); err != nil {
			return nil, err
		}

		// Fill the chunk
		if err := source.FillChunk(chunk); err != nil {
			return nil, err
		}

		// Check if we got any rows
		if chunk.GetSize() == 0 {
			break
		}

		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// ExecuteParallelRowSource executes a ParallelRowTableSource with parallel workers.
func (e *TableSourceExecutor) ExecuteParallelRowSource(
	source ParallelRowTableSource,
) ([]*DataChunk, error) {
	// Get column types
	colInfos := source.ColumnInfos()
	types := make([]TypeInfo, len(colInfos))
	for i, col := range colInfos {
		if col.T == nil {
			return nil, errTableUDFColumnTypeIsNil
		}
		types[i] = col.T
	}

	// Initialize the source
	info := source.Init()
	maxThreads := info.MaxThreads
	if maxThreads <= 0 {
		maxThreads = 1
	}

	// For now, implement sequential execution even for parallel sources
	// Full parallel implementation would use goroutine pool
	if maxThreads == 1 {
		// Single-threaded execution
		localState := source.NewLocalState()

		var chunks []*DataChunk
		var currentChunk *DataChunk
		var err error

		if e.projection != nil {
			currentChunk, err = NewDataChunkWithProjection(
				types,
				e.projection,
			)
		} else {
			currentChunk, err = NewDataChunk(types)
		}
		if err != nil {
			return nil, err
		}

		rowIdx := 0
		for {
			row := NewRow(currentChunk, rowIdx)
			hasMore, fillErr := source.FillRow(
				localState,
				row,
			)
			if fillErr != nil {
				return nil, fillErr
			}

			if !hasMore {
				if rowIdx > 0 {
					if err := currentChunk.SetSize(rowIdx); err != nil {
						return nil, err
					}
					chunks = append(
						chunks,
						currentChunk,
					)
				}

				break
			}

			rowIdx++
			if rowIdx >= GetDataChunkCapacity() {
				if err := currentChunk.SetSize(rowIdx); err != nil {
					return nil, err
				}
				chunks = append(
					chunks,
					currentChunk,
				)

				if e.projection != nil {
					currentChunk, err = NewDataChunkWithProjection(
						types,
						e.projection,
					)
				} else {
					currentChunk, err = NewDataChunk(types)
				}
				if err != nil {
					return nil, err
				}
				rowIdx = 0
			}
		}

		return chunks, nil
	}

	// Multi-threaded execution with goroutine pool
	return e.executeParallelRowSourceMultiThreaded(
		source,
		types,
		maxThreads,
	)
}

// executeParallelRowSourceMultiThreaded executes a ParallelRowTableSource with multiple workers.
func (e *TableSourceExecutor) executeParallelRowSourceMultiThreaded(
	source ParallelRowTableSource,
	types []TypeInfo,
	maxThreads int,
) ([]*DataChunk, error) {
	// Result collection
	type workerResult struct {
		chunk *DataChunk
		err   error
	}

	results := make(chan workerResult, maxThreads)
	done := make(chan struct{})
	var chunks []*DataChunk
	var collectionErr error

	// Result collector goroutine
	go func() {
		for result := range results {
			if result.err != nil {
				collectionErr = result.err

				continue
			}
			if result.chunk != nil &&
				result.chunk.GetSize() > 0 {
				chunks = append(
					chunks,
					result.chunk,
				)
			}
		}
		close(done)
	}()

	// Worker function
	worker := func(localState any) {
		var currentChunk *DataChunk
		var err error

		if e.projection != nil {
			currentChunk, err = NewDataChunkWithProjection(
				types,
				e.projection,
			)
		} else {
			currentChunk, err = NewDataChunk(types)
		}
		if err != nil {
			results <- workerResult{err: err}

			return
		}

		rowIdx := 0
		for {
			row := NewRow(currentChunk, rowIdx)
			hasMore, fillErr := source.FillRow(
				localState,
				row,
			)
			if fillErr != nil {
				results <- workerResult{err: fillErr}

				return
			}

			if !hasMore {
				if rowIdx > 0 {
					if err := currentChunk.SetSize(rowIdx); err != nil {
						results <- workerResult{err: err}

						return
					}
					results <- workerResult{chunk: currentChunk}
				}

				return
			}

			rowIdx++
			if rowIdx >= GetDataChunkCapacity() {
				if err := currentChunk.SetSize(rowIdx); err != nil {
					results <- workerResult{err: err}

					return
				}
				results <- workerResult{chunk: currentChunk}

				if e.projection != nil {
					currentChunk, err = NewDataChunkWithProjection(
						types,
						e.projection,
					)
				} else {
					currentChunk, err = NewDataChunk(types)
				}
				if err != nil {
					results <- workerResult{err: err}

					return
				}
				rowIdx = 0
			}
		}
	}

	// Start workers
	var wg sync.WaitGroup
	for range maxThreads {
		wg.Add(1)
		localState := source.NewLocalState()
		go func() {
			defer wg.Done()
			worker(localState)
		}()
	}

	// Wait for all workers to finish
	wg.Wait()
	close(results)
	<-done

	if collectionErr != nil {
		return nil, collectionErr
	}

	return chunks, nil
}

// ExecuteParallelChunkSource executes a ParallelChunkTableSource with parallel workers.
func (e *TableSourceExecutor) ExecuteParallelChunkSource(
	source ParallelChunkTableSource,
) ([]*DataChunk, error) {
	// Get column types
	colInfos := source.ColumnInfos()
	types := make([]TypeInfo, len(colInfos))
	for i, col := range colInfos {
		if col.T == nil {
			return nil, errTableUDFColumnTypeIsNil
		}
		types[i] = col.T
	}

	// Initialize the source
	info := source.Init()
	maxThreads := info.MaxThreads
	if maxThreads <= 0 {
		maxThreads = 1
	}

	// For single-threaded, execute sequentially
	if maxThreads == 1 {
		localState := source.NewLocalState()

		var chunks []*DataChunk
		for {
			var chunk *DataChunk
			var err error

			if e.projection != nil {
				chunk, err = NewDataChunkWithProjection(
					types,
					e.projection,
				)
			} else {
				chunk, err = NewDataChunk(types)
			}
			if err != nil {
				return nil, err
			}

			if err := chunk.SetSize(GetDataChunkCapacity()); err != nil {
				return nil, err
			}

			if err := source.FillChunk(localState, chunk); err != nil {
				return nil, err
			}

			if chunk.GetSize() == 0 {
				break
			}

			chunks = append(chunks, chunk)
		}

		return chunks, nil
	}

	// Multi-threaded execution
	return e.executeParallelChunkSourceMultiThreaded(
		source,
		types,
		maxThreads,
	)
}

// executeParallelChunkSourceMultiThreaded executes a ParallelChunkTableSource with multiple workers.
func (e *TableSourceExecutor) executeParallelChunkSourceMultiThreaded(
	source ParallelChunkTableSource,
	types []TypeInfo,
	maxThreads int,
) ([]*DataChunk, error) {
	type workerResult struct {
		chunk *DataChunk
		err   error
	}

	results := make(chan workerResult, maxThreads)
	done := make(chan struct{})
	var chunks []*DataChunk
	var collectionErr error

	go func() {
		for result := range results {
			if result.err != nil {
				collectionErr = result.err

				continue
			}
			if result.chunk != nil &&
				result.chunk.GetSize() > 0 {
				chunks = append(
					chunks,
					result.chunk,
				)
			}
		}
		close(done)
	}()

	worker := func(localState any) {
		for {
			var chunk *DataChunk
			var err error

			if e.projection != nil {
				chunk, err = NewDataChunkWithProjection(
					types,
					e.projection,
				)
			} else {
				chunk, err = NewDataChunk(types)
			}
			if err != nil {
				results <- workerResult{err: err}

				return
			}

			if err := chunk.SetSize(GetDataChunkCapacity()); err != nil {
				results <- workerResult{err: err}

				return
			}

			if err := source.FillChunk(localState, chunk); err != nil {
				results <- workerResult{err: err}

				return
			}

			if chunk.GetSize() == 0 {
				return
			}

			results <- workerResult{chunk: chunk}
		}
	}

	var wg sync.WaitGroup
	for range maxThreads {
		wg.Add(1)
		localState := source.NewLocalState()
		go func() {
			defer wg.Done()
			worker(localState)
		}()
	}

	wg.Wait()
	close(results)
	<-done

	if collectionErr != nil {
		return nil, collectionErr
	}

	return chunks, nil
}

// ExecuteRowSourceWithContext executes a RowTableSource with context and timeout checking.
// Uses clock.Until() for deterministic timeout checking.
func (e *TableSourceExecutor) ExecuteRowSourceWithContext(
	tfCtx *TableFunctionContext,
	source RowTableSource,
) ([]*DataChunk, error) {
	// Check deadline at start
	if deadline, ok := tfCtx.ctx.Deadline(); ok {
		if tfCtx.clock.Until(deadline) <= 0 {
			return nil, context.DeadlineExceeded
		}
	}

	return e.ExecuteRowSource(source)
}

// ExecuteChunkSourceWithContext executes a ChunkTableSource with context and timeout checking.
// Uses clock.Until() for deterministic timeout checking.
func (e *TableSourceExecutor) ExecuteChunkSourceWithContext(
	tfCtx *TableFunctionContext,
	source ChunkTableSource,
) ([]*DataChunk, error) {
	// Check deadline at start
	if deadline, ok := tfCtx.ctx.Deadline(); ok {
		if tfCtx.clock.Until(deadline) <= 0 {
			return nil, context.DeadlineExceeded
		}
	}

	return e.ExecuteChunkSource(source)
}

// ExecuteParallelRowSourceWithContext executes a ParallelRowTableSource with context support.
// Uses clock.TickerFunc() for periodic timeout checking during parallel execution.
func (e *TableSourceExecutor) ExecuteParallelRowSourceWithContext(
	tfCtx *TableFunctionContext,
	source ParallelRowTableSource,
) ([]*DataChunk, error) {
	// Check deadline at start
	if deadline, ok := tfCtx.ctx.Deadline(); ok {
		if tfCtx.clock.Until(deadline) <= 0 {
			return nil, context.DeadlineExceeded
		}
	}

	// Get column types
	colInfos := source.ColumnInfos()
	types := make([]TypeInfo, len(colInfos))
	for i, col := range colInfos {
		if col.T == nil {
			return nil, errTableUDFColumnTypeIsNil
		}
		types[i] = col.T
	}

	// Initialize the source
	info := source.Init()
	maxThreads := info.MaxThreads
	if maxThreads <= 0 {
		maxThreads = 1
	}

	// Single-threaded execution
	if maxThreads == 1 {
		return e.executeSequentialRowWithContext(
			tfCtx,
			source,
			types,
		)
	}

	// Multi-threaded execution
	return e.executeParallelRowWithContext(
		tfCtx,
		source,
		types,
		maxThreads,
	)
}

// executeSequentialRowWithContext executes a parallel row source sequentially with context.
func (e *TableSourceExecutor) executeSequentialRowWithContext(
	tfCtx *TableFunctionContext,
	source ParallelRowTableSource,
	types []TypeInfo,
) ([]*DataChunk, error) {
	localState := source.NewLocalState()

	var chunks []*DataChunk
	var currentChunk *DataChunk
	var err error

	if e.projection != nil {
		currentChunk, err = NewDataChunkWithProjection(
			types,
			e.projection,
		)
	} else {
		currentChunk, err = NewDataChunk(types)
	}
	if err != nil {
		return nil, err
	}

	rowIdx := 0
	for {
		// Check for context cancellation/timeout
		select {
		case <-tfCtx.ctx.Done():
			return nil, tfCtx.ctx.Err()
		default:
		}

		// Periodically check deadline using clock
		if deadline, ok := tfCtx.ctx.Deadline(); ok {
			if tfCtx.clock.Until(deadline) <= 0 {
				return nil, context.DeadlineExceeded
			}
		}

		row := NewRow(currentChunk, rowIdx)
		hasMore, fillErr := source.FillRow(
			localState,
			row,
		)
		if fillErr != nil {
			return nil, fillErr
		}

		if !hasMore {
			if rowIdx > 0 {
				if err := currentChunk.SetSize(rowIdx); err != nil {
					return nil, err
				}
				chunks = append(
					chunks,
					currentChunk,
				)
			}

			break
		}

		rowIdx++
		if rowIdx >= GetDataChunkCapacity() {
			if err := currentChunk.SetSize(rowIdx); err != nil {
				return nil, err
			}
			chunks = append(chunks, currentChunk)

			if e.projection != nil {
				currentChunk, err = NewDataChunkWithProjection(
					types,
					e.projection,
				)
			} else {
				currentChunk, err = NewDataChunk(types)
			}
			if err != nil {
				return nil, err
			}
			rowIdx = 0
		}
	}

	return chunks, nil
}

// executeParallelRowWithContext executes a parallel row source with multiple workers and context.
func (e *TableSourceExecutor) executeParallelRowWithContext(
	tfCtx *TableFunctionContext,
	source ParallelRowTableSource,
	types []TypeInfo,
	maxThreads int,
) ([]*DataChunk, error) {
	type workerResult struct {
		chunk *DataChunk
		err   error
	}

	results := make(chan workerResult, maxThreads)
	done := make(chan struct{})
	cancelCh := make(chan struct{})
	var chunks []*DataChunk
	var collectionErr error

	// Result collector goroutine
	go func() {
		for result := range results {
			if result.err != nil {
				collectionErr = result.err

				continue
			}
			if result.chunk != nil &&
				result.chunk.GetSize() > 0 {
				chunks = append(
					chunks,
					result.chunk,
				)
			}
		}
		close(done)
	}()

	// Worker function with context checking
	worker := func(localState any) {
		var currentChunk *DataChunk
		var err error

		if e.projection != nil {
			currentChunk, err = NewDataChunkWithProjection(
				types,
				e.projection,
			)
		} else {
			currentChunk, err = NewDataChunk(types)
		}
		if err != nil {
			results <- workerResult{err: err}

			return
		}

		rowIdx := 0
		for {
			// Check for cancellation
			select {
			case <-cancelCh:
				return
			case <-tfCtx.ctx.Done():
				results <- workerResult{err: tfCtx.ctx.Err()}

				return
			default:
			}

			row := NewRow(currentChunk, rowIdx)
			hasMore, fillErr := source.FillRow(
				localState,
				row,
			)
			if fillErr != nil {
				results <- workerResult{err: fillErr}

				return
			}

			if !hasMore {
				if rowIdx > 0 {
					if err := currentChunk.SetSize(rowIdx); err != nil {
						results <- workerResult{err: err}

						return
					}
					results <- workerResult{chunk: currentChunk}
				}

				return
			}

			rowIdx++
			if rowIdx >= GetDataChunkCapacity() {
				if err := currentChunk.SetSize(rowIdx); err != nil {
					results <- workerResult{err: err}

					return
				}
				results <- workerResult{chunk: currentChunk}

				if e.projection != nil {
					currentChunk, err = NewDataChunkWithProjection(
						types,
						e.projection,
					)
				} else {
					currentChunk, err = NewDataChunk(types)
				}
				if err != nil {
					results <- workerResult{err: err}

					return
				}
				rowIdx = 0
			}
		}
	}

	// Start workers
	var wg sync.WaitGroup
	for range maxThreads {
		wg.Add(1)
		localState := source.NewLocalState()
		go func() {
			defer wg.Done()
			worker(localState)
		}()
	}

	// Wait for workers with timeout monitoring
	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	// Monitor for context cancellation/timeout
	select {
	case <-workersDone:
		// Workers finished normally
	case <-tfCtx.ctx.Done():
		close(cancelCh)
		<-workersDone // Wait for workers to acknowledge
		close(results)
		<-done

		return nil, tfCtx.ctx.Err()
	}

	close(results)
	<-done

	if collectionErr != nil {
		return nil, collectionErr
	}

	return chunks, nil
}

// ExecuteParallelChunkSourceWithContext executes a ParallelChunkTableSource with context support.
// Uses clock.Until() for deterministic timeout checking during parallel execution.
func (e *TableSourceExecutor) ExecuteParallelChunkSourceWithContext(
	tfCtx *TableFunctionContext,
	source ParallelChunkTableSource,
) ([]*DataChunk, error) {
	// Check deadline at start
	if deadline, ok := tfCtx.ctx.Deadline(); ok {
		if tfCtx.clock.Until(deadline) <= 0 {
			return nil, context.DeadlineExceeded
		}
	}

	// Get column types
	colInfos := source.ColumnInfos()
	types := make([]TypeInfo, len(colInfos))
	for i, col := range colInfos {
		if col.T == nil {
			return nil, errTableUDFColumnTypeIsNil
		}
		types[i] = col.T
	}

	// Initialize the source
	info := source.Init()
	maxThreads := info.MaxThreads
	if maxThreads <= 0 {
		maxThreads = 1
	}

	// Single-threaded execution
	if maxThreads == 1 {
		return e.executeSequentialChunkWithContext(
			tfCtx,
			source,
			types,
		)
	}

	// Multi-threaded execution
	return e.executeParallelChunkWithContext(
		tfCtx,
		source,
		types,
		maxThreads,
	)
}

// executeSequentialChunkWithContext executes a parallel chunk source sequentially with context.
func (e *TableSourceExecutor) executeSequentialChunkWithContext(
	tfCtx *TableFunctionContext,
	source ParallelChunkTableSource,
	types []TypeInfo,
) ([]*DataChunk, error) {
	localState := source.NewLocalState()

	var chunks []*DataChunk
	for {
		// Check for context cancellation/timeout
		select {
		case <-tfCtx.ctx.Done():
			return nil, tfCtx.ctx.Err()
		default:
		}

		// Check deadline using clock
		if deadline, ok := tfCtx.ctx.Deadline(); ok {
			if tfCtx.clock.Until(deadline) <= 0 {
				return nil, context.DeadlineExceeded
			}
		}

		var chunk *DataChunk
		var err error

		if e.projection != nil {
			chunk, err = NewDataChunkWithProjection(
				types,
				e.projection,
			)
		} else {
			chunk, err = NewDataChunk(types)
		}
		if err != nil {
			return nil, err
		}

		if err := chunk.SetSize(GetDataChunkCapacity()); err != nil {
			return nil, err
		}

		if err := source.FillChunk(localState, chunk); err != nil {
			return nil, err
		}

		if chunk.GetSize() == 0 {
			break
		}

		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// executeParallelChunkWithContext executes a parallel chunk source with multiple workers and context.
func (e *TableSourceExecutor) executeParallelChunkWithContext(
	tfCtx *TableFunctionContext,
	source ParallelChunkTableSource,
	types []TypeInfo,
	maxThreads int,
) ([]*DataChunk, error) {
	type workerResult struct {
		chunk *DataChunk
		err   error
	}

	results := make(chan workerResult, maxThreads)
	done := make(chan struct{})
	cancelCh := make(chan struct{})
	var chunks []*DataChunk
	var collectionErr error

	go func() {
		for result := range results {
			if result.err != nil {
				collectionErr = result.err

				continue
			}
			if result.chunk != nil &&
				result.chunk.GetSize() > 0 {
				chunks = append(
					chunks,
					result.chunk,
				)
			}
		}
		close(done)
	}()

	worker := func(localState any) {
		for {
			// Check for cancellation
			select {
			case <-cancelCh:
				return
			case <-tfCtx.ctx.Done():
				results <- workerResult{err: tfCtx.ctx.Err()}

				return
			default:
			}

			var chunk *DataChunk
			var err error

			if e.projection != nil {
				chunk, err = NewDataChunkWithProjection(
					types,
					e.projection,
				)
			} else {
				chunk, err = NewDataChunk(types)
			}
			if err != nil {
				results <- workerResult{err: err}

				return
			}

			if err := chunk.SetSize(GetDataChunkCapacity()); err != nil {
				results <- workerResult{err: err}

				return
			}

			if err := source.FillChunk(localState, chunk); err != nil {
				results <- workerResult{err: err}

				return
			}

			if chunk.GetSize() == 0 {
				return
			}

			results <- workerResult{chunk: chunk}
		}
	}

	var wg sync.WaitGroup
	for range maxThreads {
		wg.Add(1)
		localState := source.NewLocalState()
		go func() {
			defer wg.Done()
			worker(localState)
		}()
	}

	// Wait for workers with timeout monitoring
	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	// Monitor for context cancellation/timeout
	select {
	case <-workersDone:
		// Workers finished normally
	case <-tfCtx.ctx.Done():
		close(cancelCh)
		<-workersDone
		close(results)
		<-done

		return nil, tfCtx.ctx.Err()
	}

	close(results)
	<-done

	if collectionErr != nil {
		return nil, collectionErr
	}

	return chunks, nil
}
