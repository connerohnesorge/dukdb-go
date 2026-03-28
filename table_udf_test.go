package dukdb

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/quartz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTableUDFTest(t *testing.T) {
	t.Helper()
	setupTestMockBackend(t)
}

// simpleRowSource is a test implementation of RowTableSource.
type simpleRowSource struct {
	cols      []ColumnInfo
	card      *CardinalityInfo
	rowCount  int
	current   int
	initCalls int
}

func (s *simpleRowSource) ColumnInfos() []ColumnInfo {
	return s.cols
}

func (s *simpleRowSource) Cardinality() *CardinalityInfo {
	return s.card
}

func (s *simpleRowSource) Init() {
	s.initCalls++
	s.current = 0
}

func (s *simpleRowSource) FillRow(
	row Row,
) (bool, error) {
	if s.current >= s.rowCount {
		return false, nil
	}
	if err := row.SetRowValue(0, s.current); err != nil {
		return false, err
	}
	s.current++

	return true, nil
}

// simpleChunkSource is a test implementation of ChunkTableSource.
type simpleChunkSource struct {
	cols      []ColumnInfo
	card      *CardinalityInfo
	rowCount  int
	filled    int
	initCalls int
}

func (s *simpleChunkSource) ColumnInfos() []ColumnInfo {
	return s.cols
}

func (s *simpleChunkSource) Cardinality() *CardinalityInfo {
	return s.card
}

func (s *simpleChunkSource) Init() {
	s.initCalls++
	s.filled = 0
}

func (s *simpleChunkSource) FillChunk(
	chunk *DataChunk,
) error {
	remaining := s.rowCount - s.filled
	if remaining <= 0 {
		return chunk.SetSize(0)
	}

	toFill := remaining
	if toFill > GetDataChunkCapacity() {
		toFill = GetDataChunkCapacity()
	}

	for i := range toFill {
		if err := chunk.SetValue(0, i, s.filled+i); err != nil {
			return err
		}
	}
	s.filled += toFill

	return chunk.SetSize(toFill)
}

// parallelRowSource is a test implementation of ParallelRowTableSource.
type parallelRowSource struct {
	cols       []ColumnInfo
	card       *CardinalityInfo
	maxThreads int
	rowCount   int64
	current    int64
}

func (s *parallelRowSource) ColumnInfos() []ColumnInfo {
	return s.cols
}

func (s *parallelRowSource) Cardinality() *CardinalityInfo {
	return s.card
}

func (s *parallelRowSource) Init() ParallelTableSourceInfo {
	atomic.StoreInt64(&s.current, 0)

	return ParallelTableSourceInfo{
		MaxThreads: s.maxThreads,
	}
}

func (s *parallelRowSource) NewLocalState() any {
	return &struct{}{}
}

func (s *parallelRowSource) FillRow(
	localState any,
	row Row,
) (bool, error) {
	current := atomic.AddInt64(&s.current, 1) - 1
	if current >= s.rowCount {
		return false, nil
	}
	if err := row.SetRowValue(0, int(current)); err != nil {
		return false, err
	}

	return true, nil
}

func TestRegisterTableUDF_EmptyName(
	t *testing.T,
) {
	setupTableUDFTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	f := RowTableFunction{
		Config: TableFunctionConfig{},
		BindArguments: func(named map[string]any, args ...any) (RowTableSource, error) {
			return &simpleRowSource{
				cols: []ColumnInfo{
					{Name: "id", T: intInfo},
				},
				rowCount: 0,
			}, nil
		},
	}

	err = RegisterTableUDF(conn, "", f)
	assert.ErrorIs(t, err, errTableUDFNoName)
}

func TestRegisterTableUDF_MissingBindArgs(
	t *testing.T,
) {
	setupTableUDFTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	f := RowTableFunction{
		Config: TableFunctionConfig{},
		// No BindArguments or BindArgumentsContext
	}

	err = RegisterTableUDF(conn, "test_func", f)
	assert.ErrorIs(
		t,
		err,
		errTableUDFMissingBindArgs,
	)
}

func TestRegisterTableUDF_NilArgument(
	t *testing.T,
) {
	setupTableUDFTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	f := RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{
				nil,
			}, // nil argument type
		},
		BindArguments: func(named map[string]any, args ...any) (RowTableSource, error) {
			return &simpleRowSource{
				cols: []ColumnInfo{
					{Name: "id", T: intInfo},
				},
				rowCount: 0,
			}, nil
		},
	}

	err = RegisterTableUDF(conn, "test_func", f)
	assert.ErrorIs(
		t,
		err,
		errTableUDFArgumentIsNil,
	)
}

func TestRegisterTableUDF_NilNamedArgument(
	t *testing.T,
) {
	setupTableUDFTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	f := RowTableFunction{
		Config: TableFunctionConfig{
			NamedArguments: map[string]TypeInfo{
				"count": nil,
			}, // nil named argument type
		},
		BindArguments: func(named map[string]any, args ...any) (RowTableSource, error) {
			return &simpleRowSource{
				cols: []ColumnInfo{
					{Name: "id", T: intInfo},
				},
				rowCount: 0,
			}, nil
		},
	}

	err = RegisterTableUDF(conn, "test_func", f)
	assert.ErrorIs(
		t,
		err,
		errTableUDFArgumentIsNil,
	)
}

func TestRegisterTableUDF_RowTableFunction(
	t *testing.T,
) {
	setupTableUDFTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	f := RowTableFunction{
		Config: TableFunctionConfig{},
		BindArguments: func(named map[string]any, args ...any) (RowTableSource, error) {
			return &simpleRowSource{
				cols: []ColumnInfo{
					{Name: "id", T: intInfo},
				},
				rowCount: 10,
			}, nil
		},
	}

	err = RegisterTableUDF(
		conn,
		"row_test_func",
		f,
	)
	assert.NoError(t, err)
}

func TestRegisterTableUDF_ChunkTableFunction(
	t *testing.T,
) {
	setupTableUDFTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	f := ChunkTableFunction{
		Config: TableFunctionConfig{},
		BindArguments: func(named map[string]any, args ...any) (ChunkTableSource, error) {
			return &simpleChunkSource{
				cols: []ColumnInfo{
					{Name: "id", T: intInfo},
				},
				rowCount: 10,
			}, nil
		},
	}

	err = RegisterTableUDF(
		conn,
		"chunk_test_func",
		f,
	)
	assert.NoError(t, err)
}

func TestRegisterTableUDF_ParallelRowTableFunction(
	t *testing.T,
) {
	setupTableUDFTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	f := ParallelRowTableFunction{
		Config: TableFunctionConfig{},
		BindArguments: func(named map[string]any, args ...any) (ParallelRowTableSource, error) {
			return &parallelRowSource{
				cols: []ColumnInfo{
					{Name: "id", T: intInfo},
				},
				maxThreads: 4,
				rowCount:   100,
			}, nil
		},
	}

	err = RegisterTableUDF(
		conn,
		"parallel_row_test_func",
		f,
	)
	assert.NoError(t, err)
}

func TestRegisterTableUDF_WithContext(
	t *testing.T,
) {
	setupTableUDFTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	f := RowTableFunction{
		Config: TableFunctionConfig{},
		BindArgumentsContext: func(ctx context.Context, named map[string]any, args ...any) (RowTableSource, error) {
			// Context-aware binding
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			return &simpleRowSource{
				cols: []ColumnInfo{
					{Name: "id", T: intInfo},
				},
				rowCount: 5,
			}, nil
		},
	}

	err = RegisterTableUDF(
		conn,
		"context_test_func",
		f,
	)
	assert.NoError(t, err)
}

func TestRegisterTableUDF_DuplicateName(
	t *testing.T,
) {
	setupTableUDFTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	f := RowTableFunction{
		Config: TableFunctionConfig{},
		BindArguments: func(named map[string]any, args ...any) (RowTableSource, error) {
			return &simpleRowSource{
				cols: []ColumnInfo{
					{Name: "id", T: intInfo},
				},
				rowCount: 10,
			}, nil
		},
	}

	err = RegisterTableUDF(conn, "dup_func", f)
	require.NoError(t, err)

	// Register again with same name
	err = RegisterTableUDF(conn, "dup_func", f)
	assert.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"already registered",
	)
}

func TestRegisterTableUDF_WithArguments(
	t *testing.T,
) {
	setupTableUDFTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)

	f := RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{
				intInfo,
				varcharInfo,
			},
			NamedArguments: map[string]TypeInfo{
				"limit": intInfo,
			},
		},
		BindArguments: func(named map[string]any, args ...any) (RowTableSource, error) {
			count := 10
			if len(args) > 0 {
				if c, ok := args[0].(int); ok {
					count = c
				}
			}
			if limit, ok := named["limit"]; ok {
				if l, ok := limit.(int); ok &&
					l < count {
					count = l
				}
			}

			return &simpleRowSource{
				cols: []ColumnInfo{
					{Name: "id", T: intInfo},
				},
				rowCount: count,
			}, nil
		},
	}

	err = RegisterTableUDF(
		conn,
		"args_test_func",
		f,
	)
	assert.NoError(t, err)
}

// Test wrapper functions
func TestParallelRowTSWrapper(t *testing.T) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &simpleRowSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		card: &CardinalityInfo{
			Cardinality: 10,
			Exact:       true,
		},
		rowCount: 10,
	}

	wrapper := parallelRowTSWrapper{s: source}

	// Test ColumnInfos
	cols := wrapper.ColumnInfos()
	assert.Len(t, cols, 1)
	assert.Equal(t, "id", cols[0].Name)

	// Test Cardinality
	card := wrapper.Cardinality()
	require.NotNil(t, card)
	assert.Equal(t, uint(10), card.Cardinality)
	assert.True(t, card.Exact)

	// Test Init
	info := wrapper.Init()
	assert.Equal(t, 1, info.MaxThreads)
	assert.Equal(t, 1, source.initCalls)

	// Test NewLocalState
	state := wrapper.NewLocalState()
	assert.Equal(t, struct{}{}, state)
}

func TestParallelChunkTSWrapper(t *testing.T) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &simpleChunkSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		card: &CardinalityInfo{
			Cardinality: 100,
			Exact:       false,
		},
		rowCount: 100,
	}

	wrapper := parallelChunkTSWrapper{s: source}

	// Test ColumnInfos
	cols := wrapper.ColumnInfos()
	assert.Len(t, cols, 1)
	assert.Equal(t, "id", cols[0].Name)

	// Test Cardinality
	card := wrapper.Cardinality()
	require.NotNil(t, card)
	assert.Equal(t, uint(100), card.Cardinality)
	assert.False(t, card.Exact)

	// Test Init
	info := wrapper.Init()
	assert.Equal(t, 1, info.MaxThreads)
	assert.Equal(t, 1, source.initCalls)

	// Test NewLocalState
	state := wrapper.NewLocalState()
	assert.Equal(t, struct{}{}, state)
}

// Tests for TableSourceExecutor

func TestExecuteRowSource(t *testing.T) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &simpleRowSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		rowCount: 10,
	}

	executor := NewTableSourceExecutor()
	chunks, err := executor.ExecuteRowSource(
		source,
	)
	require.NoError(t, err)

	// Should have called Init
	assert.Equal(t, 1, source.initCalls)

	// Should have at least one chunk
	require.NotEmpty(t, chunks)

	// Count total rows
	totalRows := 0
	for _, chunk := range chunks {
		totalRows += chunk.GetSize()
	}
	assert.Equal(t, 10, totalRows)
}

func TestExecuteRowSource_LargeDataset(
	t *testing.T,
) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	// Create a source with more rows than chunk capacity
	rowCount := GetDataChunkCapacity()*2 + 100
	source := &simpleRowSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		rowCount: rowCount,
	}

	executor := NewTableSourceExecutor()
	chunks, err := executor.ExecuteRowSource(
		source,
	)
	require.NoError(t, err)

	// Should have multiple chunks
	assert.GreaterOrEqual(t, len(chunks), 2)

	// Count total rows
	totalRows := 0
	for _, chunk := range chunks {
		totalRows += chunk.GetSize()
	}
	assert.Equal(t, rowCount, totalRows)
}

func TestExecuteChunkSource(t *testing.T) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &simpleChunkSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		rowCount: 100,
	}

	executor := NewTableSourceExecutor()
	chunks, err := executor.ExecuteChunkSource(
		source,
	)
	require.NoError(t, err)

	// Should have called Init
	assert.Equal(t, 1, source.initCalls)

	// Should have at least one chunk
	require.NotEmpty(t, chunks)

	// Count total rows
	totalRows := 0
	for _, chunk := range chunks {
		totalRows += chunk.GetSize()
	}
	assert.Equal(t, 100, totalRows)
}

func TestExecuteParallelRowSource_SingleThread(
	t *testing.T,
) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &parallelRowSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		maxThreads: 1,
		rowCount:   50,
	}

	executor := NewTableSourceExecutor()
	chunks, err := executor.ExecuteParallelRowSource(
		source,
	)
	require.NoError(t, err)

	// Count total rows
	totalRows := 0
	for _, chunk := range chunks {
		totalRows += chunk.GetSize()
	}
	assert.Equal(t, 50, totalRows)
}

func TestExecuteRowSource_WithProjection(
	t *testing.T,
) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)

	// Source with two columns
	source := &twoColumnRowSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
			{Name: "name", T: varcharInfo},
		},
		rowCount: 5,
	}

	// Only project the first column
	executor := NewTableSourceExecutor().WithProjection([]int{0})
	chunks, err := executor.ExecuteRowSource(
		source,
	)
	require.NoError(t, err)

	// Should have rows
	require.NotEmpty(t, chunks)
	totalRows := 0
	for _, chunk := range chunks {
		totalRows += chunk.GetSize()
	}
	assert.Equal(t, 5, totalRows)
}

// twoColumnRowSource is a test source with two columns
type twoColumnRowSource struct {
	cols     []ColumnInfo
	rowCount int
	current  int
}

func (s *twoColumnRowSource) ColumnInfos() []ColumnInfo {
	return s.cols
}

func (s *twoColumnRowSource) Cardinality() *CardinalityInfo {
	return &CardinalityInfo{
		Cardinality: uint(s.rowCount),
		Exact:       true,
	}
}

func (s *twoColumnRowSource) Init() {
	s.current = 0
}

func (s *twoColumnRowSource) FillRow(
	row Row,
) (bool, error) {
	if s.current >= s.rowCount {
		return false, nil
	}
	if row.IsProjected(0) {
		if err := row.SetRowValue(0, s.current); err != nil {
			return false, err
		}
	}
	if row.IsProjected(1) {
		if err := row.SetRowValue(1, "name"); err != nil {
			return false, err
		}
	}
	s.current++

	return true, nil
}

// Tests for TableFunctionContext and deterministic testing

func TestTableFunctionContext_NewWithNilClock(
	t *testing.T,
) {
	ctx := context.Background()
	tfCtx := NewTableFunctionContext(ctx, nil)

	require.NotNil(t, tfCtx)
	assert.NotNil(t, tfCtx.Clock())
	assert.Equal(t, ctx, tfCtx.Context())
}

func TestTableFunctionContext_WithClock(
	t *testing.T,
) {
	ctx := context.Background()
	tfCtx := NewTableFunctionContext(ctx, nil)

	// Create a new context with a different clock (mock)
	mClock := quartz.NewMock(t)
	tfCtx2 := tfCtx.WithClock(mClock)

	require.NotNil(t, tfCtx2)
	assert.Equal(t, mClock, tfCtx2.Clock())
	assert.Equal(t, ctx, tfCtx2.Context())
}

func TestTableSourceExecutor_WithClock(
	t *testing.T,
) {
	executor := NewTableSourceExecutor()
	assert.NotNil(t, executor.clock)

	mClock := quartz.NewMock(t)
	executor2 := executor.WithClock(mClock)
	assert.Equal(t, mClock, executor2.clock)
}

func TestExecuteRowSourceWithContext_DeadlineExceeded(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	// Set the mock time to a fixed point
	startTime := mClock.Now()

	// Create a context with a deadline in the past (relative to mock clock)
	deadline := startTime.Add(-1 * time.Second)
	ctx, cancel := context.WithDeadline(
		context.Background(),
		deadline,
	)
	defer cancel()

	tfCtx := NewTableFunctionContext(ctx, mClock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &simpleRowSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		rowCount: 10,
	}

	executor := NewTableSourceExecutor()
	_, err := executor.ExecuteRowSourceWithContext(
		tfCtx,
		source,
	)
	assert.ErrorIs(
		t,
		err,
		context.DeadlineExceeded,
	)
}

func TestExecuteRowSourceWithContext_Success(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	startTime := mClock.Now()

	// Create a context with a deadline in the future
	deadline := startTime.Add(1 * time.Hour)
	ctx, cancel := context.WithDeadline(
		context.Background(),
		deadline,
	)
	defer cancel()

	tfCtx := NewTableFunctionContext(ctx, mClock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &simpleRowSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		rowCount: 5,
	}

	executor := NewTableSourceExecutor()
	chunks, err := executor.ExecuteRowSourceWithContext(
		tfCtx,
		source,
	)
	require.NoError(t, err)

	totalRows := 0
	for _, chunk := range chunks {
		totalRows += chunk.GetSize()
	}
	assert.Equal(t, 5, totalRows)
}

func TestExecuteChunkSourceWithContext_DeadlineExceeded(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	startTime := mClock.Now()

	// Create a context with a deadline in the past
	deadline := startTime.Add(-1 * time.Second)
	ctx, cancel := context.WithDeadline(
		context.Background(),
		deadline,
	)
	defer cancel()

	tfCtx := NewTableFunctionContext(ctx, mClock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &simpleChunkSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		rowCount: 10,
	}

	executor := NewTableSourceExecutor()
	_, err := executor.ExecuteChunkSourceWithContext(
		tfCtx,
		source,
	)
	assert.ErrorIs(
		t,
		err,
		context.DeadlineExceeded,
	)
}

func TestExecuteParallelRowSourceWithContext_DeadlineExceeded(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	startTime := mClock.Now()

	// Create a context with a deadline in the past
	deadline := startTime.Add(-1 * time.Second)
	ctx, cancel := context.WithDeadline(
		context.Background(),
		deadline,
	)
	defer cancel()

	tfCtx := NewTableFunctionContext(ctx, mClock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &parallelRowSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		maxThreads: 2,
		rowCount:   100,
	}

	executor := NewTableSourceExecutor()
	_, err := executor.ExecuteParallelRowSourceWithContext(
		tfCtx,
		source,
	)
	assert.ErrorIs(
		t,
		err,
		context.DeadlineExceeded,
	)
}

func TestExecuteParallelRowSourceWithContext_ContextCancelled(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)

	// Create a cancellable context
	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	tfCtx := NewTableFunctionContext(ctx, mClock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	// Source that produces many rows - but we'll cancel before it finishes
	source := &slowParallelRowSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		maxThreads: 2,
		rowCount:   1000000, // Very large to ensure we cancel before completion
		cancel:     cancel,  // Cancel after a few rows
	}

	executor := NewTableSourceExecutor()
	_, err := executor.ExecuteParallelRowSourceWithContext(
		tfCtx,
		source,
	)
	assert.ErrorIs(t, err, context.Canceled)
}

// slowParallelRowSource cancels context after producing some rows
type slowParallelRowSource struct {
	cols       []ColumnInfo
	maxThreads int
	rowCount   int64
	current    int64
	cancel     context.CancelFunc
}

func (s *slowParallelRowSource) ColumnInfos() []ColumnInfo {
	return s.cols
}

func (s *slowParallelRowSource) Cardinality() *CardinalityInfo {
	return nil
}

func (s *slowParallelRowSource) Init() ParallelTableSourceInfo {
	atomic.StoreInt64(&s.current, 0)

	return ParallelTableSourceInfo{
		MaxThreads: s.maxThreads,
	}
}

func (s *slowParallelRowSource) NewLocalState() any {
	return &struct{}{}
}

func (s *slowParallelRowSource) FillRow(
	localState any,
	row Row,
) (bool, error) {
	current := atomic.AddInt64(&s.current, 1) - 1

	// Cancel after 10 rows
	if current == 10 {
		s.cancel()
	}

	if current >= s.rowCount {
		return false, nil
	}
	if err := row.SetRowValue(0, int(current)); err != nil {
		return false, err
	}

	return true, nil
}

func TestExecuteParallelChunkSourceWithContext_DeadlineExceeded(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	startTime := mClock.Now()

	// Create a context with a deadline in the past
	deadline := startTime.Add(-1 * time.Second)
	ctx, cancel := context.WithDeadline(
		context.Background(),
		deadline,
	)
	defer cancel()

	tfCtx := NewTableFunctionContext(ctx, mClock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &parallelChunkSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		maxThreads: 2,
		rowCount:   100,
	}

	executor := NewTableSourceExecutor()
	_, err := executor.ExecuteParallelChunkSourceWithContext(
		tfCtx,
		source,
	)
	assert.ErrorIs(
		t,
		err,
		context.DeadlineExceeded,
	)
}

// parallelChunkSource is a test implementation of ParallelChunkTableSource
type parallelChunkSource struct {
	cols       []ColumnInfo
	maxThreads int
	rowCount   int64
	filled     int64
}

func (s *parallelChunkSource) ColumnInfos() []ColumnInfo {
	return s.cols
}

func (s *parallelChunkSource) Cardinality() *CardinalityInfo {
	return &CardinalityInfo{
		Cardinality: uint(s.rowCount),
		Exact:       true,
	}
}

func (s *parallelChunkSource) Init() ParallelTableSourceInfo {
	atomic.StoreInt64(&s.filled, 0)

	return ParallelTableSourceInfo{
		MaxThreads: s.maxThreads,
	}
}

func (s *parallelChunkSource) NewLocalState() any {
	return &struct{}{}
}

func (s *parallelChunkSource) FillChunk(
	localState any,
	chunk *DataChunk,
) error {
	remaining := s.rowCount - atomic.LoadInt64(
		&s.filled,
	)
	if remaining <= 0 {
		return chunk.SetSize(0)
	}

	toFill := int64(GetDataChunkCapacity())
	if toFill > remaining {
		toFill = remaining
	}

	start := atomic.AddInt64(
		&s.filled,
		toFill,
	) - toFill
	if start >= s.rowCount {
		return chunk.SetSize(0)
	}

	actualFill := int(toFill)
	for i := range actualFill {
		if err := chunk.SetValue(0, i, int(start)+i); err != nil {
			return err
		}
	}

	return chunk.SetSize(actualFill)
}

func TestExecuteParallelRowSourceWithContext_Success(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)

	// Use context without deadline for success test
	ctx := context.Background()
	tfCtx := NewTableFunctionContext(ctx, mClock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &parallelRowSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		maxThreads: 1, // Single-threaded for determinism
		rowCount:   20,
	}

	executor := NewTableSourceExecutor()
	chunks, err := executor.ExecuteParallelRowSourceWithContext(
		tfCtx,
		source,
	)
	require.NoError(t, err)

	totalRows := 0
	for _, chunk := range chunks {
		totalRows += chunk.GetSize()
	}
	assert.Equal(t, 20, totalRows)
}

func TestExecuteParallelChunkSourceWithContext_Success(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)

	// Use context without deadline for success test
	ctx := context.Background()
	tfCtx := NewTableFunctionContext(ctx, mClock)

	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	source := &parallelChunkSource{
		cols: []ColumnInfo{
			{Name: "id", T: intInfo},
		},
		maxThreads: 1,
		rowCount:   50,
	}

	executor := NewTableSourceExecutor()
	chunks, err := executor.ExecuteParallelChunkSourceWithContext(
		tfCtx,
		source,
	)
	require.NoError(t, err)

	totalRows := 0
	for _, chunk := range chunks {
		totalRows += chunk.GetSize()
	}
	assert.Equal(t, 50, totalRows)
}

// Benchmarks for Table UDF execution variants

// benchmarkRowSource creates a row source for benchmarking
type benchmarkRowSource struct {
	cols     []ColumnInfo
	rowCount int
	current  int
}

func (s *benchmarkRowSource) ColumnInfos() []ColumnInfo { return s.cols }

func (s *benchmarkRowSource) Cardinality() *CardinalityInfo {
	return &CardinalityInfo{
		Cardinality: uint(s.rowCount),
		Exact:       true,
	}
}

func (s *benchmarkRowSource) Init() { s.current = 0 }

func (s *benchmarkRowSource) FillRow(
	row Row,
) (bool, error) {
	if s.current >= s.rowCount {
		return false, nil
	}
	if err := row.SetRowValue(0, s.current); err != nil {
		return false, err
	}
	s.current++

	return true, nil
}

// benchmarkChunkSource creates a chunk source for benchmarking
type benchmarkChunkSource struct {
	cols     []ColumnInfo
	rowCount int
	filled   int
}

func (s *benchmarkChunkSource) ColumnInfos() []ColumnInfo { return s.cols }

func (s *benchmarkChunkSource) Cardinality() *CardinalityInfo {
	return &CardinalityInfo{
		Cardinality: uint(s.rowCount),
		Exact:       true,
	}
}

func (s *benchmarkChunkSource) Init() { s.filled = 0 }

func (s *benchmarkChunkSource) FillChunk(
	chunk *DataChunk,
) error {
	remaining := s.rowCount - s.filled
	if remaining <= 0 {
		return chunk.SetSize(0)
	}
	toFill := remaining
	if toFill > GetDataChunkCapacity() {
		toFill = GetDataChunkCapacity()
	}
	for i := range toFill {
		if err := chunk.SetValue(0, i, s.filled+i); err != nil {
			return err
		}
	}
	s.filled += toFill

	return chunk.SetSize(toFill)
}

func BenchmarkExecuteRowSource_Small(
	b *testing.B,
) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	executor := NewTableSourceExecutor()

	b.ResetTimer()
	for range b.N {
		source := &benchmarkRowSource{
			cols: []ColumnInfo{
				{Name: "id", T: intInfo},
			},
			rowCount: 100,
		}
		_, _ = executor.ExecuteRowSource(source)
	}
}

func BenchmarkExecuteRowSource_Large(
	b *testing.B,
) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	executor := NewTableSourceExecutor()

	b.ResetTimer()
	for range b.N {
		source := &benchmarkRowSource{
			cols: []ColumnInfo{
				{Name: "id", T: intInfo},
			},
			rowCount: 10000,
		}
		_, _ = executor.ExecuteRowSource(source)
	}
}

func BenchmarkExecuteChunkSource_Small(
	b *testing.B,
) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	executor := NewTableSourceExecutor()

	b.ResetTimer()
	for range b.N {
		source := &benchmarkChunkSource{
			cols: []ColumnInfo{
				{Name: "id", T: intInfo},
			},
			rowCount: 100,
		}
		_, _ = executor.ExecuteChunkSource(source)
	}
}

func BenchmarkExecuteChunkSource_Large(
	b *testing.B,
) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	executor := NewTableSourceExecutor()

	b.ResetTimer()
	for range b.N {
		source := &benchmarkChunkSource{
			cols: []ColumnInfo{
				{Name: "id", T: intInfo},
			},
			rowCount: 10000,
		}
		_, _ = executor.ExecuteChunkSource(source)
	}
}

func BenchmarkExecuteParallelRowSource_SingleThread(
	b *testing.B,
) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	executor := NewTableSourceExecutor()

	b.ResetTimer()
	for range b.N {
		source := &parallelRowSource{
			cols: []ColumnInfo{
				{Name: "id", T: intInfo},
			},
			maxThreads: 1,
			rowCount:   1000,
		}
		_, _ = executor.ExecuteParallelRowSource(
			source,
		)
	}
}

func BenchmarkExecuteParallelRowSource_MultiThread(
	b *testing.B,
) {
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	executor := NewTableSourceExecutor()

	b.ResetTimer()
	for range b.N {
		source := &parallelRowSource{
			cols: []ColumnInfo{
				{Name: "id", T: intInfo},
			},
			maxThreads: 4,
			rowCount:   1000,
		}
		_, _ = executor.ExecuteParallelRowSource(
			source,
		)
	}
}
