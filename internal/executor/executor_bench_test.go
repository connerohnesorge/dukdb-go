package executor

import (
	"context"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// BenchmarkSequentialScan_10K benchmarks scanning 10K rows (baseline)
func BenchmarkSequentialScan_10K(b *testing.B) {
	benchmarkSequentialScan(b, 10_000)
}

// BenchmarkSequentialScan_100K benchmarks scanning 100K rows
func BenchmarkSequentialScan_100K(b *testing.B) {
	benchmarkSequentialScan(b, 100_000)
}

func benchmarkSequentialScan(b *testing.B, rowCount int) {
	// Setup: Create table with test data
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE, dukdb.TYPE_VARCHAR}
	table, err := stor.CreateTable("bench_table", columnTypes)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 0; i < rowCount; i++ {
		err := table.AppendRow([]any{int32(i), float64(i) * 1.5, "value_" + string(rune('0'+i%10))})
		if err != nil {
			b.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Create table definition
	tableDef := catalog.NewTableDef("bench_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("value", dukdb.TYPE_DOUBLE),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})

	// Create scan plan
	plan := &planner.PhysicalScan{
		TableName: "bench_table",
		TableDef:  tableDef,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		scanOp, err := NewPhysicalScanOperator(plan, stor)
		if err != nil {
			b.Fatalf("Failed to create scan operator: %v", err)
		}

		// Scan all data
		totalRows := 0
		for {
			chunk, err := scanOp.Next()
			if err != nil {
				b.Fatalf("Scan error: %v", err)
			}
			if chunk == nil {
				break
			}
			totalRows += chunk.Count()
		}

		if totalRows != rowCount {
			b.Fatalf("Expected %d rows, got %d", rowCount, totalRows)
		}
	}
}

// BenchmarkFilteredScan_1Percent benchmarks filtering with 1% selectivity
func BenchmarkFilteredScan_1Percent(b *testing.B) {
	benchmarkFilteredScan(b, 10_000, 100) // 1% of 10K
}

// BenchmarkFilteredScan_10Percent benchmarks filtering with 10% selectivity
func BenchmarkFilteredScan_10Percent(b *testing.B) {
	benchmarkFilteredScan(b, 10_000, 1_000) // 10% of 10K
}

// BenchmarkFilteredScan_50Percent benchmarks filtering with 50% selectivity
func BenchmarkFilteredScan_50Percent(b *testing.B) {
	benchmarkFilteredScan(b, 10_000, 5_000) // 50% of 10K
}

func benchmarkFilteredScan(b *testing.B, rowCount, threshold int) {
	// Setup: Create table with test data
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	table, err := stor.CreateTable("bench_filter", columnTypes)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 0; i < rowCount; i++ {
		err := table.AppendRow([]any{int32(i)})
		if err != nil {
			b.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Create table definition
	tableDef := catalog.NewTableDef("bench_filter", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
	})

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "bench_filter",
		TableDef:  tableDef,
	}

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Filter predicate: id < threshold
	// Note: OpLt is "less than", so id < threshold gives us threshold rows (0..threshold-1)
	predicate := &binder.BoundBinaryExpr{
		Op: parser.OpLt, // Less than
		Left: &binder.BoundColumnRef{
			Column:  "0",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundLiteral{
			Value:   int32(threshold),
			ValType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		scanOp, err := NewPhysicalScanOperator(scanPlan, stor)
		if err != nil {
			b.Fatalf("Failed to create scan operator: %v", err)
		}

		filterOp := NewPhysicalFilterOperator(scanOp, nil, predicate, exec, ctx)

		// Count filtered results
		totalRows := 0
		for {
			chunk, err := filterOp.Next()
			if err != nil {
				b.Fatalf("Filter error: %v", err)
			}
			if chunk == nil {
				break
			}
			totalRows += chunk.Count()
		}

		if totalRows != threshold {
			b.Fatalf("Expected %d rows, got %d", threshold, totalRows)
		}
	}
}

// BenchmarkAggregation_Sum benchmarks SUM aggregation over 10K rows
func BenchmarkAggregation_Sum(b *testing.B) {
	benchmarkAggregation(b, "SUM", 10_000)
}

// BenchmarkAggregation_Count benchmarks COUNT aggregation over 10K rows
func BenchmarkAggregation_Count(b *testing.B) {
	benchmarkAggregationCount(b, 10_000)
}

func benchmarkAggregation(b *testing.B, aggFunc string, rowCount int) {
	// Setup: Create table with test data
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	// Split data into multiple chunks
	chunks := make([]*storage.DataChunk, 0)
	chunkSize := 2048 // StandardVectorSize

	for start := 0; start < rowCount; {
		chunk := storage.NewDataChunk(types)
		for i := 0; i < chunkSize && start < rowCount; i++ {
			chunk.AppendRow([]any{int32(start)})
			start++
		}
		chunks = append(chunks, chunk)
	}

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: chunks,
		types:  typeInfos,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Create aggregate expression
	aggExpr := &binder.BoundFunctionCall{
		Name: aggFunc,
		Args: []binder.BoundExpr{
			&binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_INTEGER,
			},
		},
		ResType: dukdb.TYPE_DOUBLE,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset mock operator for each iteration
		mockChild.index = 0

		aggOp, err := NewPhysicalAggregateOperator(
			mockChild,
			nil,
			[]binder.BoundExpr{}, // No GROUP BY
			[]binder.BoundExpr{aggExpr},
			exec,
			ctx,
		)
		if err != nil {
			b.Fatalf("Failed to create aggregate operator: %v", err)
		}

		// Get result
		resultChunk, err := aggOp.Next()
		if err != nil {
			b.Fatalf("Aggregate error: %v", err)
		}
		if resultChunk == nil {
			b.Fatal("Expected result chunk, got nil")
		}
		if resultChunk.Count() != 1 {
			b.Fatalf("Expected 1 row, got %d", resultChunk.Count())
		}
	}
}

func benchmarkAggregationCount(b *testing.B, rowCount int) {
	// Setup: Create table with test data
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	// Split data into multiple chunks
	chunks := make([]*storage.DataChunk, 0)
	chunkSize := 2048 // StandardVectorSize

	for start := 0; start < rowCount; {
		chunk := storage.NewDataChunk(types)
		for i := 0; i < chunkSize && start < rowCount; i++ {
			chunk.AppendRow([]any{int32(start)})
			start++
		}
		chunks = append(chunks, chunk)
	}

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: chunks,
		types:  typeInfos,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// COUNT(*)
	countExpr := &binder.BoundFunctionCall{
		Name:    "COUNT",
		Star:    true,
		Args:    []binder.BoundExpr{},
		ResType: dukdb.TYPE_BIGINT,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset mock operator for each iteration
		mockChild.index = 0

		aggOp, err := NewPhysicalAggregateOperator(
			mockChild,
			nil,
			[]binder.BoundExpr{}, // No GROUP BY
			[]binder.BoundExpr{countExpr},
			exec,
			ctx,
		)
		if err != nil {
			b.Fatalf("Failed to create aggregate operator: %v", err)
		}

		// Get result
		resultChunk, err := aggOp.Next()
		if err != nil {
			b.Fatalf("Aggregate error: %v", err)
		}
		if resultChunk == nil {
			b.Fatal("Expected result chunk, got nil")
		}
		if resultChunk.Count() != 1 {
			b.Fatalf("Expected 1 row, got %d", resultChunk.Count())
		}
	}
}

// BenchmarkHashJoin_10K benchmarks hash join of two 10K row tables
func BenchmarkHashJoin_10K(b *testing.B) {
	benchmarkHashJoin(b, 10_000)
}

func benchmarkHashJoin(b *testing.B, rowCount int) {
	// Setup: Create two tables for join
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Create left table
	leftTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	leftTable, err := stor.CreateTable("left_table", leftTypes)
	if err != nil {
		b.Fatalf("Failed to create left table: %v", err)
	}

	for i := 0; i < rowCount; i++ {
		err := leftTable.AppendRow([]any{int64(i), "left_" + string(rune('0'+i%10))})
		if err != nil {
			b.Fatalf("Failed to insert into left table: %v", err)
		}
	}

	// Create right table
	rightTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	rightTable, err := stor.CreateTable("right_table", rightTypes)
	if err != nil {
		b.Fatalf("Failed to create right table: %v", err)
	}

	for i := 0; i < rowCount; i++ {
		err := rightTable.AppendRow([]any{int64(i), "right_" + string(rune('0'+i%10))})
		if err != nil {
			b.Fatalf("Failed to insert into right table: %v", err)
		}
	}

	// Create scan operators
	leftScan := &PhysicalScanOperator{
		plan: &planner.PhysicalScan{
			TableName: "left_table",
			Alias:     "l",
			TableDef: &catalog.TableDef{
				Name: "left_table",
				Columns: []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER},
					{Name: "name", Type: dukdb.TYPE_VARCHAR},
				},
			},
		},
		storage: stor,
		scanner: leftTable.Scan(),
		types: []dukdb.TypeInfo{
			&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
			&basicTypeInfo{typ: dukdb.TYPE_VARCHAR},
		},
	}

	rightScan := &PhysicalScanOperator{
		plan: &planner.PhysicalScan{
			TableName: "right_table",
			Alias:     "r",
			TableDef: &catalog.TableDef{
				Name: "right_table",
				Columns: []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER},
					{Name: "name", Type: dukdb.TYPE_VARCHAR},
				},
			},
		},
		storage: stor,
		scanner: rightTable.Scan(),
		types: []dukdb.TypeInfo{
			&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
			&basicTypeInfo{typ: dukdb.TYPE_VARCHAR},
		},
	}

	// Define column bindings for join
	leftColumns := []planner.ColumnBinding{
		{Table: "l", Column: "id", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "l", Column: "name", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
	}
	rightColumns := []planner.ColumnBinding{
		{Table: "r", Column: "id", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "r", Column: "name", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
	}

	// Join condition: l.id = r.id
	condition := &binder.BoundBinaryExpr{
		Op: parser.OpEq, // Equals
		Left: &binder.BoundColumnRef{
			Table:   "l",
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundColumnRef{
			Table:   "r",
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset scanners for each iteration
		leftScan.scanner = leftTable.Scan()
		rightScan.scanner = rightTable.Scan()

		joinOp, err := NewPhysicalHashJoinOperator(
			leftScan,
			rightScan,
			leftColumns,
			rightColumns,
			planner.JoinTypeInner,
			condition,
			exec,
			ctx,
		)
		if err != nil {
			b.Fatalf("Failed to create join operator: %v", err)
		}

		// Count join results
		totalRows := 0
		for {
			chunk, err := joinOp.Next()
			if err != nil {
				b.Fatalf("Join error: %v", err)
			}
			if chunk == nil {
				break
			}
			totalRows += chunk.Count()
		}

		if totalRows != rowCount {
			b.Fatalf("Expected %d rows, got %d", rowCount, totalRows)
		}
	}
}

// BenchmarkSort_10K benchmarks sorting 10K rows
func BenchmarkSort_10K(b *testing.B) {
	benchmarkSort(b, 10_000)
}

func benchmarkSort(b *testing.B, rowCount int) {
	// Setup: Create unsorted data
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	// Split data into multiple chunks (each chunk can hold up to StandardVectorSize rows)
	chunks := make([]*storage.DataChunk, 0)
	chunkSize := 2048 // StandardVectorSize

	for start := rowCount - 1; start >= 0; {
		chunk := storage.NewDataChunk(types)
		for i := 0; i < chunkSize && start >= 0; i++ {
			chunk.AppendRow([]any{int32(start)})
			start--
		}
		chunks = append(chunks, chunk)
	}

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: chunks,
		types:  typeInfos,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// ORDER BY column 0 ASC
	orderBy := []*binder.BoundOrderBy{
		{
			Expr: &binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_INTEGER,
			},
			Desc: false,
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset mock operator for each iteration
		mockChild.index = 0

		sortOp, err := NewPhysicalSortOperator(
			mockChild,
			nil,
			orderBy,
			exec,
			ctx,
		)
		if err != nil {
			b.Fatalf("Failed to create sort operator: %v", err)
		}

		// Collect all results
		totalRows := 0
		for {
			resultChunk, err := sortOp.Next()
			if err != nil {
				b.Fatalf("Sort error: %v", err)
			}
			if resultChunk == nil {
				break
			}
			totalRows += resultChunk.Count()
		}

		if totalRows != rowCount {
			b.Fatalf("Expected %d rows, got %d", rowCount, totalRows)
		}
	}
}
