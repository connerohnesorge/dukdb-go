package engine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkGenerateConnID benchmarks the ID generation function.
// This measures the raw cost of atomic increment for generating unique connection IDs.
// Target: <5ns per call.
func BenchmarkGenerateConnID(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := generateConnID()
		if id == 0 {
			b.Fatal("unexpected zero ID")
		}
	}
}

// BenchmarkGenerateConnID_Concurrent benchmarks concurrent ID generation.
// This stresses the atomic counter under contention.
func BenchmarkGenerateConnID_Concurrent(b *testing.B) {
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := generateConnID()
			if id == 0 {
				b.Error("unexpected zero ID")
				return
			}
		}
	})
}

// BenchmarkEngineConn_ID benchmarks the ID() method on an existing connection.
// This measures the overhead of calling ID() on an already-created connection.
// Should be nearly zero overhead since it just returns a field.
func BenchmarkEngineConn_ID(b *testing.B) {
	engine := NewEngine()
	defer func() {
		require.NoError(b, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer func() {
		require.NoError(b, conn.Close())
	}()

	engineConn := conn.(*EngineConn)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := engineConn.ID()
		if id == 0 {
			b.Fatal("unexpected zero ID")
		}
	}
}

// BenchmarkEngineConn_ID_Concurrent benchmarks concurrent ID() calls on the same connection.
func BenchmarkEngineConn_ID_Concurrent(b *testing.B) {
	engine := NewEngine()
	defer func() {
		require.NoError(b, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer func() {
		require.NoError(b, conn.Close())
	}()

	engineConn := conn.(*EngineConn)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := engineConn.ID()
			if id == 0 {
				b.Error("unexpected zero ID")
				return
			}
		}
	})
}

// BenchmarkEngineConn_IsClosed benchmarks the IsClosed() method.
// This involves mutex locking, so may be slower than ID().
func BenchmarkEngineConn_IsClosed(b *testing.B) {
	engine := NewEngine()
	defer func() {
		require.NoError(b, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer func() {
		require.NoError(b, conn.Close())
	}()

	engineConn := conn.(*EngineConn)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		closed := engineConn.IsClosed()
		if closed {
			b.Fatal("unexpected closed connection")
		}
	}
}

// BenchmarkEngineConn_NewAndID benchmarks creating a new connection and getting its ID.
// This measures the full overhead including connection creation.
func BenchmarkEngineConn_NewAndID(b *testing.B) {
	engine := NewEngine()
	defer func() {
		require.NoError(b, engine.Close())
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn, err := engine.Open(":memory:", nil)
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}

		engineConn := conn.(*EngineConn)
		id := engineConn.ID()
		if id == 0 {
			require.NoError(b, conn.Close())
			b.Fatal("unexpected zero ID")
		}

		require.NoError(b, conn.Close())
	}
}

// BenchmarkGenerateConnID_MultipleGoroutines benchmarks ID generation with specific goroutine counts.
func BenchmarkGenerateConnID_MultipleGoroutines(b *testing.B) {
	goroutineCounts := []int{1, 2, 4, 8, 16, 32}

	for _, numGoroutines := range goroutineCounts {
		b.Run(
			formatGoroutineCount(numGoroutines),
			func(b *testing.B) {
				benchmarkGenerateConnIDWithGoroutines(b, numGoroutines)
			},
		)
	}
}

func formatGoroutineCount(n int) string {
	switch n {
	case 1:
		return "goroutines=1"
	case 2:
		return "goroutines=2"
	case 4:
		return "goroutines=4"
	case 8:
		return "goroutines=8"
	case 16:
		return "goroutines=16"
	case 32:
		return "goroutines=32"
	default:
		return "goroutines=?"
	}
}

func benchmarkGenerateConnIDWithGoroutines(b *testing.B, numGoroutines int) {
	b.ReportAllocs()

	var wg sync.WaitGroup

	opsPerGoroutine := b.N / numGoroutines
	if opsPerGoroutine < 1 {
		opsPerGoroutine = 1
	}

	wg.Add(numGoroutines)

	b.ResetTimer()

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				id := generateConnID()
				if id == 0 {
					b.Error("unexpected zero ID")
					return
				}
			}
		}()
	}

	wg.Wait()
}
