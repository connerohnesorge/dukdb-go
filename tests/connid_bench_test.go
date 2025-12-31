// Package tests provides integration tests and benchmarks for dukdb-go.
package tests

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

// BenchmarkConnId benchmarks the ConnId() function call on an existing connection.
// This measures just the overhead of calling ConnId() on an already-established connection.
// Target: <100ns per call.
func BenchmarkConnId(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}

	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get connection: %v", err)
	}

	defer func() { _ = conn.Close() }()

	// Reset timer to exclude setup
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id, err := dukdb.ConnId(conn)
		if err != nil {
			b.Fatalf("ConnId failed: %v", err)
		}
		if id == 0 {
			b.Fatal("unexpected zero ID")
		}
	}
}

// BenchmarkConnId_NewConnection benchmarks creating a new connection and getting its ID.
// This measures the full overhead including connection creation and ID generation.
func BenchmarkConnId_NewConnection(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		db, err := sql.Open("dukdb", ":memory:")
		if err != nil {
			b.Fatalf("failed to open database: %v", err)
		}

		conn, err := db.Conn(context.Background())
		if err != nil {
			_ = db.Close()
			b.Fatalf("failed to get connection: %v", err)
		}

		id, err := dukdb.ConnId(conn)
		if err != nil {
			_ = conn.Close()
			_ = db.Close()
			b.Fatalf("ConnId failed: %v", err)
		}

		if id == 0 {
			_ = conn.Close()
			_ = db.Close()
			b.Fatal("unexpected zero ID")
		}

		_ = conn.Close()
		_ = db.Close()
	}
}

// BenchmarkConnId_Concurrent benchmarks concurrent ConnId() calls on the same connection.
// This measures thread-safety overhead when multiple goroutines call ConnId() simultaneously.
func BenchmarkConnId_Concurrent(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}

	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get connection: %v", err)
	}

	defer func() { _ = conn.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id, err := dukdb.ConnId(conn)
			if err != nil {
				b.Errorf("ConnId failed: %v", err)
				return
			}
			if id == 0 {
				b.Error("unexpected zero ID")
				return
			}
		}
	})
}

// BenchmarkConnId_ConcurrentNewConnections benchmarks concurrent creation of new connections.
// This stresses the atomic ID generation under contention.
func BenchmarkConnId_ConcurrentNewConnections(b *testing.B) {
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			db, err := sql.Open("dukdb", ":memory:")
			if err != nil {
				b.Errorf("failed to open database: %v", err)
				return
			}

			conn, err := db.Conn(context.Background())
			if err != nil {
				_ = db.Close()
				b.Errorf("failed to get connection: %v", err)
				return
			}

			id, err := dukdb.ConnId(conn)
			if err != nil {
				_ = conn.Close()
				_ = db.Close()
				b.Errorf("ConnId failed: %v", err)
				return
			}

			if id == 0 {
				_ = conn.Close()
				_ = db.Close()
				b.Error("unexpected zero ID")
				return
			}

			_ = conn.Close()
			_ = db.Close()
		}
	})
}

// BenchmarkConnId_MultipleGoroutines benchmarks ConnId with a specific number of goroutines.
func BenchmarkConnId_MultipleGoroutines(b *testing.B) {
	goroutineCounts := []int{1, 2, 4, 8, 16}

	for _, numGoroutines := range goroutineCounts {
		b.Run(
			"goroutines="+string(rune('0'+numGoroutines)),
			func(b *testing.B) {
				benchmarkConnIdWithGoroutines(b, numGoroutines)
			},
		)
	}
}

func benchmarkConnIdWithGoroutines(b *testing.B, numGoroutines int) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}

	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatalf("failed to get connection: %v", err)
	}

	defer func() { _ = conn.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup

	opsPerGoroutine := b.N / numGoroutines
	if opsPerGoroutine < 1 {
		opsPerGoroutine = 1
	}

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				id, err := dukdb.ConnId(conn)
				if err != nil {
					b.Errorf("ConnId failed: %v", err)
					return
				}
				if id == 0 {
					b.Error("unexpected zero ID")
					return
				}
			}
		}()
	}

	wg.Wait()
}
