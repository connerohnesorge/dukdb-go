package server

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPortalWithCursorState(t *testing.T) {
	t.Run("basic portal creation", func(t *testing.T) {
		portal := &Portal{
			Name: "test_portal",
			Statement: &PreparedStatement{
				Name:  "test_stmt",
				Query: "SELECT * FROM users",
			},
			Parameters: []driver.NamedValue{
				{Ordinal: 1, Value: 42},
			},
		}

		assert.Equal(t, "test_portal", portal.Name)
		assert.NotNil(t, portal.Statement)
		assert.Equal(t, 1, len(portal.Parameters))
		assert.False(t, portal.Executed)
		assert.False(t, portal.Suspended)
		assert.False(t, portal.Completed)
		assert.Equal(t, 0, portal.CursorPosition)
	})

	t.Run("unnamed portal", func(t *testing.T) {
		portal := &Portal{
			Name: "", // Empty string = unnamed portal
			Statement: &PreparedStatement{
				Name:  "",
				Query: "SELECT 1",
			},
		}

		assert.Equal(t, "", portal.Name)
	})
}

func TestPortalCacheCursorOperations(t *testing.T) {
	cache := NewPortalCache()

	// Create a portal with test data
	portal := &Portal{
		Name: "cursor_test",
		Statement: &PreparedStatement{
			Name:  "stmt1",
			Query: "SELECT id, name FROM users",
		},
	}
	cache.Set("cursor_test", portal)

	// Cache some rows
	rows := []map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
		{"id": 4, "name": "Diana"},
		{"id": 5, "name": "Eve"},
	}
	columns := []string{"id", "name"}

	t.Run("cache results", func(t *testing.T) {
		ok := cache.CacheResults("cursor_test", rows, columns)
		assert.True(t, ok)

		pos, found := cache.GetCursorPosition("cursor_test")
		assert.True(t, found)
		assert.Equal(t, 0, pos)
	})

	t.Run("fetch first batch", func(t *testing.T) {
		fetchedRows, fetchedCols, hasMore, found := cache.FetchRows("cursor_test", 2)
		assert.True(t, found)
		assert.True(t, hasMore)
		assert.Equal(t, 2, len(fetchedRows))
		assert.Equal(t, columns, fetchedCols)
		assert.Equal(t, 1, fetchedRows[0]["id"])
		assert.Equal(t, 2, fetchedRows[1]["id"])

		// Check cursor position updated
		pos, _ := cache.GetCursorPosition("cursor_test")
		assert.Equal(t, 2, pos)

		// Check suspended state
		assert.True(t, cache.IsSuspended("cursor_test"))
		assert.False(t, cache.IsCompleted("cursor_test"))
	})

	t.Run("fetch second batch", func(t *testing.T) {
		fetchedRows, _, hasMore, found := cache.FetchRows("cursor_test", 2)
		assert.True(t, found)
		assert.True(t, hasMore)
		assert.Equal(t, 2, len(fetchedRows))
		assert.Equal(t, 3, fetchedRows[0]["id"])
		assert.Equal(t, 4, fetchedRows[1]["id"])

		pos, _ := cache.GetCursorPosition("cursor_test")
		assert.Equal(t, 4, pos)
	})

	t.Run("fetch final batch", func(t *testing.T) {
		fetchedRows, _, hasMore, found := cache.FetchRows("cursor_test", 2)
		assert.True(t, found)
		assert.False(t, hasMore) // No more rows
		assert.Equal(t, 1, len(fetchedRows))
		assert.Equal(t, 5, fetchedRows[0]["id"])

		// Check completed state
		assert.False(t, cache.IsSuspended("cursor_test"))
		assert.True(t, cache.IsCompleted("cursor_test"))
	})

	t.Run("fetch from completed portal", func(t *testing.T) {
		fetchedRows, _, hasMore, found := cache.FetchRows("cursor_test", 2)
		assert.True(t, found)
		assert.False(t, hasMore)
		assert.Nil(t, fetchedRows) // No more rows to fetch
	})

	t.Run("reset cursor", func(t *testing.T) {
		ok := cache.ResetCursor("cursor_test")
		assert.True(t, ok)

		pos, _ := cache.GetCursorPosition("cursor_test")
		assert.Equal(t, 0, pos)
		assert.False(t, cache.IsSuspended("cursor_test"))
		assert.False(t, cache.IsCompleted("cursor_test"))
	})

	t.Run("fetch all at once", func(t *testing.T) {
		// Reset and fetch all with maxRows=0 (unlimited)
		cache.ResetCursor("cursor_test")
		fetchedRows, _, hasMore, found := cache.FetchRows("cursor_test", 0)
		assert.True(t, found)
		assert.False(t, hasMore)
		assert.Equal(t, 5, len(fetchedRows))
		assert.True(t, cache.IsCompleted("cursor_test"))
	})
}

func TestPortalCacheNamedPortals(t *testing.T) {
	cache := NewPortalCache()

	// Create multiple named portals
	portals := []struct {
		name  string
		query string
	}{
		{"portal_a", "SELECT * FROM table_a"},
		{"portal_b", "SELECT * FROM table_b"},
		{"portal_c", "SELECT * FROM table_c"},
		{"", "SELECT * FROM unnamed"}, // Unnamed portal
	}

	for _, p := range portals {
		portal := &Portal{
			Name: p.name,
			Statement: &PreparedStatement{
				Name:  p.name,
				Query: p.query,
			},
		}
		cache.Set(p.name, portal)
	}

	t.Run("count portals", func(t *testing.T) {
		assert.Equal(t, 4, cache.Count())
	})

	t.Run("list portal names", func(t *testing.T) {
		names := cache.Names()
		assert.Equal(t, 4, len(names))
		assert.Contains(t, names, "portal_a")
		assert.Contains(t, names, "portal_b")
		assert.Contains(t, names, "portal_c")
		assert.Contains(t, names, "") // Unnamed portal
	})

	t.Run("get named portal", func(t *testing.T) {
		portal, ok := cache.Get("portal_a")
		assert.True(t, ok)
		assert.Equal(t, "portal_a", portal.Name)
		assert.Equal(t, "SELECT * FROM table_a", portal.Statement.Query)
	})

	t.Run("get unnamed portal", func(t *testing.T) {
		portal, ok := cache.Get("")
		assert.True(t, ok)
		assert.Equal(t, "", portal.Name)
		assert.Equal(t, "SELECT * FROM unnamed", portal.Statement.Query)
	})

	t.Run("get nonexistent portal", func(t *testing.T) {
		_, ok := cache.Get("nonexistent")
		assert.False(t, ok)
	})

	t.Run("delete named portal", func(t *testing.T) {
		ok := cache.Delete("portal_b")
		assert.True(t, ok)
		assert.Equal(t, 3, cache.Count())

		_, exists := cache.Get("portal_b")
		assert.False(t, exists)
	})

	t.Run("replace portal with same name", func(t *testing.T) {
		newPortal := &Portal{
			Name: "portal_a",
			Statement: &PreparedStatement{
				Name:  "portal_a",
				Query: "SELECT * FROM new_table_a",
			},
		}
		cache.Set("portal_a", newPortal)

		portal, ok := cache.Get("portal_a")
		assert.True(t, ok)
		assert.Equal(t, "SELECT * FROM new_table_a", portal.Statement.Query)
		assert.Equal(t, 3, cache.Count()) // Count should not increase
	})
}

func TestPortalCacheMarkOperations(t *testing.T) {
	cache := NewPortalCache()

	portal := &Portal{
		Name: "mark_test",
		Statement: &PreparedStatement{
			Name:  "stmt",
			Query: "SELECT 1",
		},
	}
	cache.Set("mark_test", portal)

	t.Run("mark completed", func(t *testing.T) {
		ok := cache.MarkCompleted("mark_test")
		assert.True(t, ok)
		assert.True(t, cache.IsCompleted("mark_test"))
		assert.False(t, cache.IsSuspended("mark_test"))

		p, _ := cache.Get("mark_test")
		assert.True(t, p.Executed)
	})

	t.Run("mark suspended", func(t *testing.T) {
		ok := cache.MarkSuspended("mark_test")
		assert.True(t, ok)
		assert.True(t, cache.IsSuspended("mark_test"))
		assert.False(t, cache.IsCompleted("mark_test"))
	})

	t.Run("mark nonexistent", func(t *testing.T) {
		ok := cache.MarkCompleted("nonexistent")
		assert.False(t, ok)

		ok = cache.MarkSuspended("nonexistent")
		assert.False(t, ok)
	})
}

func TestPortalCacheRemainingRows(t *testing.T) {
	cache := NewPortalCache()

	portal := &Portal{
		Name: "remaining_test",
		Statement: &PreparedStatement{
			Name:  "stmt",
			Query: "SELECT * FROM test",
		},
	}
	cache.Set("remaining_test", portal)

	rows := []map[string]any{
		{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5},
	}
	cache.CacheResults("remaining_test", rows, []string{"id"})

	t.Run("initial remaining rows", func(t *testing.T) {
		remaining, ok := cache.GetRemainingRows("remaining_test")
		assert.True(t, ok)
		assert.Equal(t, 5, remaining)
	})

	t.Run("remaining after partial fetch", func(t *testing.T) {
		cache.FetchRows("remaining_test", 2)
		remaining, ok := cache.GetRemainingRows("remaining_test")
		assert.True(t, ok)
		assert.Equal(t, 3, remaining)
	})

	t.Run("remaining after complete fetch", func(t *testing.T) {
		cache.FetchRows("remaining_test", 10) // Fetch more than remaining
		remaining, ok := cache.GetRemainingRows("remaining_test")
		assert.True(t, ok)
		assert.Equal(t, 0, remaining)
	})

	t.Run("remaining for nonexistent portal", func(t *testing.T) {
		_, ok := cache.GetRemainingRows("nonexistent")
		assert.False(t, ok)
	})
}

func TestPortalCacheOperationsOnNonexistent(t *testing.T) {
	cache := NewPortalCache()

	t.Run("fetch from nonexistent", func(t *testing.T) {
		rows, cols, hasMore, found := cache.FetchRows("nonexistent", 10)
		assert.False(t, found)
		assert.Nil(t, rows)
		assert.Nil(t, cols)
		assert.False(t, hasMore)
	})

	t.Run("cache results for nonexistent", func(t *testing.T) {
		ok := cache.CacheResults("nonexistent", []map[string]any{{"id": 1}}, []string{"id"})
		assert.False(t, ok)
	})

	t.Run("reset cursor for nonexistent", func(t *testing.T) {
		ok := cache.ResetCursor("nonexistent")
		assert.False(t, ok)
	})

	t.Run("get cursor position for nonexistent", func(t *testing.T) {
		_, ok := cache.GetCursorPosition("nonexistent")
		assert.False(t, ok)
	})
}

func TestEnhancedPortal(t *testing.T) {
	t.Run("create enhanced portal", func(t *testing.T) {
		stmt := &PreparedStatement{
			Name:  "test_stmt",
			Query: "SELECT * FROM users WHERE id = $1",
		}
		params := []driver.NamedValue{
			{Ordinal: 1, Value: 42},
		}

		portal := NewEnhancedPortal("test_portal", stmt, params, nil)

		assert.Equal(t, "test_portal", portal.Name)
		assert.Equal(t, stmt, portal.Statement)
		assert.Equal(t, params, portal.Parameters)
		assert.Equal(t, PortalStateReady, portal.State)
		assert.Equal(t, 0, portal.CursorPosition)
		assert.Equal(t, -1, portal.TotalRows)
		assert.False(t, portal.Executed)
	})

	t.Run("portal can fetch", func(t *testing.T) {
		portal := NewEnhancedPortal("test", nil, nil, nil)

		assert.True(t, portal.CanFetch())

		portal.State = PortalStateCompleted
		assert.False(t, portal.CanFetch())

		portal.State = PortalStateSuspended
		assert.True(t, portal.CanFetch())
	})

	t.Run("portal reset", func(t *testing.T) {
		portal := NewEnhancedPortal("test", nil, nil, nil)
		portal.State = PortalStateCompleted
		portal.CursorPosition = 100
		portal.TotalRows = 500
		portal.Executed = true
		portal.CachedRows = []map[string]any{{"id": 1}}

		portal.Reset()

		assert.Equal(t, PortalStateReady, portal.State)
		assert.Equal(t, 0, portal.CursorPosition)
		assert.Equal(t, -1, portal.TotalRows)
		assert.False(t, portal.Executed)
		assert.Nil(t, portal.CachedRows)
	})

	t.Run("remaining rows with cached data", func(t *testing.T) {
		portal := NewEnhancedPortal("test", nil, nil, nil)
		portal.CachedRows = []map[string]any{
			{"id": 1}, {"id": 2}, {"id": 3},
		}
		portal.CursorPosition = 1

		assert.Equal(t, 2, portal.RemainingRows())

		portal.CursorPosition = 3
		assert.Equal(t, 0, portal.RemainingRows())

		portal.CachedRows = nil
		assert.Equal(t, -1, portal.RemainingRows())
	})
}

func TestEnhancedPortalCache(t *testing.T) {
	cache := NewEnhancedPortalCache(nil, nil, nil)

	t.Run("operations on empty cache", func(t *testing.T) {
		_, ok := cache.GetEnhanced("nonexistent")
		assert.False(t, ok)

		assert.Equal(t, 0, cache.Count())
		assert.Empty(t, cache.Names())
	})

	t.Run("delete from cache", func(t *testing.T) {
		// Add a portal first by accessing internal map
		cache.mu.Lock()
		cache.portals["test"] = NewEnhancedPortal("test", nil, nil, nil)
		cache.mu.Unlock()

		ok := cache.Delete("test")
		assert.True(t, ok)

		ok = cache.Delete("nonexistent")
		assert.False(t, ok)
	})

	t.Run("clear cache", func(t *testing.T) {
		cache.mu.Lock()
		cache.portals["p1"] = NewEnhancedPortal("p1", nil, nil, nil)
		cache.portals["p2"] = NewEnhancedPortal("p2", nil, nil, nil)
		cache.mu.Unlock()

		cache.Clear()
		assert.Equal(t, 0, cache.Count())
	})

	t.Run("state management", func(t *testing.T) {
		cache.mu.Lock()
		cache.portals["state_test"] = NewEnhancedPortal("state_test", nil, nil, nil)
		cache.mu.Unlock()

		cache.SetCompleted("state_test")
		portal, _ := cache.GetEnhanced("state_test")
		assert.Equal(t, PortalStateCompleted, portal.State)
		assert.True(t, portal.Executed)

		cache.SetSuspended("state_test")
		portal, _ = cache.GetEnhanced("state_test")
		assert.Equal(t, PortalStateSuspended, portal.State)
	})

	t.Run("cursor position management", func(t *testing.T) {
		cache.mu.Lock()
		cache.portals["cursor_test"] = NewEnhancedPortal("cursor_test", nil, nil, nil)
		cache.mu.Unlock()

		cache.UpdateCursorPosition("cursor_test", 50)
		portal, _ := cache.GetEnhanced("cursor_test")
		assert.Equal(t, 50, portal.CursorPosition)
	})

	t.Run("cache results", func(t *testing.T) {
		cache.mu.Lock()
		cache.portals["results_test"] = NewEnhancedPortal("results_test", nil, nil, nil)
		cache.mu.Unlock()

		rows := []map[string]any{{"id": 1}, {"id": 2}}
		columns := []string{"id"}
		cache.CacheResults("results_test", rows, columns)

		portal, _ := cache.GetEnhanced("results_test")
		assert.Equal(t, rows, portal.CachedRows)
		assert.Equal(t, columns, portal.CachedColumns)
		assert.Equal(t, 2, portal.TotalRows)
	})

	t.Run("fetch rows", func(t *testing.T) {
		cache.mu.Lock()
		portal := NewEnhancedPortal("fetch_test", nil, nil, nil)
		portal.CachedRows = []map[string]any{
			{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5},
		}
		portal.CachedColumns = []string{"id"}
		cache.portals["fetch_test"] = portal
		cache.mu.Unlock()

		// Fetch first batch
		rows, cols, hasMore, err := cache.FetchRows("fetch_test", 2)
		require.NoError(t, err)
		assert.True(t, hasMore)
		assert.Equal(t, 2, len(rows))
		assert.Equal(t, []string{"id"}, cols)

		// Fetch second batch
		rows, _, hasMore, err = cache.FetchRows("fetch_test", 2)
		require.NoError(t, err)
		assert.True(t, hasMore)
		assert.Equal(t, 2, len(rows))

		// Fetch final batch
		rows, _, hasMore, err = cache.FetchRows("fetch_test", 2)
		require.NoError(t, err)
		assert.False(t, hasMore)
		assert.Equal(t, 1, len(rows))

		// Verify completed state
		p, _ := cache.GetEnhanced("fetch_test")
		assert.Equal(t, PortalStateCompleted, p.State)
	})
}

func TestPortalStateString(t *testing.T) {
	tests := []struct {
		state    PortalState
		expected string
	}{
		{PortalStateReady, "ready"},
		{PortalStateActive, "active"},
		{PortalStateSuspended, "suspended"},
		{PortalStateCompleted, "completed"},
		{PortalState(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestSessionPortalCacheIntegration(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	require.NoError(t, err)

	session := NewSession(server, "user", "db", "127.0.0.1:12345")

	// Create a session portal cache wrapper
	spc := NewSessionPortalCache(session, server, nil)

	t.Run("close clears portals", func(t *testing.T) {
		// Add a portal to the session
		portal := &Portal{
			Name: "test_portal",
			Statement: &PreparedStatement{
				Name:  "stmt",
				Query: "SELECT 1",
			},
		}
		session.Portals().Set("test_portal", portal)
		assert.Equal(t, 1, session.Portals().Count())

		// Close the session portal cache
		spc.Close()

		// Verify portals are cleared
		assert.Equal(t, 0, session.Portals().Count())
	})

	err = session.Close()
	require.NoError(t, err)
}
