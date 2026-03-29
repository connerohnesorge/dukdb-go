package engine

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/cache"
	"github.com/dukdb/dukdb-go/internal/planner"
)

const (
	settingQueryCacheEnabled       = "query_cache_enabled"
	settingQueryCacheMaxBytes      = "query_cache_max_bytes"
	settingQueryCacheTTL           = "query_cache_ttl"
	settingQueryCacheParameterMode = "query_cache_parameter_mode"
)

// cacheEnabled returns whether query caching is enabled.
// Safe to call with or without lock held.
func (c *EngineConn) cacheEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(c.getSettingUnsafe(settingQueryCacheEnabled)))
	if value == "" {
		return false
	}
	return value == "1" || value == "true" || value == "on" || value == "yes"
}

// cacheMaxBytes returns the maximum cache size in bytes.
// Safe to call with or without lock held.
func (c *EngineConn) cacheMaxBytes() int64 {
	value := strings.TrimSpace(c.getSettingUnsafe(settingQueryCacheMaxBytes))
	if value == "" {
		return cache.DefaultMaxBytes
	}
	bytes, err := dukdb.ParseThreshold(value)
	if err == nil {
		return bytes
	}
	parsed, parseErr := strconv.ParseInt(value, 10, 64)
	if parseErr == nil {
		return parsed
	}
	return cache.DefaultMaxBytes
}

// cacheTTL returns the cache TTL duration.
// Safe to call with or without lock held.
func (c *EngineConn) cacheTTL() time.Duration {
	value := strings.TrimSpace(c.getSettingUnsafe(settingQueryCacheTTL))
	if value == "" {
		return cache.DefaultTTL
	}
	parsed, err := time.ParseDuration(value)
	if err == nil {
		return parsed
	}
	seconds, parseErr := strconv.ParseInt(value, 10, 64)
	if parseErr == nil {
		return time.Duration(seconds) * time.Second
	}
	return cache.DefaultTTL
}

// cacheParameterMode returns the parameter mode for cache keys.
// Safe to call with or without lock held.
func (c *EngineConn) cacheParameterMode() cache.ParameterMode {
	value := strings.ToLower(strings.TrimSpace(c.getSettingUnsafe(settingQueryCacheParameterMode)))
	if value == string(cache.ParameterModeStructure) {
		return cache.ParameterModeStructure
	}
	return cache.ParameterModeExact
}

// cacheConfigChanged updates the query cache configuration.
// Must be called with c.mu lock held.
func (c *EngineConn) cacheConfigChanged(queryCache *cache.QueryResultCache) {
	if queryCache == nil {
		return
	}
	queryCache.SetMaxBytes(c.cacheMaxBytes())
	queryCache.SetTTL(c.cacheTTL())
}

// getSettingUnsafe retrieves a setting value without acquiring the lock.
// Must be called with c.mu lock held, or when it's known no other goroutine
// will modify settings.
func (c *EngineConn) getSettingUnsafe(key string) string {
	if c.settings == nil {
		return ""
	}
	return c.settings[key]
}

func isCacheableSelect(stmt *binder.BoundSelectStmt) bool {
	if stmt == nil {
		return false
	}
	if selectUsesVolatile(stmt) {
		return false
	}
	if selectUsesTableFunction(stmt) {
		return false
	}
	return true
}

func selectUsesTableFunction(stmt *binder.BoundSelectStmt) bool {
	if stmt == nil {
		return false
	}
	for _, tableRef := range stmt.From {
		if tableRef == nil {
			continue
		}
		if tableRef.TableFunction != nil {
			return true
		}
		if tableRef.Subquery != nil && selectUsesTableFunction(tableRef.Subquery) {
			return true
		}
		if tableRef.ViewQuery != nil && selectUsesTableFunction(tableRef.ViewQuery) {
			return true
		}
		if tableRef.CTERef != nil {
			if tableRef.CTERef.Query != nil && selectUsesTableFunction(tableRef.CTERef.Query) {
				return true
			}
			if tableRef.CTERef.RecursiveQuery != nil && selectUsesTableFunction(tableRef.CTERef.RecursiveQuery) {
				return true
			}
		}
	}
	for _, join := range stmt.Joins {
		if join != nil && join.Table != nil && join.Table.TableFunction != nil {
			return true
		}
		if join != nil && join.Table != nil && join.Table.Subquery != nil && selectUsesTableFunction(join.Table.Subquery) {
			return true
		}
	}
	if stmt.Right != nil && selectUsesTableFunction(stmt.Right) {
		return true
	}
	return false
}

func selectUsesVolatile(stmt *binder.BoundSelectStmt) bool {
	if stmt == nil {
		return false
	}
	for _, col := range stmt.Columns {
		if col != nil && exprUsesVolatile(col.Expr) {
			return true
		}
	}
	for _, expr := range stmt.DistinctOn {
		if exprUsesVolatile(expr) {
			return true
		}
	}
	for _, expr := range stmt.GroupBy {
		if exprUsesVolatile(expr) {
			return true
		}
	}
	for _, order := range stmt.OrderBy {
		if order != nil && exprUsesVolatile(order.Expr) {
			return true
		}
	}
	if exprUsesVolatile(stmt.Where) || exprUsesVolatile(stmt.Having) || exprUsesVolatile(stmt.Qualify) {
		return true
	}
	if exprUsesVolatile(stmt.Limit) || exprUsesVolatile(stmt.Offset) {
		return true
	}
	if stmt.Sample != nil && stmt.Sample.Rows > 0 {
		return true
	}
	for _, join := range stmt.Joins {
		if join != nil && exprUsesVolatile(join.Condition) {
			return true
		}
	}
	if stmt.Right != nil && selectUsesVolatile(stmt.Right) {
		return true
	}
	for _, cte := range stmt.CTEs {
		if cte == nil {
			continue
		}
		if cte.Query != nil && selectUsesVolatile(cte.Query) {
			return true
		}
		if cte.RecursiveQuery != nil && selectUsesVolatile(cte.RecursiveQuery) {
			return true
		}
	}
	return false
}

func exprUsesVolatile(expr binder.BoundExpr) bool {
	if expr == nil {
		return false
	}
	volatileFuncs := map[string]struct{}{
		"RANDOM":            {},
		"NOW":               {},
		"CURRENT_TIMESTAMP": {},
		"CURRENT_TIME":      {},
		"CURRENT_DATE":      {},
		"TODAY":             {},
		"UUID":              {},
		"GEN_RANDOM_UUID":   {},
	}

	switch e := expr.(type) {
	case *binder.BoundFunctionCall:
		if _, ok := volatileFuncs[strings.ToUpper(e.Name)]; ok {
			return true
		}
		for _, arg := range e.Args {
			if exprUsesVolatile(arg) {
				return true
			}
		}
		for _, order := range e.OrderBy {
			if exprUsesVolatile(order.Expr) {
				return true
			}
		}
		return false
	case *binder.BoundScalarUDF:
		return true
	case *binder.BoundBinaryExpr:
		return exprUsesVolatile(e.Left) || exprUsesVolatile(e.Right)
	case *binder.BoundUnaryExpr:
		return exprUsesVolatile(e.Expr)
	case *binder.BoundCastExpr:
		return exprUsesVolatile(e.Expr)
	case *binder.BoundCaseExpr:
		if exprUsesVolatile(e.Operand) || exprUsesVolatile(e.Else) {
			return true
		}
		for _, when := range e.Whens {
			if when != nil && (exprUsesVolatile(when.Condition) || exprUsesVolatile(when.Result)) {
				return true
			}
		}
		return false
	case *binder.BoundBetweenExpr:
		return exprUsesVolatile(e.Expr) || exprUsesVolatile(e.Low) || exprUsesVolatile(e.High)
	case *binder.BoundInListExpr:
		if exprUsesVolatile(e.Expr) {
			return true
		}
		for _, value := range e.Values {
			if exprUsesVolatile(value) {
				return true
			}
		}
		return false
	case *binder.BoundArrayExpr:
		for _, value := range e.Elements {
			if exprUsesVolatile(value) {
				return true
			}
		}
		return false
	case *binder.BoundMapLiteralExpr:
		for _, entry := range e.Entries {
			if exprUsesVolatile(entry.Key) || exprUsesVolatile(entry.Value) {
				return true
			}
		}
		return false
	case *binder.BoundSubscriptExpr:
		return exprUsesVolatile(e.Base) || exprUsesVolatile(e.Index)
	case *binder.BoundInSubqueryExpr:
		if exprUsesVolatile(e.Expr) {
			return true
		}
		return selectUsesVolatile(e.Subquery)
	case *binder.BoundExistsExpr:
		return selectUsesVolatile(e.Subquery)
	case *binder.BoundExtractExpr:
		return exprUsesVolatile(e.Source)
	case *binder.BoundWindowExpr:
		for _, arg := range e.Args {
			if exprUsesVolatile(arg) {
				return true
			}
		}
		for _, expr := range e.PartitionBy {
			if exprUsesVolatile(expr) {
				return true
			}
		}
		for _, order := range e.OrderBy {
			if exprUsesVolatile(order.Expr) {
				return true
			}
		}
		return exprUsesVolatile(e.Filter)
	case *binder.BoundGroupingSetExpr:
		for _, set := range e.Sets {
			for _, value := range set {
				if exprUsesVolatile(value) {
					return true
				}
			}
		}
		return false
	case *binder.BoundGroupingCall:
		for _, arg := range e.Args {
			if exprUsesVolatile(arg) {
				return true
			}
		}
		return false
	case *binder.BoundSequenceCall:
		return true
	default:
		return false
	}
}

func collectPlanTables(plan planner.PhysicalPlan) []string {
	if plan == nil {
		return nil
	}
	seen := make(map[string]struct{})
	collectPlanTablesRecursive(plan, seen)
	result := make([]string, 0, len(seen))
	for name := range seen {
		result = append(result, name)
	}
	return result
}

func collectPlanTablesRecursive(plan planner.PhysicalPlan, seen map[string]struct{}) {
	if plan == nil {
		return
	}
	switch p := plan.(type) {
	case *planner.PhysicalScan:
		seen[p.TableName] = struct{}{}
	case *planner.PhysicalIndexScan:
		seen[p.TableName] = struct{}{}
	case *planner.PhysicalInsert:
		seen[p.Table] = struct{}{}
	case *planner.PhysicalUpdate:
		seen[p.Table] = struct{}{}
	case *planner.PhysicalDelete:
		seen[p.Table] = struct{}{}
	case *planner.PhysicalMerge:
		seen[p.TargetTable] = struct{}{}
	case *planner.PhysicalCopyFrom:
		seen[p.Table] = struct{}{}
	case *planner.PhysicalCreateTable:
		seen[p.Table] = struct{}{}
	case *planner.PhysicalDropTable:
		seen[p.Table] = struct{}{}
	case *planner.PhysicalAlterTable:
		seen[p.Table] = struct{}{}
	case *planner.PhysicalCreateView:
		seen[p.View] = struct{}{}
	case *planner.PhysicalDropView:
		seen[p.View] = struct{}{}
	case *planner.PhysicalCreateIndex:
		if p.Table != "" {
			seen[p.Table] = struct{}{}
		}
	case *planner.PhysicalDropIndex:
	}

	for _, child := range plan.Children() {
		collectPlanTablesRecursive(child, seen)
	}
}

func cacheKeyForQuery(query string, args []driver.NamedValue, mode cache.ParameterMode) (string, error) {
	key, err := cache.BuildCacheKey(query, args, mode)
	if err != nil {
		return "", fmt.Errorf("cache key generation failed: %w", err)
	}
	return key, nil
}
