# Design: Replacement Scans for DuckDB v1.4.3

## Architecture

This change touches two layers:

1. **Parser** (`internal/parser/parser.go`): Detect string literals in FROM position
2. **Binder** (`internal/binder/bind_stmt.go`): Rewrite file path table refs to table function calls

No executor changes needed — the rewritten table function calls use existing infrastructure.

## 1. Parser Changes (internal/parser/parser.go)

### parseTableRef() at line 761

Currently, `parseTableRef()` handles these cases at lines 770-828:
- `VALUES` keyword → ValuesRef (line 770)
- `(` → subquery or parenthesized VALUES (line 777)
- `tokenIdent` → table name or table function call (line 804)
- else → error "expected table name or subquery" (line 827)

**Add a new case** for `tokenString` before the else clause at line 826:

```go
} else if p.current().typ == tokenString {
    // Replacement scan: string literal in FROM position
    // e.g., FROM 'data.csv', FROM 's3://bucket/file.parquet'
    path := p.advance().value
    ref.ReplacementScan = &ReplacementScan{Path: path}
    ref.TableName = path // Use path as table name for alias resolution
}
```

### New AST Node (internal/parser/ast.go)

Add to the TableRef struct or as a new field:

```go
// ReplacementScan represents a file path used directly as a table reference.
// e.g., SELECT * FROM 'data.csv'
type ReplacementScan struct {
    Path string // file path or URL
}
```

Extend TableRef (currently defined around ast.go — find exact location) to include:

```go
type TableRef struct {
    // ... existing fields ...
    ReplacementScan *ReplacementScan // NEW: file path as table reference
}
```

**Note:** TableRef is a struct (not interface) at `ast.go:93-105`. It has fields: TableName, Schema, Catalog, Alias, Subquery, TableFunction, ValuesRef, PivotRef, UnpivotRef, Lateral, TimeTravel. Join-related fields (JoinType, JoinCond, etc.) are on a separate JoinClause type.

## 2. Binder Changes (internal/binder/bind_stmt.go)

### bindFrom() — table reference resolution

In `bindFrom()` (or whichever function resolves individual table references), add handling for `ReplacementScan`:

```go
if ref.ReplacementScan != nil {
    return b.bindReplacementScan(ref)
}
```

### New function: bindReplacementScan()

```go
func (b *Binder) bindReplacementScan(ref TableRef) (*BoundTableRef, error) {
    path := ref.ReplacementScan.Path

    // Detect format from file extension
    funcName := detectTableFunction(path)
    if funcName == "" {
        return nil, b.errorf("cannot determine file format for: %s", path)
    }

    // Rewrite to table function call
    // Create a synthetic TableFunction AST node
    // Note: String literals use parser.Literal (not StringLiteral) with Type: dukdb.TYPE_VARCHAR
    syntheticFunc := &parser.TableFunction{
        Name: funcName,
        Args: []parser.Expr{&parser.Literal{Value: path, Type: dukdb.TYPE_VARCHAR}},
    }

    // Create a synthetic TableRef with the table function
    syntheticRef := parser.TableRef{
        TableFunction: syntheticFunc,
        TableName:     funcName,
        Alias:         ref.Alias,
    }

    // Delegate to existing table function binding
    return b.bindTableFunction(syntheticRef)
}
```

### File extension detection helper

An existing utility `DetectFormatFromPath()` at `internal/io/format.go:155` already maps file extensions to `io.Format` constants. The binder must import `"github.com/dukdb/dukdb-go/internal/io"` (aliased as `fileio` or similar to avoid stdlib collision) and use this helper to map `Format` to function name:

```go
func detectTableFunction(path string) string {
    format := io.DetectFormatFromPath(path)
    switch format {
    case io.FormatCSV:
        return "read_csv_auto"
    case io.FormatParquet:
        return "read_parquet"
    case io.FormatJSON:
        return "read_json_auto"
    case io.FormatNDJSON:
        return "read_ndjson"
    case io.FormatXLSX:
        return "read_xlsx"
    case io.FormatArrow, io.FormatArrowStream:
        return "read_arrow"
    default:
        return ""
    }
}
```

`DetectFormatFromPath()` supports: `.csv`, `.tsv` → FormatCSV; `.parquet`, `.pq` → FormatParquet; `.json` → FormatJSON; `.ndjson`, `.jsonl` → FormatNDJSON; `.xlsx` → FormatXLSX. It also handles compressed extensions (strips `.gz`, `.zst`, etc. before checking).

## 3. Alias Handling

When no alias is given, the table alias defaults to the file path string. DuckDB behavior:

```sql
SELECT * FROM 'data.csv';           -- columns accessible without prefix
SELECT d.* FROM 'data.csv' AS d;    -- alias works normally
```

The existing alias parsing in `parseTableRef()` (after the table reference, it checks for AS keyword) will handle this automatically since we set `ref.TableName = path`.

## Import Dependencies

- `path/filepath` — for `filepath.Ext()` (likely already imported in binder)
- `strings` — for URL query param stripping (already imported)
- No new external dependencies

## Testing Strategy

1. **Parser tests:** `FROM 'file.csv'` parses to TableRef with ReplacementScan
2. **Binder tests:** ReplacementScan rewritten to correct table function
3. **Integration tests:**
   - `SELECT * FROM 'test.csv'` reads CSV file
   - `SELECT * FROM 'test.parquet'` reads Parquet file
   - `SELECT * FROM 'test.json'` reads JSON file
   - `SELECT * FROM 'test.csv' AS t` alias works
   - `FROM 'unknown.xyz'` returns error about unrecognized format
   - Cloud URLs: `FROM 's3://bucket/file.parquet'` (if S3 integration available)
