// Package compatibility provides a test framework for verifying dukdb-go
// compatibility with the duckdb-go reference implementation.
package compatibility

import (
	"context"
	"database/sql"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import engine to register the backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

// GetTableNamesCompatibilityTests contains all GetTableNames compatibility tests.
var GetTableNamesCompatibilityTests = []CompatibilityTest{
	// Basic SELECT patterns
	{Name: "GTN_SimpleSelect", Category: "gettablenames", Test: testGTNSimpleSelect},
	{Name: "GTN_SelectWithColumns", Category: "gettablenames", Test: testGTNSelectWithColumns},
	{Name: "GTN_SelectWithWhere", Category: "gettablenames", Test: testGTNSelectWithWhere},

	// JOIN patterns
	{Name: "GTN_InnerJoin", Category: "gettablenames", Test: testGTNInnerJoin},
	{Name: "GTN_LeftJoin", Category: "gettablenames", Test: testGTNLeftJoin},
	{Name: "GTN_RightJoin", Category: "gettablenames", Test: testGTNRightJoin},
	{Name: "GTN_FullOuterJoin", Category: "gettablenames", Test: testGTNFullOuterJoin},
	{Name: "GTN_CrossJoin", Category: "gettablenames", Test: testGTNCrossJoin},
	{Name: "GTN_MultipleJoins", Category: "gettablenames", Test: testGTNMultipleJoins},

	// Subquery patterns
	{Name: "GTN_SubqueryWhereIn", Category: "gettablenames", Test: testGTNSubqueryWhereIn},
	{Name: "GTN_SubqueryWhereExists", Category: "gettablenames", Test: testGTNSubqueryWhereExists},
	{Name: "GTN_SubqueryInSelect", Category: "gettablenames", Test: testGTNSubqueryInSelect},
	{Name: "GTN_SubqueryInFrom", Category: "gettablenames", Test: testGTNSubqueryInFrom},
	{Name: "GTN_SubqueryInHaving", Category: "gettablenames", Test: testGTNSubqueryInHaving},
	{
		Name:     "GTN_NestedSubqueries3Levels",
		Category: "gettablenames",
		Test:     testGTNNestedSubqueries3Levels,
	},

	// CTE patterns
	{Name: "GTN_SimpleCTE", Category: "gettablenames", Test: testGTNSimpleCTE},
	{Name: "GTN_MultipleCTEs", Category: "gettablenames", Test: testGTNMultipleCTEs},
	{Name: "GTN_CTEWithJoin", Category: "gettablenames", Test: testGTNCTEWithJoin},

	// Set operations
	{Name: "GTN_Union", Category: "gettablenames", Test: testGTNUnion},
	{Name: "GTN_UnionAll", Category: "gettablenames", Test: testGTNUnionAll},
	{Name: "GTN_Intersect", Category: "gettablenames", Test: testGTNIntersect},
	{Name: "GTN_Except", Category: "gettablenames", Test: testGTNExcept},
	{Name: "GTN_ChainedUnions", Category: "gettablenames", Test: testGTNChainedUnions},

	// DML patterns
	{Name: "GTN_InsertValues", Category: "gettablenames", Test: testGTNInsertValues},
	{Name: "GTN_InsertSelect", Category: "gettablenames", Test: testGTNInsertSelect},
	{Name: "GTN_UpdateSimple", Category: "gettablenames", Test: testGTNUpdateSimple},
	{Name: "GTN_UpdateFrom", Category: "gettablenames", Test: testGTNUpdateFrom},
	{Name: "GTN_DeleteSimple", Category: "gettablenames", Test: testGTNDeleteSimple},
	{Name: "GTN_DeleteWithSubquery", Category: "gettablenames", Test: testGTNDeleteWithSubquery},

	// DDL patterns
	{Name: "GTN_CreateTable", Category: "gettablenames", Test: testGTNCreateTable},
	{Name: "GTN_CreateTableAsSelect", Category: "gettablenames", Test: testGTNCreateTableAsSelect},
	{Name: "GTN_DropTable", Category: "gettablenames", Test: testGTNDropTable},

	// Qualified names
	{Name: "GTN_SchemaQualified", Category: "gettablenames", Test: testGTNSchemaQualified},
	{Name: "GTN_SchemaUnqualified", Category: "gettablenames", Test: testGTNSchemaUnqualified},

	// Edge cases
	{Name: "GTN_EmptyResult", Category: "gettablenames", Test: testGTNEmptyResult},
	{Name: "GTN_SelfJoin", Category: "gettablenames", Test: testGTNSelfJoin},
	{Name: "GTN_TableAliasOnly", Category: "gettablenames", Test: testGTNTableAliasOnly},
}

// TestGetTableNames_Compatibility runs 30+ query patterns to verify correct table extraction
func TestGetTableNames_Compatibility(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tests := []struct {
		name      string
		query     string
		qualified bool
		expected  []string
	}{
		// Basic SELECT patterns
		{"simple select", "SELECT * FROM users", false, []string{"users"}},
		{"select with columns", "SELECT id, name FROM users", false, []string{"users"}},
		{"select with where", "SELECT * FROM users WHERE id = 1", false, []string{"users"}},

		// JOIN patterns (all types)
		{"inner join", "SELECT * FROM a INNER JOIN b ON a.id = b.a_id", false, []string{"a", "b"}},
		{"left join", "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id", false, []string{"a", "b"}},
		{"right join", "SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id", false, []string{"a", "b"}},
		{
			"full outer join",
			"SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id",
			false,
			[]string{"a", "b"},
		},
		{"cross join", "SELECT * FROM a CROSS JOIN b", false, []string{"a", "b"}},
		{
			"multiple joins",
			"SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
			false,
			[]string{"a", "b", "c"},
		},

		// Subquery patterns
		{
			"subquery in WHERE IN",
			"SELECT * FROM a WHERE id IN (SELECT a_id FROM b)",
			false,
			[]string{"a", "b"},
		},
		{
			"subquery in WHERE EXISTS",
			"SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id)",
			false,
			[]string{"a", "b"},
		},
		{
			"subquery in SELECT",
			"SELECT id, (SELECT COUNT(*) FROM b WHERE b.a_id = a.id) FROM a",
			false,
			[]string{"a", "b"},
		},
		{"subquery in FROM", "SELECT * FROM (SELECT * FROM a) AS sub", false, []string{"a"}},
		{
			"subquery in HAVING",
			"SELECT cat FROM a GROUP BY cat HAVING COUNT(*) > (SELECT AVG(cnt) FROM b)",
			false,
			[]string{"a", "b"},
		},
		{
			"nested subqueries 3 levels",
			"SELECT * FROM a WHERE id IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))",
			false,
			[]string{"a", "b", "c"},
		},

		// CTE patterns
		{
			"simple CTE",
			"WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp",
			false,
			[]string{"users"},
		},
		{
			"multiple CTEs",
			"WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM t2) SELECT * FROM a, b",
			false,
			[]string{"t1", "t2"},
		},
		{
			"CTE with join",
			"WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp JOIN orders ON tmp.id = orders.user_id",
			false,
			[]string{"orders", "users"},
		},

		// Set operations
		{"UNION", "SELECT * FROM a UNION SELECT * FROM b", false, []string{"a", "b"}},
		{"UNION ALL", "SELECT * FROM a UNION ALL SELECT * FROM b", false, []string{"a", "b"}},
		{"INTERSECT", "SELECT * FROM a INTERSECT SELECT * FROM b", false, []string{"a", "b"}},
		{"EXCEPT", "SELECT * FROM a EXCEPT SELECT * FROM b", false, []string{"a", "b"}},
		{
			"chained unions",
			"SELECT * FROM a UNION SELECT * FROM b UNION SELECT * FROM c",
			false,
			[]string{"a", "b", "c"},
		},

		// DML patterns
		{
			"INSERT VALUES",
			"INSERT INTO users (id, name) VALUES (1, 'test')",
			false,
			[]string{"users"},
		},
		{
			"INSERT SELECT",
			"INSERT INTO archive SELECT * FROM users WHERE deleted = true",
			false,
			[]string{"archive", "users"},
		},
		{"UPDATE simple", "UPDATE users SET name = 'test' WHERE id = 1", false, []string{"users"}},
		{
			"UPDATE FROM",
			"UPDATE users SET x = s.y FROM stats s WHERE users.id = s.id",
			false,
			[]string{"stats", "users"},
		},
		{"DELETE simple", "DELETE FROM users WHERE id = 1", false, []string{"users"}},
		{
			"DELETE with subquery",
			"DELETE FROM users WHERE id IN (SELECT user_id FROM deleted)",
			false,
			[]string{"deleted", "users"},
		},

		// DDL patterns
		{"CREATE TABLE", "CREATE TABLE users (id INT, name VARCHAR)", false, []string{"users"}},
		{
			"CREATE TABLE AS SELECT",
			"CREATE TABLE archive AS SELECT * FROM users",
			false,
			[]string{"archive", "users"},
		},
		{"DROP TABLE", "DROP TABLE users", false, []string{"users"}},

		// Qualified names
		{"schema qualified", "SELECT * FROM myschema.users", true, []string{"myschema.users"}},
		{"schema unqualified", "SELECT * FROM myschema.users", false, []string{"users"}},

		// Edge cases
		{"empty result", "SELECT 1 + 1", false, []string{}},
		{
			"self join",
			"SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.manager_id",
			false,
			[]string{"users"},
		},
		{"table alias only", "SELECT u.id FROM users u", false, []string{"users"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tables, err := dukdb.GetTableNames(conn, tt.query, tt.qualified)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, tables)
		})
	}
}

// Individual test functions for the CompatibilityTest framework

func testGTNSimpleSelect(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM users", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNSelectWithColumns(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT id, name FROM users", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNSelectWithWhere(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM users WHERE id = 1", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNInnerJoin(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM a INNER JOIN b ON a.id = b.a_id", false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNLeftJoin(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id", false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNRightJoin(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id", false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNFullOuterJoin(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNCrossJoin(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM a CROSS JOIN b", false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNMultipleJoins(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, tables)
}

func testGTNSubqueryWhereIn(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"SELECT * FROM a WHERE id IN (SELECT a_id FROM b)",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNSubqueryWhereExists(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id)",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNSubqueryInSelect(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"SELECT id, (SELECT COUNT(*) FROM b WHERE b.a_id = a.id) FROM a",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNSubqueryInFrom(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM (SELECT * FROM a) AS sub", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, tables)
}

func testGTNSubqueryInHaving(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"SELECT cat FROM a GROUP BY cat HAVING COUNT(*) > (SELECT AVG(cnt) FROM b)",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNNestedSubqueries3Levels(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"SELECT * FROM a WHERE id IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, tables)
}

func testGTNSimpleCTE(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp",
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNMultipleCTEs(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM t2) SELECT * FROM a, b",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"t1", "t2"}, tables)
}

func testGTNCTEWithJoin(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp JOIN orders ON tmp.id = orders.user_id",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"orders", "users"}, tables)
}

func testGTNUnion(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM a UNION SELECT * FROM b", false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNUnionAll(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM a UNION ALL SELECT * FROM b", false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNIntersect(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM a INTERSECT SELECT * FROM b", false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNExcept(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM a EXCEPT SELECT * FROM b", false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tables)
}

func testGTNChainedUnions(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"SELECT * FROM a UNION SELECT * FROM b UNION SELECT * FROM c",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, tables)
}

func testGTNInsertValues(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"INSERT INTO users (id, name) VALUES (1, 'test')",
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNInsertSelect(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"INSERT INTO archive SELECT * FROM users WHERE deleted = true",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"archive", "users"}, tables)
}

func testGTNUpdateSimple(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "UPDATE users SET name = 'test' WHERE id = 1", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNUpdateFrom(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"UPDATE users SET x = s.y FROM stats s WHERE users.id = s.id",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"stats", "users"}, tables)
}

func testGTNDeleteSimple(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "DELETE FROM users WHERE id = 1", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNDeleteWithSubquery(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"DELETE FROM users WHERE id IN (SELECT user_id FROM deleted)",
		false,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"deleted", "users"}, tables)
}

func testGTNCreateTable(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "CREATE TABLE users (id INT, name VARCHAR)", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNCreateTableAsSelect(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "CREATE TABLE archive AS SELECT * FROM users", false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"archive", "users"}, tables)
}

func testGTNDropTable(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "DROP TABLE users", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNSchemaQualified(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM myschema.users", true)
	require.NoError(t, err)
	assert.Equal(t, []string{"myschema.users"}, tables)
}

func testGTNSchemaUnqualified(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM myschema.users", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNEmptyResult(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT 1 + 1", false)
	require.NoError(t, err)
	assert.Equal(t, []string{}, tables)
}

func testGTNSelfJoin(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(
		conn,
		"SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.manager_id",
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}

func testGTNTableAliasOnly(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tables, err := dukdb.GetTableNames(conn, "SELECT u.id FROM users u", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"users"}, tables)
}
