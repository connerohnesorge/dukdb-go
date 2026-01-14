package functions

// Schema and default constants.
const (
	schemaPgCatalog = "pg_catalog"
	schemaPgTemp    = "pg_temp"
	defaultUser     = "dukdb"
	defaultDatabase = "dukdb"
)

// CurrentSchema returns the current schema name.
// In DuckDB/dukdb-go, this is typically "main".
type CurrentSchema struct {
	DefaultSchema string
}

// NewCurrentSchema creates a CurrentSchema function with default schema "main".
func NewCurrentSchema() *CurrentSchema {
	return &CurrentSchema{DefaultSchema: "main"}
}

// Evaluate returns the current schema name.
func (f *CurrentSchema) Evaluate() string {
	return f.DefaultSchema
}

// CurrentSchemas returns the search path schemas.
type CurrentSchemas struct {
	Schemas []string
}

// NewCurrentSchemas creates a CurrentSchemas function with default search path.
func NewCurrentSchemas() *CurrentSchemas {
	return &CurrentSchemas{
		Schemas: []string{"main", schemaPgCatalog},
	}
}

// EvaluateWithImplicit returns the search path schemas including implicit schemas.
func (f *CurrentSchemas) EvaluateWithImplicit() []string {
	return f.Schemas
}

// EvaluateWithoutImplicit returns only explicit schemas (excluding pg_catalog, pg_temp).
func (f *CurrentSchemas) EvaluateWithoutImplicit() []string {
	result := make([]string, 0)
	for _, s := range f.Schemas {
		if s != schemaPgCatalog && s != schemaPgTemp {
			result = append(result, s)
		}
	}

	return result
}

// CurrentUser returns the current user name.
type CurrentUser struct {
	Username string
}

// NewCurrentUser creates a CurrentUser function.
func NewCurrentUser(username string) *CurrentUser {
	name := username
	if name == "" {
		name = defaultUser
	}

	return &CurrentUser{Username: name}
}

// Evaluate returns the current user name.
func (f *CurrentUser) Evaluate() string {
	return f.Username
}

// SessionUser is an alias for CurrentUser in our implementation.
type SessionUser = CurrentUser

// CurrentDatabase returns the current database name.
type CurrentDatabase struct {
	DatabaseName string
}

// NewCurrentDatabase creates a CurrentDatabase function.
func NewCurrentDatabase(dbName string) *CurrentDatabase {
	name := dbName
	if name == "" {
		name = defaultDatabase
	}

	return &CurrentDatabase{DatabaseName: name}
}

// Evaluate returns the current database name.
func (f *CurrentDatabase) Evaluate() string {
	return f.DatabaseName
}
