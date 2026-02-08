package functions

const defaultServerVersion = "15.0 (dukdb-go)"

// Version returns a PostgreSQL-compatible version string.
type Version struct {
	ServerVersion string
}

// NewVersion creates a Version function with a default server version.
func NewVersion(serverVersion string) *Version {
	version := serverVersion
	if version == "" {
		version = defaultServerVersion
	}

	return &Version{ServerVersion: version}
}

// Evaluate returns the formatted PostgreSQL version string.
func (f *Version) Evaluate() string {
	return "PostgreSQL " + f.ServerVersion + " (dukdb-go compatible)"
}
