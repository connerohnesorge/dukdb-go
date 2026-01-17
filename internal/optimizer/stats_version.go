package optimizer

// StatisticsVersion represents the version of the statistics binary format.
// This allows us to support multiple versions and gracefully handle format changes.
type StatisticsVersion uint8

const (
	// VersionDuckDB represents DuckDB v1.4.3 format (version 0).
	// This is the reference format we must be compatible with.
	VersionDuckDB StatisticsVersion = 0

	// VersionDukDBGo represents dukdb-go native format (version 1).
	// This is the current implementation version with possible enhancements
	// beyond DuckDB v1.4.3.
	VersionDukDBGo StatisticsVersion = 1

	// CurrentVersion is the version used when serializing new statistics.
	// We use dukdb-go version by default since it's our native format.
	CurrentVersion = VersionDukDBGo
)

// IsValidVersion returns true if the given version is supported.
// We support both DuckDB v1.4.3 format (0) and dukdb-go format (1).
func IsValidVersion(version StatisticsVersion) bool {
	switch version {
	case VersionDuckDB, VersionDukDBGo:
		return true
	default:
		return false
	}
}

// VersionInfo provides metadata about a statistics version.
type VersionInfo struct {
	Version     StatisticsVersion
	Name        string
	Description string
	// CompatibleUpTo is the maximum version we can upgrade to
	CompatibleUpTo StatisticsVersion
}

// GetVersionInfo returns metadata about a specific statistics version.
func GetVersionInfo(version StatisticsVersion) VersionInfo {
	switch version {
	case VersionDuckDB:
		return VersionInfo{
			Version:        VersionDuckDB,
			Name:           "DuckDB v1.4.3",
			Description:    "Original DuckDB statistics format from v1.4.3",
			CompatibleUpTo: VersionDukDBGo,
		}
	case VersionDukDBGo:
		return VersionInfo{
			Version:        VersionDukDBGo,
			Name:           "dukdb-go v1",
			Description:    "dukdb-go native statistics format v1",
			CompatibleUpTo: VersionDukDBGo,
		}
	default:
		return VersionInfo{
			Version:        version,
			Name:           "Unknown",
			Description:    "Unknown statistics format version",
			CompatibleUpTo: version,
		}
	}
}
