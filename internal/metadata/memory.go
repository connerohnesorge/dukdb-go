package metadata

import (
	"runtime"

	dukdb "github.com/dukdb/dukdb-go"
)

// GetMemoryUsage returns memory usage metadata.
func GetMemoryUsage(provider SettingProvider) []MemoryUsageMetadata {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	maxMemory := int64(0)
	if provider != nil {
		setting := provider.GetSetting("max_memory")
		if setting == "" {
			setting = provider.GetSetting("memory_limit")
		}
		if setting != "" {
			if resolved, err := dukdb.ResolveMaxMemory(setting); err == nil {
				maxMemory = resolved
			}
		}
	}

	return []MemoryUsageMetadata{
		{
			MemoryUsage:  int64(stats.Alloc),
			MaxMemory:    maxMemory,
			SystemMemory: int64(stats.Sys),
		},
	}
}
