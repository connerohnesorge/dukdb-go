package metadata

import "github.com/dukdb/dukdb-go/internal/extension"

// GetExtensions returns extension metadata from a registry.
// If registry is nil, returns an empty slice.
func GetExtensions(registry *extension.Registry) []ExtensionMetadata {
	if registry == nil {
		return []ExtensionMetadata{}
	}
	infos := registry.ListExtensions()
	result := make([]ExtensionMetadata, len(infos))
	for i, info := range infos {
		result[i] = ExtensionMetadata{
			ExtensionName: info.Name,
			Loaded:        info.Loaded,
			Installed:     info.Installed,
			Description:   info.Description,
		}
	}
	return result
}
