package metadata

// GetTempDirectory returns temp directory metadata.
func GetTempDirectory(provider SettingProvider) []TempDirectoryMetadata {
	tempDir := "/tmp"
	if provider != nil {
		if value := provider.GetSetting("temp_directory"); value != "" {
			tempDir = value
		}
	}

	return []TempDirectoryMetadata{{TempDirectory: tempDir}}
}
