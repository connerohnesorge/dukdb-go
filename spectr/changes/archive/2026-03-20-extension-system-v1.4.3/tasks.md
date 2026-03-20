# Tasks: Extension System v1.4.3

## 1. Core Extension Framework

- [x] 1.1. Implement Extension interface (Name, Description, Version, Load, Unload)
- [x] 1.2. Implement thread-safe Extension Registry with RWMutex
- [x] 1.3. Implement ExtensionEntry state tracking (Installed, Loaded)
- [x] 1.4. Register built-in extensions (csv, json, parquet, icu)

## 2. Parser and AST Support

- [x] 2.1. Implement INSTALL statement parsing (identifier and string literal)
- [x] 2.2. Implement LOAD statement parsing (identifier and string literal)
- [x] 2.3. Add InstallStmt and LoadStmt AST nodes

## 3. Engine and Executor Integration

- [x] 3.1. Integrate extension registry into Engine
- [x] 3.2. Implement handleInstall in connection handler
- [x] 3.3. Implement handleLoad in connection handler
- [x] 3.4. Implement duckdb_extensions() system table function
- [x] 3.5. Wire INSTALL/LOAD through statement dispatch

## 4. Testing

- [x] 4.1. Add TestInstallExtension e2e test
- [x] 4.2. Add TestLoadExtension e2e test
- [x] 4.3. Add TestLoadUnknownExtension error handling test
- [x] 4.4. Add TestDuckDBExtensions table function test
- [x] 4.5. Add TestDuckDBExtensionsAfterLoad state tracking test
- [x] 4.6. Add TestInstallLoadWithStringLiteral parsing test
