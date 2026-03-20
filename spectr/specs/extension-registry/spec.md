# Extension Registry Specification

## Requirements

### Requirement: Overview

The system MUST implement the following functionality.


This specification defines the Extension Registry system for dukdb-go, which manages extension discovery, metadata, versioning, and distribution. The registry enables secure, reliable extension management while maintaining compatibility with DuckDB v1.4.3 extension behaviors.


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Requirements

The system MUST implement the following functionality.


#### Functional Requirements

1. **Extension Discovery**: Discover extensions locally and remotely
2. **Metadata Management**: Store and manage extension metadata
3. **Version Management**: Handle multiple versions of extensions
4. **Dependency Resolution**: Resolve and validate dependencies
5. **Search and Filter**: Search and filter extensions by various criteria
6. **Update Notifications**: Notify about available updates
7. **Compatibility Checking**: Verify DuckDB version compatibility
8. **Security Validation**: Validate extension signatures and permissions

#### Non-Functional Requirements

1. **Performance**: Query response time < 100ms
2. **Scalability**: Support 10,000+ extensions
3. **Reliability**: 99.9% availability
4. **Security**: All extensions cryptographically verified
5. **Consistency**: Strong consistency for metadata
6. **Availability**: Offline access to cached extensions


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Architecture

The system MUST implement the following functionality.


#### Registry Components

```
┌─────────────────────────────────────────────────────────────┐
│                  Extension Registry                         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Local     │  │   Remote    │  │   Cache     │        │
│  │  Registry   │  │  Registry   │  │  Manager    │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Search    │  │  Validator  │  │   Index     │        │
│  │   Engine    │  │             │  │  Service    │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │ Dependency  │  │  Security   │  │  Metrics    │        │
│  │  Resolver   │  │   Manager   │  │  Collector  │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

#### Data Model

```go
// ExtensionInfo contains extension metadata
type ExtensionInfo struct {
    ID          string    `json:"id"`
    Name        string    `json:"name"`
    Version     string    `json:"version"`
    Description string    `json:"description"`
    Author      string    `json:"author"`
    License     string    `json:"license"`
    Homepage    string    `json:"homepage"`
    Repository  string    `json:"repository"`

    // Compatibility
    MinDuckDBVersion string `json:"min_duckdb_version"`
    MaxDuckDBVersion string `json:"max_duckdb_version"`

    // Categories
    Categories []string `json:"categories"`
    Tags       []string `json:"tags"`

    // Statistics
    Downloads   int64     `json:"downloads"`
    Rating      float64   `json:"rating"`
    UpdatedAt   time.Time `json:"updated_at"`
    PublishedAt time.Time `json:"published_at"`

    // Security
    Signature   *SignatureInfo `json:"signature"`
    Permissions []Permission   `json:"permissions"`

    // Dependencies
    Dependencies []Dependency `json:"dependencies"`
}

// VersionInfo contains version-specific information
type VersionInfo struct {
    Version     string    `json:"version"`
    ReleaseDate time.Time `json:"release_date"`
    Changelog   string    `json:"changelog"`
    SHA256      string    `json:"sha256"`
    Size        int64     `json:"size"`
    Downloads   int64     `json:"downloads"`

    // Compatibility
    MinDuckDBVersion string `json:"min_duckdb_version"`
    MaxDuckDBVersion string `json:"max_duckdb_version"`
}

// Dependency represents an extension dependency
type Dependency struct {
    Name     string `json:"name"`
    Version  string `json:"version"`
    Optional bool   `json:"optional"`
}

// SignatureInfo contains signature metadata
type SignatureInfo struct {
    Algorithm   string    `json:"algorithm"`
    Fingerprint string    `json:"fingerprint"`
    SignedAt    time.Time `json:"signed_at"`
    Trusted     bool      `json:"trusted"`
}
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Local Registry

The system MUST implement the following functionality.


#### File System Layout

```
${DUCKDB_EXTENSION_DIR}/
├── index.json              # Local index
├── cache/
│   ├── manifests/          # Cached manifests
│   └── binaries/           # Cached binaries
├── installed/
│   ├── parquet/
│   │   ├── manifest.json
│   │   └── binary.dukdb
│   └── json/
│       ├── manifest.json
│       └── binary.dukdb
└── metadata/
    ├── extensions.db       # SQLite database
    └── lock                # Registry lock
```

#### Local Registry Implementation

```go
type LocalRegistry struct {
    root    string
    db      *sql.DB
    index   *ExtensionIndex
    cache   *ExtensionCache
    mutex   sync.RWMutex
}

func (r *LocalRegistry) Initialize(root string) error {
    r.root = root

    // Create directories
    dirs := []string{
        filepath.Join(root, "cache", "manifests"),
        filepath.Join(root, "cache", "binaries"),
        filepath.Join(root, "installed"),
        filepath.Join(root, "metadata"),
    }

    for _, dir := range dirs {
        if err := os.MkdirAll(dir, 0755); err != nil {
            return err
        }
    }

    // Initialize database
    dbPath := filepath.Join(root, "metadata", "extensions.db")
    db, err := sql.Open("sqlite", dbPath)
    if err != nil {
        return err
    }

    if err := r.createSchema(db); err != nil {
        return err
    }

    r.db = db
    r.index = NewExtensionIndex(db)
    r.cache = NewExtensionCache(filepath.Join(root, "cache"))

    return nil
}

func (r *LocalRegistry) createSchema(db *sql.DB) error {
    schema := `
    CREATE TABLE IF NOT EXISTS extensions (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        version TEXT NOT NULL,
        description TEXT,
        author TEXT,
        license TEXT,
        homepage TEXT,
        repository TEXT,
        min_duckdb_version TEXT,
        max_duckdb_version TEXT,
        categories TEXT,
        tags TEXT,
        downloads INTEGER DEFAULT 0,
        rating REAL DEFAULT 0,
        updated_at TIMESTAMP,
        published_at TIMESTAMP,
        manifest_path TEXT,
        binary_path TEXT
    );

    CREATE TABLE IF NOT EXISTS versions (
        extension_id TEXT,
        version TEXT,
        release_date TIMESTAMP,
        changelog TEXT,
        sha256 TEXT,
        size INTEGER,
        downloads INTEGER DEFAULT 0,
        min_duckdb_version TEXT,
        max_duckdb_version TEXT,
        PRIMARY KEY (extension_id, version),
        FOREIGN KEY (extension_id) REFERENCES extensions(id)
    );

    CREATE TABLE IF NOT EXISTS dependencies (
        extension_id TEXT,
        dependency_id TEXT,
        version_constraint TEXT,
        optional BOOLEAN DEFAULT FALSE,
        PRIMARY KEY (extension_id, dependency_id),
        FOREIGN KEY (extension_id) REFERENCES extensions(id),
        FOREIGN KEY (dependency_id) REFERENCES extensions(id)
    );

    CREATE TABLE IF NOT EXISTS signatures (
        extension_id TEXT,
        version TEXT,
        algorithm TEXT,
        fingerprint TEXT,
        signed_at TIMESTAMP,
        trusted BOOLEAN DEFAULT FALSE,
        PRIMARY KEY (extension_id, version),
        FOREIGN KEY (extension_id, version) REFERENCES versions(extension_id, version)
    );

    CREATE INDEX IF NOT EXISTS idx_extensions_name ON extensions(name);
    CREATE INDEX IF NOT EXISTS idx_extensions_categories ON extensions(categories);
    CREATE INDEX IF NOT EXISTS idx_extensions_tags ON extensions(tags);
    `

    _, err := db.Exec(schema)
    return err
}
```

#### CRUD Operations

```go
func (r *LocalRegistry) RegisterExtension(info *ExtensionInfo) error {
    r.mutex.Lock()
    defer r.mutex.Unlock()

    tx, err := r.db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // Insert extension
    _, err = tx.Exec(`
        INSERT INTO extensions (
            id, name, version, description, author, license, homepage,
            repository, min_duckdb_version, max_duckdb_version,
            categories, tags, downloads, rating, updated_at, published_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            version = excluded.version,
            description = excluded.description,
            updated_at = excluded.updated_at
    `,
        info.ID, info.Name, info.Version, info.Description,
        info.Author, info.License, info.Homepage, info.Repository,
        info.MinDuckDBVersion, info.MaxDuckDBVersion,
        strings.Join(info.Categories, ","),
        strings.Join(info.Tags, ","),
        info.Downloads, info.Rating,
        info.UpdatedAt, info.PublishedAt,
    )
    if err != nil {
        return err
    }

    // Insert version info
    for _, version := range info.Versions {
        _, err = tx.Exec(`
            INSERT INTO versions (
                extension_id, version, release_date, changelog,
                sha256, size, downloads, min_duckdb_version, max_duckdb_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
            info.ID, version.Version, version.ReleaseDate,
            version.Changelog, version.SHA256, version.Size,
            version.Downloads, version.MinDuckDBVersion,
            version.MaxDuckDBVersion,
        )
        if err != nil {
            return err
        }
    }

    // Insert dependencies
    for _, dep := range info.Dependencies {
        _, err = tx.Exec(`
            INSERT INTO dependencies (
                extension_id, dependency_id, version_constraint, optional
            ) VALUES (?, ?, ?, ?)
        `,
            info.ID, dep.Name, dep.Version, dep.Optional,
        )
        if err != nil {
            return err
        }
    }

    // Insert signature
    if info.Signature != nil {
        _, err = tx.Exec(`
            INSERT INTO signatures (
                extension_id, version, algorithm, fingerprint, signed_at, trusted
            ) VALUES (?, ?, ?, ?, ?, ?)
        `,
            info.ID, info.Version, info.Signature.Algorithm,
            info.Signature.Fingerprint, info.Signature.SignedAt,
            info.Signature.Trusted,
        )
        if err != nil {
            return err
        }
    }

    // Cache manifest
    manifestPath := filepath.Join(r.root, "cache", "manifests", info.ID+".json")
    if err := r.cache.SaveManifest(manifestPath, info); err != nil {
        return err
    }

    return tx.Commit()
}

func (r *LocalRegistry) GetExtension(name string) (*ExtensionInfo, error) {
    r.mutex.RLock()
    defer r.mutex.RUnlock()

    row := r.db.QueryRow(`
        SELECT id, name, version, description, author, license, homepage,
               repository, min_duckdb_version, max_duckdb_version,
               categories, tags, downloads, rating, updated_at, published_at
        FROM extensions
        WHERE name = ?
        ORDER BY updated_at DESC
        LIMIT 1
    `, name)

    var info ExtensionInfo
    var categories, tags string

    err := row.Scan(
        &info.ID, &info.Name, &info.Version, &info.Description,
        &info.Author, &info.License, &info.Homepage, &info.Repository,
        &info.MinDuckDBVersion, &info.MaxDuckDBVersion,
        &categories, &tags, &info.Downloads, &info.Rating,
        &info.UpdatedAt, &info.PublishedAt,
    )
    if err != nil {
        if err == sql.ErrNoRows {
            return nil, ErrExtensionNotFound
        }
        return nil, err
    }

    info.Categories = strings.Split(categories, ",")
    info.Tags = strings.Split(tags, ",")

    // Load versions
    versions, err := r.getVersions(info.ID)
    if err != nil {
        return nil, err
    }
    info.Versions = versions

    // Load dependencies
    dependencies, err := r.getDependencies(info.ID)
    if err != nil {
        return nil, err
    }
    info.Dependencies = dependencies

    return &info, nil
}
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Remote Registry

The system MUST implement the following functionality.


#### Registry API

```go
type RemoteRegistry struct {
    baseURL    string
    client     *http.Client
    auth       AuthProvider
    cache      *ExtensionCache
    rateLimiter *RateLimiter
}

// Registry API endpoints
type RegistryAPI interface {
    // Search extensions
    Search(ctx context.Context, query SearchQuery) (*SearchResult, error)

    // Get extension info
    GetExtension(ctx context.Context, name string) (*ExtensionInfo, error)

    // Get extension versions
    GetVersions(ctx context.Context, name string) ([]VersionInfo, error)

    // Download extension
    Download(ctx context.Context, name, version string) (io.ReadCloser, error)

    // Get manifest
    GetManifest(ctx context.Context, name, version string) (*Manifest, error)
}

// Search query
type SearchQuery struct {
    Text           string
    Categories     []string
    Tags           []string
    MinRating      float64
    CompatibleWith string
    SortBy         string
    SortOrder      string
    Limit          int
    Offset         int
}

// Search result
type SearchResult struct {
    Total   int             `json:"total"`
    Results []*ExtensionInfo `json:"results"`
}
```

#### API Implementation

```go
func (r *RemoteRegistry) Search(ctx context.Context, query SearchQuery) (*SearchResult, error) {
    // Build URL
    u, err := url.Parse(r.baseURL + "/api/v1/extensions/search")
    if err != nil {
        return nil, err
    }

    // Add query parameters
    q := u.Query()
    if query.Text != "" {
        q.Set("q", query.Text)
    }
    if len(query.Categories) > 0 {
        q.Set("categories", strings.Join(query.Categories, ","))
    }
    if len(query.Tags) > 0 {
        q.Set("tags", strings.Join(query.Tags, ","))
    }
    if query.MinRating > 0 {
        q.Set("min_rating", fmt.Sprintf("%.1f", query.MinRating))
    }
    if query.CompatibleWith != "" {
        q.Set("compatible_with", query.CompatibleWith)
    }
    if query.SortBy != "" {
        q.Set("sort", query.SortBy)
    }
    if query.SortOrder != "" {
        q.Set("order", query.SortOrder)
    }
    if query.Limit > 0 {
        q.Set("limit", strconv.Itoa(query.Limit))
    }
    if query.Offset > 0 {
        q.Set("offset", strconv.Itoa(query.Offset))
    }
    u.RawQuery = q.Encode()

    // Check rate limit
    if err := r.rateLimiter.Wait(ctx); err != nil {
        return nil, err
    }

    // Make request
    req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
    if err != nil {
        return nil, err
    }

    // Add authentication
    if err := r.auth.AddAuth(req); err != nil {
        return nil, err
    }

    resp, err := r.client.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("search failed: %s", resp.Status)
    }

    // Parse response
    var result SearchResult
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return nil, err
    }

    return &result, nil
}

func (r *RemoteRegistry) Download(ctx context.Context, name, version string) (io.ReadCloser, error) {
    // Build URL
    url := fmt.Sprintf("%s/api/v1/extensions/%s/versions/%s/download",
        r.baseURL, name, version)

    // Check rate limit
    if err := r.rateLimiter.Wait(ctx); err != nil {
        return nil, err
    }

    // Make request
    req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
    if err != nil {
        return nil, err
    }

    // Add authentication
    if err := r.auth.AddAuth(req); err != nil {
        return nil, err
    }

    resp, err := r.client.Do(req)
    if err != nil {
        return nil, err
    }

    if resp.StatusCode != http.StatusOK {
        resp.Body.Close()
        return nil, fmt.Errorf("download failed: %s", resp.Status)
    }

    return resp.Body, nil
}
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Search Engine

The system MUST implement the following functionality.


#### Search Implementation

```go
type SearchEngine struct {
    index   *ExtensionIndex
    analyzer *TextAnalyzer
    scorer  *ScoringAlgorithm
}

// Search request
type SearchRequest struct {
    Query      string
    Filters    []Filter
    Sort       []SortField
    Pagination Pagination
}

// Filter for refining results
type Filter interface {
    Apply(query *Query) *Query
}

// Category filter
type CategoryFilter struct {
    Categories []string
}

func (f *CategoryFilter) Apply(query *Query) *Query {
    return query.Where("categories", "IN", f.Categories)
}

// Compatibility filter
type CompatibilityFilter struct {
    DuckDBVersion string
}

func (f *CompatibilityFilter) Apply(query *Query) *Query {
    return query.Where("min_duckdb_version", "<=", f.DuckDBVersion).
        Where("max_duckdb_version", ">=", f.DuckDBVersion)
}

// Sort field
type SortField struct {
    Field     string
    Direction string // "asc" or "desc"
}

// Scoring algorithm
type ScoringAlgorithm interface {
    Score(extension *ExtensionInfo, query *SearchQuery) float64
}

// TF-IDF scoring
type TFIDFScorer struct {
    index *ExtensionIndex
}

func (s *TFIDFScorer) Score(extension *ExtensionInfo, query *SearchQuery) float64 {
    score := 0.0

    // Score name match
    score += s.scoreField(query.Terms, extension.Name, "name")

    // Score description match
    score += s.scoreField(query.Terms, extension.Description, "description")

    // Score tag matches
    for _, tag := range extension.Tags {
        score += s.scoreField(query.Terms, tag, "tags")
    }

    // Boost recent extensions
    daysSinceUpdate := time.Since(extension.UpdatedAt).Hours() / 24
    recencyBoost := math.Exp(-daysSinceUpdate / 365)
    score *= (1 + recencyBoost)

    // Boost popular extensions
    popularityBoost := math.Log10(float64(extension.Downloads) + 1) / 10
    score *= (1 + popularityBoost)

    return score
}

func (s *TFIDFScorer) scoreField(terms []string, field, fieldName string) float64 {
    score := 0.0
    fieldTerms := s.analyzer.Analyze(field)

    for _, term := range terms {
        tf := s.calculateTF(term, fieldTerms)
        idf := s.index.GetIDF(term, fieldName)
        score += tf * idf
    }

    return score
}
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Dependency Management

The system MUST implement the following functionality.


#### Dependency Graph

```go
type DependencyGraph struct {
    nodes map[string]*DependencyNode
    edges map[string]map[string]*DependencyEdge
}

type DependencyNode struct {
    Extension *ExtensionInfo
    Level     int
}

type DependencyEdge struct {
    From       string
    To         string
    Constraint string
    Optional   bool
}

// Dependency resolver
type DependencyResolver struct {
    registry ExtensionRegistry
    graph    *DependencyGraph
}

func (r *DependencyResolver) Resolve(extension string) ([]string, error) {
    // Build dependency graph
    graph, err := r.buildGraph(extension)
    if err != nil {
        return nil, err
    }

    // Check for cycles
    if err := r.checkCycles(graph); err != nil {
        return nil, err
    }

    // Topological sort
    order, err := r.topologicalSort(graph)
    if err != nil {
        return nil, err
    }

    return order, nil
}

func (r *DependencyResolver) buildGraph(extension string) (*DependencyGraph, error) {
    graph := NewDependencyGraph()
    visited := make(map[string]bool)

    var build func(string) error
    build = func(name string) error {
        if visited[name] {
            return nil
        }
        visited[name] = true

        // Get extension info
        info, err := r.registry.GetExtension(name)
        if err != nil {
            return err
        }

        // Add node
        graph.AddNode(name, info)

        // Process dependencies
        for _, dep := range info.Dependencies {
            // Add edge
            graph.AddEdge(name, dep.Name, dep.Version, dep.Optional)

            // Recursively build
            if err := build(dep.Name); err != nil {
                return err
            }
        }

        return nil
    }

    if err := build(extension); err != nil {
        return nil, err
    }

    return graph, nil
}

func (r *DependencyResolver) checkCycles(graph *DependencyGraph) error {
    visited := make(map[string]bool)
    recStack := make(map[string]bool)

    var hasCycle func(string) bool
    hasCycle = func(node string) bool {
        visited[node] = true
        recStack[node] = true

        for neighbor := range graph.edges[node] {
            if !visited[neighbor] {
                if hasCycle(neighbor) {
                    return true
                }
            } else if recStack[neighbor] {
                return true
            }
        }

        recStack[node] = false
        return false
    }

    for node := range graph.nodes {
        if !visited[node] {
            if hasCycle(node) {
                return fmt.Errorf("circular dependency detected involving %s", node)
            }
        }
    }

    return nil
}
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Version Management

The system MUST implement the following functionality.


#### Version Constraints

```go
// VersionConstraint represents a version constraint
type VersionConstraint interface {
    // Check checks if a version satisfies the constraint
    Check(version string) bool

    // String returns string representation
    String() string
}

// Semantic version constraint
type SemanticVersionConstraint struct {
    operator string
    version  string
}

func (c *SemanticVersionConstraint) Check(version string) bool {
    v1, err := semver.NewVersion(version)
    if err != nil {
        return false
    }

    v2, err := semver.NewVersion(c.version)
    if err != nil {
        return false
    }

    switch c.operator {
    case "=":
        return v1.Equal(v2)
    case ">":
        return v1.GreaterThan(v2)
    case ">=":
        return v1.GreaterThan(v2) || v1.Equal(v2)
    case "<":
        return v1.LessThan(v2)
    case "<=":
        return v1.LessThan(v2) || v1.Equal(v2)
    case "~":
        return c.checkTilde(v1, v2)
    case "^":
        return c.checkCaret(v1, v2)
    }

    return false
}

// Version selector
type VersionSelector struct {
    constraints []VersionConstraint
}

func (s *VersionSelector) Select(versions []string) (string, error) {
    var candidates []string

    for _, version := range versions {
        valid := true
        for _, constraint := range s.constraints {
            if !constraint.Check(version) {
                valid = false
                break
            }
        }
        if valid {
            candidates = append(candidates, version)
        }
    }

    if len(candidates) == 0 {
        return "", fmt.Errorf("no version satisfies constraints")
    }

    // Return highest version
    sort.Sort(sort.Reverse(sort.StringSlice(candidates)))
    return candidates[0], nil
}
```

#### Update Management

```go
// UpdateChecker checks for updates
type UpdateChecker struct {
    registry ExtensionRegistry
    interval time.Duration
    notifier UpdateNotifier
}

// Update notification
type UpdateNotification struct {
    Extension   string
    CurrentVersion string
    LatestVersion  string
    ReleaseDate    time.Time
    Changelog      string
    Priority       UpdatePriority
}

// Update priority
type UpdatePriority int

const (
    UpdatePriorityLow UpdatePriority = iota
    UpdatePriorityMedium
    UpdatePriorityHigh
    UpdatePriorityCritical
)

func (c *UpdateChecker) CheckUpdates() ([]UpdateNotification, error) {
    var notifications []UpdateNotification

    // Get installed extensions
    installed, err := c.registry.ListInstalledExtensions()
    if err != nil {
        return nil, err
    }

    for _, ext := range installed {
        // Get latest version
        latest, err := c.registry.GetLatestVersion(ext.Name)
        if err != nil {
            continue
        }

        // Check if update available
        if c.isUpdateAvailable(ext.Version, latest.Version) {
            priority := c.calculatePriority(ext.Version, latest.Version)

            notifications = append(notifications, UpdateNotification{
                Extension:      ext.Name,
                CurrentVersion: ext.Version,
                LatestVersion:  latest.Version,
                ReleaseDate:    latest.ReleaseDate,
                Changelog:      latest.Changelog,
                Priority:       priority,
            })
        }
    }

    return notifications, nil
}

func (c *UpdateChecker) calculatePriority(current, latest string) UpdatePriority {
    // Parse versions
    v1, _ := semver.NewVersion(current)
    v2, _ := semver.NewVersion(latest)

    if v1 == nil || v2 == nil {
        return UpdatePriorityMedium
    }

    // Check for major version change
    if v2.Major() > v1.Major() {
        return UpdatePriorityHigh
    }

    // Check for minor version change
    if v2.Minor() > v1.Minor() {
        return UpdatePriorityMedium
    }

    // Patch version change
    return UpdatePriorityLow
}
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Cache Management

The system MUST implement the following functionality.


#### Cache Implementation

```go
// ExtensionCache manages extension cache
type ExtensionCache struct {
    root       string
    maxSize    int64
    ttl        time.Duration
    index      *CacheIndex
    evictor    *EvictionPolicy
}

// Cache entry
type CacheEntry struct {
    Key        string
    Path       string
    Size       int64
    AccessedAt time.Time
    CreatedAt  time.Time
    Hits       int64
}

// Cache index
type CacheIndex struct {
    entries map[string]*CacheEntry
    mutex   sync.RWMutex
}

// LRU eviction policy
type LRUEvictionPolicy struct {
    list *list.List
    idx  map[string]*list.Element
}

func (c *ExtensionCache) Get(key string) (string, error) {
    c.index.mutex.RLock()
    entry, ok := c.index.entries[key]
    c.index.mutex.RUnlock()

    if !ok {
        return "", ErrCacheMiss
    }

    // Check TTL
    if time.Since(entry.CreatedAt) > c.ttl {
        c.Remove(key)
        return "", ErrCacheExpired
    }

    // Update access time
    c.index.mutex.Lock()
    entry.AccessedAt = time.Now()
    entry.Hits++
    c.index.mutex.Unlock()

    // Check if file exists
    if _, err := os.Stat(entry.Path); err != nil {
        c.Remove(key)
        return "", ErrCacheMiss
    }

    return entry.Path, nil
}

func (c *ExtensionCache) Put(key string, reader io.Reader) (string, error) {
    // Generate path
    path := filepath.Join(c.root, key)

    // Create directory
    dir := filepath.Dir(path)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return "", err
    }

    // Write file
    file, err := os.Create(path)
    if err != nil {
        return "", err
    }
    defer file.Close()

    size, err := io.Copy(file, reader)
    if err != nil {
        os.Remove(path)
        return "", err
    }

    // Create entry
    entry := &CacheEntry{
        Key:        key,
        Path:       path,
        Size:       size,
        AccessedAt: time.Now(),
        CreatedAt:  time.Now(),
        Hits:       0,
    }

    // Add to index
    c.index.mutex.Lock()
    c.index.entries[key] = entry
    c.index.mutex.Unlock()

    // Check if eviction needed
    go c.checkEviction()

    return path, nil
}

func (c *ExtensionCache) checkEviction() {
    // Get total size
    totalSize := c.getTotalSize()

    // Check if over limit
    if totalSize <= c.maxSize {
        return
    }

    // Evict entries
    for totalSize > c.maxSize {
        entry := c.evictor.SelectEvictionCandidate()
        if entry == nil {
            break
        }

        c.Remove(entry.Key)
        totalSize -= entry.Size
    }
}
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Security

The system MUST implement the following functionality.


#### Signature Validation

```go
// SignatureValidator validates extension signatures
type SignatureValidator struct {
    trustedKeys []PublicKey
    crl         *CertificateRevocationList
}

func (v *SignatureValidator) Validate(info *ExtensionInfo) error {
    if info.Signature == nil {
        return fmt.Errorf("extension not signed")
    }

    // Find trusted key
    var key PublicKey
    found := false
    for _, k := range v.trustedKeys {
        if k.Fingerprint() == info.Signature.Fingerprint {
            key = k
            found = true
            break
        }
    }

    if !found {
        return fmt.Errorf("signing key not trusted")
    }

    // Verify signature
    data := v.prepareSignatureData(info)
    if err := key.Verify(data, info.Signature.Data); err != nil {
        return fmt.Errorf("signature verification failed: %w", err)
    }

    // Check revocation
    if v.crl.IsRevoked(info.Signature.Fingerprint) {
        return fmt.Errorf("signing key revoked")
    }

    return nil
}
```

#### Permission Validation

```go
// PermissionValidator validates extension permissions
type PermissionValidator struct {
    policies []PermissionPolicy
}

type PermissionPolicy interface {
    // Check checks if permission is allowed
    Check(permission Permission) error

    // Name returns policy name
    Name() string
}

// Default permission policy
type DefaultPermissionPolicy struct {
    allowedTypes []string
    maxFileSize  int64
    maxNetworkHosts int
}

func (p *DefaultPermissionPolicy) Check(permission Permission) error {
    // Check permission type
    allowed := false
    for _, t := range p.allowedTypes {
        if t == permission.Type() {
            allowed = true
            break
        }
    }

    if !allowed {
        return fmt.Errorf("permission type %s not allowed", permission.Type())
    }

    // Type-specific checks
    switch perm := permission.(type) {
    case *FilePermission:
        // Check file size
        if perm.MaxSize > p.maxFileSize {
            return fmt.Errorf("file size %d exceeds limit %d",
                perm.MaxSize, p.maxFileSize)
        }

    case *NetworkPermission:
        // Check host count
        if len(perm.Hosts) > p.maxNetworkHosts {
            return fmt.Errorf("too many network hosts: %d > %d",
                len(perm.Hosts), p.maxNetworkHosts)
        }
    }

    return nil
}
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: API Reference

The system MUST implement the following functionality.


#### Registry Client

```go
// RegistryClient provides registry access
type RegistryClient interface {
    // Search extensions
    Search(ctx context.Context, query SearchQuery) (*SearchResult, error)

    // Get extension
    GetExtension(ctx context.Context, name string) (*ExtensionInfo, error)

    // List categories
    ListCategories(ctx context.Context) ([]string, error)

    // List tags
    ListTags(ctx context.Context) ([]string, error)

    // Get popular extensions
    GetPopular(ctx context.Context, limit int) ([]*ExtensionInfo, error)

    // Get recent extensions
    GetRecent(ctx context.Context, limit int) ([]*ExtensionInfo, error)

    // Check for updates
    CheckUpdates(ctx context.Context, installed []string) ([]UpdateNotification, error)
}
```

#### Configuration

```yaml
# Registry configuration
registry:
  # Local registry
  local:
    path: "${HOME}/.duckdb/extensions"
    max_size: 10GB

  # Remote registry
  remote:
    url: "https://extensions.duckdb.org"
    timeout: 30s
    retries: 3

  # Cache settings
  cache:
    enabled: true
    size: 1GB
    ttl: 24h

  # Security
  security:
    verify_signatures: true
    trusted_keys:
      - "SHA256:abcdef..."
      - "SHA256:123456..."

  # Rate limiting
  rate_limit:
    requests_per_minute: 60
    burst: 10
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Version History

The system MUST implement the following functionality.


| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | 2024-01-20 | Initial specification | dukdb-go Team |

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

