// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file contains tests for the REST catalog client.
package iceberg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOAuth2TokenExpiration tests the token expiration logic.
func TestOAuth2TokenExpiration(t *testing.T) {
	tests := []struct {
		name      string
		token     *OAuth2Token
		wantExp   bool
	}{
		{
			name:    "nil token is expired",
			token:   nil,
			wantExp: true,
		},
		{
			name: "future expiration is not expired",
			token: &OAuth2Token{
				AccessToken: "test-token",
				ExpiresAt:   time.Now().Add(1 * time.Hour),
			},
			wantExp: false,
		},
		{
			name: "past expiration is expired",
			token: &OAuth2Token{
				AccessToken: "test-token",
				ExpiresAt:   time.Now().Add(-1 * time.Hour),
			},
			wantExp: true,
		},
		{
			name: "expiration within 30 seconds is considered expired",
			token: &OAuth2Token{
				AccessToken: "test-token",
				ExpiresAt:   time.Now().Add(20 * time.Second),
			},
			wantExp: true,
		},
		{
			name: "expiration beyond 30 seconds is not expired",
			token: &OAuth2Token{
				AccessToken: "test-token",
				ExpiresAt:   time.Now().Add(60 * time.Second),
			},
			wantExp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.token.IsExpired()
			assert.Equal(t, tt.wantExp, got)
		})
	}
}

// TestNewRESTCatalog tests creating a new REST catalog client.
func TestNewRESTCatalog(t *testing.T) {
	ctx := context.Background()

	t.Run("requires URI", func(t *testing.T) {
		_, err := NewRESTCatalog(ctx, &RESTCatalogOptions{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "URI is required")
	})

	t.Run("accepts pre-existing token", func(t *testing.T) {
		// Start a mock server that returns 404 for config (optional)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
			URI:   server.URL,
			Token: "pre-existing-token",
		})
		require.NoError(t, err)
		defer func() { _ = catalog.Close() }()

		token := catalog.GetToken()
		require.NotNil(t, token)
		assert.Equal(t, "pre-existing-token", token.AccessToken)
	})
}

// TestRESTCatalogOAuth2Authentication tests OAuth2 authentication flow.
func TestRESTCatalogOAuth2Authentication(t *testing.T) {
	ctx := context.Background()

	t.Run("successful authentication", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/oauth/tokens":
				assert.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))
				err := r.ParseForm()
				require.NoError(t, err)
				assert.Equal(t, "client_credentials", r.Form.Get("grant_type"))
				assert.Equal(t, "test-client", r.Form.Get("client_id"))
				assert.Equal(t, "test-secret", r.Form.Get("client_secret"))
				assert.Equal(t, "catalog", r.Form.Get("scope"))

				w.Header().Set("Content-Type", "application/json")
				err = json.NewEncoder(w).Encode(OAuth2Token{
					AccessToken: "test-access-token",
					TokenType:   "bearer",
					ExpiresIn:   3600,
					Scope:       "catalog",
				})
				require.NoError(t, err)

			case "/v1/config":
				// Verify auth header
				assert.Contains(t, r.Header.Get("Authorization"), "bearer test-access-token")
				w.Header().Set("Content-Type", "application/json")
				err := json.NewEncoder(w).Encode(map[string]any{
					"defaults":  map[string]string{"warehouse": "test-warehouse"},
					"overrides": map[string]string{},
				})
				require.NoError(t, err)

			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
			URI:        server.URL,
			Credential: "test-client:test-secret",
			Scope:      "catalog",
		})
		require.NoError(t, err)
		defer func() { _ = catalog.Close() }()

		token := catalog.GetToken()
		require.NotNil(t, token)
		assert.Equal(t, "test-access-token", token.AccessToken)
		assert.Equal(t, "bearer", token.TokenType)
		assert.False(t, token.IsExpired())
	})

	t.Run("invalid credential format", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		_, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
			URI:        server.URL,
			Credential: "invalid-format-no-colon",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid credential format")
	})

	t.Run("authentication failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/oauth/tokens" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error": "invalid_client"}`))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		_, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
			URI:        server.URL,
			Credential: "bad-client:bad-secret",
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRESTCatalogAuth)
	})
}

// TestRESTCatalogGetConfig tests the GetConfig method.
func TestRESTCatalogGetConfig(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/config" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults": map[string]string{
					"warehouse":        "s3://my-warehouse",
					"default-catalog":  "my-catalog",
				},
				"overrides": map[string]string{
					"s3.access-key": "override-key",
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI: server.URL,
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	config, err := catalog.GetConfig(ctx)
	require.NoError(t, err)

	assert.Equal(t, "s3://my-warehouse", config["warehouse"])
	assert.Equal(t, "my-catalog", config["default-catalog"])
	assert.Equal(t, "override-key", config["s3.access-key"])
}

// TestRESTCatalogListNamespaces tests the ListNamespaces method.
func TestRESTCatalogListNamespaces(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
			})

		case "/v1/namespaces":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"namespaces": [][]string{
					{"default"},
					{"production"},
					{"production", "analytics"},
				},
			})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI: server.URL,
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	namespaces, err := catalog.ListNamespaces(ctx, nil)
	require.NoError(t, err)

	assert.Len(t, namespaces, 3)
	assert.Equal(t, []string{"default"}, namespaces[0])
	assert.Equal(t, []string{"production"}, namespaces[1])
	assert.Equal(t, []string{"production", "analytics"}, namespaces[2])
}

// TestRESTCatalogListTables tests the ListTables method.
func TestRESTCatalogListTables(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
			})

		case "/v1/namespaces/production/tables":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"identifiers": []TableIdentifier{
					{Namespace: []string{"production"}, Name: "orders"},
					{Namespace: []string{"production"}, Name: "customers"},
				},
			})

		case "/v1/namespaces/production\x1fanalytics/tables":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"identifiers": []TableIdentifier{
					{Namespace: []string{"production", "analytics"}, Name: "events"},
				},
			})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI: server.URL,
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	t.Run("single-level namespace", func(t *testing.T) {
		tables, err := catalog.ListTables(ctx, []string{"production"})
		require.NoError(t, err)

		assert.Len(t, tables, 2)
		assert.Equal(t, "orders", tables[0].Name)
		assert.Equal(t, "customers", tables[1].Name)
	})

	t.Run("multi-level namespace", func(t *testing.T) {
		tables, err := catalog.ListTables(ctx, []string{"production", "analytics"})
		require.NoError(t, err)

		assert.Len(t, tables, 1)
		assert.Equal(t, "events", tables[0].Name)
		assert.Equal(t, []string{"production", "analytics"}, tables[0].Namespace)
	})
}

// TestRESTCatalogLoadTableMetadata tests the LoadTableMetadata method.
func TestRESTCatalogLoadTableMetadata(t *testing.T) {
	ctx := context.Background()

	// Sample table metadata
	sampleMetadata := map[string]any{
		"format-version":    2,
		"table-uuid":        "550e8400-e29b-41d4-a716-446655440000",
		"location":          "s3://bucket/table",
		"last-updated-ms":   1704067200000,
		"last-column-id":    3,
		"current-schema-id": 0,
		"schemas": []map[string]any{
			{
				"type":      "struct",
				"schema-id": 0,
				"fields": []map[string]any{
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "name", "required": false, "type": "string"},
					{"id": 3, "name": "created_at", "required": false, "type": "timestamp"},
				},
			},
		},
		"default-spec-id": 0,
		"partition-specs": []map[string]any{
			{
				"spec-id": 0,
				"fields":  []map[string]any{},
			},
		},
		"properties": map[string]string{
			"write.format.default": "parquet",
		},
		"snapshots":    []any{},
		"snapshot-log": []any{},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
			})

		case "/v1/namespaces/default/tables/orders":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata-location": "s3://bucket/table/metadata/v1.metadata.json",
				"metadata":          sampleMetadata,
			})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI: server.URL,
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	metadata, metadataLocation, err := catalog.LoadTableMetadata(ctx, []string{"default"}, "orders")
	require.NoError(t, err)

	assert.Equal(t, FormatVersionV2, metadata.Version)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", metadata.TableUUID.String())
	assert.Equal(t, "s3://bucket/table", metadata.Location)
	assert.Equal(t, "s3://bucket/table/metadata/v1.metadata.json", metadataLocation)

	// Check schema
	schema := metadata.CurrentSchema()
	require.NotNil(t, schema)
	assert.Equal(t, 3, len(schema.Fields()))
}

// TestRESTCatalogLoadTable tests the LoadTable method.
func TestRESTCatalogLoadTable(t *testing.T) {
	ctx := context.Background()

	sampleMetadata := map[string]any{
		"format-version":    2,
		"table-uuid":        "550e8400-e29b-41d4-a716-446655440000",
		"location":          "s3://bucket/table",
		"last-updated-ms":   1704067200000,
		"last-column-id":    2,
		"current-schema-id": 0,
		"schemas": []map[string]any{
			{
				"type":      "struct",
				"schema-id": 0,
				"fields": []map[string]any{
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "value", "required": false, "type": "string"},
				},
			},
		},
		"default-spec-id": 0,
		"partition-specs": []map[string]any{
			{"spec-id": 0, "fields": []map[string]any{}},
		},
		"properties":   map[string]string{},
		"snapshots":    []any{},
		"snapshot-log": []any{},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
			})

		case "/v1/namespaces/production/tables/events":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata-location": "s3://bucket/events/metadata/v2.metadata.json",
				"metadata":          sampleMetadata,
			})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI: server.URL,
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	table, err := catalog.LoadTable(ctx, []string{"production"}, "events")
	require.NoError(t, err)

	assert.Equal(t, "s3://bucket/table", table.Location())
	assert.NotNil(t, table.Schema())
	assert.Equal(t, 2, len(table.Schema().Fields()))
}

// TestRESTCatalogRetry tests retry behavior.
func TestRESTCatalogRetry(t *testing.T) {
	ctx := context.Background()
	attempts := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			attempts++
			if attempts < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
			})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI: server.URL,
		RetryConfig: &RetryConfig{
			MaxRetries:    3,
			InitialDelay:  10 * time.Millisecond,
			MaxDelay:      100 * time.Millisecond,
			BackoffFactor: 2.0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	assert.Equal(t, 3, attempts)
}

// TestRESTCatalogErrorHandling tests error response handling.
func TestRESTCatalogErrorHandling(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
			})

		case "/v1/namespaces/missing/tables/notable":
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "Table not found: missing.notable",
					"type":    "NoSuchTableException",
					"code":    404,
				},
			})

		case "/v1/namespaces/forbidden/tables/secret":
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "Access denied",
					"type":    "ForbiddenException",
					"code":    403,
				},
			})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI: server.URL,
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	t.Run("not found error", func(t *testing.T) {
		_, _, err := catalog.LoadTableMetadata(ctx, []string{"missing"}, "notable")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRESTCatalogNotFound)
		assert.Contains(t, err.Error(), "Table not found")
	})

	t.Run("forbidden error", func(t *testing.T) {
		_, _, err := catalog.LoadTableMetadata(ctx, []string{"forbidden"}, "secret")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRESTCatalogAuth)
		assert.Contains(t, err.Error(), "Access denied")
	})
}

// TestParseCatalogURI tests the ParseCatalogURI function.
func TestParseCatalogURI(t *testing.T) {
	tests := []struct {
		name          string
		uri           string
		wantCatalog   string
		wantNamespace []string
		wantTable     string
		wantErr       bool
	}{
		{
			name:          "simple path",
			uri:           "iceberg://catalog.example.com/production/orders",
			wantCatalog:   "https://catalog.example.com",
			wantNamespace: []string{"production"},
			wantTable:     "orders",
		},
		{
			name:          "multi-level namespace",
			uri:           "iceberg://catalog.example.com/prod/analytics/events",
			wantCatalog:   "https://catalog.example.com",
			wantNamespace: []string{"prod", "analytics"},
			wantTable:     "events",
		},
		{
			name:          "with port",
			uri:           "iceberg://localhost:8181/default/test_table",
			wantCatalog:   "https://localhost:8181",
			wantNamespace: []string{"default"},
			wantTable:     "test_table",
		},
		{
			name:    "missing prefix",
			uri:     "http://catalog.example.com/ns/table",
			wantErr: true,
		},
		{
			name:    "missing table",
			uri:     "iceberg://catalog.example.com/namespace",
			wantErr: true,
		},
		{
			name:    "empty path",
			uri:     "iceberg://catalog.example.com",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog, namespace, table, err := ParseCatalogURI(tt.uri)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantCatalog, catalog)
			assert.Equal(t, tt.wantNamespace, namespace)
			assert.Equal(t, tt.wantTable, table)
		})
	}
}

// TestTableIdentifierString tests the TableIdentifier.String method.
func TestTableIdentifierString(t *testing.T) {
	tests := []struct {
		name string
		id   TableIdentifier
		want string
	}{
		{
			name: "simple",
			id:   TableIdentifier{Namespace: []string{"default"}, Name: "orders"},
			want: "default.orders",
		},
		{
			name: "multi-level namespace",
			id:   TableIdentifier{Namespace: []string{"prod", "analytics"}, Name: "events"},
			want: "prod.analytics.events",
		},
		{
			name: "empty namespace",
			id:   TableIdentifier{Namespace: []string{}, Name: "table"},
			want: "table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.id.String())
		})
	}
}

// TestEncodeNamespace tests the encodeNamespace function.
func TestEncodeNamespace(t *testing.T) {
	tests := []struct {
		name      string
		namespace []string
		want      string
	}{
		{
			name:      "single part",
			namespace: []string{"default"},
			want:      "default",
		},
		{
			name:      "multi-part",
			namespace: []string{"prod", "analytics"},
			want:      "prod%1Fanalytics",
		},
		{
			name:      "with special characters",
			namespace: []string{"my-ns", "sub/ns"},
			want:      "my-ns%1Fsub%2Fns",
		},
		{
			name:      "empty",
			namespace: []string{},
			want:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeNamespace(tt.namespace)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestRESTCatalogCreateNamespace tests namespace creation.
func TestRESTCatalogCreateNamespace(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
			})

		case "/v1/namespaces":
			if r.Method == http.MethodPost {
				var body struct {
					Namespace  []string          `json:"namespace"`
					Properties map[string]string `json:"properties"`
				}
				err := json.NewDecoder(r.Body).Decode(&body)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				assert.Equal(t, []string{"new_namespace"}, body.Namespace)
				assert.Equal(t, "value1", body.Properties["key1"])
				w.WriteHeader(http.StatusCreated)
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI: server.URL,
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	err = catalog.CreateNamespace(ctx, []string{"new_namespace"}, map[string]string{
		"key1": "value1",
	})
	require.NoError(t, err)
}

// TestRESTCatalogDropNamespace tests namespace deletion.
func TestRESTCatalogDropNamespace(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
			})

		case "/v1/namespaces/old_namespace":
			if r.Method == http.MethodDelete {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI: server.URL,
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	err = catalog.DropNamespace(ctx, []string{"old_namespace"})
	require.NoError(t, err)
}

// TestRESTCatalogCustomHeaders tests that custom headers are applied.
func TestRESTCatalogCustomHeaders(t *testing.T) {
	ctx := context.Background()
	receivedHeaders := make(map[string]string)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Capture headers
		receivedHeaders["X-Custom-Header"] = r.Header.Get("X-Custom-Header")
		receivedHeaders["X-Another-Header"] = r.Header.Get("X-Another-Header")

		switch r.URL.Path {
		case "/v1/config":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
			})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI: server.URL,
		Headers: map[string]string{
			"X-Custom-Header":  "custom-value",
			"X-Another-Header": "another-value",
		},
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	assert.Equal(t, "custom-value", receivedHeaders["X-Custom-Header"])
	assert.Equal(t, "another-value", receivedHeaders["X-Another-Header"])
}

// TestRESTCatalogTokenRefresh tests automatic token refresh.
func TestRESTCatalogTokenRefresh(t *testing.T) {
	ctx := context.Background()
	tokenRequestCount := 0
	listNamespacesCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/oauth/tokens":
			tokenRequestCount++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(OAuth2Token{
				AccessToken: "token-" + string(rune('0'+tokenRequestCount)),
				TokenType:   "bearer",
				ExpiresIn:   1, // Expires in 1 second
			})

		case "/v1/config":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
			})

		case "/v1/namespaces":
			listNamespacesCount++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"namespaces": [][]string{{"default"}},
			})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	catalog, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI:        server.URL,
		Credential: "client:secret",
	})
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	// Initial auth + config load request
	initialTokenCount := tokenRequestCount

	// Wait for token to expire (token expires in 1 second, + 30 second buffer in IsExpired check = wait 2 seconds)
	time.Sleep(2 * time.Second)

	// Make a request - should trigger token refresh
	_, err = catalog.ListNamespaces(ctx, nil)
	require.NoError(t, err)

	// Should have made another token request after the initial one
	assert.Greater(t, tokenRequestCount, initialTokenCount, "Token should have been refreshed")
	assert.Equal(t, 1, listNamespacesCount)
}

// TestRESTCatalogContextCancellation tests context cancellation handling.
func TestRESTCatalogContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Slow server
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := NewRESTCatalog(ctx, &RESTCatalogOptions{
		URI:        server.URL,
		Credential: "client:secret",
	})

	// Should fail due to timeout
	require.Error(t, err)
}
