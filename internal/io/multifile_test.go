package io

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeSchemasByName(t *testing.T) {
	tests := []struct {
		name        string
		schemas     []FileSchema
		wantCols    []ColumnInfo
		wantMaps    [][]int
		wantErr     bool
		errContains string
	}{
		{
			name: "identical schemas",
			schemas: []FileSchema{
				{
					Path: "file1.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
						{Name: "name", Type: dukdb.TYPE_VARCHAR},
					},
				},
				{
					Path: "file2.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
						{Name: "name", Type: dukdb.TYPE_VARCHAR},
					},
				},
			},
			wantCols: []ColumnInfo{
				{Name: "id", Type: dukdb.TYPE_INTEGER},
				{Name: "name", Type: dukdb.TYPE_VARCHAR},
			},
			wantMaps: [][]int{
				{0, 1}, // file1: id->0, name->1
				{0, 1}, // file2: id->0, name->1
			},
		},
		{
			name: "different column order",
			schemas: []FileSchema{
				{
					Path: "file1.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
						{Name: "name", Type: dukdb.TYPE_VARCHAR},
					},
				},
				{
					Path: "file2.csv",
					Columns: []ColumnInfo{
						{Name: "name", Type: dukdb.TYPE_VARCHAR},
						{Name: "id", Type: dukdb.TYPE_INTEGER},
					},
				},
			},
			wantCols: []ColumnInfo{
				{Name: "id", Type: dukdb.TYPE_INTEGER},
				{Name: "name", Type: dukdb.TYPE_VARCHAR},
			},
			wantMaps: [][]int{
				{0, 1}, // file1: id->0, name->1
				{1, 0}, // file2: name is at position 0 (maps to merged 1), id is at 1 (maps to merged 0)
			},
		},
		{
			name: "missing columns filled with NULL",
			schemas: []FileSchema{
				{
					Path: "file1.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
						{Name: "name", Type: dukdb.TYPE_VARCHAR},
					},
				},
				{
					Path: "file2.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
						{Name: "email", Type: dukdb.TYPE_VARCHAR},
					},
				},
			},
			wantCols: []ColumnInfo{
				{Name: "id", Type: dukdb.TYPE_INTEGER},
				{Name: "name", Type: dukdb.TYPE_VARCHAR},
				{Name: "email", Type: dukdb.TYPE_VARCHAR},
			},
			wantMaps: [][]int{
				{0, 1, -1}, // file1: id->0, name->1, email->missing
				{0, -1, 1}, // file2: id->0, name->missing, email->1
			},
		},
		{
			name: "type widening INTEGER to BIGINT",
			schemas: []FileSchema{
				{
					Path: "file1.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
					},
				},
				{
					Path: "file2.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_BIGINT},
					},
				},
			},
			wantCols: []ColumnInfo{
				{Name: "id", Type: dukdb.TYPE_BIGINT},
			},
			wantMaps: [][]int{
				{0},
				{0},
			},
		},
		{
			name: "type widening FLOAT to DOUBLE",
			schemas: []FileSchema{
				{
					Path: "file1.csv",
					Columns: []ColumnInfo{
						{Name: "value", Type: dukdb.TYPE_FLOAT},
					},
				},
				{
					Path: "file2.csv",
					Columns: []ColumnInfo{
						{Name: "value", Type: dukdb.TYPE_DOUBLE},
					},
				},
			},
			wantCols: []ColumnInfo{
				{Name: "value", Type: dukdb.TYPE_DOUBLE},
			},
			wantMaps: [][]int{
				{0},
				{0},
			},
		},
		{
			name: "incompatible types INTEGER and VARCHAR",
			schemas: []FileSchema{
				{
					Path: "file1.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
					},
				},
				{
					Path: "file2.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_VARCHAR},
					},
				},
			},
			wantErr:     true,
			errContains: "incompatible column types",
		},
		{
			name: "three files with different schemas",
			schemas: []FileSchema{
				{
					Path: "file1.csv",
					Columns: []ColumnInfo{
						{Name: "a", Type: dukdb.TYPE_INTEGER},
					},
				},
				{
					Path: "file2.csv",
					Columns: []ColumnInfo{
						{Name: "b", Type: dukdb.TYPE_VARCHAR},
					},
				},
				{
					Path: "file3.csv",
					Columns: []ColumnInfo{
						{Name: "a", Type: dukdb.TYPE_INTEGER},
						{Name: "c", Type: dukdb.TYPE_DOUBLE},
					},
				},
			},
			wantCols: []ColumnInfo{
				{Name: "a", Type: dukdb.TYPE_INTEGER},
				{Name: "b", Type: dukdb.TYPE_VARCHAR},
				{Name: "c", Type: dukdb.TYPE_DOUBLE},
			},
			wantMaps: [][]int{
				{0, -1, -1}, // file1: a->0, b->missing, c->missing
				{-1, 0, -1}, // file2: a->missing, b->0, c->missing
				{0, -1, 1},  // file3: a->0, b->missing, c->1
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergeSchemas(tt.schemas, true)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Equal(t, tt.wantCols, result.Columns)
			assert.Equal(t, tt.wantMaps, result.Mappings)
		})
	}
}

func TestMergeSchemasByPosition(t *testing.T) {
	tests := []struct {
		name        string
		schemas     []FileSchema
		wantCols    []ColumnInfo
		wantErr     bool
		errContains string
	}{
		{
			name: "identical schemas",
			schemas: []FileSchema{
				{
					Path: "file1.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
						{Name: "name", Type: dukdb.TYPE_VARCHAR},
					},
				},
				{
					Path: "file2.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
						{Name: "name", Type: dukdb.TYPE_VARCHAR},
					},
				},
			},
			wantCols: []ColumnInfo{
				{Name: "id", Type: dukdb.TYPE_INTEGER},
				{Name: "name", Type: dukdb.TYPE_VARCHAR},
			},
		},
		{
			name: "different column count",
			schemas: []FileSchema{
				{
					Path: "file1.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
					},
				},
				{
					Path: "file2.csv",
					Columns: []ColumnInfo{
						{Name: "id", Type: dukdb.TYPE_INTEGER},
						{Name: "name", Type: dukdb.TYPE_VARCHAR},
					},
				},
			},
			wantErr:     true,
			errContains: "column count mismatch",
		},
		{
			name: "type widening by position",
			schemas: []FileSchema{
				{
					Path: "file1.csv",
					Columns: []ColumnInfo{
						{Name: "col", Type: dukdb.TYPE_INTEGER},
					},
				},
				{
					Path: "file2.csv",
					Columns: []ColumnInfo{
						{Name: "col", Type: dukdb.TYPE_BIGINT},
					},
				},
			},
			wantCols: []ColumnInfo{
				{Name: "col", Type: dukdb.TYPE_BIGINT},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergeSchemas(tt.schemas, false)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Equal(t, tt.wantCols, result.Columns)
		})
	}
}

func TestValidateTypeCompatibility(t *testing.T) {
	tests := []struct {
		name    string
		t1      dukdb.Type
		t2      dukdb.Type
		want    dukdb.Type
		wantErr bool
	}{
		// Same types
		{name: "same INTEGER", t1: dukdb.TYPE_INTEGER, t2: dukdb.TYPE_INTEGER, want: dukdb.TYPE_INTEGER},
		{name: "same VARCHAR", t1: dukdb.TYPE_VARCHAR, t2: dukdb.TYPE_VARCHAR, want: dukdb.TYPE_VARCHAR},

		// Signed integer widening
		{name: "TINYINT to SMALLINT", t1: dukdb.TYPE_TINYINT, t2: dukdb.TYPE_SMALLINT, want: dukdb.TYPE_SMALLINT},
		{name: "SMALLINT to INTEGER", t1: dukdb.TYPE_SMALLINT, t2: dukdb.TYPE_INTEGER, want: dukdb.TYPE_INTEGER},
		{name: "INTEGER to BIGINT", t1: dukdb.TYPE_INTEGER, t2: dukdb.TYPE_BIGINT, want: dukdb.TYPE_BIGINT},
		{name: "TINYINT to BIGINT", t1: dukdb.TYPE_TINYINT, t2: dukdb.TYPE_BIGINT, want: dukdb.TYPE_BIGINT},

		// Unsigned integer widening
		{name: "UTINYINT to USMALLINT", t1: dukdb.TYPE_UTINYINT, t2: dukdb.TYPE_USMALLINT, want: dukdb.TYPE_USMALLINT},
		{name: "USMALLINT to UINTEGER", t1: dukdb.TYPE_USMALLINT, t2: dukdb.TYPE_UINTEGER, want: dukdb.TYPE_UINTEGER},
		{name: "UINTEGER to UBIGINT", t1: dukdb.TYPE_UINTEGER, t2: dukdb.TYPE_UBIGINT, want: dukdb.TYPE_UBIGINT},

		// Floating point widening
		{name: "FLOAT to DOUBLE", t1: dukdb.TYPE_FLOAT, t2: dukdb.TYPE_DOUBLE, want: dukdb.TYPE_DOUBLE},

		// Integer to float
		{name: "INTEGER to DOUBLE", t1: dukdb.TYPE_INTEGER, t2: dukdb.TYPE_DOUBLE, want: dukdb.TYPE_DOUBLE},
		{name: "BIGINT to FLOAT", t1: dukdb.TYPE_BIGINT, t2: dukdb.TYPE_FLOAT, want: dukdb.TYPE_DOUBLE},

		// Signed to unsigned (widen to larger signed)
		{name: "INTEGER to UTINYINT", t1: dukdb.TYPE_INTEGER, t2: dukdb.TYPE_UTINYINT, want: dukdb.TYPE_INTEGER},
		{name: "TINYINT to USMALLINT", t1: dukdb.TYPE_TINYINT, t2: dukdb.TYPE_USMALLINT, want: dukdb.TYPE_INTEGER},

		// Timestamp types
		{name: "TIMESTAMP to TIMESTAMP_NS", t1: dukdb.TYPE_TIMESTAMP, t2: dukdb.TYPE_TIMESTAMP_NS, want: dukdb.TYPE_TIMESTAMP_NS},

		// Incompatible types
		{name: "INTEGER to VARCHAR", t1: dukdb.TYPE_INTEGER, t2: dukdb.TYPE_VARCHAR, wantErr: true},
		{name: "VARCHAR to DOUBLE", t1: dukdb.TYPE_VARCHAR, t2: dukdb.TYPE_DOUBLE, wantErr: true},
		{name: "BOOLEAN to INTEGER", t1: dukdb.TYPE_BOOLEAN, t2: dukdb.TYPE_INTEGER, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ValidateTypeCompatibility(tt.t1, tt.t2)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrIncompatibleTypes)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestWidenType(t *testing.T) {
	tests := []struct {
		name string
		t1   dukdb.Type
		t2   dukdb.Type
		want dukdb.Type
	}{
		{name: "same type", t1: dukdb.TYPE_INTEGER, t2: dukdb.TYPE_INTEGER, want: dukdb.TYPE_INTEGER},
		{name: "INT to BIGINT", t1: dukdb.TYPE_INTEGER, t2: dukdb.TYPE_BIGINT, want: dukdb.TYPE_BIGINT},
		{name: "BIGINT to INT", t1: dukdb.TYPE_BIGINT, t2: dukdb.TYPE_INTEGER, want: dukdb.TYPE_BIGINT},
		{name: "FLOAT to DOUBLE", t1: dukdb.TYPE_FLOAT, t2: dukdb.TYPE_DOUBLE, want: dukdb.TYPE_DOUBLE},
		{name: "TINYINT to HUGEINT", t1: dukdb.TYPE_TINYINT, t2: dukdb.TYPE_HUGEINT, want: dukdb.TYPE_HUGEINT},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := WidenType(tt.t1, tt.t2)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestAlignChunk(t *testing.T) {
	t.Run("no alignment needed", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})
		chunk.AppendRow([]any{int32(1), "hello"})
		chunk.AppendRow([]any{int32(2), "world"})

		mapping := []int{0, 1}
		targetTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

		aligned := AlignChunk(chunk, mapping, targetTypes)

		require.NotNil(t, aligned)
		assert.Equal(t, 2, aligned.Count())
		assert.Equal(t, int32(1), aligned.GetValue(0, 0))
		assert.Equal(t, "hello", aligned.GetValue(0, 1))
		assert.Equal(t, int32(2), aligned.GetValue(1, 0))
		assert.Equal(t, "world", aligned.GetValue(1, 1))
	})

	t.Run("reorder columns", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER})
		chunk.AppendRow([]any{"hello", int32(1)})

		mapping := []int{1, 0} // swap columns
		targetTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

		aligned := AlignChunk(chunk, mapping, targetTypes)

		require.NotNil(t, aligned)
		assert.Equal(t, 1, aligned.Count())
		assert.Equal(t, int32(1), aligned.GetValue(0, 0))
		assert.Equal(t, "hello", aligned.GetValue(0, 1))
	})

	t.Run("missing columns filled with NULL", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
		chunk.AppendRow([]any{int32(42)})

		mapping := []int{0, -1} // second column missing
		targetTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

		aligned := AlignChunk(chunk, mapping, targetTypes)

		require.NotNil(t, aligned)
		assert.Equal(t, 1, aligned.Count())
		assert.Equal(t, int32(42), aligned.GetValue(0, 0))
		assert.Nil(t, aligned.GetValue(0, 1))
	})

	t.Run("type widening INTEGER to BIGINT", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
		chunk.AppendRow([]any{int32(1000000)})

		mapping := []int{0}
		targetTypes := []dukdb.Type{dukdb.TYPE_BIGINT}

		aligned := AlignChunk(chunk, mapping, targetTypes)

		require.NotNil(t, aligned)
		assert.Equal(t, 1, aligned.Count())
		// Value should be converted to int64
		assert.Equal(t, int64(1000000), aligned.GetValue(0, 0))
	})

	t.Run("nil chunk returns empty", func(t *testing.T) {
		mapping := []int{0}
		targetTypes := []dukdb.Type{dukdb.TYPE_INTEGER}

		aligned := AlignChunk(nil, mapping, targetTypes)

		require.NotNil(t, aligned)
		assert.Equal(t, 0, aligned.Count())
	})
}

func TestConcatenateChunks(t *testing.T) {
	t.Run("concatenate two chunks", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

		chunk1 := storage.NewDataChunk(types)
		chunk1.AppendRow([]any{int32(1), "a"})
		chunk1.AppendRow([]any{int32(2), "b"})

		chunk2 := storage.NewDataChunk(types)
		chunk2.AppendRow([]any{int32(3), "c"})

		result := ConcatenateChunks([]*storage.DataChunk{chunk1, chunk2})

		require.NotNil(t, result)
		assert.Equal(t, 3, result.Count())
		assert.Equal(t, int32(1), result.GetValue(0, 0))
		assert.Equal(t, "a", result.GetValue(0, 1))
		assert.Equal(t, int32(2), result.GetValue(1, 0))
		assert.Equal(t, "b", result.GetValue(1, 1))
		assert.Equal(t, int32(3), result.GetValue(2, 0))
		assert.Equal(t, "c", result.GetValue(2, 1))
	})

	t.Run("skip nil chunks", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER}

		chunk1 := storage.NewDataChunk(types)
		chunk1.AppendRow([]any{int32(1)})

		result := ConcatenateChunks([]*storage.DataChunk{nil, chunk1, nil})

		require.NotNil(t, result)
		assert.Equal(t, 1, result.Count())
		assert.Equal(t, int32(1), result.GetValue(0, 0))
	})

	t.Run("all nil returns nil", func(t *testing.T) {
		result := ConcatenateChunks([]*storage.DataChunk{nil, nil})
		assert.Nil(t, result)
	})

	t.Run("empty slice returns nil", func(t *testing.T) {
		result := ConcatenateChunks(nil)
		assert.Nil(t, result)
	})
}

func TestAddMetadataColumns(t *testing.T) {
	t.Run("add filename column", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
		chunk.AppendRow([]any{int32(1)})
		chunk.AppendRow([]any{int32(2)})

		opts := &MultiFileOptions{Filename: true}
		result := AddMetadataColumns(chunk, "/path/to/file.csv", 0, 0, opts)

		require.NotNil(t, result)
		assert.Equal(t, 2, result.Count())
		assert.Equal(t, 2, result.ColumnCount())
		assert.Equal(t, int32(1), result.GetValue(0, 0))
		assert.Equal(t, "/path/to/file.csv", result.GetValue(0, 1))
		assert.Equal(t, int32(2), result.GetValue(1, 0))
		assert.Equal(t, "/path/to/file.csv", result.GetValue(1, 1))
	})

	t.Run("add file_row_number column", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
		chunk.AppendRow([]any{int32(1)})
		chunk.AppendRow([]any{int32(2)})
		chunk.AppendRow([]any{int32(3)})

		opts := &MultiFileOptions{FileRowNumber: true}
		result := AddMetadataColumns(chunk, "file.csv", 0, 0, opts)

		require.NotNil(t, result)
		assert.Equal(t, 3, result.Count())
		// Row numbers are 1-indexed
		assert.Equal(t, int64(1), result.GetValue(0, 1))
		assert.Equal(t, int64(2), result.GetValue(1, 1))
		assert.Equal(t, int64(3), result.GetValue(2, 1))
	})

	t.Run("add file_row_number with offset", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
		chunk.AppendRow([]any{int32(1)})
		chunk.AppendRow([]any{int32(2)})

		opts := &MultiFileOptions{FileRowNumber: true}
		result := AddMetadataColumns(chunk, "file.csv", 0, 100, opts) // start at row 100

		require.NotNil(t, result)
		assert.Equal(t, int64(101), result.GetValue(0, 1))
		assert.Equal(t, int64(102), result.GetValue(1, 1))
	})

	t.Run("add file_index column", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
		chunk.AppendRow([]any{int32(1)})

		opts := &MultiFileOptions{FileIndex: true}
		result := AddMetadataColumns(chunk, "file.csv", 5, 0, opts)

		require.NotNil(t, result)
		assert.Equal(t, int32(5), result.GetValue(0, 1))
	})

	t.Run("add all metadata columns", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
		chunk.AppendRow([]any{int32(42)})

		opts := &MultiFileOptions{
			Filename:      true,
			FileRowNumber: true,
			FileIndex:     true,
		}
		result := AddMetadataColumns(chunk, "/data/file.csv", 3, 10, opts)

		require.NotNil(t, result)
		assert.Equal(t, 1, result.Count())
		assert.Equal(t, 4, result.ColumnCount())
		assert.Equal(t, int32(42), result.GetValue(0, 0))     // original data
		assert.Equal(t, "/data/file.csv", result.GetValue(0, 1)) // filename
		assert.Equal(t, int64(11), result.GetValue(0, 2))       // row number (1-indexed)
		assert.Equal(t, int32(3), result.GetValue(0, 3))        // file index
	})

	t.Run("no metadata columns returns original", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
		chunk.AppendRow([]any{int32(1)})

		opts := &MultiFileOptions{}
		result := AddMetadataColumns(chunk, "file.csv", 0, 0, opts)

		assert.Equal(t, chunk, result)
		assert.Equal(t, 1, result.ColumnCount())
	})

	t.Run("nil options returns original", func(t *testing.T) {
		chunk := storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER})
		chunk.AppendRow([]any{int32(1)})

		result := AddMetadataColumns(chunk, "file.csv", 0, 0, nil)
		assert.Equal(t, chunk, result)
	})
}

func TestValidateMaxFiles(t *testing.T) {
	t.Run("under limit with default", func(t *testing.T) {
		err := ValidateMaxFiles(100, nil)
		assert.NoError(t, err)
	})

	t.Run("at default limit", func(t *testing.T) {
		err := ValidateMaxFiles(MaxFilesPerGlob, nil)
		assert.NoError(t, err)
	})

	t.Run("over default limit", func(t *testing.T) {
		err := ValidateMaxFiles(MaxFilesPerGlob+1, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTooManyFiles)
	})

	t.Run("custom limit under", func(t *testing.T) {
		opts := &MultiFileOptions{MaxFiles: 50}
		err := ValidateMaxFiles(50, opts)
		assert.NoError(t, err)
	})

	t.Run("custom limit over", func(t *testing.T) {
		opts := &MultiFileOptions{MaxFiles: 50}
		err := ValidateMaxFiles(51, opts)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTooManyFiles)
	})
}

func TestIsGlobPattern(t *testing.T) {
	tests := []struct {
		path   string
		isGlob bool
	}{
		{"/path/to/file.csv", false},
		{"/path/to/*.csv", true},
		{"/path/to/file?.csv", true},
		{"/path/to/[ab].csv", true},
		{"/path/**/file.csv", true},
		{"simple.txt", false},
		{"file[0-9].txt", true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := IsGlobPattern(tt.path)
			assert.Equal(t, tt.isGlob, result)
		})
	}
}

func TestNewMultiFileReader(t *testing.T) {
	t.Run("empty paths", func(t *testing.T) {
		_, err := NewMultiFileReader(nil, nil, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNoFilesMatched)
	})

	t.Run("valid paths", func(t *testing.T) {
		paths := []string{"file1.csv", "file2.csv"}
		reader, err := NewMultiFileReader(paths, nil, nil)
		require.NoError(t, err)
		assert.NotNil(t, reader)
		assert.Equal(t, paths, reader.Paths)
	})

	t.Run("too many files", func(t *testing.T) {
		paths := make([]string, MaxFilesPerGlob+1)
		for i := range paths {
			paths[i] = "file.csv"
		}
		_, err := NewMultiFileReader(paths, nil, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTooManyFiles)
	})

	t.Run("custom max files limit", func(t *testing.T) {
		opts := &MultiFileOptions{MaxFiles: 5}
		paths := make([]string, 6)
		for i := range paths {
			paths[i] = "file.csv"
		}
		_, err := NewMultiFileReader(paths, opts, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTooManyFiles)
	})
}

func TestSchemaMergeResult_Types(t *testing.T) {
	result := &SchemaMergeResult{
		Columns: []ColumnInfo{
			{Name: "id", Type: dukdb.TYPE_INTEGER},
			{Name: "name", Type: dukdb.TYPE_VARCHAR},
			{Name: "value", Type: dukdb.TYPE_DOUBLE},
		},
	}

	types := result.Types()
	assert.Equal(t, []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}, types)
}

func TestSchemaMergeResult_Names(t *testing.T) {
	result := &SchemaMergeResult{
		Columns: []ColumnInfo{
			{Name: "id", Type: dukdb.TYPE_INTEGER},
			{Name: "name", Type: dukdb.TYPE_VARCHAR},
			{Name: "value", Type: dukdb.TYPE_DOUBLE},
		},
	}

	names := result.Names()
	assert.Equal(t, []string{"id", "name", "value"}, names)
}

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name       string
		val        any
		srcType    dukdb.Type
		targetType dukdb.Type
		want       any
	}{
		{name: "nil value", val: nil, srcType: dukdb.TYPE_INTEGER, targetType: dukdb.TYPE_BIGINT, want: nil},
		{name: "same type", val: int32(42), srcType: dukdb.TYPE_INTEGER, targetType: dukdb.TYPE_INTEGER, want: int32(42)},
		{name: "int32 to int64", val: int32(42), srcType: dukdb.TYPE_INTEGER, targetType: dukdb.TYPE_BIGINT, want: int64(42)},
		{name: "int8 to int64", val: int8(42), srcType: dukdb.TYPE_TINYINT, targetType: dukdb.TYPE_BIGINT, want: int64(42)},
		{name: "int32 to float64", val: int32(42), srcType: dukdb.TYPE_INTEGER, targetType: dukdb.TYPE_DOUBLE, want: float64(42)},
		{name: "float32 to float64", val: float32(3.14), srcType: dukdb.TYPE_FLOAT, targetType: dukdb.TYPE_DOUBLE, want: float64(float32(3.14))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertValue(tt.val, tt.srcType, tt.targetType)
			assert.Equal(t, tt.want, result)
		})
	}
}
