package xlsx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnLettersToIndex(t *testing.T) {
	tests := []struct {
		letters string
		want    int
	}{
		{"A", 0},
		{"B", 1},
		{"Z", 25},
		{"AA", 26},
		{"AB", 27},
		{"AZ", 51},
		{"BA", 52},
		{"ZZ", 701},
		{"AAA", 702},
		{"", -1},
		{"a", 0},  // lowercase should work
		{"z", 25}, // lowercase should work
	}

	for _, tt := range tests {
		t.Run(tt.letters, func(t *testing.T) {
			got := ColumnLettersToIndex(tt.letters)
			assert.Equal(t, tt.want, got, "ColumnLettersToIndex(%q)", tt.letters)
		})
	}
}

func TestIndexToColumnLetters(t *testing.T) {
	tests := []struct {
		index int
		want  string
	}{
		{0, "A"},
		{1, "B"},
		{25, "Z"},
		{26, "AA"},
		{27, "AB"},
		{51, "AZ"},
		{52, "BA"},
		{701, "ZZ"},
		{702, "AAA"},
		{-1, ""},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := IndexToColumnLetters(tt.index)
			assert.Equal(t, tt.want, got, "IndexToColumnLetters(%d)", tt.index)
		})
	}
}

func TestColumnLettersRoundTrip(t *testing.T) {
	// Test round-trip conversion for a range of values
	for i := 0; i < 1000; i++ {
		letters := IndexToColumnLetters(i)
		index := ColumnLettersToIndex(letters)
		assert.Equal(t, i, index, "Round trip failed for index %d (letters: %q)", i, letters)
	}
}

func TestParseCell(t *testing.T) {
	tests := []struct {
		cell    string
		wantCol int
		wantRow int
		wantErr bool
	}{
		{"A1", 0, 1, false},
		{"B2", 1, 2, false},
		{"Z10", 25, 10, false},
		{"AA100", 26, 100, false},
		{"AZ1000", 51, 1000, false},
		{"a1", 0, 1, false},   // lowercase
		{" A1 ", 0, 1, false}, // whitespace
		{"", 0, 0, true},
		{"1", 0, 0, true},     // no column
		{"A", 0, 0, true},     // no row
		{"A0", 0, 0, true},    // row must be >= 1
		{"A-1", 0, 0, true},   // invalid row
		{"1A", 0, 0, true},    // wrong order
		{"A1B", 0, 0, true},   // extra letters
	}

	for _, tt := range tests {
		t.Run(tt.cell, func(t *testing.T) {
			col, row, err := ParseCell(tt.cell)
			if tt.wantErr {
				assert.Error(t, err, "ParseCell(%q) expected error", tt.cell)
			} else {
				require.NoError(t, err, "ParseCell(%q) unexpected error", tt.cell)
				assert.Equal(t, tt.wantCol, col, "ParseCell(%q) column", tt.cell)
				assert.Equal(t, tt.wantRow, row, "ParseCell(%q) row", tt.cell)
			}
		})
	}
}

func TestParseRange(t *testing.T) {
	tests := []struct {
		rangeStr     string
		wantStartCol int
		wantStartRow int
		wantEndCol   int
		wantEndRow   int
		wantErr      bool
	}{
		{"A1:B2", 0, 1, 1, 2, false},
		{"A1:D100", 0, 1, 3, 100, false},
		{"AA1:AZ50", 26, 1, 51, 50, false},
		{" A1 : B2 ", 0, 1, 1, 2, false}, // whitespace
		{"", 0, 0, 0, 0, true},
		{"A1", 0, 0, 0, 0, true},     // missing colon
		{"A1:A1", 0, 1, 0, 1, false}, // same cell is valid
		{"B1:A1", 0, 0, 0, 0, true},  // end col before start
		{"A2:A1", 0, 0, 0, 0, true},  // end row before start
		{":B2", 0, 0, 0, 0, true},    // missing start
		{"A1:", 0, 0, 0, 0, true},    // missing end
	}

	for _, tt := range tests {
		t.Run(tt.rangeStr, func(t *testing.T) {
			startCol, startRow, endCol, endRow, err := ParseRange(tt.rangeStr)
			if tt.wantErr {
				assert.Error(t, err, "ParseRange(%q) expected error", tt.rangeStr)
			} else {
				require.NoError(t, err, "ParseRange(%q) unexpected error", tt.rangeStr)
				assert.Equal(t, tt.wantStartCol, startCol, "ParseRange(%q) startCol", tt.rangeStr)
				assert.Equal(t, tt.wantStartRow, startRow, "ParseRange(%q) startRow", tt.rangeStr)
				assert.Equal(t, tt.wantEndCol, endCol, "ParseRange(%q) endCol", tt.rangeStr)
				assert.Equal(t, tt.wantEndRow, endRow, "ParseRange(%q) endRow", tt.rangeStr)
			}
		})
	}
}

func TestCellAddress(t *testing.T) {
	tests := []struct {
		col  int
		row  int
		want string
	}{
		{0, 1, "A1"},
		{1, 2, "B2"},
		{25, 10, "Z10"},
		{26, 100, "AA100"},
		{51, 1000, "AZ1000"},
		{-1, 1, ""},  // invalid column
		{0, 0, ""},   // invalid row
		{0, -1, ""},  // invalid row
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := CellAddress(tt.col, tt.row)
			assert.Equal(t, tt.want, got, "CellAddress(%d, %d)", tt.col, tt.row)
		})
	}
}

func TestDefaultReaderOptions(t *testing.T) {
	opts := DefaultReaderOptions()

	assert.Equal(t, "", opts.Sheet)
	assert.Equal(t, DefaultSheetIndex, opts.SheetIndex)
	assert.Equal(t, "", opts.Range)
	assert.Equal(t, 0, opts.StartRow)
	assert.Equal(t, 0, opts.EndRow)
	assert.Equal(t, "", opts.StartCol)
	assert.Equal(t, "", opts.EndCol)
	assert.True(t, opts.Header)
	assert.Equal(t, 0, opts.Skip)
	assert.Nil(t, opts.Columns)
	assert.True(t, opts.InferTypes)
	assert.Equal(t, "", opts.DateFormat)
	assert.Equal(t, DefaultChunkSize, opts.ChunkSize)
	assert.True(t, opts.EmptyAsNull)
	assert.Nil(t, opts.NullValues)
}

func TestDefaultWriterOptions(t *testing.T) {
	opts := DefaultWriterOptions()

	assert.Equal(t, "Sheet1", opts.SheetName)
	assert.True(t, opts.Header)
	assert.Equal(t, "", opts.DateFormat)
	assert.Equal(t, "", opts.TimeFormat)
	assert.True(t, opts.AutoWidth)
	assert.Equal(t, DefaultCompressionLevel, opts.CompressionLevel)
}

func TestReaderOptionsApplyDefaults(t *testing.T) {
	opts := &ReaderOptions{}
	opts.applyDefaults()

	assert.Equal(t, DefaultChunkSize, opts.ChunkSize)
}

func TestWriterOptionsApplyDefaults(t *testing.T) {
	t.Run("empty sheet name", func(t *testing.T) {
		opts := &WriterOptions{}
		opts.applyDefaults()
		assert.Equal(t, "Sheet1", opts.SheetName)
	})

	t.Run("compression level clamping", func(t *testing.T) {
		opts := &WriterOptions{CompressionLevel: -5}
		opts.applyDefaults()
		assert.Equal(t, 0, opts.CompressionLevel)

		opts = &WriterOptions{CompressionLevel: 15}
		opts.applyDefaults()
		assert.Equal(t, 9, opts.CompressionLevel)
	})
}
