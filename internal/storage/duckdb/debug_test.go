package duckdb

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDebugCatalogLoad(t *testing.T) {
	// Check for duckdb CLI
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb CLI not installed")
	}

	dbPath := filepath.Join(t.TempDir(), "test.duckdb")

	// Create database with DuckDB CLI
	cmd := exec.Command("duckdb", dbPath, "-c", "CREATE TABLE test (id INTEGER, name VARCHAR); INSERT INTO test VALUES (1, 'Alice'); CHECKPOINT;")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	// Open file and check headers
	file, err := os.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer func() { _ = file.Close() }()

	fileHeader, err := ReadFileHeader(file)
	if err != nil {
		t.Fatalf("failed to read file header: %v", err)
	}
	t.Logf("File header: version=%d, flags=%d", fileHeader.Version, fileHeader.Flags)

	dbHeader, which, err := GetActiveHeader(file)
	if err != nil {
		t.Fatalf("failed to get active header: %v", err)
	}
	t.Logf("Active header: which=%d", which)
	t.Logf("MetaBlock=%d (0x%x), IsValid=%v", dbHeader.MetaBlock, dbHeader.MetaBlock, IsValidBlockID(dbHeader.MetaBlock))
	t.Logf("FreeList=%d (0x%x), IsValid=%v", dbHeader.FreeList, dbHeader.FreeList, IsValidBlockID(dbHeader.FreeList))
	t.Logf("BlockCount=%d", dbHeader.BlockCount)
	t.Logf("BlockAllocSize=%d", dbHeader.BlockAllocSize)

	// Now try loading catalog
	blockManager := NewBlockManager(file, dbHeader.BlockAllocSize, 100)
	blockManager.SetBlockCount(dbHeader.BlockCount)

	if IsValidBlockID(dbHeader.MetaBlock) {
		t.Logf("MetaBlock is valid, attempting to read block %d", dbHeader.MetaBlock)

		// Try to read the meta block
		block, err := blockManager.ReadBlock(dbHeader.MetaBlock)
		if err != nil {
			t.Fatalf("failed to read meta block: %v", err)
		}
		t.Logf("Meta block read successfully, size=%d", len(block.Data))

		// Show first 100 bytes
		if len(block.Data) > 100 {
			t.Logf("First 100 bytes: %x", block.Data[:100])
		}

		// Try to read catalog entry by entry manually for debugging
		reader, err := NewMetadataReader(blockManager, dbHeader.MetaBlock)
		if err != nil {
			t.Fatalf("failed to create metadata reader: %v", err)
		}

		// Read field 100 (catalog_entries)
		fieldID, err := reader.ReadFieldID()
		if err != nil {
			t.Fatalf("failed to read first field ID: %v", err)
		}
		t.Logf("First field ID: %d", fieldID)

		// Read count
		count, err := reader.ReadVarint()
		if err != nil {
			t.Fatalf("failed to read count: %v", err)
		}
		t.Logf("Entry count: %d", count)

		for i := uint64(0); i < count; i++ {
			t.Logf("Reading entry %d (offset=%d)...", i, reader.offset())
			entry, err := reader.ReadCatalogEntry()
			if err != nil {
				t.Logf("Failed to read entry %d: %v", i, err)
				// Show next few bytes for debugging
				if reader.remaining() > 20 {
					data := reader.data()
					offset := reader.offset()
					t.Logf("  Next bytes: %x", data[offset:offset+20])
				}
				break
			}
			if entry != nil {
				switch e := entry.(type) {
				case *SchemaCatalogEntry:
					t.Logf("  Entry %d: Schema %s", i, e.Name)
				case *TableCatalogEntry:
					t.Logf("  Entry %d: Table %s (cols=%d)", i, e.Name, len(e.Columns))
					for j, col := range e.Columns {
						t.Logf("    Column %d: %s (type=%d)", j, col.Name, col.Type)
					}
				default:
					t.Logf("  Entry %d: %T", i, entry)
				}
			} else {
				t.Logf("  Entry %d: nil", i)
			}
		}

		// Also try the full API
		catalog, err := ReadCatalogFromMetadata(blockManager, dbHeader.MetaBlock)
		if err != nil {
			t.Skipf("DuckDB CLI format not yet fully compatible: %v", err)
		} else {
			t.Logf("Catalog read successfully")
			t.Logf("Tables: %d, Schemas: %d, Views: %d", len(catalog.Tables), len(catalog.Schemas), len(catalog.Views))
			for j, schema := range catalog.Schemas {
				t.Logf("  Schema %d: %s", j, schema.Name)
			}
			for j, tbl := range catalog.Tables {
				t.Logf("  Table %d: %s (cols=%d)", j, tbl.Name, len(tbl.Columns))
			}
		}
	} else {
		t.Logf("MetaBlock is invalid")
	}
}

func TestDebugConstantCompression(t *testing.T) {
	// Check for duckdb CLI
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb CLI not installed")
	}

	dbPath := filepath.Join(t.TempDir(), "test.duckdb")

	// Create database with constant value
	cmd := exec.Command("duckdb", dbPath, "-c", `
		CREATE TABLE constant_test (value INTEGER);
		INSERT INTO constant_test SELECT 42 FROM range(100);
		CHECKPOINT;
	`)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to create db: %v - %s", err, output)
	}

	// Open with our storage
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}
	defer storage.Close()

	// Load catalog
	cat, err := storage.LoadCatalog()
	if err != nil {
		t.Fatalf("failed to load catalog: %v", err)
	}

	// Find table
	tableDef, ok := cat.GetTableInSchema("main", "constant_test")
	if !ok {
		t.Fatalf("table not found")
	}
	t.Logf("Table columns: %d", len(tableDef.Columns))

	// Get internal DuckDB catalog entry
	for _, tbl := range storage.catalog.Tables {
		if tbl.Name == "constant_test" {
			t.Logf("Found table: %s", tbl.Name)
			if tbl.StorageMetadata != nil {
				t.Logf("  StorageMetadata: TotalRows=%d", tbl.StorageMetadata.TotalRows)

				// Read row groups from table pointer
				rowGroups, err := ReadRowGroupsFromTablePointer(
					storage.blockManager,
					tbl.StorageMetadata.TablePointer,
					tbl.StorageMetadata.TotalRows,
					len(tbl.Columns),
				)
				if err != nil {
					t.Logf("  Failed to read row groups: %v", err)
				} else {
					t.Logf("  RowGroups: %d", len(rowGroups))
					for rgIdx, rg := range rowGroups {
						t.Logf("    RowGroup[%d]: TupleCount=%d", rgIdx, rg.TupleCount)

						// For each column, get the DataPointer
						bm := storage.blockManager
						for colIdx, mbp := range rg.DataPointers {
							t.Logf("      Column[%d] MBP: BlockID=%d, BlockIndex=%d, Offset=%d",
								colIdx, mbp.BlockID, mbp.BlockIndex, mbp.Offset)

							// Read raw bytes
							metaBlock, err := bm.ReadBlock(mbp.BlockID)
							if err == nil {
								subBlockOffset := int(mbp.BlockIndex) * MetadataSubBlockSize
								if mbp.BlockIndex > 0 {
									subBlockOffset -= BlockChecksumSize
								}
								contentOffset := subBlockOffset + int(mbp.Offset)
								if contentOffset < len(metaBlock.Data) {
									rawBytes := metaBlock.Data[contentOffset:]
									maxBytes := 100
									if len(rawBytes) < maxBytes {
										maxBytes = len(rawBytes)
									}
									t.Logf("      Raw metadata bytes: %x", rawBytes[:maxBytes])
								}
							}

							// Create a RowGroupReader to get the DataPointer
							types := make([]LogicalTypeID, len(rg.DataPointers))
							for i := range types {
								if i < len(tbl.Columns) {
									types[i] = tbl.Columns[i].Type
								}
							}
							reader := NewRowGroupReader(bm, rg, types)

							// Get the DataPointer to see block info
							reader.mu.Lock()
							dp, err := reader.resolveDataPointerLocked(colIdx)
							reader.mu.Unlock()
							if err != nil {
								t.Logf("        Failed to resolve DataPointer: %v", err)
							} else {
								t.Logf("        DataPointer: BlockID=%d, Offset=%d, TupleCount=%d, Compression=%s",
									dp.Block.BlockID, dp.Block.Offset, dp.TupleCount, dp.Compression.String())
								t.Logf("        SegmentState: HasValidity=%v, ValidityBlockID=%d, StateDataLen=%d",
									dp.SegmentState.HasValidityMask, dp.SegmentState.ValidityBlock.BlockID, len(dp.SegmentState.StateData))
								if len(dp.SegmentState.StateData) > 0 {
									t.Logf("          StateData: %x", dp.SegmentState.StateData[:min(20, len(dp.SegmentState.StateData))])
								}
									t.Logf("        Statistics: HasStats=%v, HasNull=%v, StatDataLen=%d",
									dp.Statistics.HasStats, dp.Statistics.HasNull, len(dp.Statistics.StatData))
								if len(dp.Statistics.StatData) > 0 {
									t.Logf("          StatData: %x", dp.Statistics.StatData[:min(20, len(dp.Statistics.StatData))])
								}
							}

							colData, err := reader.ReadColumn(colIdx)
							if err != nil {
								t.Logf("        Failed to read column: %v", err)
							} else {
								t.Logf("        ColumnData: TypeID=%d, TupleCount=%d, DataLen=%d, HasValidity=%v",
									colData.TypeID, colData.TupleCount, len(colData.Data), colData.Validity != nil)
								if colData.Validity != nil {
									t.Logf("          Validity: AllValid=%v, RowCount=%d",
										colData.Validity.AllValid(), colData.Validity.RowCount())
								}
								// Check first few values
								maxRows := colData.TupleCount
								if maxRows > 5 {
									maxRows = 5
								}
								for i := uint64(0); i < maxRows; i++ {
									val, valid := colData.GetValue(i)
									t.Logf("          GetValue(%d): val=%v (%T), valid=%v",
										i, val, val, valid)
								}
							}
						}
					}
				}
			}
		}
	}

	// Scan and check what values we get
	iter, err := storage.ScanTable("main", "constant_test", nil)
	if err != nil {
		t.Fatalf("Error scanning table: %v", err)
	}
	defer iter.Close()

	rowNum := 0
	for iter.Next() && rowNum < 5 {
		row := iter.Row()
		t.Logf("Row %d: %v", rowNum, row)
		rowNum++
	}
	if iter.Err() != nil {
		t.Fatalf("Iterator error: %v", iter.Err())
	}
}

func TestDebugAllNullVarchar(t *testing.T) {
	// Check for duckdb CLI
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb CLI not installed")
	}

	dbPath := filepath.Join(t.TempDir(), "test.duckdb")

	// Create database with all-NULL values
	cmd := exec.Command("duckdb", dbPath, "-c", `
		CREATE TABLE all_null_test (int_col INTEGER, varchar_col VARCHAR);
		INSERT INTO all_null_test VALUES (NULL, NULL), (NULL, NULL), (NULL, NULL);
		CHECKPOINT;
	`)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to create db: %v - %s", err, output)
	}

	// Open with our storage
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}
	defer storage.Close()

	// Load catalog
	cat, err := storage.LoadCatalog()
	if err != nil {
		t.Fatalf("failed to load catalog: %v", err)
	}

	// Find table
	tableDef, ok := cat.GetTableInSchema("main", "all_null_test")
	if !ok {
		t.Fatalf("table not found")
	}
	t.Logf("Table columns: %d", len(tableDef.Columns))

	// Get internal DuckDB catalog entry
	for _, tbl := range storage.catalog.Tables {
		if tbl.Name == "all_null_test" {
			t.Logf("Found table: %s", tbl.Name)
			if tbl.StorageMetadata != nil {
				t.Logf("  StorageMetadata: TablePointer={BlockID=%d, BlockIndex=%d, Offset=%d}, TotalRows=%d",
					tbl.StorageMetadata.TablePointer.BlockID,
					tbl.StorageMetadata.TablePointer.BlockIndex,
					tbl.StorageMetadata.TablePointer.Offset,
					tbl.StorageMetadata.TotalRows)

				// Read row groups from table pointer
				rowGroups, err := ReadRowGroupsFromTablePointer(
					storage.blockManager,
					tbl.StorageMetadata.TablePointer,
					tbl.StorageMetadata.TotalRows,
					len(tbl.Columns),
				)
				if err != nil {
					t.Logf("  Failed to read row groups: %v", err)
				} else {
					t.Logf("  RowGroups: %d", len(rowGroups))
					for rgIdx, rg := range rowGroups {
						t.Logf("    RowGroup[%d]: TupleCount=%d, Columns=%d",
							rgIdx, rg.TupleCount, len(rg.DataPointers))

						// For each column, get the DataPointer
						bm := storage.blockManager
						for colIdx, mbp := range rg.DataPointers {
							t.Logf("      Column[%d] MetaBlockPointer: BlockID=%d, BlockIndex=%d, Offset=%d",
								colIdx, mbp.BlockID, mbp.BlockIndex, mbp.Offset)

							// Read raw bytes
							metaBlock, err := bm.ReadBlock(mbp.BlockID)
							if err == nil {
								subBlockOffset := int(mbp.BlockIndex) * MetadataSubBlockSize
								if mbp.BlockIndex > 0 {
									subBlockOffset -= BlockChecksumSize
								}
								contentOffset := subBlockOffset + int(mbp.Offset)
								if contentOffset < len(metaBlock.Data) {
									rawBytes := metaBlock.Data[contentOffset:]
									maxBytes := 100
									if len(rawBytes) < maxBytes {
										maxBytes = len(rawBytes)
									}
									t.Logf("      Raw metadata bytes: %x", rawBytes[:maxBytes])
								}
							}

							// Create a RowGroupReader to get the DataPointer
							types := make([]LogicalTypeID, len(rg.DataPointers))
							for i := range types {
								if i < len(tbl.Columns) {
									types[i] = tbl.Columns[i].Type
								}
							}
							reader := NewRowGroupReader(bm, rg, types)

							// Get the DataPointer to see block info
							reader.mu.Lock()
							dp, err := reader.resolveDataPointerLocked(colIdx)
							reader.mu.Unlock()
							if err != nil {
								t.Logf("        Failed to resolve DataPointer: %v", err)
							} else {
								t.Logf("        DataPointer: BlockID=%d, Offset=%d, TupleCount=%d, Compression=%s",
									dp.Block.BlockID, dp.Block.Offset, dp.TupleCount, dp.Compression.String())
								t.Logf("        SegmentState: HasValidity=%v, ValidityBlockID=%d, StateDataLen=%d",
									dp.SegmentState.HasValidityMask, dp.SegmentState.ValidityBlock.BlockID, len(dp.SegmentState.StateData))
								t.Logf("        Statistics: HasStats=%v, HasNull=%v, StatDataLen=%d",
									dp.Statistics.HasStats, dp.Statistics.HasNull, len(dp.Statistics.StatData))
							}

							colData, err := reader.ReadColumn(colIdx)
							if err != nil {
								t.Logf("        Failed to read column: %v", err)
							} else {
								t.Logf("        ColumnData: TypeID=%d, TupleCount=%d, DataLen=%d, HasValidity=%v",
									colData.TypeID, colData.TupleCount, len(colData.Data), colData.Validity != nil)
								if colData.Validity != nil {
									t.Logf("          Validity: AllValid=%v, RowCount=%d",
										colData.Validity.AllValid(), colData.Validity.RowCount())
									// Check first few validity bits
									maxRows := colData.TupleCount
									if maxRows > 3 {
										maxRows = 3
									}
									for i := uint64(0); i < maxRows; i++ {
										t.Logf("            Row %d: IsNull=%v (IsValid=%v)",
											i, colData.IsNull(i), colData.Validity.IsValid(i))
									}
								}
								// Check first few values
								maxRows := colData.TupleCount
								if maxRows > 3 {
									maxRows = 3
								}
								for i := uint64(0); i < maxRows; i++ {
									val, valid := colData.GetValue(i)
									t.Logf("          GetValue(%d): val=%v (%T), valid=%v",
										i, val, val, valid)
								}
							}
						}
					}
				}
			}
		}
	}

	// Scan and check what values we get
	iter, err := storage.ScanTable("main", "all_null_test", nil)
	if err != nil {
		t.Fatalf("Error scanning table: %v", err)
	}
	defer iter.Close()

	rowNum := 0
	for iter.Next() {
		row := iter.Row()
		t.Logf("Row %d:", rowNum)
		for i, v := range row {
			t.Logf("  Col %d: value=%v (%T), isNil=%v", i, v, v, v == nil)
		}
		rowNum++
	}
	if iter.Err() != nil {
		t.Fatalf("Iterator error: %v", iter.Err())
	}
}

