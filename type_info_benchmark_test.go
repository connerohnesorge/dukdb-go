package dukdb

import "testing"

// BenchmarkNewTypeInfo benchmarks primitive type creation.
func BenchmarkNewTypeInfo(b *testing.B) {
	b.Run("cached/INTEGER", func(b *testing.B) {
		ClearTypeInfoCache()
		// Warm the cache
		NewTypeInfo(TYPE_INTEGER)
		b.ResetTimer()

		for range b.N {
			_, _ = NewTypeInfo(TYPE_INTEGER)
		}
	})

	b.Run("uncached/first_call", func(b *testing.B) {
		for range b.N {
			ClearTypeInfoCache()
			_, _ = NewTypeInfo(TYPE_INTEGER)
		}
	})

	b.Run("VARCHAR", func(b *testing.B) {
		ClearTypeInfoCache()
		NewTypeInfo(TYPE_VARCHAR)
		b.ResetTimer()

		for range b.N {
			_, _ = NewTypeInfo(TYPE_VARCHAR)
		}
	})

	b.Run("TIMESTAMP", func(b *testing.B) {
		ClearTypeInfoCache()
		NewTypeInfo(TYPE_TIMESTAMP)
		b.ResetTimer()

		for range b.N {
			_, _ = NewTypeInfo(TYPE_TIMESTAMP)
		}
	})
}

// BenchmarkNewDecimalInfo benchmarks DECIMAL type creation.
func BenchmarkNewDecimalInfo(b *testing.B) {
	b.Run("DECIMAL(10,2)", func(b *testing.B) {
		for range b.N {
			_, _ = NewDecimalInfo(10, 2)
		}
	})

	b.Run("DECIMAL(38,0)", func(b *testing.B) {
		for range b.N {
			_, _ = NewDecimalInfo(38, 0)
		}
	})
}

// BenchmarkNewEnumInfo benchmarks ENUM type creation.
func BenchmarkNewEnumInfo(b *testing.B) {
	b.Run("3_values", func(b *testing.B) {
		for range b.N {
			_, _ = NewEnumInfo("a", "b", "c")
		}
	})

	b.Run("10_values", func(b *testing.B) {
		values := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}
		for range b.N {
			_, _ = NewEnumInfo("first", values...)
		}
	})

	b.Run("100_values", func(b *testing.B) {
		values := make([]string, 99)
		for i := range values {
			values[i] = string(rune('a' + (i % 26)))
		}
		b.ResetTimer()

		for range b.N {
			_, _ = NewEnumInfo("first", values...)
		}
	})
}

// BenchmarkNewListInfo benchmarks LIST type creation.
func BenchmarkNewListInfo(b *testing.B) {
	ClearTypeInfoCache()
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)

	b.Run("simple", func(b *testing.B) {
		for range b.N {
			_, _ = NewListInfo(intInfo)
		}
	})

	b.Run("nested_3_levels", func(b *testing.B) {
		for range b.N {
			list1, _ := NewListInfo(intInfo)
			list2, _ := NewListInfo(list1)
			_, _ = NewListInfo(list2)
		}
	})
}

// BenchmarkNewArrayInfo benchmarks ARRAY type creation.
func BenchmarkNewArrayInfo(b *testing.B) {
	ClearTypeInfoCache()
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)

	for range b.N {
		_, _ = NewArrayInfo(intInfo, 100)
	}
}

// BenchmarkNewMapInfo benchmarks MAP type creation.
func BenchmarkNewMapInfo(b *testing.B) {
	ClearTypeInfoCache()
	strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)

	for range b.N {
		_, _ = NewMapInfo(strInfo, intInfo)
	}
}

// BenchmarkNewStructInfo benchmarks STRUCT type creation.
func BenchmarkNewStructInfo(b *testing.B) {
	ClearTypeInfoCache()
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	strInfo, _ := NewTypeInfo(TYPE_VARCHAR)

	b.Run("2_fields", func(b *testing.B) {
		for range b.N {
			entry1, _ := NewStructEntry(intInfo, "id")
			entry2, _ := NewStructEntry(strInfo, "name")
			_, _ = NewStructInfo(entry1, entry2)
		}
	})

	b.Run("10_fields", func(b *testing.B) {
		for range b.N {
			entry1, _ := NewStructEntry(intInfo, "f1")
			entries := make([]StructEntry, 9)
			for j := range 9 {
				entries[j], _ = NewStructEntry(intInfo, string(rune('a'+j)))
			}
			_, _ = NewStructInfo(entry1, entries...)
		}
	})
}

// BenchmarkNewUnionInfo benchmarks UNION type creation.
func BenchmarkNewUnionInfo(b *testing.B) {
	ClearTypeInfoCache()
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
	boolInfo, _ := NewTypeInfo(TYPE_BOOLEAN)

	b.Run("2_members", func(b *testing.B) {
		for range b.N {
			_, _ = NewUnionInfo(
				[]TypeInfo{intInfo, strInfo},
				[]string{"num", "str"},
			)
		}
	})

	b.Run("3_members", func(b *testing.B) {
		for range b.N {
			_, _ = NewUnionInfo(
				[]TypeInfo{intInfo, strInfo, boolInfo},
				[]string{"num", "str", "flag"},
			)
		}
	})
}

// BenchmarkSQLType benchmarks SQLType() string generation.
func BenchmarkSQLType(b *testing.B) {
	ClearTypeInfoCache()

	b.Run("primitive", func(b *testing.B) {
		info, _ := NewTypeInfo(TYPE_INTEGER)
		b.ResetTimer()

		for range b.N {
			_ = info.SQLType()
		}
	})

	b.Run("DECIMAL", func(b *testing.B) {
		info, _ := NewDecimalInfo(18, 6)
		b.ResetTimer()

		for range b.N {
			_ = info.SQLType()
		}
	})

	b.Run("ENUM_10_values", func(b *testing.B) {
		values := []string{"b", "c", "d", "e", "f", "g", "h", "i", "j"}
		info, _ := NewEnumInfo("a", values...)
		b.ResetTimer()

		for range b.N {
			_ = info.SQLType()
		}
	})

	b.Run("LIST", func(b *testing.B) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		info, _ := NewListInfo(intInfo)
		b.ResetTimer()

		for range b.N {
			_ = info.SQLType()
		}
	})

	b.Run("nested_LIST_3_deep", func(b *testing.B) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		list1, _ := NewListInfo(intInfo)
		list2, _ := NewListInfo(list1)
		info, _ := NewListInfo(list2)
		b.ResetTimer()

		for range b.N {
			_ = info.SQLType()
		}
	})

	b.Run("STRUCT_5_fields", func(b *testing.B) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		entry1, _ := NewStructEntry(intInfo, "a")
		entry2, _ := NewStructEntry(intInfo, "b")
		entry3, _ := NewStructEntry(intInfo, "c")
		entry4, _ := NewStructEntry(intInfo, "d")
		entry5, _ := NewStructEntry(intInfo, "e")
		info, _ := NewStructInfo(entry1, entry2, entry3, entry4, entry5)
		b.ResetTimer()

		for range b.N {
			_ = info.SQLType()
		}
	})

	b.Run("complex_nested", func(b *testing.B) {
		// MAP[VARCHAR, LIST[STRUCT(id INTEGER, name VARCHAR)]]
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		strInfo, _ := NewTypeInfo(TYPE_VARCHAR)
		idEntry, _ := NewStructEntry(intInfo, "id")
		nameEntry, _ := NewStructEntry(strInfo, "name")
		structInfo, _ := NewStructInfo(idEntry, nameEntry)
		listInfo, _ := NewListInfo(structInfo)
		info, _ := NewMapInfo(strInfo, listInfo)
		b.ResetTimer()

		for range b.N {
			_ = info.SQLType()
		}
	})
}

// BenchmarkDetails benchmarks Details() method.
func BenchmarkDetails(b *testing.B) {
	b.Run("primitive_nil", func(b *testing.B) {
		info, _ := NewTypeInfo(TYPE_INTEGER)
		b.ResetTimer()

		for range b.N {
			_ = info.Details()
		}
	})

	b.Run("DECIMAL", func(b *testing.B) {
		info, _ := NewDecimalInfo(18, 6)
		b.ResetTimer()

		for range b.N {
			_ = info.Details()
		}
	})

	b.Run("ENUM_10_values", func(b *testing.B) {
		values := []string{"b", "c", "d", "e", "f", "g", "h", "i", "j"}
		info, _ := NewEnumInfo("a", values...)
		b.ResetTimer()

		for range b.N {
			_ = info.Details()
		}
	})

	b.Run("STRUCT_10_fields", func(b *testing.B) {
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		entry1, _ := NewStructEntry(intInfo, "a")
		entries := make([]StructEntry, 9)
		for j := range 9 {
			entries[j], _ = NewStructEntry(intInfo, string(rune('b'+j)))
		}
		info, _ := NewStructInfo(entry1, entries...)
		b.ResetTimer()

		for range b.N {
			_ = info.Details()
		}
	})
}

// BenchmarkTypeInfoCacheParallel benchmarks cache access under contention.
func BenchmarkTypeInfoCacheParallel(b *testing.B) {
	ClearTypeInfoCache()
	// Warm the cache
	NewTypeInfo(TYPE_INTEGER)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = NewTypeInfo(TYPE_INTEGER)
		}
	})
}
