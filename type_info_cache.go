package dukdb

import (
	"sync"
)

// primitiveTypeInfoCache caches TypeInfo instances for primitive types.
// This reduces allocations when creating TypeInfo for the same primitive type multiple times.
var primitiveTypeInfoCache sync.Map

// getCachedPrimitiveTypeInfo returns a cached TypeInfo for a primitive type,
// creating and caching it if not already present.
func getCachedPrimitiveTypeInfo(t Type) (TypeInfo, error) {
	// Check if already cached
	if cached, ok := primitiveTypeInfoCache.Load(t); ok {
		return cached.(TypeInfo), nil
	}

	// Create new TypeInfo (this validates the type)
	info := &typeInfo{typ: t}

	// Validate the type is a supported primitive
	name, inMap := unsupportedTypeToStringMap[t]
	if inMap && t != TYPE_ANY {
		return nil, getError(errAPI, unsupportedTypeError(name))
	}

	// Check for complex types that need special constructors
	switch t {
	case TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY, TYPE_UNION, TYPE_SQLNULL:
		// Don't cache these - they need special constructors
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewDecimalInfo)))
	}

	// Store and return (LoadOrStore handles race conditions)
	actual, _ := primitiveTypeInfoCache.LoadOrStore(t, info)
	return actual.(TypeInfo), nil
}

// ClearTypeInfoCache clears the primitive type info cache.
// This is primarily useful for testing.
func ClearTypeInfoCache() {
	primitiveTypeInfoCache.Range(func(key, value any) bool {
		primitiveTypeInfoCache.Delete(key)
		return true
	})
}

// TypeInfoCacheSize returns the number of cached primitive TypeInfo instances.
// This is primarily useful for testing and debugging.
func TypeInfoCacheSize() int {
	count := 0
	primitiveTypeInfoCache.Range(func(key, value any) bool {
		count++
		return true
	})
	return count
}
