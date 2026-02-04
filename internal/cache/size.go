package cache

import (
	"encoding/json"
	"time"
)

// EstimateResultSize provides a best-effort size estimate for a cached result.
func EstimateResultSize(result QueryResult) int64 {
	var total int64
	for _, col := range result.Columns {
		total += int64(len(col))
	}
	for _, row := range result.Rows {
		for key, val := range row {
			total += int64(len(key))
			total += estimateValueSize(val)
		}
	}
	return total
}

func estimateValueSize(val any) int64 {
	if val == nil {
		return 1
	}

	switch v := val.(type) {
	case string:
		return int64(len(v))
	case []byte:
		return int64(len(v))
	case bool:
		return 1
	case int, int8, int16, int32, int64:
		return 8
	case uint, uint8, uint16, uint32, uint64:
		return 8
	case float32:
		return 4
	case float64:
		return 8
	case time.Time:
		return 16
	case map[string]any:
		var total int64
		for key, value := range v {
			total += int64(len(key))
			total += estimateValueSize(value)
		}
		return total
	case []any:
		var total int64
		for _, value := range v {
			total += estimateValueSize(value)
		}
		return total
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return 16
		}
		return int64(len(data))
	}
}
