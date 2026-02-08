package types

import (
	"testing"

	"github.com/dukdb/dukdb-go"
)

func TestParseTypeExpression(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		typ      dukdb.Type
		wantInfo bool
	}{
		{name: "json", expr: "JSON", typ: dukdb.TYPE_JSON, wantInfo: true},
		{name: "array", expr: "INTEGER[3]", typ: dukdb.TYPE_ARRAY, wantInfo: true},
		{name: "list", expr: "INTEGER[]", typ: dukdb.TYPE_LIST, wantInfo: true},
		{name: "map", expr: "MAP(VARCHAR, INTEGER)", typ: dukdb.TYPE_MAP, wantInfo: true},
		{name: "struct", expr: "STRUCT(id INTEGER, name VARCHAR)", typ: dukdb.TYPE_STRUCT, wantInfo: true},
		{name: "union", expr: "UNION(num INTEGER, text VARCHAR)", typ: dukdb.TYPE_UNION, wantInfo: true},
		{name: "decimal", expr: "DECIMAL(10,2)", typ: dukdb.TYPE_DECIMAL, wantInfo: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := ParseTypeExpression(tt.expr)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			if info.InternalType() != tt.typ {
				t.Fatalf("got %s, want %s", info.InternalType(), tt.typ)
			}
			if tt.wantInfo && info.Details() == nil {
				t.Fatalf("expected details for %s", tt.expr)
			}
		})
	}
}

func TestParseTypeExpressionErrors(t *testing.T) {
	tests := []string{
		"STRUCT()",
		"MAP(VARCHAR)",
		"UNION()",
		"ARRAY(INTEGER)",
	}

	for _, expr := range tests {
		t.Run(expr, func(t *testing.T) {
			if _, err := ParseTypeExpression(expr); err == nil {
				t.Fatalf("expected error for %q", expr)
			}
		})
	}
}
