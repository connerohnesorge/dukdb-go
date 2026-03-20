package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSqlRegexToGoRegex(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		escape  rune
		want    string
		wantErr bool
	}{
		{
			name:    "percent wildcard",
			pattern: "hel%",
			escape:  '\\',
			want:    `^hel.*$`,
		},
		{
			name:    "underscore wildcard",
			pattern: "he__",
			escape:  '\\',
			want:    `^he..$`,
		},
		{
			name:    "alternation",
			pattern: "(hello|world)",
			escape:  '\\',
			want:    `^(hello|world)$`,
		},
		{
			name:    "character class",
			pattern: "[abc]",
			escape:  '\\',
			want:    `^[abc]$`,
		},
		{
			name:    "negated character class",
			pattern: "[!abc]",
			escape:  '\\',
			want:    `^[^abc]$`,
		},
		{
			name:    "escaped percent",
			pattern: `100\%`,
			escape:  '\\',
			want:    `^100%$`,
		},
		{
			name:    "custom escape character",
			pattern: "100#%",
			escape:  '#',
			want:    `^100%$`,
		},
		{
			name:    "literal dot",
			pattern: "a.b",
			escape:  '\\',
			want:    `^a\.b$`,
		},
		{
			name:    "empty pattern",
			pattern: "",
			escape:  '\\',
			want:    `^$`,
		},
		{
			name:    "complex pattern",
			pattern: "%[a-z]_(foo|bar)%",
			escape:  '\\',
			want:    `^.*[a-z].(foo|bar).*$`,
		},
		{
			name:    "trailing escape error",
			pattern: `abc\`,
			escape:  '\\',
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sqlRegexToGoRegex(tt.pattern, tt.escape)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMatchSimilarTo(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		pattern string
		escape  rune
		want    bool
		wantErr bool
	}{
		{"exact match", "hello", "hello", '\\', true, false},
		{"no match", "hello", "world", '\\', false, false},
		{"percent wildcard", "hello", "hel%", '\\', true, false},
		{"underscore wildcard", "hello", "hell_", '\\', true, false},
		{"underscore too short", "hi", "h__o", '\\', false, false},
		{"alternation match", "hello", "(hello|world)", '\\', true, false},
		{"alternation no match", "foo", "(hello|world)", '\\', false, false},
		{"char class match", "hat", "h[aeiou]t", '\\', true, false},
		{"char class no match", "hxt", "h[aeiou]t", '\\', false, false},
		{"negated char class match", "hxt", "h[!aeiou]t", '\\', true, false},
		{"negated char class no match", "hat", "h[!aeiou]t", '\\', false, false},
		{"empty empty", "", "", '\\', true, false},
		{"empty pattern", "abc", "", '\\', false, false},
		{"empty value", "", "abc", '\\', false, false},
		{"percent matches empty", "", "%", '\\', true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := matchSimilarTo(tt.value, tt.pattern, tt.escape)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, result.(bool))
		})
	}
}
