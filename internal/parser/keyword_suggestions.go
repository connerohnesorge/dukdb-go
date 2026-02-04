package parser

import (
	"strings"
)

// sqlKeywords contains common SQL keywords that users might mistype.
// These are used for suggesting corrections when an unexpected token is encountered.
var sqlKeywords = []string{
	// DML keywords
	"SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
	"DELETE", "MERGE", "USING", "MATCHED", "NOT", "WHEN", "THEN",

	// Clauses
	"GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "DISTINCT", "ON",
	"AS", "ALL", "UNION", "INTERSECT", "EXCEPT", "JOIN", "INNER", "LEFT",
	"RIGHT", "FULL", "OUTER", "CROSS", "NATURAL",

	// DDL keywords
	"CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW", "SCHEMA", "DATABASE",
	"SEQUENCE", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK",
	"DEFAULT", "CONSTRAINT", "CASCADE", "RESTRICT", "IF", "EXISTS",

	// Transaction keywords
	"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "TRANSACTION",
	"ISOLATION", "LEVEL", "READ", "COMMITTED", "UNCOMMITTED", "REPEATABLE",
	"SERIALIZABLE",

	// Type keywords
	"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE",
	"REAL", "DECIMAL", "NUMERIC", "CHAR", "VARCHAR", "TEXT", "BOOLEAN",
	"DATE", "TIME", "TIMESTAMP", "INTERVAL", "BLOB", "JSON",

	// Other keywords
	"AND", "OR", "IS", "NULL", "TRUE", "FALSE", "LIKE", "ILIKE", "IN",
	"BETWEEN", "CASE", "ELSE", "END", "CAST", "COALESCE", "NULLIF",
	"ASC", "DESC", "NULLS", "FIRST", "LAST", "WITH", "RECURSIVE",
	"RETURNING", "COPY", "EXPLAIN", "ANALYZE", "PRAGMA", "SHOW",
	"QUALIFY", "OVER", "PARTITION", "WINDOW", "ROWS", "RANGE", "GROUPS",
	"PRECEDING", "FOLLOWING", "CURRENT", "ROW", "UNBOUNDED",
}

// SQLKeywords returns the list of known SQL keywords.
func SQLKeywords() []string {
	keywords := make([]string, len(sqlKeywords))
	copy(keywords, sqlKeywords)
	return keywords
}

// levenshteinDistance calculates the edit distance between two strings.
// This is used to find similar keywords for typo suggestions.
func levenshteinDistance(s1, s2 string) int {
	s1 = strings.ToUpper(s1)
	s2 = strings.ToUpper(s2)

	if s1 == s2 {
		return 0
	}
	if len(s1) == 0 {
		return len(s2)
	}
	if len(s2) == 0 {
		return len(s1)
	}

	// Create distance matrix
	d := make([][]int, len(s1)+1)
	for i := range d {
		d[i] = make([]int, len(s2)+1)
	}

	// Initialize first row and column
	for i := 0; i <= len(s1); i++ {
		d[i][0] = i
	}
	for j := 0; j <= len(s2); j++ {
		d[0][j] = j
	}

	// Fill in the rest
	for i := 1; i <= len(s1); i++ {
		for j := 1; j <= len(s2); j++ {
			cost := 0
			if s1[i-1] != s2[j-1] {
				cost = 1
			}

			d[i][j] = min(
				d[i-1][j]+1,      // deletion
				d[i][j-1]+1,      // insertion
				d[i-1][j-1]+cost, // substitution
			)
		}
	}

	return d[len(s1)][len(s2)]
}

// suggestKeyword returns a suggested keyword if the given token looks like a typo
// of a known SQL keyword. Returns empty string if no good match is found.
// If the token is already a valid keyword, returns empty string.
func suggestKeyword(token string) string {
	if token == "" {
		return ""
	}

	token = strings.ToUpper(token)
	maxLen := len(token)

	// If the token is already a valid keyword, don't suggest anything
	for _, keyword := range sqlKeywords {
		if token == keyword {
			return ""
		}
	}

	// Maximum distance to consider as a typo depends on word length
	// Short words (1-3 chars): max distance 1
	// Medium words (4-6 chars): max distance 2
	// Long words (7+ chars): max distance 3
	maxDistance := 1
	if maxLen >= 4 && maxLen <= 6 {
		maxDistance = 2
	} else if maxLen >= 7 {
		maxDistance = 3
	}

	bestMatch := ""
	bestDistance := maxDistance + 1
	bestScore := 0 // Secondary score: prefer keywords that share more starting characters

	for _, keyword := range sqlKeywords {
		// Skip if lengths differ too much
		lenDiff := len(keyword) - maxLen
		if lenDiff < 0 {
			lenDiff = -lenDiff
		}
		if lenDiff > maxDistance {
			continue
		}

		distance := levenshteinDistance(token, keyword)
		if distance > 0 && distance <= maxDistance {
			// Calculate a prefix match score (how many chars at the start match)
			prefixScore := 0
			minLen := len(token)
			if len(keyword) < minLen {
				minLen = len(keyword)
			}
			for i := 0; i < minLen; i++ {
				if token[i] == keyword[i] {
					prefixScore++
				} else {
					break
				}
			}

			// Prefer lower distance, but use prefix score as a tie-breaker
			if distance < bestDistance || (distance == bestDistance && prefixScore > bestScore) {
				bestDistance = distance
				bestMatch = keyword
				bestScore = prefixScore
			}
		}
	}

	if bestDistance <= maxDistance {
		return bestMatch
	}
	return ""
}

// isProbableKeywordTypo checks if a token looks like a typo of a SQL keyword.
// This is used to reject potential typos as table aliases, column aliases, etc.
// Returns true if the token is likely a typo of a SQL keyword.
func isProbableKeywordTypo(token string) bool {
	if token == "" {
		return false
	}

	token = strings.ToUpper(token)
	maxLen := len(token)

	// Skip very short tokens (1-2 chars) as they're more likely to be legitimate aliases
	if maxLen <= 2 {
		return false
	}

	// First, check if this token IS a valid keyword - if so, it's not a typo
	for _, keyword := range sqlKeywords {
		if token == keyword {
			return false
		}
	}

	// Maximum distance to consider as a typo depends on word length
	// This should match the logic in suggestKeyword for consistency
	// Short words (3 chars): max distance 1
	// Medium words (4-6 chars): max distance 2
	// Long words (7+ chars): max distance 3
	maxDistance := 1
	if maxLen >= 4 && maxLen <= 6 {
		maxDistance = 2
	} else if maxLen >= 7 {
		maxDistance = 3
	}

	for _, keyword := range sqlKeywords {
		// Skip if lengths differ too much
		lenDiff := len(keyword) - maxLen
		if lenDiff < 0 {
			lenDiff = -lenDiff
		}
		if lenDiff > maxDistance {
			continue
		}

		distance := levenshteinDistance(token, keyword)
		if distance > 0 && distance <= maxDistance {
			return true
		}
	}

	return false
}
