package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// String Distance and Similarity Functions

// Helper functions for distance calculations
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Levenshtein edit distance (dynamic programming)
func levenshteinValue(str1 any, str2 any) (any, error) {
	// NULL check FIRST before any processing
	if str1 == nil || str2 == nil {
		return nil, nil
	}

	s1 := toString(str1)
	s2 := toString(str2)

	// Use rune slices for UTF-8 safety
	runes1 := []rune(s1)
	runes2 := []rune(s2)
	len1 := len(runes1)
	len2 := len(runes2)

	// Create matrix
	matrix := make([][]int, len1+1)
	for i := range matrix {
		matrix[i] = make([]int, len2+1)
	}

	// Initialize first row and column
	for i := 0; i <= len1; i++ {
		matrix[i][0] = i
	}
	for j := 0; j <= len2; j++ {
		matrix[0][j] = j
	}

	// Fill matrix
	for i := 1; i <= len1; i++ {
		for j := 1; j <= len2; j++ {
			cost := 1
			if runes1[i-1] == runes2[j-1] {
				cost = 0
			}

			matrix[i][j] = minInt(
				minInt(matrix[i-1][j]+1, matrix[i][j-1]+1), // deletion, insertion
				matrix[i-1][j-1]+cost,                       // substitution
			)
		}
	}

	return int64(matrix[len1][len2]), nil
}

// Damerau-Levenshtein edit distance (with transpositions)
func damerauLevenshteinValue(str1 any, str2 any) (any, error) {
	// NULL check FIRST before any processing
	if str1 == nil || str2 == nil {
		return nil, nil
	}

	s1 := toString(str1)
	s2 := toString(str2)

	// Use rune slices for UTF-8 safety
	runes1 := []rune(s1)
	runes2 := []rune(s2)
	len1 := len(runes1)
	len2 := len(runes2)

	// Create matrix with extra row/column for transpositions
	maxDist := len1 + len2
	matrix := make([][]int, len1+2)
	for i := range matrix {
		matrix[i] = make([]int, len2+2)
	}

	matrix[0][0] = maxDist
	for i := 0; i <= len1; i++ {
		matrix[i+1][0] = maxDist
		matrix[i+1][1] = i
	}
	for j := 0; j <= len2; j++ {
		matrix[0][j+1] = maxDist
		matrix[1][j+1] = j
	}

	// Track last occurrence of each character
	lastMatch := make(map[rune]int)

	// Fill matrix
	for i := 1; i <= len1; i++ {
		lastMatchCol := 0
		for j := 1; j <= len2; j++ {
			lastMatchRow := lastMatch[runes2[j-1]]
			cost := 1
			if runes1[i-1] == runes2[j-1] {
				cost = 0
				lastMatchCol = j
			}

			matrix[i+1][j+1] = minInt(
				minInt(
					matrix[i][j]+cost,    // substitution
					matrix[i+1][j]+1,     // insertion
				),
				minInt(
					matrix[i][j+1]+1,     // deletion
					matrix[lastMatchRow][lastMatchCol]+((i-lastMatchRow-1)+1+(j-lastMatchCol-1)), // transposition
				),
			)
		}
		lastMatch[runes1[i-1]] = i
	}

	return int64(matrix[len1+1][len2+1]), nil
}

// Hamming distance (only for equal-length strings)
func hammingValue(str1 any, str2 any) (any, error) {
	// NULL check FIRST before any processing
	if str1 == nil || str2 == nil {
		return nil, nil
	}

	s1 := toString(str1)
	s2 := toString(str2)

	// Use rune slices for UTF-8 safety
	runes1 := []rune(s1)
	runes2 := []rune(s2)

	if len(runes1) != len(runes2) {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "HAMMING requires strings of equal length",
		}
	}

	distance := 0
	for i := 0; i < len(runes1); i++ {
		if runes1[i] != runes2[i] {
			distance++
		}
	}

	return int64(distance), nil
}

// Jaccard similarity coefficient
func jaccardValue(str1 any, str2 any) (any, error) {
	// NULL check FIRST before any processing
	if str1 == nil || str2 == nil {
		return nil, nil
	}

	s1 := toString(str1)
	s2 := toString(str2)

	// Convert to character sets (use runes for Unicode support)
	set1 := make(map[rune]bool)
	set2 := make(map[rune]bool)

	for _, ch := range s1 {
		set1[ch] = true
	}
	for _, ch := range s2 {
		set2[ch] = true
	}

	// Calculate intersection and union
	intersection := 0
	for ch := range set1 {
		if set2[ch] {
			intersection++
		}
	}

	union := len(set1) + len(set2) - intersection

	if union == 0 {
		return float64(1.0), nil
	}

	return float64(intersection) / float64(union), nil
}

// Jaro similarity
func jaroSimilarityValue(str1 any, str2 any) (any, error) {
	// NULL check FIRST before any processing
	if str1 == nil || str2 == nil {
		return nil, nil
	}

	s1 := toString(str1)
	s2 := toString(str2)

	if s1 == s2 {
		return float64(1.0), nil
	}

	// Use rune slices for UTF-8 safety
	runes1 := []rune(s1)
	runes2 := []rune(s2)
	len1 := len(runes1)
	len2 := len(runes2)

	if len1 == 0 || len2 == 0 {
		return float64(0.0), nil
	}

	matchDistance := maxInt(len1, len2)/2 - 1
	if matchDistance < 1 {
		matchDistance = 1
	}

	s1Matches := make([]bool, len1)
	s2Matches := make([]bool, len2)

	matches := 0
	transpositions := 0

	// Find matches
	for i := 0; i < len1; i++ {
		start := maxInt(0, i-matchDistance)
		end := minInt(i+matchDistance+1, len2)

		for j := start; j < end; j++ {
			if s2Matches[j] || runes1[i] != runes2[j] {
				continue
			}
			s1Matches[i] = true
			s2Matches[j] = true
			matches++
			break
		}
	}

	if matches == 0 {
		return float64(0.0), nil
	}

	// Count transpositions
	k := 0
	for i := 0; i < len1; i++ {
		if !s1Matches[i] {
			continue
		}
		for !s2Matches[k] {
			k++
		}
		if runes1[i] != runes2[k] {
			transpositions++
		}
		k++
	}

	similarity := (float64(matches)/float64(len1) +
		float64(matches)/float64(len2) +
		(float64(matches)-float64(transpositions)/2.0)/float64(matches)) / 3.0

	return similarity, nil
}

// Jaro-Winkler similarity (with prefix bonus)
func jaroWinklerSimilarityValue(str1 any, str2 any) (any, error) {
	// NULL check FIRST before any processing
	if str1 == nil || str2 == nil {
		return nil, nil
	}

	jaroSim, err := jaroSimilarityValue(str1, str2)
	if err != nil {
		return nil, err
	}

	jaro := jaroSim.(float64)

	s1 := toString(str1)
	s2 := toString(str2)

	// Use rune slices for UTF-8 safety
	runes1 := []rune(s1)
	runes2 := []rune(s2)

	// Find common prefix length (up to 4 chars)
	prefixLen := 0
	maxPrefix := minInt(minInt(len(runes1), len(runes2)), 4)
	for i := 0; i < maxPrefix; i++ {
		if runes1[i] == runes2[i] {
			prefixLen++
		} else {
			break
		}
	}

	// Apply Winkler modification
	p := 0.1 // scaling factor
	similarity := jaro + float64(prefixLen)*p*(1.0-jaro)

	return similarity, nil
}
