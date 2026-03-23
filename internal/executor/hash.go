package executor

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"hash/fnv"
)

// Hash Functions

func md5Value(str any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil {
		return nil, nil
	}

	s := toString(str)
	hash := md5.Sum([]byte(s))
	// Convert to lowercase hex string
	return hex.EncodeToString(hash[:]), nil
}

func sha256Value(str any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil {
		return nil, nil
	}

	s := toString(str)
	hash := sha256.Sum256([]byte(s))
	// Convert to lowercase hex string
	return hex.EncodeToString(hash[:]), nil
}

func sha1Value(str any) (any, error) {
	if str == nil {
		return nil, nil
	}

	s := toString(str)
	hash := sha1.Sum([]byte(s))
	return hex.EncodeToString(hash[:]), nil
}

func sha512Value(str any) (any, error) {
	if str == nil {
		return nil, nil
	}

	s := toString(str)
	hash := sha512.Sum512([]byte(s))
	return hex.EncodeToString(hash[:]), nil
}

func hashStringValue(str any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil {
		return nil, nil
	}

	s := toString(str)

	// Use FNV-1a hash (64-bit) as DuckDB's default hash
	h := fnv.New64a()
	h.Write([]byte(s))

	return int64(h.Sum64()), nil
}
