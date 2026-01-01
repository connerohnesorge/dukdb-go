// Package io provides file I/O interfaces and utilities for reading and writing
// various file formats (CSV, JSON, Parquet) to and from DataChunks.
package io

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Magic bytes for compression format detection.
var (
	// GZIP magic bytes: 0x1f 0x8b
	gzipMagic = []byte{0x1f, 0x8b}
	// ZSTD magic bytes: 0x28 0xb5 0x2f 0xfd
	zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}
	// LZ4 frame magic bytes: 0x04 0x22 0x4d 0x18
	lz4Magic = []byte{0x04, 0x22, 0x4d, 0x18}
)

// Errors for compression operations.
var (
	ErrUnsupportedCompression = errors.New("unsupported compression format")
	ErrDetectionFailed        = errors.New("failed to detect compression format")
)

// Constants for magic byte detection.
const (
	// maxMagicBytes is the maximum number of bytes to peek for magic byte detection.
	maxMagicBytes = 4
	// minGZIPMagicLen is the minimum number of bytes required to detect GZIP format.
	minGZIPMagicLen = 2
	// minFourByteMagicLen is the minimum number of bytes required for 4-byte magic detection.
	minFourByteMagicLen = 4
	// snappyFrameMarker is the first byte of snappy framing format stream identifier.
	snappyFrameMarker = 0xff
)

// DetectCompression detects the compression format from magic bytes at the
// start of the reader. It returns the detected compression type and a new
// reader that includes the peeked bytes. The original reader is consumed
// and should not be used after this call.
func DetectCompression(r io.Reader) (Compression, io.Reader, error) {
	// Use a buffered reader to peek at magic bytes
	bufReader := bufio.NewReader(r)
	header, err := bufReader.Peek(maxMagicBytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return CompressionNone, bufReader, fmt.Errorf("peek magic bytes: %w", err)
	}

	// Check for each compression format's magic bytes
	compression := detectCompressionFromBytes(header)

	return compression, bufReader, nil
}

// detectCompressionFromBytes checks the header bytes against known magic
// sequences and returns the detected compression type.
func detectCompressionFromBytes(header []byte) Compression {
	if len(header) >= minGZIPMagicLen && bytes.HasPrefix(header, gzipMagic) {
		return CompressionGZIP
	}

	if len(header) >= minFourByteMagicLen && bytes.HasPrefix(header, zstdMagic) {
		return CompressionZSTD
	}

	if len(header) >= minFourByteMagicLen && bytes.HasPrefix(header, lz4Magic) {
		return CompressionLZ4
	}

	// Snappy framing format starts with the stream identifier chunk
	// The chunk type 0xff followed by "sNaPpY" identifier
	if len(header) >= minFourByteMagicLen && header[0] == snappyFrameMarker {
		// Could be snappy framing format, but we need more bytes to be sure
		// For snappy, we rely more on extension detection since the magic is less reliable
		return CompressionNone
	}

	// Brotli has no reliable magic bytes, rely on extension detection
	return CompressionNone
}

// DetectCompressionFromPath detects compression format from file extension.
func DetectCompressionFromPath(path string) Compression {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".gz", ".gzip":
		return CompressionGZIP
	case ".zst", ".zstd":
		return CompressionZSTD
	case ".snappy":
		return CompressionSnappy
	case ".lz4":
		return CompressionLZ4
	case ".br", ".brotli":
		return CompressionBrotli
	default:
		return CompressionNone
	}
}

// NewDecompressReader wraps a reader with the appropriate decompressor
// based on the compression type. The returned ReadCloser must be closed
// when done to release resources.
func NewDecompressReader(r io.Reader, c Compression) (io.ReadCloser, error) {
	switch c {
	case CompressionNone:
		return io.NopCloser(r), nil
	case CompressionGZIP:
		return gzip.NewReader(r)
	case CompressionZSTD:
		decoder, err := zstd.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("create zstd decoder: %w", err)
		}

		return &zstdReadCloser{decoder: decoder}, nil
	case CompressionSnappy:
		return io.NopCloser(snappy.NewReader(r)), nil
	case CompressionLZ4:
		return io.NopCloser(lz4.NewReader(r)), nil
	case CompressionBrotli:
		return io.NopCloser(brotli.NewReader(r)), nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnsupportedCompression, c)
	}
}

// zstdReadCloser wraps a zstd.Decoder to implement io.ReadCloser.
type zstdReadCloser struct {
	decoder *zstd.Decoder
}

func (z *zstdReadCloser) Read(p []byte) (int, error) {
	return z.decoder.Read(p)
}

func (z *zstdReadCloser) Close() error {
	z.decoder.Close()

	return nil
}

// NewCompressWriter wraps a writer with the appropriate compressor
// based on the compression type. The returned WriteCloser must be closed
// when done to flush and finalize the compressed stream.
func NewCompressWriter(w io.Writer, c Compression) (io.WriteCloser, error) {
	switch c {
	case CompressionNone:
		return &nopWriteCloser{w: w}, nil
	case CompressionGZIP:
		return gzip.NewWriter(w), nil
	case CompressionZSTD:
		encoder, err := zstd.NewWriter(w)
		if err != nil {
			return nil, fmt.Errorf("create zstd encoder: %w", err)
		}

		return encoder, nil
	case CompressionSnappy:
		return snappy.NewBufferedWriter(w), nil
	case CompressionLZ4:
		return lz4.NewWriter(w), nil
	case CompressionBrotli:
		return brotli.NewWriter(w), nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnsupportedCompression, c)
	}
}

// nopWriteCloser wraps an io.Writer to implement io.WriteCloser with a no-op Close.
type nopWriteCloser struct {
	w io.Writer
}

func (n *nopWriteCloser) Write(p []byte) (int, error) {
	return n.w.Write(p)
}

func (*nopWriteCloser) Close() error {
	return nil
}
