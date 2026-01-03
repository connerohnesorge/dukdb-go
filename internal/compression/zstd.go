package compression

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// ZstdCodec implements Zstandard compression.
// Zstandard is a general-purpose compression algorithm that provides
// good compression ratios with fast compression and decompression speeds.
// It's used as a fallback for data that doesn't benefit from specialized
// compression algorithms like RLE, BitPacking, or Chimp.
//
// This implementation uses encoder/decoder pooling for efficiency.
type ZstdCodec struct {
	level        zstd.EncoderLevel
	encoderPool  sync.Pool
	decoderPool  sync.Pool
	encoderOnce  sync.Once
	decoderOnce  sync.Once
}

// NewZstdCodec creates a new Zstandard codec with the specified compression level.
// The level parameter maps to standard zstd compression levels:
//   - 1-3: Fast compression (zstd.SpeedFastest to zstd.SpeedDefault)
//   - 4-9: Better compression (zstd.SpeedBetterCompression)
//   - 10+: Best compression (zstd.SpeedBestCompression)
//
// Level 3 (default) provides a good balance between compression ratio and speed.
func NewZstdCodec(level int) *ZstdCodec {
	var encoderLevel zstd.EncoderLevel

	switch {
	case level <= 1:
		encoderLevel = zstd.SpeedFastest
	case level <= 3:
		encoderLevel = zstd.SpeedDefault
	case level <= 9:
		encoderLevel = zstd.SpeedBetterCompression
	default:
		encoderLevel = zstd.SpeedBestCompression
	}

	return &ZstdCodec{
		level: encoderLevel,
	}
}

// initEncoderPool initializes the encoder pool on first use.
func (c *ZstdCodec) initEncoderPool() {
	c.encoderOnce.Do(func() {
		c.encoderPool = sync.Pool{
			New: func() interface{} {
				// Create encoder with specified level
				encoder, err := zstd.NewWriter(nil,
					zstd.WithEncoderLevel(c.level),
					zstd.WithEncoderConcurrency(1), // Single-threaded for predictable behavior
				)
				if err != nil {
					// This should never happen with valid options
					panic(fmt.Sprintf("failed to create zstd encoder: %v", err))
				}
				return encoder
			},
		}
	})
}

// initDecoderPool initializes the decoder pool on first use.
func (c *ZstdCodec) initDecoderPool() {
	c.decoderOnce.Do(func() {
		c.decoderPool = sync.Pool{
			New: func() interface{} {
				// Create decoder
				decoder, err := zstd.NewReader(nil,
					zstd.WithDecoderConcurrency(1), // Single-threaded for predictable behavior
				)
				if err != nil {
					// This should never happen with valid options
					panic(fmt.Sprintf("failed to create zstd decoder: %v", err))
				}
				return decoder
			},
		}
	})
}

// Compress compresses the input data using Zstandard.
func (c *ZstdCodec) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	// Initialize encoder pool if needed
	c.initEncoderPool()

	// Get encoder from pool
	encoder := c.encoderPool.Get().(*zstd.Encoder)
	defer c.encoderPool.Put(encoder)

	// Compress data
	var buf bytes.Buffer
	encoder.Reset(&buf)

	if _, err := encoder.Write(data); err != nil {
		return nil, fmt.Errorf("zstd compression failed: %w", err)
	}

	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("zstd compression finalization failed: %w", err)
	}

	return buf.Bytes(), nil
}

// Decompress decompresses Zstandard-encoded data.
// The destSize parameter is used to validate the decompressed size.
func (c *ZstdCodec) Decompress(data []byte, destSize int) ([]byte, error) {
	if destSize == 0 {
		return []byte{}, nil
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("cannot decompress empty data to size %d", destSize)
	}

	// Initialize decoder pool if needed
	c.initDecoderPool()

	// Get decoder from pool
	decoder := c.decoderPool.Get().(*zstd.Decoder)
	defer c.decoderPool.Put(decoder)

	// Reset decoder with input data
	if err := decoder.Reset(bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("zstd decoder reset failed: %w", err)
	}

	// Pre-allocate result buffer with expected size
	result := make([]byte, 0, destSize)
	buf := bytes.NewBuffer(result)

	// Decompress data
	if _, err := io.Copy(buf, decoder); err != nil {
		return nil, fmt.Errorf("zstd decompression failed: %w", err)
	}

	result = buf.Bytes()

	// Validate decompressed size matches expected size
	if len(result) != destSize {
		return nil, fmt.Errorf("decompressed size mismatch: got %d, expected %d", len(result), destSize)
	}

	return result, nil
}

// Type returns CompressionZstd.
func (c *ZstdCodec) Type() CompressionType {
	return CompressionZstd
}
