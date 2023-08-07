package common

import (
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/op/go-logging"
)

var (
	log = logging.MustGetLogger("profile.common")

	decoder *zstd.Decoder
	encoder *zstd.Encoder

	encoderOnce, decoderOnce sync.Once
)

func ZstdDecompress(dst, src []byte) ([]byte, error) {
	decoderOnce.Do(func() {
		var err error
		decoder, err = zstd.NewReader(nil)
		if err != nil {
			log.Error(err)
		}
	})
	return decoder.DecodeAll(src, dst[:0])
}

func ZstdCompress(dst, src []byte, l zstd.EncoderLevel) ([]byte, error) {
	encoderOnce.Do(func() {
		var err error
		encoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(l))
		if err != nil {
			log.Error(err)
		}
	})
	return encoder.EncodeAll(src, dst[:0]), nil
}
