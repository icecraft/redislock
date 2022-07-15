package redislock

import (
	"crypto/rand"
	"encoding/base64"
	"io"
)

func randomToken(buf []byte) (string, error) {
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
