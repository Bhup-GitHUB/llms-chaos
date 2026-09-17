package trace

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

func Parse(s string) (string, string, bool, bool) {
	parts := strings.Split(s, "-")
	if len(parts) != 4 {
		return "", "", false, false
	}
	if len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return "", "", false, false
	}
	if _, err := hex.DecodeString(parts[0]); err != nil {
		return "", "", false, false
	}
	tidBytes, err := hex.DecodeString(parts[1])
	if err != nil {
		return "", "", false, false
	}
	sidBytes, err := hex.DecodeString(parts[2])
	if err != nil {
		return "", "", false, false
	}
	flagBytes, err := hex.DecodeString(parts[3])
	if err != nil {
		return "", "", false, false
	}
	zero := func(b []byte) bool {
		for _, v := range b {
			if v != 0 {
				return false
			}
		}
		return true
	}
	if zero(tidBytes) || zero(sidBytes) {
		return "", "", false, false
	}
	sampled := flagBytes[0]&1 == 1
	return strings.ToLower(parts[1]), strings.ToLower(parts[2]), sampled, true
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		seed := uint64(time.Now().UnixNano())
		for i := range b {
			seed = seed*6364136223846793005 + 1
			b[i] = byte(seed >> 56)
		}
	}
	return hex.EncodeToString(b)
}

func Generate() (string, string, string) {
	tid := randHex(16)
	sid := randHex(8)
	return tid, sid, "00-" + tid + "-" + sid + "-01"
}

func FromRequest(r *http.Request) (string, string) {
	if r != nil {
		in := r.Header.Get("traceparent")
		if tid, _, sampled, ok := Parse(in); ok {
			sid := randHex(8)
			flags := "00"
			if sampled {
				flags = "01"
			}
			return tid, "00-" + tid + "-" + sid + "-" + flags
		}
	}
	tid, _, tp := Generate()
	return tid, tp
}
