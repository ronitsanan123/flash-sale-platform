package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrMalformed = errors.New("malformed token")
	ErrSignature = errors.New("invalid token signature")
	ErrExpired   = errors.New("token expired")
	ErrAudience  = errors.New("invalid token audience")
	ErrPurpose   = errors.New("invalid token purpose")
	ErrKey       = errors.New("unknown signing key")
)

type Claims struct {
	JTI       string `json:"jti"`
	Subject   string `json:"sub"`
	Item      string `json:"item"`
	Audience  string `json:"aud"`
	Purpose   string `json:"purpose"`
	KeyID     string `json:"kid"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

type Signer struct {
	keys   map[string][]byte
	active string
	now    func() time.Time
}

func NewSigner(active string, keys map[string][]byte) (*Signer, error) {
	if active == "" || len(keys[active]) < 32 {
		return nil, fmt.Errorf("active key must exist and be at least 32 bytes")
	}
	copyKeys := make(map[string][]byte, len(keys))
	for id, key := range keys {
		if id == "" || len(key) < 32 {
			return nil, fmt.Errorf("key %q must be at least 32 bytes", id)
		}
		copyKeys[id] = append([]byte(nil), key...)
	}
	return &Signer{keys: copyKeys, active: active, now: time.Now}, nil
}

func (s *Signer) Issue(c Claims, ttl time.Duration) (string, error) {
	if ttl <= 0 || c.Subject == "" || c.Item == "" || c.Purpose == "" || c.Audience == "" {
		return "", fmt.Errorf("invalid token claims or ttl")
	}
	now := s.now()
	c.KeyID = s.active
	c.IssuedAt = now.Unix()
	c.ExpiresAt = now.Add(ttl).Unix()
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	sig := sign(s.keys[s.active], encoded)
	return "v1." + s.active + "." + encoded + "." + sig, nil
}

func (s *Signer) Verify(raw, audience, purpose string) (Claims, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 4 || parts[0] != "v1" {
		return Claims{}, ErrMalformed
	}
	key := s.keys[parts[1]]
	if len(key) == 0 {
		return Claims{}, ErrKey
	}
	want := sign(key, parts[2])
	if !hmac.Equal([]byte(want), []byte(parts[3])) {
		return Claims{}, ErrSignature
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Claims{}, ErrMalformed
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, ErrMalformed
	}
	if c.KeyID != parts[1] || c.Subject == "" || c.Item == "" || c.JTI == "" {
		return Claims{}, ErrMalformed
	}
	if c.Audience != audience {
		return Claims{}, ErrAudience
	}
	if c.Purpose != purpose {
		return Claims{}, ErrPurpose
	}
	if s.now().Unix() >= c.ExpiresAt {
		return Claims{}, ErrExpired
	}
	return c, nil
}

func sign(key []byte, value string) string {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
