package token

import (
	"errors"
	"testing"
	"time"
)

func TestSignerIssueVerifyAndRejectTampering(t *testing.T) {
	s, err := NewSigner("k1", map[string][]byte{"k1": []byte("01234567890123456789012345678901")})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := s.Issue(Claims{JTI: "j1", Subject: "u1", Item: "item", Audience: "sale-api", Purpose: "admission"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Verify(raw, "sale-api", "admission")
	if err != nil {
		t.Fatal(err)
	}
	if c.JTI != "j1" {
		t.Fatalf("got %s", c.JTI)
	}
	if _, err := s.Verify(raw+"x", "sale-api", "admission"); !errors.Is(err, ErrSignature) {
		t.Fatalf("got %v", err)
	}
	if _, err := s.Verify(raw, "other", "admission"); !errors.Is(err, ErrAudience) {
		t.Fatalf("got %v", err)
	}
	if _, err := s.Verify(raw, "sale-api", "queue"); !errors.Is(err, ErrPurpose) {
		t.Fatalf("got %v", err)
	}
}

func TestSignerExpiryAndKeyRotation(t *testing.T) {
	s, _ := NewSigner("k1", map[string][]byte{"k1": []byte("01234567890123456789012345678901"), "k2": []byte("abcdefghijklmnopqrstuvwxyz123456")})
	base := time.Now()
	s.now = func() time.Time { return base }
	raw, err := s.Issue(Claims{JTI: "j", Subject: "u", Item: "i", Audience: "a", Purpose: "p"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	base = base.Add(2 * time.Second)
	if _, err := s.Verify(raw, "a", "p"); !errors.Is(err, ErrExpired) {
		t.Fatalf("got %v", err)
	}
}
