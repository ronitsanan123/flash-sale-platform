package rediswire

import "testing"

func TestHashSlotHonorsHashTag(t *testing.T) {
	if HashSlot("a:{same}:one") != HashSlot("b:{same}:two") {
		t.Fatal("same hash tag must map to same slot")
	}
	if HashSlot("hold-claim:{a}") == HashSlot("hold-claim:{b}") {
		t.Fatal("per-hold tags should usually distribute")
	}
}
