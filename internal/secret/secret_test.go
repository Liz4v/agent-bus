package secret

import (
	"bytes"
	"regexp"
	"testing"
)

func TestChannelIDFormat(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	for i := 0; i < 100; i++ {
		if id := NewChannelID(); !re.MatchString(id) {
			t.Fatalf("bad UUIDv4: %q", id)
		}
	}
}

func TestChannelIDUnique(t *testing.T) {
	seen := make(map[string]struct{}, 10000)
	for i := 0; i < 10000; i++ {
		id := NewChannelID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ID %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestTokenFormat(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{64}$`)
	a, b := NewToken(), NewToken()
	if !re.MatchString(a) || !re.MatchString(b) {
		t.Fatalf("bad token format: %q %q", a, b)
	}
	if a == b {
		t.Fatal("tokens collide")
	}
}

func TestHashDeterministic(t *testing.T) {
	if !bytes.Equal(Hash("x"), Hash("x")) {
		t.Fatal("hash not deterministic")
	}
	if bytes.Equal(Hash("x"), Hash("y")) {
		t.Fatal("different inputs hash equal")
	}
	if len(Hash("x")) != 32 {
		t.Fatal("want 32-byte hash")
	}
}

func TestShort(t *testing.T) {
	in := "00000000"
	s := Short(in)
	if len(s) != 8 || s == in {
		t.Fatalf("short form %q", s)
	}
	if s != Short(in) || s != ShortHash(Hash(in)) {
		t.Fatal("short form not stable")
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}$`).MatchString(s) {
		t.Fatalf("bad short form %q", s)
	}
}
