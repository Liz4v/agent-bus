package protocol

import "testing"

func TestSyncWaitCapBelowDisconnectAfter(t *testing.T) {
	if !(SyncWaitCap < DisconnectAfter) {
		t.Fatalf("SyncWaitCap %v must be < DisconnectAfter %v", SyncWaitCap, DisconnectAfter)
	}
	if !(SyncWait <= SyncWaitCap) {
		t.Fatalf("SyncWait %v must be <= SyncWaitCap %v", SyncWait, SyncWaitCap)
	}
}

func TestIsWaking(t *testing.T) {
	for kind, want := range map[string]bool{
		KindText: true, KindPeerLeft: true, KindPeerRemoved: true, KindRegistrationPending: true,
		KindPeerDisconnected: false, KindPeerReconnected: false,
		"peer_joined": false, "channel_expired": false, "bogus": false, "": false,
	} {
		if got := IsWaking(kind); got != want {
			t.Errorf("IsWaking(%q) = %v, want %v", kind, got, want)
		}
	}
}

func TestIsWakingAccountKinds(t *testing.T) {
	for _, kind := range []string{KindPeerRemoved, KindRegistrationPending} {
		if !IsWaking(kind) {
			t.Errorf("IsWaking(%q) = false, want true", kind)
		}
	}
}

func TestValidName(t *testing.T) {
	long64 := ""
	for i := 0; i < 64; i++ {
		long64 += "a"
	}
	for s, want := range map[string]bool{
		"alice": true, "Alice": true, "a.b-c_d": true, "x": true, long64: true,
		long64 + "a": false, "": false, "a b": false, "a/b": false, "*": false,
		`a"b`: false, "a<b": false, "a\nb": false, "a&b": false, "é": false,
		"</account-name>": false, "name\x00": false,
	} {
		if got := ValidName(s); got != want {
			t.Errorf("ValidName(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestNameKey(t *testing.T) {
	for in, want := range map[string]string{
		"bob": "bob", "Bob": "bob", "BOB": "bob", "a.B-c_D": "a.b-c_d", "": "",
	} {
		if got := NameKey(in); got != want {
			t.Errorf("NameKey(%q) = %q, want %q", in, got, want)
		}
	}
	if NameKey("Bob") != NameKey("bOb") {
		t.Error("names differing only in case must share a key")
	}
}

func TestReservedName(t *testing.T) {
	for s, want := range map[string]bool{
		"admin": true, "Admin": true, "ADMIN": true,
		"agent-bus": true, "Agent-Bus": true, "AGENT-BUS": true,
		"admins": false, "bob": false, "agent": false, "agent-bus2": false, "": false,
	} {
		if got := ReservedName(s); got != want {
			t.Errorf("ReservedName(%q) = %v, want %v", s, got, want)
		}
	}
}
