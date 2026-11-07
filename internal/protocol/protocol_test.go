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
		KindText: true, KindPeerJoined: true, KindPeerLeft: true, KindChannelExpired: true,
		KindPeerDisconnected: false, KindPeerReconnected: false, "bogus": false,
	} {
		if got := IsWaking(kind); got != want {
			t.Errorf("IsWaking(%q) = %v, want %v", kind, got, want)
		}
	}
}

func TestValidLabel(t *testing.T) {
	for s, want := range map[string]bool{
		"host/name": true, "h.o-s_t/n1": true, "host": false, "host/": false, "/n": false,
		`ho"st/n`: false, "ho<st/n": false, "a/b/c": false, "host/na me": false,
	} {
		if got := ValidLabel(s); got != want {
			t.Errorf("ValidLabel(%q) = %v, want %v", s, got, want)
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
