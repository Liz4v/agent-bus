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
