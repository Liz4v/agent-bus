// Package wait implements `agent-bus wait`: block on the shim's wake socket
// until unread messages are waiting, then print who sent them and exit.
package wait

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// Exit codes (Architect D7).
const (
	ExitPending    = 0
	ExitNoChannels = 1
	ExitRearm      = 2
)

// Run connects to the wake socket, sends WAIT and prints one line describing
// the outcome. It never prints message content. It returns the exit code.
func Run(ctx context.Context, socket string, stdout, stderr io.Writer) int {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		fmt.Fprintf(stderr, "agent-bus: cannot reach the agent-bus shim at %s: %v\n", socket, err)
		return ExitNoChannels
	}
	defer conn.Close()
	go func() { // cancel by closing the connection
		<-ctx.Done()
		conn.Close()
	}()
	if _, err := conn.Write([]byte(protocol.WakeCmdWait + "\n")); err != nil {
		fmt.Fprintf(stderr, "agent-bus: the agent-bus shim went away: %v\n", err)
		return ExitNoChannels
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil && strings.TrimSpace(line) == "" {
		fmt.Fprintln(stdout, "agent-bus: the agent-bus shim went away (the Claude Code session may have ended); no more wakes will arrive.")
		return ExitNoChannels
	}
	var r protocol.WaitReply
	if err := json.Unmarshal([]byte(line), &r); err != nil {
		fmt.Fprintf(stderr, "agent-bus: unreadable reply from the shim: %v\n", err)
		return ExitNoChannels
	}
	switch r.Status {
	case protocol.WaitPending:
		fmt.Fprintf(stdout, "agent-bus: %d unread message(s)/event(s) from %s. Call the agent-bus read tool, then re-run this wait command in the background.\n",
			r.Count, safeLabels(r.From))
		return ExitPending
	case protocol.WaitNoChannels:
		fmt.Fprintf(stdout, "agent-bus: nothing to wait for: %s\n", r.Reason)
		return ExitNoChannels
	case protocol.WaitRearm:
		fmt.Fprintln(stdout, "agent-bus: no messages yet; re-run this same wait command in the background to keep listening.")
		return ExitRearm
	}
	fmt.Fprintf(stderr, "agent-bus: unexpected reply status %q\n", r.Status)
	return ExitNoChannels
}

// safeLabels joins only well-formed labels, so nothing a peer controls beyond
// the broker's label pattern can reach the agent's context.
func safeLabels(from []string) string {
	var out []string
	for _, l := range from {
		if protocol.ValidLabel(l) || l == protocol.SystemSender {
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		return "unknown senders"
	}
	return strings.Join(out, ", ")
}
