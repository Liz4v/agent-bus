// Package hermeswake is the sidecar behind `agent-bus hermes-wake`: it watches
// the shim's wake socket and POSTs a signed, content-free notification to
// Hermes' webhook platform when unread messages exist.
package hermeswake

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// errShimGone is returned when the connection ends before a reply arrives.
var errShimGone = errors.New("the agent-bus shim went away before replying")

// Wait sends WAIT on the wake socket and returns the shim's reply. Cancelling
// ctx closes the connection and returns ctx.Err().
func Wait(ctx context.Context, path string) (protocol.WaitReply, error) {
	var r protocol.WaitReply
	err := roundTrip(ctx, path, protocol.WakeCmdWait, &r)
	return r, err
}

// Status sends STATUS on the wake socket and returns the shim's reply.
func Status(ctx context.Context, path string) (protocol.StatusReply, error) {
	var r protocol.StatusReply
	err := roundTrip(ctx, path, protocol.WakeCmdStatus, &r)
	return r, err
}

func roundTrip(ctx context.Context, path, cmd string, out any) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return fmt.Errorf("connect to wake socket: %w", err)
	}
	defer conn.Close()
	done := make(chan struct{})
	defer close(done)
	go func() { // cancel by closing the connection
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()
	if _, err := conn.Write([]byte(cmd + "\n")); err != nil {
		return ctxOr(ctx, fmt.Errorf("%w: %v", errShimGone, err))
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil && strings.TrimSpace(line) == "" {
		return ctxOr(ctx, errShimGone)
	}
	if err := json.Unmarshal([]byte(line), out); err != nil {
		return fmt.Errorf("unreadable reply from the shim: %w", err)
	}
	return nil
}

func ctxOr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
