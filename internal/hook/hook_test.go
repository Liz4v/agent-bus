package hook

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

func shortDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "h")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

// statusServer answers STATUS with raw (a JSON line) on a fresh socket.
func statusServer(t *testing.T, raw string) string {
	t.Helper()
	path := filepath.Join(shortDir(t), "s.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				line, _ := bufio.NewReader(c).ReadString('\n')
				if strings.TrimSpace(line) == "STATUS" && raw != "" {
					c.Write([]byte(raw + "\n"))
				}
			}()
		}
	}()
	return path
}

func statusJSON(s protocol.StatusReply) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func deadSocket(t *testing.T) string {
	t.Helper()
	path := filepath.Join(shortDir(t), "dead.sock")
	ln, _ := net.Listen("unix", path)
	ln.Close()
	os.Remove(path)
	return path
}

type result struct {
	code           int
	stdout, stderr string
}

func (r result) context(t *testing.T) string {
	t.Helper()
	if r.stdout == "" {
		return ""
	}
	var v struct {
		HookSpecificOutput struct {
			HookEventName     string `json:"hookEventName"`
			AdditionalContext string `json:"additionalContext"`
		} `json:"hookSpecificOutput"`
	}
	if err := json.Unmarshal([]byte(r.stdout), &v); err != nil {
		t.Fatalf("stdout is not valid JSON: %q", r.stdout)
	}
	if v.HookSpecificOutput.HookEventName != "Stop" {
		t.Fatalf("hookEventName = %q", v.HookSpecificOutput.HookEventName)
	}
	return v.HookSpecificOutput.AdditionalContext
}

func execHook(t *testing.T, input string, mod func(*Env)) result {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin: strings.NewReader(input), Stdout: &out, Stderr: &errb,
		Getpid:     func() int { return 100 },
		ParentOf:   func(int) (int, error) { return 0, fmt.Errorf("no process table") },
		RuntimeDir: "", TempDir: shortDir(t), UID: os.Getuid(),
		Timeout: 2 * time.Second,
	}
	if mod != nil {
		mod(&env)
	}
	code := Run(env)
	return result{code, out.String(), errb.String()}
}

func inputWithTasks(cmds ...string) string {
	var ts []map[string]string
	for _, c := range cmds {
		ts = append(ts, map[string]string{"type": "shell", "status": "running", "command": c})
	}
	b, _ := json.Marshal(map[string]any{"hook_event_name": "Stop", "background_tasks": ts})
	return string(b)
}

var unread = statusJSON(protocol.StatusReply{Count: 2, From: []string{"h/a"}, LiveChannels: 1, Armed: true})

func TestStopHookActiveIsSilent(t *testing.T) {
	sock := statusServer(t, unread)
	in := fmt.Sprintf(`{"stop_hook_active":true,"background_tasks":[{"type":"shell","status":"running","command":"agent-bus wait --socket %s"}]}`, sock)
	if r := execHook(t, in, nil); r.code != 0 || r.stdout != "" {
		t.Fatalf("%+v", r)
	}
}

func TestBackgroundTasksLocatorForms(t *testing.T) {
	sock := statusServer(t, unread)
	forms := []string{
		"/home/user/bin/agent-bus wait --socket " + sock,
		"/nix/store/abc123-agent-bus-0.1/bin/agent-bus wait --socket '" + sock + "'",
		`agent-bus wait --socket "` + sock + `" &`,
		"agent-bus wait --socket=" + sock,
		"agent-bus wait --socket " + sock + " > /dev/null 2>&1 &",
	}
	for _, f := range forms {
		r := execHook(t, inputWithTasks(f), nil)
		if ctx := r.context(t); !strings.Contains(ctx, "2 unread") {
			t.Errorf("form %q not located: %+v", f, r)
		}
	}
}

func TestNonRunningAndUnrelatedTasksSkipped(t *testing.T) {
	sock := statusServer(t, unread)
	in := fmt.Sprintf(`{"background_tasks":[
		{"type":"shell","status":"completed","command":"agent-bus wait --socket %[1]s"},
		{"type":"agent","status":"running","command":"agent-bus wait --socket %[1]s"},
		{"type":"shell","status":"running","command":"sleep 100"}]}`, sock)
	if r := execHook(t, in, nil); r.stdout != "" {
		t.Fatalf("matched a task that should be skipped: %+v", r)
	}
}

func TestDeadFirstSocketSkippedForLaterLiveOne(t *testing.T) {
	live := statusServer(t, unread)
	dead := deadSocket(t)
	r := execHook(t, inputWithTasks("agent-bus wait --socket "+dead, "agent-bus wait --socket "+live), nil)
	if !strings.Contains(r.context(t), "2 unread") {
		t.Fatalf("%+v", r)
	}
}

// ancestry sets up an index file for a fake process table 100 -> 200 -> 300 -> 1.
func ancestry(t *testing.T, sock string, dirMode os.FileMode, index string) func(*Env) {
	runtime := shortDir(t)
	dir := filepath.Join(runtime, "agent-bus")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	os.Chmod(dir, dirMode)
	if err := os.WriteFile(filepath.Join(dir, index), []byte(sock), 0o600); err != nil {
		t.Fatal(err)
	}
	table := map[int]int{100: 200, 200: 300, 300: 1}
	return func(e *Env) {
		e.RuntimeDir = runtime
		e.ParentOf = func(pid int) (int, error) {
			if p, ok := table[pid]; ok {
				return p, nil
			}
			return 0, fmt.Errorf("no such process")
		}
	}
}

func TestAncestryFallback(t *testing.T) {
	sock := statusServer(t, unread)
	r := execHook(t, `{}`, ancestry(t, sock, 0o700, "ppid-300"))
	if !strings.Contains(r.context(t), "2 unread") {
		t.Fatalf("%+v", r)
	}
	// an index for a PID that is not an ancestor is ignored
	if r := execHook(t, `{}`, ancestry(t, sock, 0o700, "ppid-999")); r.stdout != "" {
		t.Fatalf("unrelated index used: %+v", r)
	}
}

func TestAncestryIgnoresUntrustedDirectory(t *testing.T) {
	sock := statusServer(t, unread)
	if r := execHook(t, `{}`, ancestry(t, sock, 0o777, "ppid-300")); r.stdout != "" {
		t.Fatalf("mode 0777 directory trusted: %+v", r)
	}
	mod := ancestry(t, sock, 0o700, "ppid-300")
	r := execHook(t, `{}`, func(e *Env) { mod(e); e.UID = os.Getuid() + 1 })
	if r.stdout != "" {
		t.Fatalf("directory with the wrong owner trusted: %+v", r)
	}
}

func TestUnreadContextHasLabelsNotContent(t *testing.T) {
	sock := statusServer(t, statusJSON(protocol.StatusReply{
		Count: 3, From: []string{"h/a", `evil" label`, "agent-bus", "<x>/y"}, LiveChannels: 1, Armed: true}))
	r := execHook(t, inputWithTasks("agent-bus wait --socket "+sock), nil)
	ctx := r.context(t)
	if !strings.Contains(ctx, "3 unread") || !strings.Contains(ctx, "h/a, agent-bus") || strings.Contains(ctx, "evil") ||
		strings.Contains(ctx, "<x>") || !strings.Contains(ctx, "read tool") {
		t.Fatalf("context: %q", ctx)
	}
}

func TestNotArmed(t *testing.T) {
	sock := statusServer(t, statusJSON(protocol.StatusReply{LiveChannels: 1, Armed: false, From: []string{}}))
	// located by ancestry: no wait task is running
	r := execHook(t, `{}`, ancestry(t, sock, 0o700, "ppid-300"))
	if !strings.Contains(r.context(t), "wake is not armed") {
		t.Fatalf("%+v", r)
	}
	// located by a running wait task that has not connected yet: counts as armed
	r = execHook(t, inputWithTasks("agent-bus wait --socket "+sock), nil)
	if r.stdout != "" {
		t.Fatalf("running wait task ignored: %+v", r)
	}
}

func TestNothingToReport(t *testing.T) {
	for name, st := range map[string]protocol.StatusReply{
		"armed":       {LiveChannels: 1, Armed: true},
		"no channels": {},
	} {
		sock := statusServer(t, statusJSON(st))
		if r := execHook(t, inputWithTasks("agent-bus wait --socket "+sock), nil); r.stdout != "" || r.code != 0 {
			t.Errorf("%s: %+v", name, r)
		}
	}
}

func TestStatusErrorHandling(t *testing.T) {
	msg := "broker down"
	quiet := statusServer(t, statusJSON(protocol.StatusReply{Count: 0, LiveChannels: 1, Error: &msg}))
	if r := execHook(t, inputWithTasks("agent-bus wait --socket "+quiet), nil); r.stdout != "" {
		t.Fatalf("error with count 0 must be silent: %+v", r)
	}
	loud := statusServer(t, statusJSON(protocol.StatusReply{Count: 1, From: []string{"h/a"}, LiveChannels: 1, Error: &msg}))
	r := execHook(t, inputWithTasks("agent-bus wait --socket "+loud), nil)
	if !strings.Contains(r.context(t), "1 unread") {
		t.Fatalf("unread rows must be reported even with an error: %+v", r)
	}
}

func TestEveryErrorPathExitsZeroSilently(t *testing.T) {
	garbage := statusServer(t, "not json")
	silentServer := statusServer(t, "") // never replies
	cases := map[string]string{
		"bad input":   `{not json`,
		"empty input": ``,
		"no sockets":  `{}`,
		"dead socket": inputWithTasks("agent-bus wait --socket " + deadSocket(t)),
		"garbage":     inputWithTasks("agent-bus wait --socket " + garbage),
		"no reply":    inputWithTasks("agent-bus wait --socket " + silentServer),
	}
	for name, in := range cases {
		start := time.Now()
		r := execHook(t, in, func(e *Env) { e.Timeout = 500 * time.Millisecond })
		if r.code != 0 || r.stdout != "" {
			t.Errorf("%s: %+v", name, r)
		}
		if time.Since(start) > 3*time.Second {
			t.Errorf("%s: took %v", name, time.Since(start))
		}
	}
	if r := execHook(t, `{not json`, nil); r.stderr == "" {
		t.Error("the reason should go to stderr")
	}
}

func TestSubscribedUnarmedSessionGetsTheReminder(t *testing.T) {
	// No live channels, but subscribed to admin notifications: the wake must be armed.
	sock := statusServer(t, statusJSON(protocol.StatusReply{LiveChannels: 0, Subscribed: true, Armed: false, From: []string{}}))
	r := execHook(t, `{}`, ancestry(t, sock, 0o700, "ppid-300"))
	if !strings.Contains(r.context(t), "wake is not armed") {
		t.Fatalf("%+v", r)
	}
	// A running wait task counts as armed here too.
	if r := execHook(t, inputWithTasks("agent-bus wait --socket "+sock), nil); r.stdout != "" {
		t.Fatalf("running wait task ignored: %+v", r)
	}
}

func TestSubscribedArmedSessionGetsNoReminder(t *testing.T) {
	sock := statusServer(t, statusJSON(protocol.StatusReply{LiveChannels: 0, Subscribed: true, Armed: true, From: []string{}}))
	if r := execHook(t, `{}`, ancestry(t, sock, 0o700, "ppid-300")); r.stdout != "" || r.code != 0 {
		t.Fatalf("%+v", r)
	}
	// Not subscribed and no channels: nothing to say either.
	sock = statusServer(t, statusJSON(protocol.StatusReply{From: []string{}}))
	if r := execHook(t, `{}`, ancestry(t, sock, 0o700, "ppid-301")); r.stdout != "" {
		t.Fatalf("%+v", r)
	}
}
