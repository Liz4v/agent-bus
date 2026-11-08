// Package hook implements `agent-bus hook stop`, the Claude Code Stop hook.
// It finds this session's shim, asks it for the unread state and, if anything
// needs the agent's attention, prints additional context. It fails open: on any
// error it prints nothing and exits 0.
package hook

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// Input is the part of the Stop hook JSON that this hook uses.
type Input struct {
	StopHookActive  bool             `json:"stop_hook_active"`
	BackgroundTasks []BackgroundTask `json:"background_tasks"`
}

// BackgroundTask is one entry of background_tasks. Command is present only for
// shell tasks.
type BackgroundTask struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Command string `json:"command"`
}

func (b BackgroundTask) running() bool {
	if b.Type != "" && b.Type != "shell" {
		return false
	}
	return b.Status == "" || strings.EqualFold(b.Status, "running")
}

// Env holds everything the hook touches outside its own logic, so tests can
// replace it.
type Env struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	Getpid     func() int
	ParentOf   func(pid int) (int, error) // reads /proc/<pid>/stat by default
	RuntimeDir string                     // $XDG_RUNTIME_DIR
	TempDir    string
	UID        int
	Timeout    time.Duration // overall STATUS deadline (default 3 s)
}

// DefaultEnv returns the real environment.
func DefaultEnv() Env {
	return Env{
		Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr,
		Getpid:     os.Getpid,
		ParentOf:   procParent,
		RuntimeDir: os.Getenv("XDG_RUNTIME_DIR"),
		TempDir:    os.TempDir(),
		UID:        os.Getuid(),
	}
}

// procParent reads the parent PID from /proc/<pid>/stat. The command name is in
// parentheses and may contain spaces, so parse from the last ')'.
func procParent(pid int) (int, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 2 {
		return 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	return strconv.Atoi(f[1])
}

var taskSocketRE = regexp.MustCompile(`agent-bus\S*\s+wait\s+--socket[=\s]+['"]?([^'"\s]+)`)

// Run executes the hook and returns the process exit code, which is always 0.
func Run(env Env) int {
	if err := run(env); err != nil {
		fmt.Fprintf(env.Stderr, "agent-bus hook: %v\n", err)
	}
	return 0
}

func run(env Env) error {
	var in Input
	data, err := io.ReadAll(io.LimitReader(env.Stdin, 8<<20))
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := json.Unmarshal(data, &in); err != nil {
			return fmt.Errorf("bad hook input: %w", err)
		}
	}
	if in.StopHookActive {
		return nil
	}
	timeout := env.Timeout
	if timeout == 0 {
		timeout = 3 * time.Second
	}
	deadline := time.Now().Add(timeout)

	sock, taskRunning := locate(env, in)
	if sock == "" {
		return nil
	}
	st, err := queryStatus(sock, deadline)
	if err != nil {
		return err
	}
	switch {
	case st.Count > 0:
		return emit(env, fmt.Sprintf("agent-bus: %d unread peer message(s)/event(s) from %s. Call the agent-bus read tool before finishing.",
			st.Count, names(st.From)))
	case st.Error != nil:
		return nil // fail open: the broker is unreachable and nothing is unread locally
	case (st.Named || st.Subscribed) && !st.Armed && !taskRunning:
		return emit(env, "agent-bus: wake is not armed; run the command from wake_command in the background so peers can wake this session.")
	}
	return nil
}

// names keeps only well-formed session names (or agent-bus), so nothing a peer
// controls beyond the broker's name rules reaches the agent's context.
func names(from []string) string {
	var out []string
	for _, l := range from {
		if protocol.ValidName(l) || l == protocol.SystemSender {
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		return "peers"
	}
	return strings.Join(out, ", ")
}

func emit(env Env, context string) error {
	out := map[string]any{"hookSpecificOutput": map[string]string{
		"hookEventName":     "Stop",
		"additionalContext": context,
	}}
	b, err := json.Marshal(out)
	if err != nil {
		return err
	}
	_, err = env.Stdout.Write(append(b, '\n'))
	return err
}

func alive(sock string) bool {
	c, err := net.DialTimeout("unix", sock, time.Second)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// locate finds this session's wake socket (D8). The bool reports whether a
// running `wait` task for that socket appears in background_tasks.
func locate(env Env, in Input) (string, bool) {
	var taskSockets []string
	for _, t := range in.BackgroundTasks {
		if !t.running() || t.Command == "" {
			continue
		}
		if m := taskSocketRE.FindStringSubmatch(t.Command); m != nil {
			taskSockets = append(taskSockets, m[1])
		}
	}
	has := func(sock string) bool {
		for _, s := range taskSockets {
			if s == sock {
				return true
			}
		}
		return false
	}
	for _, s := range taskSockets {
		if alive(s) {
			return s, true
		}
	}
	// Fallback: walk the process ancestry looking for ppid-<pid> index files.
	var dirs []string
	if env.RuntimeDir != "" {
		dirs = append(dirs, filepath.Join(env.RuntimeDir, "agent-bus"))
	}
	dirs = append(dirs, filepath.Join(env.TempDir, fmt.Sprintf("agent-bus-%d", env.UID)))
	var trusted []string
	for _, d := range dirs {
		if checkDir(d, env.UID) == nil {
			trusted = append(trusted, d)
		}
	}
	if len(trusted) == 0 {
		return "", false
	}
	pid := env.Getpid()
	seen := map[int]bool{}
	for i := 0; i < 64; i++ {
		ppid, err := env.ParentOf(pid)
		if err != nil || ppid <= 1 || seen[ppid] {
			return "", false
		}
		seen[ppid] = true
		pid = ppid
		for _, d := range trusted {
			b, err := os.ReadFile(filepath.Join(d, fmt.Sprintf("ppid-%d", pid)))
			if err != nil {
				continue
			}
			if s := strings.TrimSpace(string(b)); s != "" && alive(s) {
				return s, has(s)
			}
		}
	}
	return "", false
}

// checkDir verifies that dir is a real directory owned by uid with no group or
// other permission bits.
func checkDir(dir string, uid int) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !fi.Mode().IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s is not a real directory", dir)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || int(st.Uid) != uid {
		return fmt.Errorf("%s is not owned by uid %d", dir, uid)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%s has mode %o", dir, fi.Mode().Perm())
	}
	return nil
}

func queryStatus(sock string, deadline time.Time) (protocol.StatusReply, error) {
	var st protocol.StatusReply
	c, err := net.DialTimeout("unix", sock, time.Until(deadline))
	if err != nil {
		return st, err
	}
	defer c.Close()
	c.SetDeadline(deadline)
	if _, err := c.Write([]byte(protocol.WakeCmdStatus + "\n")); err != nil {
		return st, err
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return st, fmt.Errorf("reading STATUS reply: %w", err)
	}
	if err := json.Unmarshal([]byte(line), &st); err != nil {
		return st, fmt.Errorf("bad STATUS reply: %w", err)
	}
	return st, nil
}
