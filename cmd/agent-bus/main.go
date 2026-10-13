// Command agent-bus is the broker, the per-session shim, the wake-up wait
// command and the Stop hook, in one binary.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Liz4v/agent-bus/internal/broker"
	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/hook"
	"github.com/Liz4v/agent-bus/internal/shim"
	"github.com/Liz4v/agent-bus/internal/wait"
)

var version = "dev"

const usage = `usage: agent-bus <command> [flags]

commands:
  broker      run the broker (on the Pi):   --listen :7700 --db /var/lib/agent-bus/agent-bus.db
  shim        run the MCP shim (stdio):     --broker http://host:7700  (or AGENT_BUS_URL)
  wait        block until messages wait:    --socket <path from the wake_command tool>
  hook stop   Claude Code Stop hook (reads the hook JSON on stdin)
  version     print the version
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "broker":
		return runBroker(ctx, args[1:], stderr)
	case "shim":
		return runShim(ctx, args[1:], stderr)
	case "wait":
		return runWait(ctx, args[1:], stdout, stderr)
	case "hook":
		if len(args) == 2 && args[1] == "stop" {
			env := hook.DefaultEnv()
			env.Stdin, env.Stdout, env.Stderr = stdin, stdout, stderr
			return hook.Run(env)
		}
		fmt.Fprint(stderr, "usage: agent-bus hook stop\n")
		return 2
	case "version", "--version":
		fmt.Fprintln(stdout, "agent-bus", version)
		return 0
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	}
	fmt.Fprintf(stderr, "agent-bus: unknown command %q\n\n%s", args[0], usage)
	return 2
}

func runBroker(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("broker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", ":7700", "address to listen on (all interfaces by default)")
	db := fs.String("db", "/var/lib/agent-bus/agent-bus.db", "SQLite database path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	log := slog.New(slog.NewTextHandler(stderr, nil))
	if err := broker.Run(ctx, broker.Config{Addr: *listen, DBPath: *db, Log: log}); err != nil {
		fmt.Fprintf(stderr, "agent-bus broker: %v\n", err)
		return 1
	}
	return 0
}

func runShim(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("shim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	brokerURL := fs.String("broker", os.Getenv("AGENT_BUS_URL"), "broker URL, for example http://pi:7700 (or set AGENT_BUS_URL)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	u := strings.TrimSpace(*brokerURL)
	if u == "" {
		fmt.Fprintln(stderr, "agent-bus shim: no broker URL; pass --broker http://<host>:7700 or set AGENT_BUS_URL")
		return 2
	}
	if !strings.Contains(u, "://") {
		u = "http://" + u
	}
	log := slog.New(slog.NewTextHandler(stderr, nil)) // stdout belongs to MCP

	c := client.New(u)
	sess := shim.NewSession(c)
	mirror := shim.NewMirror(sess, shim.MirrorConfig{})
	var wakeInfo shim.WakeInfo // stays a true nil interface if the socket cannot start
	if w, err := shim.StartWake(mirror, sess, shim.WakeOptions{Log: log}); err != nil {
		log.Warn("running without a wake socket", "err", err)
	} else {
		defer w.Close()
		wakeInfo = w
	}
	tools := shim.NewTools(sess, mirror, os.Hostname, wakeInfo)
	srv := tools.NewMCPServer(version)
	if err := tools.Run(ctx, srv, &mcp.StdioTransport{}); err != nil && ctx.Err() == nil {
		fmt.Fprintf(stderr, "agent-bus shim: %v\n", err)
		return 1
	}
	return 0
}

func runWait(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("wait", flag.ContinueOnError)
	fs.SetOutput(stderr)
	socket := fs.String("socket", "", "wake socket path (from the wake_command tool)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *socket == "" {
		fmt.Fprintln(stderr, "agent-bus wait: --socket is required; get the full command from the wake_command tool")
		return 2
	}
	return wait.Run(ctx, *socket, stdout, stderr)
}
