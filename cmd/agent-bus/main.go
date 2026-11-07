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
	"path/filepath"
	"strings"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Liz4v/agent-bus/internal/broker"
	"github.com/Liz4v/agent-bus/internal/brokerfile"
	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/enroll"
	"github.com/Liz4v/agent-bus/internal/hermeswake"
	"github.com/Liz4v/agent-bus/internal/hook"
	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/shim"
	"github.com/Liz4v/agent-bus/internal/wait"
)

var version = "dev"

const usage = `usage: agent-bus <command> [flags]

commands:
  broker      run the broker (HTTPS):       --listen :7700 --db /var/lib/agent-bus/agent-bus.db
              [--key <broker-key.pem>] [--control <control.sock>]
  broker recover      print a fresh claim or recovery code (run on the broker host, as root)
  broker fingerprint  print the broker's key fingerprint (run on the broker host, as root)
              (both take --control <control.sock>)
  pin         trust a broker's key:         --broker https://host:7700   (human only, needs a terminal)
  unpin       forget a broker's pinned key: --broker https://host:7700   (human only, needs a terminal)
  register    ask for an account:           --broker https://host:7700 --name NAME   (human only)
  claim       become the hostmaster:        --broker https://host:7700 --name NAME   (human only)
  status      show your account's state:    [--broker https://host:7700]  (works without a terminal)
  shim        run the MCP shim (stdio):     --broker https://host:7700  (or AGENT_BUS_URL)
  wait        block until messages wait:    --socket <path from the wake_command tool>
  hermes-wake wake a Hermes gateway:        --socket <path> --url <webhook> --secret-file <file>
              (shim side: shim --wake-socket <path>; both default to AGENT_BUS_WAKE_SOCKET)
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
		if len(args) > 1 && (args[1] == "recover" || args[1] == "fingerprint") {
			return runBrokerControl(args[1], args[2:], stdout, stderr)
		}
		return runBroker(ctx, args[1:], stderr)
	case "pin", "unpin", "register", "claim", "status":
		return runEnroll(ctx, args[0], args[1:], stdin, stdout, stderr)
	case "shim":
		return runShim(ctx, args[1:], stderr)
	case "wait":
		return runWait(ctx, args[1:], stdout, stderr)
	case "hermes-wake":
		return runHermesWake(ctx, args[1:], stderr)
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

const defaultDB = "/var/lib/agent-bus/agent-bus.db"

// runBrokerControl is `broker recover` and `broker fingerprint`: thin clients
// of the broker's host-side control socket.
func runBrokerControl(cmd string, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("broker "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	control := fs.String("control", broker.DefaultControlPath(defaultDB), "the broker's control socket")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	switch cmd {
	case "fingerprint":
		fp, err := broker.ControlFingerprint(*control)
		if err != nil {
			fmt.Fprintf(stderr, "agent-bus broker fingerprint: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, fp)
	default:
		r, err := broker.ControlRecover(*control)
		if err != nil {
			fmt.Fprintf(stderr, "agent-bus broker recover: %v\n", err)
			return 1
		}
		what := "claim code (this broker has no hostmaster yet)"
		if r.Mode == protocol.ClaimModeRecover {
			what = "recovery code (using it replaces the current hostmaster)"
		}
		fmt.Fprintf(stdout, "%s: %s\n", what, r.ClaimCode)
		fmt.Fprintln(stdout, "Give it to the person who will run `agent-bus claim --broker URL --name NAME` in a terminal. It works once and stops working when the broker restarts or another code is issued.")
	}
	return 0
}

// runEnroll runs the human-side setup commands.
func runEnroll(ctx context.Context, cmd string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	brokerURL := fs.String("broker", os.Getenv("AGENT_BUS_URL"), "broker URL, for example https://broker-host:7700 (or set AGENT_BUS_URL)")
	var name *string
	if cmd == "register" || cmd == "claim" {
		name = fs.String("name", "", "the account name: letters, digits, '.', '_' and '-', at most 64 characters")
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*brokerURL) == "" {
		fmt.Fprintf(stderr, "agent-bus %s: no broker URL; pass --broker https://<host>:7700 or set AGENT_BUS_URL\n", cmd)
		return 2
	}
	if name != nil && *name == "" {
		fmt.Fprintf(stderr, "agent-bus %s: --name is required\n", cmd)
		return 2
	}
	dir, err := brokerfile.Dir()
	if err != nil {
		fmt.Fprintf(stderr, "agent-bus %s: %v\n", cmd, err)
		return 1
	}
	env := &enroll.Env{Stdin: stdin, Stdout: stdout, Stderr: stderr, ConfigDir: dir,
		IsTTY: func() bool { return enroll.IsTTYReader(stdin) }}
	switch cmd {
	case "pin":
		err = env.Pin(ctx, *brokerURL)
	case "unpin":
		err = env.Unpin(ctx, *brokerURL)
	case "register":
		err = env.Register(ctx, *brokerURL, *name)
	case "claim":
		err = env.Claim(ctx, *brokerURL, *name)
	case "status":
		err = env.Status(ctx, *brokerURL)
	}
	if err != nil {
		fmt.Fprintf(stderr, "agent-bus %s: %v\n", cmd, err)
		return 1
	}
	return 0
}

func runBroker(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("broker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", ":7700", "address to listen on (all interfaces by default)")
	db := fs.String("db", defaultDB, "SQLite database path")
	key := fs.String("key", "", "broker key file (default broker-key.pem next to the database)")
	control := fs.String("control", "", "host-side control socket (default control.sock next to the database)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	log := slog.New(slog.NewTextHandler(stderr, nil))
	if err := broker.Run(ctx, broker.Config{Addr: *listen, DBPath: *db, KeyPath: *key, ControlPath: *control, Log: log}); err != nil {
		fmt.Fprintf(stderr, "agent-bus broker: %v\n", err)
		return 1
	}
	return 0
}

func runShim(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("shim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	wakeSocket := fs.String("wake-socket", os.Getenv("AGENT_BUS_WAKE_SOCKET"), "fixed wake socket path (or AGENT_BUS_WAKE_SOCKET); for the Hermes sidecar")
	brokerURL := fs.String("broker", os.Getenv("AGENT_BUS_URL"), "broker URL, for example https://broker-host:7700 (or set AGENT_BUS_URL)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	log := slog.New(slog.NewTextHandler(stderr, nil)) // stdout belongs to MCP

	// The broker file (pin and credential) is read once, here. If it is missing
	// or unusable the shim still serves MCP so its tools can explain what a
	// person must do; nothing then touches the missing client.
	var c *client.Client
	var setupErr error
	hostmaster := false
	dir, err := brokerfile.Dir()
	if err != nil {
		setupErr = &shim.SetupError{Msg: "agent-bus cannot find its configuration directory: " + err.Error()}
	} else {
		var cfg client.Config
		cfg, hostmaster, setupErr = shim.CheckSetup(*brokerURL, dir)
		if setupErr == nil {
			if c, err = client.New(cfg); err != nil {
				setupErr = &shim.SetupError{Msg: "agent-bus cannot use the broker file: " + err.Error()}
			}
		}
	}
	if setupErr != nil {
		log.Warn("agent-bus is not set up; every tool will say so", "reason", setupErr.Error())
		hostmaster = false
	}
	sess := shim.NewSession(c)
	mirror := shim.NewMirror(sess, shim.MirrorConfig{})
	var wakeInfo shim.WakeInfo // stays a true nil interface if the socket cannot start
	wakeOpt := shim.WakeOptionsFromEnv()
	wakeOpt.Log = log
	wakeOpt.Socket = *wakeSocket
	if w, err := shim.StartWake(mirror, sess, wakeOpt); err != nil {
		log.Warn("running without a wake socket", "err", err)
	} else {
		defer w.Close()
		wakeInfo = w
	}
	tools := shim.NewTools(sess, mirror, os.Hostname, wakeInfo, setupErr, hostmaster)
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

func runHermesWake(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("hermes-wake", flag.ContinueOnError)
	fs.SetOutput(stderr)
	socket := fs.String("socket", os.Getenv("AGENT_BUS_WAKE_SOCKET"), "shim wake socket path (or AGENT_BUS_WAKE_SOCKET)")
	url := fs.String("url", "http://127.0.0.1:8644/webhooks/agent-bus", "Hermes webhook route URL")
	home, _ := os.UserHomeDir()
	secretFile := fs.String("secret-file", filepath.Join(home, ".config", "agent-bus", "hermes-webhook-secret"), "file holding the webhook secret (mode 0600)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *socket == "" {
		fmt.Fprintln(stderr, "agent-bus hermes-wake: no wake socket; pass --socket or set AGENT_BUS_WAKE_SOCKET")
		return 2
	}
	secret, err := hermeswake.LoadSecret(*secretFile)
	if err != nil {
		fmt.Fprintf(stderr, "agent-bus hermes-wake: %v\n", err)
		return 2
	}
	log := slog.New(slog.NewTextHandler(stderr, nil))
	err = hermeswake.Run(ctx, hermeswake.Config{
		Shim:     hermeswake.SocketShim{Path: *socket},
		Notifier: &hermeswake.Poster{URL: *url, Secret: secret},
		Log:      log,
	})
	if err != nil && ctx.Err() == nil {
		fmt.Fprintf(stderr, "agent-bus hermes-wake: %v\n", err)
		return 1
	}
	return 0
}
