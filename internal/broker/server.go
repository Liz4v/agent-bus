package broker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/Liz4v/agent-bus/internal/identity"
	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/secret"
)

// Server is the HTTP layer of the broker.
type Server struct {
	store   *Store
	tracker *Tracker
	log     *slog.Logger
}

// NewServer wires the HTTP handlers to a store and a liveness tracker.
func NewServer(store *Store, tracker *Tracker, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{store: store, tracker: tracker, log: log}
}

type authed func(w http.ResponseWriter, r *http.Request, session int64)

// Handler returns the mux for the broker routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/register", s.register)
	mux.HandleFunc("GET /v1/account", s.account)
	mux.HandleFunc("GET /v1/admin/accounts", s.adminAuth(s.adminList))
	mux.HandleFunc("POST /v1/admin/approve", s.adminAuth(s.adminApprove))
	mux.HandleFunc("POST /v1/admin/reject", s.adminAuth(s.adminReject))
	mux.HandleFunc("POST /v1/admin/remove", s.adminAuth(s.adminRemove))
	mux.HandleFunc("POST /v1/admin/max-accounts", s.adminAuth(s.adminMaxAccounts))
	mux.HandleFunc("POST /v1/admin/subscribe", s.adminAuth(s.adminSubscribe))
	mux.HandleFunc("POST /v1/sessions", s.createSession)
	mux.HandleFunc("DELETE /v1/sessions/self", s.auth(s.deleteSelf))
	mux.HandleFunc("POST /v1/name", s.auth(s.claimName))
	mux.HandleFunc("DELETE /v1/name", s.auth(s.releaseName))
	mux.HandleFunc("POST /v1/messages", s.auth(s.send))
	mux.HandleFunc("POST /v1/sync", s.auth(s.sync))
	// The channel routes of older versions: answered before authentication, so
	// an old shim reads the upgrade message instead of a bare 401.
	mux.HandleFunc("POST /v1/channels", s.gone)
	mux.HandleFunc("POST /v1/channels/join", s.gone)
	mux.HandleFunc("POST /v1/leave", s.gone)
	return s.logging(mux)
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// logging logs method, path, status and a short hash of the bearer value. It
// never logs bodies, tokens or credentials. On the routes
// authenticated by the account credential the hash is labelled "cred".
func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		attrs := []any{"method", r.Method, "path", r.URL.Path, "status", sw.status, "dur", time.Since(start).Round(time.Millisecond)}
		if tok := bearer(r); tok != "" {
			key := "session"
			if (r.Method == http.MethodPost && r.URL.Path == "/v1/sessions") || (r.Method == http.MethodGet && r.URL.Path == "/v1/account") {
				key = "cred"
			}
			attrs = append(attrs, key, secret.Short(tok))
		}
		s.log.Info("request", attrs...)
	})
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

// auth resolves the bearer token and touches liveness at the start and at the
// end of the request.
func (s *Server) auth(h authed) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := bearer(r)
		if tok == "" {
			s.writeErr(w, r, ErrUnauthorized)
			return
		}
		id, err := s.store.SessionByToken(r.Context(), tok)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		s.tracker.Touch(id)
		defer s.tracker.Touch(id)
		h(w, r, id)
	}
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		json.NewEncoder(w).Encode(v)
	}
}

func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, err error) {
	var be *Error
	if !errors.As(err, &be) {
		if r.Context().Err() != nil {
			return // the client went away
		}
		s.log.Error("internal error", "path", r.URL.Path, "err", err)
		be = newErr(http.StatusInternalServerError, protocol.CodeInternal, "internal error")
	}
	s.writeJSON(w, be.Status, protocol.ErrorBody{Error: protocol.ErrorDetail{Code: be.Code, Message: be.Message}})
}

// decode reads a JSON body limited to protocol.MaxBodyBytes.
func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, protocol.MaxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		var mb *http.MaxBytesError
		if errors.As(err, &mb) {
			s.writeErr(w, r, errTooLarge())
		} else {
			s.writeErr(w, r, newErr(http.StatusBadRequest, protocol.CodeBadRequest, "malformed JSON body"))
		}
		return false
	}
	return true
}

// createSession authenticates the account credential and opens a session.
func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	cred := bearer(r)
	if cred == "" {
		s.writeErr(w, r, ErrBadCredential)
		return
	}
	accountID, account, err := s.store.SessionAccount(r.Context(), cred)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	token, id, err := s.store.CreateSession(r.Context(), accountID)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.tracker.Touch(id)
	s.writeJSON(w, http.StatusCreated, protocol.CreateSessionResponse{SessionToken: token, Account: account})
}

func (s *Server) deleteSelf(w http.ResponseWriter, r *http.Request, session int64) {
	if err := s.store.DeleteSelf(r.Context(), session); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.tracker.Forget(session)
	w.WriteHeader(http.StatusNoContent)
}

// gone answers the removed channel routes with 410 upgrade_required.
func (s *Server) gone(w http.ResponseWriter, r *http.Request) {
	s.writeErr(w, r, newErr(http.StatusGone, protocol.CodeUpgradeRequired,
		"this broker addresses peers by name, not by channel: upgrade agent-bus on this host and restart the session"))
}

func (s *Server) claimName(w http.ResponseWriter, r *http.Request, session int64) {
	var req protocol.ClaimNameRequest
	if !s.decode(w, r, &req) {
		return
	}
	name, err := s.store.ClaimName(r.Context(), session, req.Name)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, protocol.ClaimNameResponse{Name: name})
}

func (s *Server) releaseName(w http.ResponseWriter, r *http.Request, session int64) {
	released, err := s.store.ReleaseName(r.Context(), session)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, protocol.ReleaseNameResponse{Released: released})
}

func (s *Server) send(w http.ResponseWriter, r *http.Request, session int64) {
	var req protocol.SendRequest
	if !s.decode(w, r, &req) {
		return
	}
	if err := s.store.Send(r.Context(), session, req.To, req.Text); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, protocol.SendResponse{})
}

// sync is the long-poll: subscribe, transact, then respond or wait.
func (s *Server) sync(w http.ResponseWriter, r *http.Request, session int64) {
	var req protocol.SyncRequest
	if !s.decode(w, r, &req) {
		return
	}
	wait := time.Duration(req.Wait) * time.Second
	if wait < 0 {
		wait = 0
	}
	if wait > protocol.SyncWaitCap {
		wait = protocol.SyncWaitCap
	}
	ctx := r.Context()
	signal, cancel := s.store.Notify().Subscribe(session)
	defer cancel()

	resp, err := s.store.Sync(ctx, session, req)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	if wait > 0 && resp.Version <= req.Version && len(resp.Messages) == 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-signal:
		case <-timer.C:
		case <-ctx.Done():
			return
		}
		req.Ack = 0 // do not ack again
		resp, err = s.store.Sync(ctx, session, req)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// HTTPServer builds the http.Server with the D11 timeouts: no WriteTimeout, so
// long-polls are not cut.
func HTTPServer(addr string, h http.Handler, baseCtx context.Context) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
	}
}

// Config configures Run.
type Config struct {
	Addr   string
	DBPath string
	// KeyPath is the broker key file (default broker-key.pem next to DBPath).
	KeyPath string
	// ControlPath is the host-side control socket (default control.sock next
	// to DBPath).
	ControlPath string
	Log         *slog.Logger
	// SweepInterval overrides the sweeper period (default protocol.SweepInterval).
	SweepInterval time.Duration
	// Ready, if set, is called with the bound listen address once the broker
	// accepts connections (used by tests).
	Ready func(addr string)
}

// DefaultKeyPath and DefaultControlPath return the defaults next to the
// database.
func DefaultKeyPath(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), "broker-key.pem")
}

func DefaultControlPath(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), "control.sock")
}

// announceClaim issues and logs a claim code, but only when no hostmaster
// exists. A start with a hostmaster issues and logs nothing, so the journal
// can never be used to take over an already claimed broker (M4). It reports
// whether a code was issued.
func announceClaim(ctx context.Context, store *Store, log *slog.Logger) (bool, error) {
	has, err := store.HasHostmaster(ctx)
	if err != nil || has {
		return false, err
	}
	code, _, err := store.IssueClaim(ctx)
	if err != nil {
		return false, err
	}
	log.Warn("broker has no hostmaster; claim it with `agent-bus claim` using this one-time code (valid until the broker restarts)",
		"claim_code", code)
	return true, nil
}

// Run opens the store, loads the TLS identity, initialises liveness tracking,
// starts the sweeper and the control socket and then serves HTTPS until ctx is
// cancelled. Init runs before the sweeper starts and before the listener
// accepts requests (D10).
func Run(ctx context.Context, cfg Config) error {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	if cfg.KeyPath == "" {
		cfg.KeyPath = DefaultKeyPath(cfg.DBPath)
	}
	if cfg.ControlPath == "" {
		cfg.ControlPath = DefaultControlPath(cfg.DBPath)
	}
	key, err := identity.LoadOrCreateKey(cfg.KeyPath)
	if err != nil {
		return err
	}
	cert, err := identity.SelfSignedCert(key, time.Now())
	if err != nil {
		return err
	}
	fingerprint, err := identity.Fingerprint(cert)
	if err != nil {
		return err
	}
	store, err := OpenStore(cfg.DBPath, nil, nil)
	if err != nil {
		return err
	}
	defer store.Close()
	store.SetLogger(log)
	tracker := NewTracker(nil)
	if err := tracker.Init(ctx, store); err != nil {
		return err
	}
	sweeper := NewSweeper(store, tracker, log)
	if cfg.SweepInterval > 0 {
		sweeper.Interval = cfg.SweepInterval
	}
	sweepDone := make(chan struct{})
	sweepCtx, stopSweep := context.WithCancel(ctx)
	go func() { defer close(sweepDone); sweeper.Run(sweepCtx) }()
	defer func() { stopSweep(); <-sweepDone }()

	if _, err := announceClaim(ctx, store, log); err != nil {
		return err
	}
	control, err := ListenControl(cfg.ControlPath, store, fingerprint, log)
	if err != nil {
		return err
	}
	defer control.Close()

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	tlsLn := tls.NewListener(ln, identity.ServerTLSConfig(cert))
	srv := HTTPServer(cfg.Addr, NewServer(store, tracker, log).Handler(), ctx)
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(tlsLn) }()
	log.Info("broker listening", "addr", ln.Addr().String(), "db", cfg.DBPath, "control", cfg.ControlPath)
	log.Info("broker identity", "fingerprint", fingerprint)
	if cfg.Ready != nil {
		cfg.Ready(ln.Addr().String())
	}
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdown); err != nil {
		srv.Close()
	}
	return nil
}
