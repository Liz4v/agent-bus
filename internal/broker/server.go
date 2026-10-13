package broker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

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

// Handler returns the mux for the seven routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", s.createSession)
	mux.HandleFunc("DELETE /v1/sessions/self", s.auth(s.deleteSelf))
	mux.HandleFunc("POST /v1/channels", s.auth(s.createChannel))
	mux.HandleFunc("POST /v1/channels/join", s.auth(s.joinChannel))
	mux.HandleFunc("POST /v1/messages", s.auth(s.send))
	mux.HandleFunc("POST /v1/leave", s.auth(s.leave))
	mux.HandleFunc("POST /v1/sync", s.auth(s.sync))
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

// logging logs method, path, status and a short token hash. It never logs
// bodies, tokens or channel IDs.
func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		attrs := []any{"method", r.Method, "path", r.URL.Path, "status", sw.status, "dur", time.Since(start).Round(time.Millisecond)}
		if tok := bearer(r); tok != "" {
			attrs = append(attrs, "session", secret.Short(tok))
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
	s.writeJSON(w, be.Status, protocol.ErrorBody{Error: protocol.ErrorDetail{
		Code: be.Code, Message: be.Message, KnownPeers: be.KnownPeers,
	}})
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

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	token, id, err := s.store.CreateSession(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.tracker.Touch(id)
	s.writeJSON(w, http.StatusCreated, protocol.CreateSessionResponse{SessionToken: token})
}

func (s *Server) deleteSelf(w http.ResponseWriter, r *http.Request, session int64) {
	if err := s.store.DeleteSelf(r.Context(), session); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.tracker.Forget(session)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) createChannel(w http.ResponseWriter, r *http.Request, session int64) {
	var req protocol.CreateChannelRequest
	if !s.decode(w, r, &req) {
		return
	}
	id, handle, err := s.store.CreateChannel(r.Context(), session, req.Label)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusCreated, protocol.CreateChannelResponse{ChannelID: id, Handle: handle})
}

func (s *Server) joinChannel(w http.ResponseWriter, r *http.Request, session int64) {
	var req protocol.JoinChannelRequest
	if !s.decode(w, r, &req) {
		return
	}
	handle, peer, err := s.store.JoinChannel(r.Context(), session, req.ChannelID, req.Label)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, protocol.JoinChannelResponse{Handle: handle, PeerLabel: peer})
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

func (s *Server) leave(w http.ResponseWriter, r *http.Request, session int64) {
	var req protocol.LeaveRequest
	if !s.decode(w, r, &req) {
		return
	}
	if err := s.store.Leave(r.Context(), session, req.To); err != nil {
		s.writeErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	Log    *slog.Logger
	// SweepInterval overrides the sweeper period (default protocol.SweepInterval).
	SweepInterval time.Duration
}

// Run opens the store, initialises liveness tracking, starts the sweeper and
// then serves until ctx is cancelled. Init runs before the sweeper starts and
// before the listener accepts requests (D10).
func Run(ctx context.Context, cfg Config) error {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	store, err := OpenStore(cfg.DBPath, nil, nil)
	if err != nil {
		return err
	}
	defer store.Close()
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

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	srv := HTTPServer(cfg.Addr, NewServer(store, tracker, log).Handler(), ctx)
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	log.Info("broker listening", "addr", ln.Addr().String(), "db", cfg.DBPath)
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
