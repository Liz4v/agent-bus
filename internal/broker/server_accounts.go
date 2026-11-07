package broker

import (
	"net/http"
	"strconv"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// adminHandler runs for an authenticated hostmaster session. actor is the
// hostmaster's account name, for the audit log.
type adminHandler func(w http.ResponseWriter, r *http.Request, session int64, actor string)

// adminAuth resolves the session token, then requires the hostmaster before
// anything else (in particular before the body is read), so non-hostmaster
// callers learn nothing from the admin routes (M10).
func (s *Server) adminAuth(h adminHandler) http.HandlerFunc {
	return s.auth(func(w http.ResponseWriter, r *http.Request, session int64) {
		name, host, err := s.store.SessionIdentity(r.Context(), session)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		if !host {
			s.writeErr(w, r, ErrForbidden)
			return
		}
		h(w, r, session, name)
	})
}

func (s *Server) audit(action, target, actor string) {
	s.log.Info("admin action", "action", action, "target", target, "actor", actor)
}

func (s *Server) forget(ids []int64) {
	for _, id := range ids {
		s.tracker.Forget(id)
	}
}

// register is POST /v1/register. It needs no authentication.
func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var req protocol.RegisterRequest
	if !s.decode(w, r, &req) {
		return
	}
	res, err := s.store.Register(r.Context(), req.Name, req.ClaimCode)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.forget(res.DeletedSessions)
	s.writeJSON(w, http.StatusCreated, protocol.RegisterResponse{
		Credential: res.Credential, Name: res.Name, Status: res.Status, Hostmaster: res.Hostmaster,
	})
}

// account is GET /v1/account, authenticated by the account credential.
func (s *Server) account(w http.ResponseWriter, r *http.Request) {
	cred := bearer(r)
	if cred == "" {
		s.writeErr(w, r, ErrBadCredential)
		return
	}
	a, err := s.store.AccountByCredential(r.Context(), cred)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, protocol.AccountResponse{Name: a.Name, Status: a.Status, Hostmaster: a.Hostmaster})
}

func (s *Server) adminList(w http.ResponseWriter, r *http.Request, _ int64, _ string) {
	resp, err := s.store.ListAccounts(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// decodeName reads an AdminNameRequest and validates the single named account.
func (s *Server) decodeName(w http.ResponseWriter, r *http.Request) (string, bool) {
	var req protocol.AdminNameRequest
	if !s.decode(w, r, &req) {
		return "", false
	}
	if !protocol.ValidName(req.Name) {
		s.writeErr(w, r, newErr(http.StatusBadRequest, protocol.CodeBadRequest, "name must be exactly one account name (letters, digits, '.', '_' and '-', up to 64)"))
		return "", false
	}
	return req.Name, true
}

func (s *Server) adminApprove(w http.ResponseWriter, r *http.Request, _ int64, actor string) {
	name, ok := s.decodeName(w, r)
	if !ok {
		return
	}
	canonical, err := s.store.Approve(r.Context(), name)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.audit("approve", canonical, actor)
	s.writeJSON(w, http.StatusOK, protocol.ApproveResponse{Name: canonical, Status: protocol.AccountActive})
}

func (s *Server) adminReject(w http.ResponseWriter, r *http.Request, _ int64, actor string) {
	name, ok := s.decodeName(w, r)
	if !ok {
		return
	}
	canonical, err := s.store.Reject(r.Context(), name)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.audit("reject", canonical, actor)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) adminRemove(w http.ResponseWriter, r *http.Request, _ int64, actor string) {
	name, ok := s.decodeName(w, r)
	if !ok {
		return
	}
	canonical, deleted, err := s.store.Remove(r.Context(), name)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.forget(deleted)
	s.audit("remove", canonical, actor)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) adminMaxAccounts(w http.ResponseWriter, r *http.Request, _ int64, actor string) {
	var req protocol.MaxAccountsRequest
	if !s.decode(w, r, &req) {
		return
	}
	active, err := s.store.SetMaxAccounts(r.Context(), req.Max)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.audit("max-accounts", itoa(req.Max), actor)
	s.writeJSON(w, http.StatusOK, protocol.MaxAccountsResponse{MaxAccounts: req.Max, Active: active})
}

func (s *Server) adminSubscribe(w http.ResponseWriter, r *http.Request, session int64, actor string) {
	var req protocol.SubscribeRequest
	if !s.decode(w, r, &req) {
		return
	}
	pending, err := s.store.SetAdminSub(r.Context(), session, req.Subscribed)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	action := "unsubscribe"
	if req.Subscribed {
		action = "subscribe"
	}
	s.audit(action, "registration notifications", actor)
	s.writeJSON(w, http.StatusOK, protocol.SubscribeResponse{Subscribed: req.Subscribed, Pending: pending})
}

func itoa(n int) string { return strconv.Itoa(n) }
