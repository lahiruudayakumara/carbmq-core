package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/opencorex/crabmq-core/api/db"
	"github.com/opencorex/crabmq-core/api/services"
	"github.com/opencorex/crabmq-core/internal/config"
)

type Server struct {
	cfg     config.Config
	store   *db.Store
	devices *services.DeviceService
	bridge  *services.Bridge
	logger  *slog.Logger
	server  *http.Server
	client  *http.Client
}

func NewServer(
	cfg config.Config,
	store *db.Store,
	devices *services.DeviceService,
	bridge *services.Bridge,
	logger *slog.Logger,
) *Server {
	handlerServer := &Server{
		cfg:     cfg,
		store:   store,
		devices: devices,
		bridge:  bridge,
		logger:  logger,
	}

	handlerServer.server = &http.Server{
		Addr:              cfg.API.ListenAddr,
		Handler:           handlerServer.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.API.ReadTimeout,
		WriteTimeout:      cfg.API.WriteTimeout,
		IdleTimeout:       cfg.API.IdleTimeout,
	}
	handlerServer.client = &http.Client{Timeout: cfg.API.RequestTimeout}

	return handlerServer
}

func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.server.Shutdown(shutdownCtx)
	}()

	s.logger.Info("api listening", "addr", s.cfg.API.ListenAddr)
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /bridge/status", s.bridgeStatus)
	mux.HandleFunc("POST /devices/register", s.registerDevice)
	mux.HandleFunc("POST /devices/{id}/token", s.issueToken)
	mux.HandleFunc("GET /devices", s.listDevices)
	mux.HandleFunc("GET /devices/{id}/telemetry", s.deviceTelemetry)
	mux.HandleFunc("POST /devices/{id}/command", s.sendCommand)
	mux.HandleFunc("GET /messages", s.listMessages)
	mux.HandleFunc("GET /metrics/summary", s.metricsSummary)
	mux.HandleFunc("GET /metrics/raw", s.metricsRaw)
	mux.HandleFunc("GET /ws/telemetry", s.websocketTelemetry)

	return chain(
		mux,
		s.requestID,
		s.logging,
		s.recoverer,
		s.securityHeaders,
		s.cors,
		s.timeout,
	)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "crabmq-api",
	})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	checkCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	checks := map[string]string{
		"database": "ok",
		"bridge":   "ok",
	}
	statusCode := http.StatusOK

	if err := s.store.Ping(checkCtx); err != nil {
		checks["database"] = err.Error()
		statusCode = http.StatusServiceUnavailable
	}
	if !s.bridge.Ready() {
		checks["bridge"] = "bridge is not connected to broker"
		statusCode = http.StatusServiceUnavailable
	}

	status := "ready"
	if statusCode != http.StatusOK {
		status = "degraded"
	}

	writeJSON(w, statusCode, map[string]any{
		"status": status,
		"checks": checks,
		"bridge": s.bridge.Status(),
	})
}

func (s *Server) bridgeStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.bridge.Status())
}

func (s *Server) registerDevice(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ID       string         `json:"id"`
		Name     string         `json:"name"`
		Metadata map[string]any `json:"metadata"`
	}
	if err := decodeJSON(w, r, &request, s.cfg.API.MaxBodyBytes); err != nil {
		return
	}

	device, err := s.devices.Register(r.Context(), request.ID, request.Name, request.Metadata)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, device)
}

func (s *Server) issueToken(w http.ResponseWriter, r *http.Request) {
	token, err := s.devices.IssueToken(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"token":     token,
		"expiresIn": s.cfg.API.DefaultTokenTTL.String(),
	})
}

func (s *Server) listDevices(w http.ResponseWriter, r *http.Request) {
	devices, err := s.devices.List(r.Context())
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, devices)
}

func (s *Server) deviceTelemetry(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r.URL.Query().Get("limit"), 50)
	messages, err := s.devices.Telemetry(r.Context(), r.PathValue("id"), limit)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, messages)
}

func (s *Server) sendCommand(w http.ResponseWriter, r *http.Request) {
	if err := services.ValidateDeviceID(r.PathValue("id")); err != nil {
		writeServiceError(w, r, err)
		return
	}

	var payload json.RawMessage
	if err := decodeJSON(w, r, &payload, s.cfg.API.MaxBodyBytes); err != nil {
		return
	}
	if len(bytes.TrimSpace(payload)) == 0 {
		payload = json.RawMessage(`{}`)
	}

	packet, err := s.bridge.PublishCommand(r.Context(), r.PathValue("id"), payload)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	writeJSON(w, http.StatusAccepted, packet)
}

func (s *Server) listMessages(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r.URL.Query().Get("limit"), 100)
	messages, err := s.devices.Messages(r.Context(), limit)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, messages)
}

func (s *Server) metricsSummary(w http.ResponseWriter, r *http.Request) {
	summary, err := s.store.GetMetricsSummary(r.Context())
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, summary)
}

func (s *Server) metricsRaw(w http.ResponseWriter, r *http.Request) {
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, s.cfg.API.BrokerMetricsURL, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	response, err := s.client.Do(request)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	if response.StatusCode >= http.StatusBadRequest {
		writeError(w, http.StatusBadGateway, errors.New(strings.TrimSpace(string(body))))
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) websocketTelemetry(w http.ResponseWriter, r *http.Request) {
	s.bridge.Hub().ServeWS(w, r)
}

func parseLimit(value string, fallback int) int {
	if value == "" {
		return fallback
	}

	limit, err := strconv.Atoi(value)
	if err != nil || limit <= 0 {
		return fallback
	}
	if limit > 500 {
		return 500
	}

	return limit
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	writeError(w, statusForError(err), withRequestIDError(r, err))
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any, maxBodyBytes int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, errors.New("request body must contain a single JSON object"))
		return err
	}

	return nil
}

func statusForError(err error) int {
	switch {
	case err == nil:
		return http.StatusInternalServerError
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	case strings.Contains(err.Error(), "not found"):
		return http.StatusNotFound
	case strings.Contains(err.Error(), "bridge is not connected"):
		return http.StatusServiceUnavailable
	case strings.Contains(err.Error(), "must") || strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "missing") || strings.Contains(err.Error(), "invalid"):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}
