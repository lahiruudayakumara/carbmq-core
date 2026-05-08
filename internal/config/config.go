package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env           string
	Broker        BrokerConfig
	API           APIConfig
	Auth          AuthConfig
	Database      DatabaseConfig
	Observability ObservabilityConfig
	Simulator     SimulatorConfig
}

type BrokerConfig struct {
	ListenAddr      string
	MetricsAddr     string
	ProtocolName    string
	MaxClients      int
	ConnectTimeout  time.Duration
	WriteTimeout    time.Duration
	ReadTimeout     time.Duration
	OfflineQueueTTL time.Duration
	RateLimitPerMin int
	CertFile        string
	KeyFile         string
}

type APIConfig struct {
	ListenAddr       string
	AllowedOrigins   []string
	BridgeClientID   string
	BrokerAddr       string
	BrokerMetricsURL string
	DefaultTokenTTL  time.Duration
	CommandQoS       int
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	IdleTimeout      time.Duration
	RequestTimeout   time.Duration
	MaxBodyBytes     int64
}

type AuthConfig struct {
	JWTSecret      string
	AdminTokenTTL  time.Duration
	DeviceTokenTTL time.Duration
}

type DatabaseConfig struct {
	URL string
}

type ObservabilityConfig struct {
	LogLevel string
}

type SimulatorConfig struct {
	PublishInterval time.Duration
	DeviceCount     int
}

func Load() Config {
	return Config{
		Env: env("CRABMQ_ENV", "development"),
		Broker: BrokerConfig{
			ListenAddr:      env("CRABMQ_BROKER_ADDR", "0.0.0.0:1884"),
			MetricsAddr:     env("CRABMQ_METRICS_ADDR", "0.0.0.0:9100"),
			ProtocolName:    env("CRABMQ_QUIC_ALPN", "crabmq-qtt"),
			MaxClients:      envInt("CRABMQ_MAX_CLIENTS", 5000),
			ConnectTimeout:  envDuration("CRABMQ_CONNECT_TIMEOUT", 10*time.Second),
			WriteTimeout:    envDuration("CRABMQ_WRITE_TIMEOUT", 5*time.Second),
			ReadTimeout:     envDuration("CRABMQ_READ_TIMEOUT", 45*time.Second),
			OfflineQueueTTL: envDuration("CRABMQ_OFFLINE_QUEUE_TTL", 24*time.Hour),
			RateLimitPerMin: envInt("CRABMQ_RATE_LIMIT_PER_MIN", 120),
			CertFile:        os.Getenv("CRABMQ_TLS_CERT_FILE"),
			KeyFile:         os.Getenv("CRABMQ_TLS_KEY_FILE"),
		},
		API: APIConfig{
			ListenAddr:       env("CRABMQ_API_ADDR", "0.0.0.0:8080"),
			AllowedOrigins:   envCSV("CRABMQ_ALLOWED_ORIGINS", []string{"http://localhost:3000", "http://localhost:5173"}),
			BridgeClientID:   env("CRABMQ_BRIDGE_CLIENT_ID", "api-bridge"),
			BrokerAddr:       env("CRABMQ_API_BROKER_ADDR", "127.0.0.1:1884"),
			BrokerMetricsURL: env("CRABMQ_BROKER_METRICS_URL", "http://127.0.0.1:9100/metrics"),
			DefaultTokenTTL:  envDuration("CRABMQ_DEFAULT_TOKEN_TTL", 12*time.Hour),
			CommandQoS:       envInt("CRABMQ_COMMAND_QOS", 1),
			ReadTimeout:      envDuration("CRABMQ_API_READ_TIMEOUT", 10*time.Second),
			WriteTimeout:     envDuration("CRABMQ_API_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:      envDuration("CRABMQ_API_IDLE_TIMEOUT", 60*time.Second),
			RequestTimeout:   envDuration("CRABMQ_API_REQUEST_TIMEOUT", 10*time.Second),
			MaxBodyBytes:     envInt64("CRABMQ_API_MAX_BODY_BYTES", 1<<20),
		},
		Auth: AuthConfig{
			JWTSecret:      env("CRABMQ_JWT_SECRET", "change-me"),
			AdminTokenTTL:  envDuration("CRABMQ_ADMIN_TOKEN_TTL", 24*time.Hour),
			DeviceTokenTTL: envDuration("CRABMQ_DEVICE_TOKEN_TTL", 12*time.Hour),
		},
		Database: DatabaseConfig{
			URL: env("CRABMQ_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/crabmq?sslmode=disable"),
		},
		Observability: ObservabilityConfig{
			LogLevel: env("CRABMQ_LOG_LEVEL", "info"),
		},
		Simulator: SimulatorConfig{
			PublishInterval: envDuration("CRABMQ_SIM_INTERVAL", 5*time.Second),
			DeviceCount:     envInt("CRABMQ_SIM_DEVICE_COUNT", 50),
		},
	}
}

func (c Config) Validate() error {
	issues := make([]string, 0)

	if c.Auth.JWTSecret == "" {
		issues = append(issues, "CRABMQ_JWT_SECRET must not be empty")
	}
	if c.Env == "production" && c.Auth.JWTSecret == "change-me" {
		issues = append(issues, "CRABMQ_JWT_SECRET must be changed for production")
	}
	if c.Auth.AdminTokenTTL <= 0 {
		issues = append(issues, "CRABMQ_ADMIN_TOKEN_TTL must be greater than zero")
	}
	if c.Auth.DeviceTokenTTL <= 0 {
		issues = append(issues, "CRABMQ_DEVICE_TOKEN_TTL must be greater than zero")
	}
	if c.API.DefaultTokenTTL <= 0 {
		issues = append(issues, "CRABMQ_DEFAULT_TOKEN_TTL must be greater than zero")
	}
	if c.API.CommandQoS < 0 || c.API.CommandQoS > 1 {
		issues = append(issues, "CRABMQ_COMMAND_QOS must be 0 or 1")
	}
	if len(c.API.AllowedOrigins) == 0 {
		issues = append(issues, "CRABMQ_ALLOWED_ORIGINS must include at least one origin")
	}
	if c.API.RequestTimeout <= 0 {
		issues = append(issues, "CRABMQ_API_REQUEST_TIMEOUT must be greater than zero")
	}
	if c.API.ReadTimeout <= 0 || c.API.WriteTimeout <= 0 || c.API.IdleTimeout <= 0 {
		issues = append(issues, "API timeouts must be greater than zero")
	}
	if c.API.MaxBodyBytes <= 0 {
		issues = append(issues, "CRABMQ_API_MAX_BODY_BYTES must be greater than zero")
	}
	if c.Broker.MaxClients <= 0 {
		issues = append(issues, "CRABMQ_MAX_CLIENTS must be greater than zero")
	}
	if c.Broker.RateLimitPerMin <= 0 {
		issues = append(issues, "CRABMQ_RATE_LIMIT_PER_MIN must be greater than zero")
	}
	if c.Broker.OfflineQueueTTL <= 0 {
		issues = append(issues, "CRABMQ_OFFLINE_QUEUE_TTL must be greater than zero")
	}
	if c.Broker.ReadTimeout <= 0 || c.Broker.WriteTimeout <= 0 {
		issues = append(issues, "broker timeouts must be greater than zero")
	}
	if c.Simulator.DeviceCount <= 0 {
		issues = append(issues, "CRABMQ_SIM_DEVICE_COUNT must be greater than zero")
	}
	if c.Simulator.PublishInterval <= 0 {
		issues = append(issues, "CRABMQ_SIM_INTERVAL must be greater than zero")
	}

	if len(issues) == 0 {
		return nil
	}

	return errors.New(strings.Join(issues, "; "))
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}

	return value
}

func envInt64(key string, fallback int64) int64 {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}

	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}

	return value
}

func envCSV(key string, fallback []string) []string {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			values = append(values, trimmed)
		}
	}

	if len(values) == 0 {
		return fallback
	}

	return values
}
