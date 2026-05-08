package config

import (
	"testing"
	"time"
)

func TestValidateAcceptsReasonableDevelopmentConfig(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Env: "development",
		Broker: BrokerConfig{
			MaxClients:      1,
			ReadTimeout:     time.Second,
			WriteTimeout:    time.Second,
			OfflineQueueTTL: time.Minute,
			RateLimitPerMin: 1,
		},
		API: APIConfig{
			AllowedOrigins:  []string{"http://localhost:3000"},
			DefaultTokenTTL: time.Hour,
			CommandQoS:      1,
			ReadTimeout:     time.Second,
			WriteTimeout:    time.Second,
			IdleTimeout:     time.Second,
			RequestTimeout:  time.Second,
			MaxBodyBytes:    1024,
		},
		Auth: AuthConfig{
			JWTSecret:      "dev-secret",
			AdminTokenTTL:  time.Hour,
			DeviceTokenTTL: time.Hour,
		},
		Simulator: SimulatorConfig{
			DeviceCount:     1,
			PublishInterval: time.Second,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected config to validate, got %v", err)
	}
}

func TestValidateRejectsUnsafeProductionSecret(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Env: "production",
		Broker: BrokerConfig{
			MaxClients:      1,
			ReadTimeout:     time.Second,
			WriteTimeout:    time.Second,
			OfflineQueueTTL: time.Minute,
			RateLimitPerMin: 1,
		},
		API: APIConfig{
			AllowedOrigins:  []string{"https://dashboard.example.com"},
			DefaultTokenTTL: time.Hour,
			CommandQoS:      1,
			ReadTimeout:     time.Second,
			WriteTimeout:    time.Second,
			IdleTimeout:     time.Second,
			RequestTimeout:  time.Second,
			MaxBodyBytes:    1024,
		},
		Auth: AuthConfig{
			JWTSecret:      "change-me",
			AdminTokenTTL:  time.Hour,
			DeviceTokenTTL: time.Hour,
		},
		Simulator: SimulatorConfig{
			DeviceCount:     1,
			PublishInterval: time.Second,
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected config validation to fail for production default secret")
	}
}
