package config

import (
	"testing"
)

func TestLoad_PanicsOnMissingEnv(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected Load() to panic when required env vars are missing, but it did not")
		}
	}()

	t.Setenv("PORT", "")
	t.Setenv("DATABASE_URL", "")
	_ = Load()
}

func TestLoad_SuccessWithAllEnv(t *testing.T) {
	t.Setenv("PORT", "50053")
	t.Setenv("HTTP_PORT", "8080")
	t.Setenv("DATABASE_URL", "postgres://pregao_user:pregao_pass@bolsa:5432/bolsa?sslmode=disable&search_path=pregao")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("OPENBAO_ADDR", "http://openbao:8200")
	t.Setenv("OPENBAO_TOKEN", "root")
	t.Setenv("OPENBAO_TRANSIT_KEY", "broker-keys")
	t.Setenv("BINANCE_BASE_URL", "https://api.binance.com")
	t.Setenv("LIVRO_GRPC_ADDR", "livro:50051")
	t.Setenv("ATIVOS_GRPC_ADDR", "ativos:50052")
	t.Setenv("DEFAULT_BROKER", "BINANCE")

	cfg := Load()
	if cfg.GRPCPort != "50053" {
		t.Errorf("expected GRPCPort 50053, got %s", cfg.GRPCPort)
	}
	if cfg.LivroGRPCAddr != "livro:50051" {
		t.Errorf("expected LivroGRPCAddr livro:50051, got %s", cfg.LivroGRPCAddr)
	}
}
