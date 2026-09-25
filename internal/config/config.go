package config

import (
	"os"
	"strings"
	"time"

	ativosv1 "github.com/akhiljames/proto/gen/go/ativos/v1"
	"github.com/akhiljames/pregao/internal/broker"
)

// Config holds the service configuration loaded from the environment.
type Config struct {
	GRPCPort           string
	HTTPPort           string
	DatabaseURL        string
	RedisAddr          string
	RedisPassword      string
	RedisDB            int
	OpenBaoAddr        string
	OpenBaoToken       string
	OpenBaoTransitKey  string
	BinanceBaseURL     string
	LivroGRPCAddr      string
	AtivosGRPCAddr     string
	DefaultBroker      ativosv1.Broker
	CacheFreshTTL      time.Duration
	CacheStaleTTL      time.Duration
	BinanceRecvWindow  int64
	BinanceHTTPTimeout time.Duration
}

// Load loads the configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		GRPCPort:           getEnv("PORT", "50053"),
		HTTPPort:           getEnv("HTTP_PORT", "8080"),
		DatabaseURL:        getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/pregao?sslmode=disable"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
		RedisDB:            0,
		OpenBaoAddr:        getEnv("OPENBAO_ADDR", "http://localhost:8200"),
		OpenBaoToken:       getEnv("OPENBAO_TOKEN", ""),
		OpenBaoTransitKey:  getEnv("OPENBAO_TRANSIT_KEY", "broker-keys"),
		BinanceBaseURL:     strings.TrimRight(getEnv("BINANCE_BASE_URL", "https://api.binance.com"), "/"),
		LivroGRPCAddr:      getEnv("LIVRO_GRPC_ADDR", "localhost:50051"),
		AtivosGRPCAddr:     getEnv("ATIVOS_GRPC_ADDR", "localhost:50052"),
		DefaultBroker:      broker.ParseBroker(strings.ToUpper(getEnv("DEFAULT_BROKER", "BINANCE"))),
		CacheFreshTTL:      5 * time.Second,
		CacheStaleTTL:      24 * time.Hour,
		BinanceRecvWindow:  5000,
		BinanceHTTPTimeout: 10 * time.Second,
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return defaultVal
}
