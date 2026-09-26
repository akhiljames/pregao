package config

import (
	"fmt"
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

func requireEnv(key string) string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		panic(fmt.Sprintf("missing required environment variable: %s", key))
	}
	return val
}

// Load loads the configuration from environment variables, panicking if any required variable is empty.
func Load() *Config {
	return &Config{
		GRPCPort:           requireEnv("PORT"),
		HTTPPort:           requireEnv("HTTP_PORT"),
		DatabaseURL:        requireEnv("DATABASE_URL"),
		RedisAddr:          requireEnv("REDIS_ADDR"),
		RedisPassword:      strings.TrimSpace(os.Getenv("REDIS_PASSWORD")),
		RedisDB:            0,
		OpenBaoAddr:        requireEnv("OPENBAO_ADDR"),
		OpenBaoToken:       requireEnv("OPENBAO_TOKEN"),
		OpenBaoTransitKey:  requireEnv("OPENBAO_TRANSIT_KEY"),
		BinanceBaseURL:     strings.TrimRight(requireEnv("BINANCE_BASE_URL"), "/"),
		LivroGRPCAddr:      requireEnv("LIVRO_GRPC_ADDR"),
		AtivosGRPCAddr:     requireEnv("ATIVOS_GRPC_ADDR"),
		DefaultBroker:      broker.ParseBroker(strings.ToUpper(requireEnv("DEFAULT_BROKER"))),
		CacheFreshTTL:      5 * time.Second,
		CacheStaleTTL:      24 * time.Hour,
		BinanceRecvWindow:  5000,
		BinanceHTTPTimeout: 10 * time.Second,
	}
}
