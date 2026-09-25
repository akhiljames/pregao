package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pregaov1 "github.com/akhiljames/proto/gen/go/pregao/v1"
	"github.com/akhiljames/pregao/internal/broker/binance"
	"github.com/akhiljames/pregao/internal/cache"
	"github.com/akhiljames/pregao/internal/client/ativos"
	"github.com/akhiljames/pregao/internal/client/livro"
	"github.com/akhiljames/pregao/internal/config"
	"github.com/akhiljames/pregao/internal/db"
	"github.com/akhiljames/pregao/internal/service"
	"github.com/akhiljames/pregao/internal/vault"
	"github.com/akhiljames/pregao/internal/webhook"
	"github.com/akhiljames/pregao/internal/worker"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg := config.Load()
	log.Printf("[PREGÃO] Initializing microservice (gRPC :%s, HTTP :%s)...", cfg.GRPCPort, cfg.HTTPPort)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. PostgreSQL Database & Migrations
	log.Printf("[PREGÃO] Connecting to PostgreSQL at %s...", cfg.DatabaseURL)
	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Printf("[PREGÃO_WARN] Database connection failed: %v. Running in degraded mode.", err)
	} else {
		defer pool.Close()
		if err := db.RunMigrations(ctx, pool); err != nil {
			log.Fatalf("[PREGÃO_FATAL] Database migrations failed: %v", err)
		}
	}

	var (
		credRepo db.CredentialsRepository
		ordRepo  db.OrdersRepository
	)
	if pool != nil {
		credRepo = db.NewCredentialsRepository(pool)
		ordRepo = db.NewOrdersRepository(pool)
	}

	// 2. Redis Quotes Cache (5s fresh + 24h stale fallback)
	log.Printf("[PREGÃO] Connecting to Redis at %s...", cfg.RedisAddr)
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer rdb.Close()

	quotesCache := cache.NewRedisQuotesCache(rdb, cfg.CacheFreshTTL, cfg.CacheStaleTTL)

	// 3. OpenBao Transit Client
	vaultClient := vault.NewTransitClient(
		cfg.OpenBaoAddr,
		cfg.OpenBaoToken,
		cfg.OpenBaoTransitKey,
		5*time.Second,
	)

	// 4. Broker Client (Binance)
	brokerClient := binance.NewClient(
		cfg.BinanceBaseURL,
		cfg.BinanceRecvWindow,
		cfg.BinanceHTTPTimeout,
	)

	// 5. Downstream gRPC Clients (Livro & Ativos)
	var livroClient livro.Client
	if lClient, err := livro.NewClient(cfg.LivroGRPCAddr); err == nil {
		livroClient = lClient
		defer livroClient.Close()
	} else {
		log.Printf("[PREGÃO_WARN] Failed to connect to Livro at %s: %v", cfg.LivroGRPCAddr, err)
	}

	var ativosClient ativos.Client
	if aClient, err := ativos.NewClient(cfg.AtivosGRPCAddr); err == nil {
		ativosClient = aClient
		defer ativosClient.Close()
	} else {
		log.Printf("[PREGÃO_WARN] Failed to connect to Ativos at %s: %v", cfg.AtivosGRPCAddr, err)
	}

	// 6. Fill Processor & Webhook HTTP Server
	fillProcessor := worker.NewFillProcessor(ordRepo, livroClient, ativosClient)
	webhookServer := webhook.NewServer(fillProcessor)

	httpMux := http.NewServeMux()
	webhookServer.RegisterRoutes(httpMux)
	httpSrv := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.HTTPPort),
		Handler: httpMux,
	}

	go func() {
		log.Printf("[PREGÃO] HTTP server listening on :%s", cfg.HTTPPort)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[PREGÃO_ERROR] HTTP server failed: %v", err)
		}
	}()

	// 7. Core gRPC Server
	pregaoServer := service.NewPregaoServer(service.ServerParams{
		DefaultProvider: cfg.DefaultProvider,
		Cache:           quotesCache,
		BrokerClient:    brokerClient,
		VaultClient:     vaultClient,
		CredentialsRepo: credRepo,
		OrdersRepo:      ordRepo,
		LivroClient:     livroClient,
		FillProcessor:   fillProcessor,
	})

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("[PREGÃO_FATAL] Failed to listen on gRPC port %s: %v", cfg.GRPCPort, err)
	}

	grpcServer := grpc.NewServer()
	pregaov1.RegisterPregaoServiceServer(grpcServer, pregaoServer)
	reflection.Register(grpcServer)

	go func() {
		log.Printf("[PREGÃO] gRPC server listening on :%s", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("[PREGÃO_ERROR] gRPC server terminated: %v", err)
		}
	}()

	// 8. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[PREGÃO] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	grpcServer.GracefulStop()
	_ = httpSrv.Shutdown(shutdownCtx)

	log.Println("[PREGÃO] Service stopped.")
}
