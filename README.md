# Pregão (Execution & Market Data Engine)

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-blue.svg)](https://golang.org)
[![gRPC](https://img.shields.io/badge/gRPC-v1.64%2B-green.svg)](https://grpc.io)
[![Precision](https://img.shields.io/badge/Precision-Zero--Float%20(shopspring%2Fdecimal)-critical.svg)](#financial-math--zero-float-policy)

Pregão is the consolidated Order Management System (OMS) and Market Data microservice for algorithmic robo-advisors. It sits in the **Execution & Integration Layer**, handling all external broker networking, market data caching, zero-trust envelope encryption, and idempotent trade routing.

---

## Documentation

- 📖 **Comprehensive Integration Guide & API Reference**: [docs/INTEGRATION_GUIDE.md](docs/INTEGRATION_GUIDE.md)

---

## Architectural Highlights

- **Strict Zero-Float Policy**: Floating-point types (`float32`, `float64`) are banned across all production packages. All arithmetic uses `shopspring/decimal`. Verified continuously by AST inspection (`TestNoFloat32OrFloat64Enforcement`).
- **Dual-TTL Resilient Cache**: Redis caches market data quotes with a **5s fresh TTL** and **24h stale fallback TTL** to gracefully absorb broker rate limits (HTTP 429).
- **Zero-Trust Memory Clearing**: Broker API keys/secrets are decrypted just-in-time from OpenBao Transit and wiped byte-by-byte via `defer creds.Zero()` immediately after use.
- **Idempotent Order Routing**: In-memory execution locks (`sync.Map`) and PostgreSQL unique constraints on `(tenant_id, trade_intent_id)` prevent race conditions and duplicate order placements under high concurrency.
- **Two-Phase Async Settlement**: Webhook fills are ingested over HTTP, settling 2PC holds with **Livro** (double-entry ledger) and syncing portfolio holdings with **Ativos** (PMS).

---

## Quickstart

### 1. Build and Run Local Server
```bash
make build
./bin/pregao-server
```
- gRPC Server: `:50053`
- Webhook Server: `:8080`

### 2. Run via Docker Compose
```bash
make docker-up
```

### 3. Run Automated Tests
```bash
# Unit tests
make test

# AST zero-float precision test
make test-precision

# End-to-end 6-suite verification client
make test-e2e
```
