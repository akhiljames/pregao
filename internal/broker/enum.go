package broker

import (
	ativosv1 "github.com/akhiljames/proto/gen/go/ativos/v1"
)

// ProviderName returns the canonical uppercase string identifier used in
// PostgreSQL rows and Redis cache keys for the given Broker enum value.
// This is the single source of truth for broker string → DB mapping.
func ProviderName(b ativosv1.Broker) string {
	switch b {
	case ativosv1.Broker_BROKER_BINANCE:
		return "BINANCE"
	default:
		return "BINANCE" // safe default; callers should validate before reaching here
	}
}

// ParseBroker converts a provider string (as received on the proto wire, e.g.
// "BINANCE") to the canonical Broker enum. Returns BROKER_UNSPECIFIED if
// the value is unrecognised so callers can handle it explicitly.
func ParseBroker(provider string) ativosv1.Broker {
	switch provider {
	case "BINANCE":
		return ativosv1.Broker_BROKER_BINANCE
	default:
		return ativosv1.Broker_BROKER_UNSPECIFIED
	}
}

// DefaultBroker is the canonical default Broker used when no explicit provider
// is supplied on a request.
const DefaultBroker = ativosv1.Broker_BROKER_BINANCE
