# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Download dependencies first for optimal layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/pregao-server cmd/server/main.go

# Runtime stage
FROM alpine:3.20

WORKDIR /app
RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app/bin/pregao-server /app/bin/pregao-server
COPY --from=builder /app/migrations /app/migrations

EXPOSE 50053
EXPOSE 8080

ENTRYPOINT ["/app/bin/pregao-server"]
