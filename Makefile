.PHONY: all build test test-precision test-e2e run clean docker-build docker-up docker-down

all: build test

build:
	go build -o bin/pregao-server cmd/server/main.go

test:
	go test -v -count=1 ./...

test-precision:
	go test -v -run TestNoFloat32OrFloat64Enforcement ./test/...

test-e2e:
	go run scripts/test_client.go

run: build
	./bin/pregao-server

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	rm -rf bin/
