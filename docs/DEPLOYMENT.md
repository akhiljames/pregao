# Deploying Pregão to Kubernetes

How to build the Pregão image, push it to GitHub Container Registry (GHCR), and
run it in the `prod` namespace of a Kubernetes cluster.

Manifests live in [`deploy/k8s/`](../deploy/k8s/):

| File | Purpose |
| --- | --- |
| `namespace.yaml` | The `prod` namespace (applied once, by hand) |
| `configmap.yaml` | `pregao-config` — non-sensitive environment variables |
| `secrets.env.example` | Template for `pregao-secrets` — sensitive environment variables |
| `deployment.yaml` | One replica of the server |
| `service.yaml` | ClusterIP service `pregao` — gRPC on `50053`, HTTP on `8080` |
| `kustomization.yaml` | Ties the ConfigMap, Deployment and Service together and pins the image tag |

## Prerequisites

- Docker with `buildx`, and `kubectl` 1.21 or newer.
- A GitHub personal access token (classic) with `write:packages` to push, and
  one with `read:packages` for the cluster to pull.
- Reachable from the `prod` namespace:
  - PostgreSQL with the `pregao` schema already created and a user that can
    create tables in it. Pregão applies its own schema at startup.
  - Redis.
  - OpenBao (or Vault) with the transit engine enabled and the `broker-keys`
    key created.
  - Livro and Ativos gRPC services.
- `kubectl` pointed at the target cluster. Check before every step that
  changes the cluster:

  ```bash
  kubectl config current-context
  ```

## 1. Build and push the image

Run from the repository root, on a clean checkout of the commit you want to
release. The image is tagged with that commit's short SHA.

```bash
export IMAGE=ghcr.io/akhiljames/pregao
export TAG=$(git rev-parse --short HEAD)

# Log in to GHCR (GHCR_TOKEN = token with write:packages)
echo "$GHCR_TOKEN" | docker login ghcr.io -u akhiljames --password-stdin

# Build for the cluster's CPU architecture and push
docker buildx build \
  --platform linux/amd64 \
  --label "org.opencontainers.image.source=https://github.com/akhiljames/pregao" \
  --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" \
  -t "$IMAGE:$TAG" \
  --push .
```

`--platform` must match the cluster nodes, not your laptop. An image built on
an Apple Silicon Mac without it fails in the cluster with `exec format error`.
Use `linux/arm64` for ARM nodes (for example AWS Graviton). To check:

```bash
kubectl get nodes -o jsonpath='{.items[*].status.nodeInfo.architecture}'
```

Confirm the push:

```bash
docker buildx imagetools inspect "$IMAGE:$TAG"
```

The package is private the first time it is pushed. Either leave it private
and create the pull secret in step 2, or make it public under
GitHub → Packages → pregao → Package settings.

## 2. One-time cluster setup

### Namespace

```bash
kubectl apply -f deploy/k8s/namespace.yaml
```

### Image pull secret

Lets the cluster pull the private image. `GHCR_PULL_TOKEN` is a token with
`read:packages` only. Skip this if `ghcr-pull` already exists in `prod` from
another service.

```bash
kubectl -n prod create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io \
  --docker-username=akhiljames \
  --docker-password="$GHCR_PULL_TOKEN"
```

### Application secrets

```bash
cp deploy/k8s/secrets.env.example deploy/k8s/.env.prod
# edit deploy/k8s/.env.prod and fill in the real values

kubectl -n prod create secret generic pregao-secrets \
  --from-env-file=deploy/k8s/.env.prod \
  --dry-run=client -o yaml | kubectl apply -f -
```

`.env.prod` is gitignored. Do not commit it. The same command updates the
secret later; restart the pods afterwards (see step 5).

`OPENBAO_TOKEN` only needs to decrypt with the transit key. A policy for it:

```hcl
path "transit/decrypt/broker-keys" {
  capabilities = ["update"]
}
```

Pregão does not renew the token. When it expires, order routing fails until
the secret is updated and the pod restarted.

## 3. Review the configuration

Edit [`deploy/k8s/configmap.yaml`](../deploy/k8s/configmap.yaml) so the
hostnames match the services in your cluster. A bare name such as `livro`
resolves only inside the `prod` namespace; for a service elsewhere use
`<service>.<namespace>.svc.cluster.local`.

Every variable except `REDIS_PASSWORD` is required. The server exits at
startup if one is missing or empty.

| Variable | Source | Description |
| --- | --- | --- |
| `PORT` | ConfigMap | gRPC listen port. Keep at `50053` unless you also change the Deployment and Service. |
| `HTTP_PORT` | ConfigMap | Webhook and health-check listen port. Keep at `8080` unless you also change the Deployment and Service. |
| `REDIS_ADDR` | ConfigMap | Redis `host:port` for the quotes cache. |
| `OPENBAO_ADDR` | ConfigMap | OpenBao base URL, including the scheme. |
| `OPENBAO_TRANSIT_KEY` | ConfigMap | Transit key that encrypts broker credentials. |
| `BINANCE_BASE_URL` | ConfigMap | Binance REST API base URL. |
| `LIVRO_GRPC_ADDR` | ConfigMap | Livro ledger `host:port`. |
| `ATIVOS_GRPC_ADDR` | ConfigMap | Ativos PMS `host:port`. |
| `DEFAULT_BROKER` | ConfigMap | Broker used when a request names none. `BINANCE`. |
| `DATABASE_URL` | Secret | PostgreSQL connection string, including `search_path=pregao`. |
| `OPENBAO_TOKEN` | Secret | Token sent to OpenBao as `X-Vault-Token`. |
| `REDIS_PASSWORD` | Secret | Optional. Leave out if Redis has no password. |

## 4. Deploy

Pin the tag you pushed, then apply:

```bash
# macOS sed; on Linux drop the '' after -i
sed -i '' "s/newTag: .*/newTag: \"$TAG\"/" deploy/k8s/kustomization.yaml

kubectl diff -k deploy/k8s     # optional: preview the changes
kubectl apply -k deploy/k8s
kubectl -n prod rollout status deployment/pregao
```

Commit the `kustomization.yaml` change so the repository records what is
running in production.

### Verify

```bash
kubectl -n prod get pods -l app.kubernetes.io/name=pregao
kubectl -n prod logs deployment/pregao --tail=50
```

A healthy pod logs `[DB] Schema migrations applied successfully.` followed by
`[PREGÃO] gRPC server listening on :50053`.

A `Running` and `Ready` pod is not enough. If the log contains
`[PREGÃO_WARN] Database connection failed ... Running in degraded mode`, the
pod started without a database and order routing does not work. It does not
reconnect on its own. Fix `DATABASE_URL` or the database, then restart the pod
(see step 5).

To call the service from your machine:

```bash
kubectl -n prod port-forward svc/pregao 50053:50053 8080:8080
grpcurl -plaintext localhost:50053 list
curl localhost:8080/healthz
```

Inside the cluster, clients connect to `pregao.prod.svc.cluster.local:50053`
(gRPC) and `http://pregao.prod.svc.cluster.local:8080` (webhooks).

## 5. Day-to-day operations

**Release a new version** — repeat step 1, then step 4. The Deployment stops
the old pod before it starts the new one, so expect a few seconds during which
calls fail.

**Change configuration** — pods read environment variables only at startup, so
restart them after changing the ConfigMap or the Secret:

```bash
kubectl apply -k deploy/k8s            # after editing configmap.yaml
kubectl -n prod rollout restart deployment/pregao
```

**Roll back** — return to the previous revision immediately, then set `newTag`
back to the matching tag so the next apply does not undo the rollback:

```bash
kubectl -n prod rollout undo deployment/pregao
```

Rolling back the image does not roll back the database schema.

**Remove** — deletes the ConfigMap, Deployment and Service. The namespace, the
secrets and the database are left alone:

```bash
kubectl delete -k deploy/k8s
```

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `ImagePullBackOff` | `newTag` still `CHANGE_ME`, tag not pushed, or `ghcr-pull` missing or its token expired. |
| `exec format error` in logs | Image built for the wrong architecture. Rebuild with the right `--platform`. |
| `CreateContainerConfigError` | `pregao-secrets` or `pregao-config` does not exist in `prod`. |
| `panic: missing required environment variable: X` | `X` is absent or empty in the ConfigMap or Secret. |
| `Database connection failed ... Running in degraded mode` | Wrong `DATABASE_URL`, database unreachable from the cluster, wrong `sslmode`, or an unencoded special character in the password. |
| `Database migrations failed` | Database user cannot create tables in the `pregao` schema. |
| `openbao transit decryption failed` on trades | `OPENBAO_TOKEN` expired or lacks the policy above, wrong `OPENBAO_TRANSIT_KEY`, or the transit key was lost (see below). |

## Operational notes

- **Replicas.** `ExecuteTrade` reserves a `PENDING` row in `broker_orders`
  before it calls the broker. Unique constraints on `trade_intent_id` and
  `(tenant_id, idempotency_key)` reject duplicates from any pod, so duplicate
  protection no longer depends on a single replica. The Deployment still has
  `replicas: 1` and the `Recreate` strategy; more than one replica has not been
  tested.
- **Stuck `PENDING` orders.** A `broker_orders` row that stays `PENDING` with
  no `provider_order_id` means a pod died, or lost the database, between
  reserving the trade and recording the broker's answer. Retries of that trade
  intent wait 30 seconds and then return `ABORTED` until the row is reconciled
  by hand. Look for a
  broker order whose client order ID is the trade intent ID. If one exists,
  fill in the row's `provider_order_id` and `status`; if not, delete the row.
- **Health checks.** Startup and readiness are TCP checks on the gRPC port;
  liveness is `GET /healthz`. None of them checks PostgreSQL, Redis, OpenBao,
  Livro or Ativos, so read the logs after every deploy.
- **Logs contain the database password.** The server logs `DATABASE_URL` in
  full at startup. Treat pod logs as sensitive.
- **Webhooks are unauthenticated.** Anything that can reach port `8080` can
  post fills to `/webhooks/binance/order`. The Service is cluster-internal and
  no Ingress is included. Do not expose the port without authentication in
  front of it.
- **OpenBao storage.** Broker credentials in PostgreSQL can only be decrypted
  with the transit key. An OpenBao running in dev mode keeps that key in
  memory and loses it on restart, which makes every stored credential
  unreadable. Use persistent storage for it in production.
- **Schema.** The schema is applied on every pod start and is idempotent.
- **Container user.** The pod runs as UID `65532` with all Linux capabilities
  dropped and a read-only root filesystem.
