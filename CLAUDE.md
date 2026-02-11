# data-sync

A Go data synchronization service that provides authenticated, versioned record storage with real-time change notifications over gRPC. Built by Breez.

## Purpose

Synchronizes user data across client applications. Each user's records are isolated, versioned, and protected by ECDSA signature verification. Clients can create/update records, poll for changes, or subscribe to a real-time notification stream.

## Tech Stack

- **Go 1.22+** (toolchain 1.23.1)
- **gRPC** with Protocol Buffers for the API layer
- **gRPC-Web proxy** on a second HTTP port for browser clients
- **SQLite** or **PostgreSQL** as pluggable storage backends
- **ECDSA (secp256k1)** signature-based authentication — user identity is derived from the public key

## API (gRPC)

Defined in `proto/sync.proto`:

| RPC | Description |
|-----|-------------|
| `SetRecord` | Create or update a record. Returns `CONFLICT` if the client's revision doesn't match the server's (optimistic concurrency). |
| `ListChanges` | List all records changed since a given revision. |
| `ListenChanges` | Server-streaming RPC that pushes real-time notifications when a user's data changes. |
| `SetLock` | Acquire or release a named distributed lock. Supports per-instance identity and TTL-based expiration (default 30s). |
| `GetLock` | Check if any instance currently holds an active (non-expired) lock. |

Every request must include a signature over the request payload. The server derives the user ID from the recovered public key (compressed, hex-encoded).

## Directory Structure

```
config/           Environment-based configuration (ports, DB path, certs)
middleware/       ECDSA signature verification and optional X.509 cert validation
proto/            Protobuf definitions and generated Go code
store/            Storage interface and implementations
  sqlite/         SQLite backend with golang-migrate migrations
  postgres/       PostgreSQL backend with golang-migrate migrations
main.go           Entry point — starts gRPC server and web proxy
syncer_server.go  Core server logic: SetRecord, ListChanges, ListenChanges, SetLock, GetLock, EventsManager
```

## Configuration (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_LISTEN_ADDRESS` | `0.0.0.0:8080` | gRPC server address |
| `GRPC_WEB_LISTEN_ADDRESS` | `0.0.0.0:8081` | gRPC-Web/HTTP proxy address |
| `SQLITE_DIR_PATH` | `db` | Directory for SQLite database file |
| `DATABASE_URL` | — | PostgreSQL connection string (uses Postgres when set) |
| `CA_CERT` | — | Base64-encoded CA certificate for client cert validation |

## Build and Run

```bash
# Build
go build -v -o data-sync .

# Run (SQLite, default)
./data-sync

# Run (PostgreSQL)
DATABASE_URL="postgres://user:pass@host:5432/dbname" ./data-sync
```

## Docker

```bash
docker build -t data-sync .
docker run -p 8080:8080 -p 8081:8081 data-sync
```

## Deploy

Deployed to **Fly.io** (primary region: `lhr`). See `fly.toml` for configuration. CI/CD via GitHub Actions (`.github/workflows/fly-deploy.yml`).

## Key Design Decisions

- **Optimistic concurrency control**: Clients must supply the current revision when updating. Mismatches return `CONFLICT` — no automatic merge.
- **Signature-based identity**: No passwords or tokens. User ID is the compressed public key recovered from the ECDSA signature on each request.
- **Pluggable storage**: The `store.SyncStorage` interface allows swapping between SQLite (single-instance) and PostgreSQL (multi-instance) without changing server logic.
- **Serializable transactions**: Both storage backends use serializable isolation to prevent race conditions.
- **Channel-based streaming**: `EventsManager` maintains per-user Go channels to fan out real-time change notifications.

## Testing

```bash
go test ./...
```

Tests cover the storage layer (SQLite and PostgreSQL) and end-to-end server behavior including conflict detection and real-time notifications.

## Regenerate Protobuf

```bash
cd proto && ./genproto.sh
```
