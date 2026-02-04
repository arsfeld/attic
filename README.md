# Attic (libSQL Fork)

> **Note**: This is a fork of [zhaofengli/attic](https://github.com/zhaofengli/attic) that uses libSQL/Turso exclusively as the database backend.

**Attic** is a self-hostable Nix Binary Cache server backed by an S3-compatible storage provider.
It has support for global deduplication and garbage collection.

```
⚙️ Pushing 5 paths to "demo" on "local" (566 already cached, 2001 in upstream)...
✅ gnvi1x7r8kl3clzx0d266wi82fgyzidv-steam-run-fhs (29.69 MiB/s)
✅ rw7bx7ak2p02ljm3z4hhpkjlr8rzg6xz-steam-fhs (30.56 MiB/s)
✅ y92f9y7qhkpcvrqhzvf6k40j6iaxddq8-0p36ammvgyr55q9w75845kw4fw1c65ln-source (19.96 MiB/s)
🕒 vscode-1.74.2        ███████████████████████████████████████  345.66 MiB (41.32 MiB/s)
🕓 zoom-5.12.9.367      ███████████████████████████              329.36 MiB (39.47 MiB/s)
```

## Goals

- **Multi-Tenancy**: Create a private cache for yourself, and one for friends and co-workers. Tenants are mutually untrusting and cannot pollute the views of other caches.
- **Global Deduplication**: Individual caches (tenants) are simply restricted views of the content-addressed NAR Store and Chunk Store. When paths are uploaded, a mapping is created to grant the local cache access to the global NAR.
- **Managed Signing**: Signing is done on-the-fly by the server when store paths are fetched. The user pushing store paths does not have access to the signing key.
- **Scalabilty**: Attic can be easily replicated. It's designed to be deployed to serverless platforms like fly.io but also works nicely in a single-machine setup.
- **Garbage Collection**: Unused store paths can be garbage-collected in an LRU manner.

## Database

This fork uses [libSQL](https://github.com/tursodatabase/libsql) (the database behind [Turso](https://turso.tech/)) as its only database backend. This provides:

- **No cold starts**: Turso databases are always-on
- **Microsecond reads**: Embedded replicas sync locally for ultra-fast reads
- **SQLite compatibility**: Uses familiar SQLite SQL syntax
- **Edge deployment**: Perfect for serverless platforms like Fly.io

### Building

```bash
cargo build -p attic-server
```

### Configuration

```toml
[database]
# Remote Turso database
url = "libsql://your-db.turso.io"
auth_token = "your-turso-auth-token"

# Optional: Use embedded replica for microsecond reads
local_replica_path = "/tmp/attic-replica.db"
sync_interval = 60  # seconds
```

For local development, you can use a local SQLite file:

```toml
[database]
url = "file:///path/to/attic.db"
```

## Licensing

Attic is available under the **Apache License, Version 2.0**.
See `LICENSE` for details.

By contributing to the project, you agree to license your work under the aforementioned license.
