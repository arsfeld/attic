# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Attic is a self-hostable, multi-tenant Nix binary cache server backed by S3-compatible storage. It features global deduplication through content-addressed chunking, server-side signing, and LRU-based garbage collection.

## Build Commands

```bash
# Enter development shell (required for most operations)
nix develop

# Build all packages
cargo build --release

# Build individual crates
cargo build -p attic-client     # CLI tool
cargo build -p attic-server     # Server (atticd + atticadm)

# Run tests
cargo test

# Format and lint
cargo fmt --check
cargo clippy

# Build with Nix
nix build .#attic               # Full package
nix build .#attic-server-static # Static server binary

# WebAssembly build (no libnixstore)
cargo build --target wasm32-unknown-unknown --no-default-features -F chunking -F io
```

## Running Locally

```bash
# Start server (uses demo config from nix develop '.#demo')
atticd

# Server modes
atticd --mode monolithic          # API + GC (default)
atticd --mode api-server          # API only
atticd --mode garbage-collector   # GC only
atticd --mode db-migrations       # Run migrations
atticd --mode check-config        # Validate config
```

## Architecture

```
attic/                    # Cargo workspace root
├── attic/               # Core library - shared types, API schemas, chunking, signing
├── client/              # CLI client (attic command) - push/pull operations
├── server/              # Server (atticd + atticadm) - Axum web framework
└── token/               # JWT token handling
```

### Server Structure (`server/src/`)

- `api/` - HTTP endpoints (Axum routes, binary cache compatibility)
- `storage/` - Backend abstraction (`StorageBackend` trait: local filesystem, S3)
- `database/` - libSQL database layer (connection, models, queries) and migrations
- `gc.rs` - Garbage collection daemon
- `compression.rs` - Stream compression (zstd, brotli, xz)

### Core Library (`attic/src/`)

- `api/` - API data structures (v1 endpoints, binary cache format)
- `chunking/` - FastCDC-based content chunking
- `hash/` - SHA256, NAR hashing with `HashReader`
- `nix_store/` - Native bindings to Nix store (cxx)
- `signing/` - Ed25519 cryptographic signing

### Key Types

```rust
// Server state management
type State = Arc<StateInner>;           // Global state: config, db, storage
type RequestState = Arc<RequestStateInner>;  // Per-request: auth, endpoints

// Storage abstraction
trait StorageBackend: Send + Sync       // Implemented by LocalStorage, S3Storage
```

## Feature Flags

The `attic` crate has compile-time features:
- `chunking` - FastCDC content chunking (requires `io`)
- `nix_store` - Native Nix store bindings (requires tokio)
- `io` - Async I/O utilities
- `tokio` - Tokio runtime integration

Default: `["chunking", "io", "nix_store", "tokio"]`

## Database

This fork uses libSQL/Turso exclusively (no SeaORM):
- Remote Turso databases for production (`libsql://` URLs)
- Local SQLite files for development (`file://` URLs)
- Migrations run automatically or via `atticd --mode db-migrations`
- Database layer in `server/src/database/`: connection, models, queries, migrations

## Code Quality

The codebase uses strict lint attributes:
```rust
#![deny(unsafe_code, unused_must_use, deprecated, missing_abi)]
```
