# syntax=docker/dockerfile:1.4

# ============================================
# Stage 1: Build atticd from source
# ============================================
FROM rust:1.75-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for better caching
COPY Cargo.toml Cargo.lock ./
COPY attic/Cargo.toml ./attic/
COPY client/Cargo.toml ./client/
COPY server/Cargo.toml ./server/
COPY token/Cargo.toml ./token/

# Create dummy source files to build dependencies
RUN mkdir -p attic/src client/src server/src token/src && \
    echo "fn main() {}" > attic/src/lib.rs && \
    echo "fn main() {}" > client/src/main.rs && \
    echo "fn main() {}" > server/src/main.rs && \
    echo "fn main() {}" > token/src/lib.rs

# Build dependencies only (cached layer)
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release -p attic-server --no-default-features && \
    rm -rf attic/src client/src server/src token/src

# Copy actual source code
COPY attic ./attic
COPY client ./client
COPY server ./server
COPY token ./token

# Build the actual binary
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release -p attic-server --no-default-features && \
    cp /app/target/release/atticd /atticd && \
    cp /app/target/release/atticadm /atticadm

# ============================================
# Stage 2: Runtime image for atticd
# ============================================
FROM debian:bookworm-slim AS server

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /atticd /usr/local/bin/atticd
COPY --from=builder /atticadm /usr/local/bin/atticadm

# Create data directory
RUN mkdir -p /data

EXPOSE 8080

CMD ["atticd"]

# ============================================
# Stage 3: Playwright test runner
# ============================================
FROM mcr.microsoft.com/playwright:v1.49.0-jammy AS playwright

WORKDIR /e2e

# Copy package files first for caching
COPY e2e/package.json ./
COPY e2e/package-lock.json* ./

# Install dependencies with cache
# Use npm install if no lock file, npm ci if lock file exists
RUN --mount=type=cache,target=/root/.npm \
    if [ -f package-lock.json ]; then npm ci; else npm install; fi

# Copy test files
COPY e2e/tsconfig.json ./
COPY e2e/playwright.config.ts ./
COPY e2e/tests ./tests

# Default command runs tests
CMD ["npx", "playwright", "test"]

# ============================================
# Stage 4: All-in-one test runner
# ============================================
FROM mcr.microsoft.com/playwright:v1.49.0-jammy AS e2e-runner

# Install dependencies for running atticd
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libsqlite3-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy atticd binary
COPY --from=builder /atticd /usr/local/bin/atticd
COPY --from=builder /atticadm /usr/local/bin/atticadm

WORKDIR /e2e

# Copy package files first for caching
COPY e2e/package.json ./
COPY e2e/package-lock.json* ./

# Install dependencies with cache
# Use npm install if no lock file, npm ci if lock file exists
RUN --mount=type=cache,target=/root/.npm \
    if [ -f package-lock.json ]; then npm ci; else npm install; fi

# Copy test files
COPY e2e/tsconfig.json ./
COPY e2e/playwright.config.ts ./
COPY e2e/tests ./tests

# Copy the entrypoint script
COPY e2e/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Create directories
RUN mkdir -p /data /config

# Copy default config
COPY e2e/atticd-test.toml /config/atticd.toml

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["npx", "playwright", "test"]
