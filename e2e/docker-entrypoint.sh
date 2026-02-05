#!/bin/bash
set -e

echo "=== Starting Attic E2E Tests ==="

# Start atticd in background
echo "Starting atticd server..."
atticd -f /config/atticd.toml &
ATTICD_PID=$!

# Wait for server to be ready
echo "Waiting for server to be ready..."
MAX_RETRIES=30
RETRY_COUNT=0

while ! curl -s http://localhost:8080/version > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "ERROR: Server failed to start after $MAX_RETRIES attempts"
        kill $ATTICD_PID 2>/dev/null || true
        exit 1
    fi
    echo "  Waiting... (attempt $RETRY_COUNT/$MAX_RETRIES)"
    sleep 1
done

echo "Server is ready!"

# Run the command (default: playwright tests)
echo "Running: $@"
"$@"
TEST_EXIT_CODE=$?

# Cleanup
echo "Stopping atticd server..."
kill $ATTICD_PID 2>/dev/null || true
wait $ATTICD_PID 2>/dev/null || true

echo "=== Tests completed with exit code: $TEST_EXIT_CODE ==="
exit $TEST_EXIT_CODE
