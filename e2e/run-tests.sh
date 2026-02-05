#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo -e "${GREEN}=== Attic E2E Tests ===${NC}"

# Parse arguments
MODE="${1:-all-in-one}"
EXTRA_ARGS="${@:2}"

usage() {
    echo "Usage: $0 [mode] [playwright-args...]"
    echo ""
    echo "Modes:"
    echo "  all-in-one    Run atticd + tests in single container (default)"
    echo "  separate      Run atticd and tests as separate services"
    echo "  headed        Run tests with visible browser (requires X11)"
    echo "  debug         Run tests in debug mode"
    echo "  build-only    Just build the Docker images"
    echo ""
    echo "Examples:"
    echo "  $0                           # Run all tests"
    echo "  $0 all-in-one -g 'login'     # Run tests matching 'login'"
    echo "  $0 separate                  # Use separate containers"
    echo "  $0 build-only                # Just build images"
}

# Ensure BuildKit is enabled
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

case "$MODE" in
    all-in-one)
        echo -e "${YELLOW}Building and running all-in-one E2E tests...${NC}"
        docker build \
            --target e2e-runner \
            --cache-from type=local,src=/tmp/.buildx-cache \
            --build-arg BUILDKIT_INLINE_CACHE=1 \
            -t attic-e2e:latest \
            -f e2e/Dockerfile \
            .

        echo -e "${YELLOW}Running tests...${NC}"
        docker run --rm \
            -v "$SCRIPT_DIR/test-results:/e2e/test-results" \
            -v "$SCRIPT_DIR/playwright-report:/e2e/playwright-report" \
            -e CI="${CI:-}" \
            attic-e2e:latest \
            npx playwright test $EXTRA_ARGS
        ;;

    separate)
        echo -e "${YELLOW}Running with separate services...${NC}"
        cd "$SCRIPT_DIR"
        docker compose up --build --abort-on-container-exit playwright
        docker compose down
        ;;

    headed)
        echo -e "${YELLOW}Running headed tests (requires X11 forwarding)...${NC}"
        docker build \
            --target e2e-runner \
            -t attic-e2e:latest \
            -f e2e/Dockerfile \
            .

        docker run --rm \
            -v "$SCRIPT_DIR/test-results:/e2e/test-results" \
            -v "$SCRIPT_DIR/playwright-report:/e2e/playwright-report" \
            -v /tmp/.X11-unix:/tmp/.X11-unix \
            -e DISPLAY="${DISPLAY}" \
            -e CI="${CI:-}" \
            attic-e2e:latest \
            npx playwright test --headed $EXTRA_ARGS
        ;;

    debug)
        echo -e "${YELLOW}Running in debug mode...${NC}"
        docker build \
            --target e2e-runner \
            -t attic-e2e:latest \
            -f e2e/Dockerfile \
            .

        docker run --rm -it \
            -v "$SCRIPT_DIR/test-results:/e2e/test-results" \
            -v "$SCRIPT_DIR/playwright-report:/e2e/playwright-report" \
            -e CI="${CI:-}" \
            attic-e2e:latest \
            npx playwright test --debug $EXTRA_ARGS
        ;;

    build-only)
        echo -e "${YELLOW}Building Docker images...${NC}"
        docker build \
            --target e2e-runner \
            --cache-from type=local,src=/tmp/.buildx-cache \
            --build-arg BUILDKIT_INLINE_CACHE=1 \
            -t attic-e2e:latest \
            -f e2e/Dockerfile \
            .
        echo -e "${GREEN}Build complete!${NC}"
        ;;

    -h|--help|help)
        usage
        exit 0
        ;;

    *)
        echo -e "${RED}Unknown mode: $MODE${NC}"
        usage
        exit 1
        ;;
esac

# Check exit code
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}=== Tests passed! ===${NC}"
else
    echo -e "${RED}=== Tests failed with exit code: $EXIT_CODE ===${NC}"
fi

# Show report location
if [ -d "$SCRIPT_DIR/playwright-report" ]; then
    echo -e "${YELLOW}View report: npx playwright show-report e2e/playwright-report${NC}"
fi

exit $EXIT_CODE
