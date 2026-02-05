# Attic Web UI E2E Tests

End-to-end tests for the Attic web UI using [Playwright](https://playwright.dev/).

## Quick Start with Docker (Recommended)

Run everything in Docker with a single command - no local dependencies needed:

```bash
# From the project root
./e2e/run-tests.sh
```

This builds atticd from source, starts the server, and runs all tests with proper caching.

### Docker Options

```bash
# Run all tests (default)
./e2e/run-tests.sh

# Run specific tests
./e2e/run-tests.sh all-in-one -g "login"
./e2e/run-tests.sh all-in-one tests/auth.spec.ts

# Run with separate containers (atticd + playwright)
./e2e/run-tests.sh separate

# Build images only (useful for CI caching)
./e2e/run-tests.sh build-only

# Debug mode
./e2e/run-tests.sh debug
```

### Direct Docker Command

If you prefer running Docker directly:

```bash
# Build the image
docker build --target e2e-runner -t attic-e2e -f e2e/Dockerfile .

# Run tests
docker run --rm \
  -v ./e2e/test-results:/e2e/test-results \
  -v ./e2e/playwright-report:/e2e/playwright-report \
  attic-e2e

# Run specific tests
docker run --rm attic-e2e npx playwright test -g "registration"
```

### Docker Compose

For more control, use docker-compose:

```bash
cd e2e

# Run with separate services
docker compose up --build playwright

# Clean up
docker compose down -v
```

---

## Local Development Setup

### Prerequisites

- Node.js 18+
- A running Attic server (or use the included configuration to auto-start)

### Setup

```bash
cd e2e

# Install dependencies
npm install

# Install Playwright browsers
npx playwright install chromium
```

### Running Tests Locally

Start the Attic Server first in a separate terminal:

```bash
# From the project root
nix develop
atticd
```

The server should be running at `http://localhost:8080`.

### Run Tests

```bash
# Run all tests
npm test

# Run tests with UI mode (interactive)
npm run test:ui

# Run tests in headed mode (see the browser)
npm run test:headed

# Run tests in debug mode
npm run test:debug

# Run specific test file
npx playwright test tests/auth.spec.ts

# Run tests matching a pattern
npx playwright test -g "registration"
```

### View Test Report

After running tests:

```bash
npm run report
```

## Test Structure

```
e2e/
├── playwright.config.ts   # Playwright configuration
├── tests/
│   ├── fixtures.ts        # Test utilities and WebAuthn setup
│   ├── auth.spec.ts       # Registration, login, logout tests
│   ├── dashboard.spec.ts  # Dashboard functionality tests
│   ├── caches.spec.ts     # Cache management tests
│   ├── tokens.spec.ts     # Token generation tests
│   └── admin.spec.ts      # Admin panel tests
```

## How WebAuthn Testing Works

The tests use Playwright's CDP (Chrome DevTools Protocol) to set up virtual WebAuthn authenticators. This allows testing the passkey registration and login flows without requiring physical security keys.

```typescript
// Example: Setting up virtual authenticator
const cdpSession = await context.newCDPSession(page);
await cdpSession.send("WebAuthn.enable");
await cdpSession.send("WebAuthn.addVirtualAuthenticator", {
  options: {
    protocol: "ctap2",
    transport: "internal",
    hasResidentKey: true,
    hasUserVerification: true,
    isUserVerified: true,
  },
});
```

## Test Isolation

Each test:
- Uses a unique username to avoid conflicts
- Sets up its own WebAuthn authenticator
- Starts from a fresh browser context

**Note:** Some tests depend on database state (e.g., first user becomes admin). For complete isolation, consider:
- Using a fresh database for each test run
- Adding database reset between tests
- Using test fixtures that set up required state

## Environment Variables

- `ATTIC_BASE_URL`: Override the server URL (default: `http://localhost:8080`)

```bash
ATTIC_BASE_URL=https://my-attic.example.com npm test
```

## CI Integration

For CI environments, uncomment the `webServer` section in `playwright.config.ts` to auto-start the server:

```typescript
webServer: {
  command: 'atticd',
  url: 'http://localhost:8080',
  reuseExistingServer: !process.env.CI,
},
```

## Debugging Tips

1. **Use headed mode** to see what's happening:
   ```bash
   npm run test:headed
   ```

2. **Use debug mode** for step-by-step execution:
   ```bash
   npm run test:debug
   ```

3. **Check screenshots** in `test-results/` directory for failed tests

4. **View traces** by opening the HTML report:
   ```bash
   npm run report
   ```

5. **Add `page.pause()`** in tests to stop execution and inspect:
   ```typescript
   await page.pause(); // Opens Playwright Inspector
   ```

## Writing New Tests

Use the provided fixtures:

```typescript
import { test, expect, setupWebAuthn, uniqueUsername } from "./fixtures";

test("my new test", async ({ page, context }) => {
  // Set up WebAuthn for passkey operations
  await setupWebAuthn(context, page);

  // Use unique username for isolation
  const username = uniqueUsername("mytest");

  // Test implementation...
});
```
