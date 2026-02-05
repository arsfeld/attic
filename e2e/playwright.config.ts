import { defineConfig, devices } from "@playwright/test";

/**
 * Playwright configuration for Attic Web UI E2E tests.
 *
 * Running options:
 * 1. Docker (recommended): ./e2e/run-tests.sh
 * 2. Local: Start atticd first, then npm test
 *
 * Set ATTIC_BASE_URL to point to a different server.
 */
export default defineConfig({
  testDir: "./tests",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: [["html", { open: "never" }], ["list"]],

  // Increase timeout for WebAuthn operations
  timeout: 60000,
  expect: {
    timeout: 10000,
  },

  use: {
    baseURL: process.env.ATTIC_BASE_URL || "http://localhost:8080",
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },

  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],

  // Output directories for artifacts
  outputDir: "./test-results",
});
