import { test, expect, setupWebAuthn, uniqueUsername } from "./fixtures";

test.describe("Authentication", () => {
  test.describe("Registration", () => {
    test("first user can register and becomes admin", async ({ page, context }) => {
      await setupWebAuthn(context, page);

      // Navigate to registration page
      await page.goto("/ui/register");

      // Should show first user badge when no users exist
      // (This test assumes a fresh database)
      const firstUserBadge = page.locator(".badge-info");

      // Fill registration form
      const username = uniqueUsername("admin");
      await page.fill("#username", username);
      await page.fill("#display_name", "Test Admin");
      await page.fill("#credential_name", "Test Device");

      // Submit and wait for redirect
      await Promise.all([
        page.waitForURL(/\/ui\/dashboard|\/ui$/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Should be on dashboard after successful registration
      await expect(page).toHaveURL(/\/ui/);

      // First user should see admin menu items
      await expect(page.locator("text=Users")).toBeVisible();
    });

    test("subsequent users can register when policy allows", async ({
      page,
      context,
    }) => {
      await setupWebAuthn(context, page);

      await page.goto("/ui/register");

      // Check if registration is enabled
      const registerBtn = page.locator("#register-btn");
      const registrationDisabled = page.locator("text=Registration is currently disabled");

      // If registration is disabled, skip this test
      if (await registrationDisabled.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("user");
      await page.fill("#username", username);
      await page.fill("#display_name", "Regular User");

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await expect(page).toHaveURL(/\/ui/);
    });

    test("shows error for duplicate username", async ({ page, context }) => {
      await setupWebAuthn(context, page);

      // First registration
      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      if (await registrationDisabled.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("duplicate");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Logout
      await page.goto("/ui/logout", { waitUntil: "networkidle" });

      // Try to register with same username
      await page.goto("/ui/register");
      await page.fill("#username", username);
      await page.click("#register-btn");

      // Should show error
      await expect(page.locator("#error")).toBeVisible({ timeout: 10000 });
      await expect(page.locator("#error-text")).toContainText(/already exists|taken/i);
    });
  });

  test.describe("Login", () => {
    test("registered user can login", async ({ page, context }) => {
      await setupWebAuthn(context, page);

      // First register a user
      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      if (await registrationDisabled.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("logintest");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Logout
      await page.click("text=Logout");
      await page.waitForURL(/\/ui\/login/);

      // Login with the same user
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui\/dashboard|\/ui$/, { timeout: 15000 }),
        page.click("#login-btn"),
      ]);

      await expect(page).toHaveURL(/\/ui/);
    });

    test("shows error for non-existent user", async ({ page, context }) => {
      await setupWebAuthn(context, page);

      await page.goto("/ui/login");

      await page.fill("#username", "nonexistent_user_12345");
      await page.click("#login-btn");

      // Should show error
      await expect(page.locator("#error")).toBeVisible({ timeout: 10000 });
      await expect(page.locator("#error-text")).toContainText(/not found|does not exist/i);
    });

    test("login page has link to registration", async ({ page }) => {
      await page.goto("/ui/login");

      const registerLink = page.locator("text=Need an account? Register");
      await expect(registerLink).toBeVisible();

      await registerLink.click();
      await expect(page).toHaveURL(/\/ui\/register/);
    });
  });

  test.describe("Logout", () => {
    test("user can logout", async ({ page, context }) => {
      await setupWebAuthn(context, page);

      // Register and login
      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      if (await registrationDisabled.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("logouttest");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Click logout
      await page.click("text=Logout");

      // Should be redirected to login
      await expect(page).toHaveURL(/\/ui\/login/);

      // Trying to access dashboard should redirect to login
      await page.goto("/ui/dashboard");
      await expect(page).toHaveURL(/\/ui\/login/);
    });
  });

  test.describe("Session persistence", () => {
    test("session persists across page reloads", async ({ page, context }) => {
      await setupWebAuthn(context, page);

      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      if (await registrationDisabled.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("session");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Reload the page
      await page.reload();

      // Should still be logged in
      await expect(page).toHaveURL(/\/ui/);
      await expect(page.locator("text=Logout")).toBeVisible();
    });
  });
});
