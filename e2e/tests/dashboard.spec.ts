import { test, expect, setupWebAuthn, uniqueUsername } from "./fixtures";

test.describe("Dashboard", () => {
  test.beforeEach(async ({ page, context }) => {
    // Set up WebAuthn and register a user for each test
    await setupWebAuthn(context, page);
  });

  test("shows user dashboard after login", async ({ page, context }) => {
    await setupWebAuthn(context, page);
    await page.goto("/ui/register");

    const registrationDisabled = page.locator("text=Registration is currently disabled");
    if (await registrationDisabled.isVisible()) {
      test.skip();
      return;
    }

    const username = uniqueUsername("dashboard");
    await page.fill("#username", username);

    await Promise.all([
      page.waitForURL(/\/ui/, { timeout: 15000 }),
      page.click("#register-btn"),
    ]);

    // Should show dashboard elements
    await expect(page.locator("text=Dashboard")).toBeVisible();
    await expect(page.locator("text=Accessible Caches")).toBeVisible();
    await expect(page.locator("text=Quick Actions")).toBeVisible();
  });

  test("displays cache statistics", async ({ page, context }) => {
    await setupWebAuthn(context, page);
    await page.goto("/ui/register");

    const registrationDisabled = page.locator("text=Registration is currently disabled");
    if (await registrationDisabled.isVisible()) {
      test.skip();
      return;
    }

    const username = uniqueUsername("stats");
    await page.fill("#username", username);

    await Promise.all([
      page.waitForURL(/\/ui/, { timeout: 15000 }),
      page.click("#register-btn"),
    ]);

    // Check stats display
    const stats = page.locator(".stats");
    await expect(stats).toBeVisible();
    await expect(page.locator("text=Total Objects")).toBeVisible();
    await expect(page.locator("text=Role")).toBeVisible();
  });

  test("shows admin badge for admin users", async ({ page, context }) => {
    await setupWebAuthn(context, page);
    await page.goto("/ui/register");

    // First user is admin
    const firstUserBadge = page.locator(".badge-info");
    if (await firstUserBadge.isVisible()) {
      const username = uniqueUsername("admin");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Admin should see "Admin" role and admin link
      await expect(page.locator("text=Admin").first()).toBeVisible();
    } else {
      test.skip();
    }
  });

  test("shows user role for non-admin users", async ({ page, context }) => {
    await setupWebAuthn(context, page);

    // This test needs an existing admin user first
    await page.goto("/ui/register");

    const registrationDisabled = page.locator("text=Registration is currently disabled");
    const firstUserBadge = page.locator(".badge-info");

    // Skip if we can't register or this is the first user
    if (await registrationDisabled.isVisible() || await firstUserBadge.isVisible()) {
      test.skip();
      return;
    }

    const username = uniqueUsername("regular");
    await page.fill("#username", username);

    await Promise.all([
      page.waitForURL(/\/ui/, { timeout: 15000 }),
      page.click("#register-btn"),
    ]);

    // Regular user should see "User" role
    await expect(page.locator(".stat-value:has-text('User')")).toBeVisible();
  });

  test("has working navigation links", async ({ page, context }) => {
    await setupWebAuthn(context, page);
    await page.goto("/ui/register");

    const registrationDisabled = page.locator("text=Registration is currently disabled");
    if (await registrationDisabled.isVisible()) {
      test.skip();
      return;
    }

    const username = uniqueUsername("nav");
    await page.fill("#username", username);

    await Promise.all([
      page.waitForURL(/\/ui/, { timeout: 15000 }),
      page.click("#register-btn"),
    ]);

    // Test Generate Token link
    await page.click("text=Generate Token");
    await expect(page).toHaveURL(/\/ui\/tokens/);

    // Go back to dashboard
    await page.goto("/ui");

    // Test View Caches link
    await page.click("text=View Caches");
    await expect(page).toHaveURL(/\/ui\/caches/);
  });

  test("shows empty state when user has no caches", async ({ page, context }) => {
    await setupWebAuthn(context, page);
    await page.goto("/ui/register");

    const registrationDisabled = page.locator("text=Registration is currently disabled");
    const firstUserBadge = page.locator(".badge-info");

    // Skip if first user (admin has access to all)
    if (await registrationDisabled.isVisible() || await firstUserBadge.isVisible()) {
      test.skip();
      return;
    }

    const username = uniqueUsername("nocaches");
    await page.fill("#username", username);

    await Promise.all([
      page.waitForURL(/\/ui/, { timeout: 15000 }),
      page.click("#register-btn"),
    ]);

    // Should show empty state message
    await expect(
      page.locator("text=You don't have access to any caches yet")
    ).toBeVisible();
  });
});
