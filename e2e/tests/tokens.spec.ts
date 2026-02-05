import { test, expect, setupWebAuthn, uniqueUsername } from "./fixtures";

test.describe("Token Management", () => {
  test.describe("Token page access", () => {
    test("authenticated user can access tokens page", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      if (await registrationDisabled.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("tokenaccess");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/tokens");

      // Should show tokens page
      await expect(page.locator("h1")).toContainText(/Token/);
      await expect(page.locator("text=Create Token")).toBeVisible();
    });

    test("unauthenticated user is redirected to login", async ({ page }) => {
      await page.goto("/ui/tokens");

      // Should redirect to login
      await expect(page).toHaveURL(/\/ui\/login/);
    });
  });

  test.describe("Token creation", () => {
    test("admin can create token with all permissions", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      // Need to be first user (admin)
      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("tokenadmin");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // First create a cache to reference
      await page.goto("/ui/caches");
      await page.click("text=Create Cache");

      const cacheName = `token-cache-${Date.now()}`;
      await page.fill('input[name="name"]', cacheName);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/caches") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Now create a token
      await page.goto("/ui/tokens");
      await page.click("text=Create Token");

      // Fill token form
      await page.fill('input[name="cache_pattern"]', cacheName);
      await page.fill('input[name="subject"]', "test-ci");

      // Check all permissions
      await page.check('input[name="can_pull"]');
      await page.check('input[name="can_push"]');
      await page.check('input[name="can_delete"]');
      await page.check('input[name="can_create_cache"]');

      // Submit
      await page.click('button:has-text("Generate Token")');

      // Token result modal should appear
      const resultModal = page.locator("#token_result_modal");
      await expect(resultModal).toBeVisible({ timeout: 10000 });
      await expect(page.locator("text=Token Created Successfully")).toBeVisible();

      // Token should be displayed
      const tokenInput = page.locator("#generated_token");
      const tokenValue = await tokenInput.inputValue();
      expect(tokenValue).toBeTruthy();
      expect(tokenValue.length).toBeGreaterThan(20); // JWT tokens are long
    });

    test("can copy token to clipboard", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("tokencopy");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/tokens");
      await page.click("text=Create Token");

      await page.fill('input[name="cache_pattern"]', "*");
      await page.click('button:has-text("Generate Token")');

      // Wait for modal
      await expect(page.locator("#token_result_modal")).toBeVisible({ timeout: 10000 });

      // Click copy button
      await page.click("text=Copy");

      // Button should change to "Copied!"
      await expect(page.locator("text=Copied!")).toBeVisible();
    });

    test("wildcard pattern works", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("tokenwild");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/tokens");
      await page.click("text=Create Token");

      // Use wildcard pattern
      await page.fill('input[name="cache_pattern"]', "team-*");
      await page.click('button:has-text("Generate Token")');

      // Should succeed
      await expect(page.locator("#token_result_modal")).toBeVisible({ timeout: 10000 });
    });

    test("validity period options are available", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      if (await registrationDisabled.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("tokenvalidity");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/tokens");
      await page.click("text=Create Token");

      // Check validity dropdown options
      const validitySelect = page.locator('select[name="validity"]');
      await expect(validitySelect).toBeVisible();

      // Check some options exist
      await expect(validitySelect.locator('option[value="1d"]')).toBeVisible();
      await expect(validitySelect.locator('option[value="7d"]')).toBeVisible();
      await expect(validitySelect.locator('option[value="30d"]')).toBeVisible();
      await expect(validitySelect.locator('option[value="90d"]')).toBeVisible();
    });
  });

  test.describe("Admin vs User differences", () => {
    test("admin sees subject field", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("adminsubject");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/tokens");
      await page.click("text=Create Token");

      // Admin should see subject field
      await expect(page.locator('input[name="subject"]')).toBeVisible();
    });

    test("admin sees 2-year validity option", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("admin2year");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/tokens");
      await page.click("text=Create Token");

      // Admin should see 2-year option
      const validitySelect = page.locator('select[name="validity"]');
      await expect(validitySelect.locator('option[value="730d"]')).toBeVisible();
    });
  });

  test.describe("Token page content", () => {
    test("shows available caches", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("showcaches");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Create a cache first
      await page.goto("/ui/caches");
      await page.click("text=Create Cache");

      const cacheName = `listed-cache-${Date.now()}`;
      await page.fill('input[name="name"]', cacheName);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/caches") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Go to tokens page
      await page.goto("/ui/tokens");

      // Should show "Available Caches" section with the cache
      await expect(page.locator("text=Available Caches")).toBeVisible();
      await expect(page.locator(`.badge:has-text("${cacheName}")`)).toBeVisible();
    });

    test("shows warning about token storage", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      if (await registrationDisabled.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("tokenwarning");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/tokens");

      // Should show security warning
      await expect(
        page.locator("text=Tokens are shown only once when created")
      ).toBeVisible();
    });
  });
});
