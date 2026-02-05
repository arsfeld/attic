import { test, expect, setupWebAuthn, uniqueUsername } from "./fixtures";

test.describe("Cache Management", () => {
  test.describe("Cache listing", () => {
    test("shows caches page with table", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      if (await registrationDisabled.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("cachelist");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Navigate to caches page
      await page.goto("/ui/caches");

      // Should show caches heading
      await expect(page.locator("h1:has-text('Caches')")).toBeVisible();

      // Should have back to dashboard link
      await expect(page.locator("text=Back to Dashboard")).toBeVisible();
    });

    test("admin sees all caches", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      // Need to be first user (admin)
      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("adminlist");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/caches");

      // Admin should see "Manage all binary caches" text
      await expect(page.locator("text=Manage all binary caches")).toBeVisible();
    });

    test("regular user sees only accessible caches", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      const firstUserBadge = page.locator(".badge-info");

      if (await registrationDisabled.isVisible() || await firstUserBadge.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("userlist");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/caches");

      // Regular user should see "Caches you have access to" text
      await expect(page.locator("text=Caches you have access to")).toBeVisible();
    });
  });

  test.describe("Cache creation", () => {
    test("admin can create a cache", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      // Need to be first user (admin)
      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("createcache");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/caches");

      // Click create cache button
      await page.click("text=Create Cache");

      // Modal should appear
      const modal = page.locator("#create_cache_modal");
      await expect(modal).toBeVisible();

      // Fill in cache name
      const cacheName = `test-cache-${Date.now()}`;
      await page.fill('input[name="name"]', cacheName);

      // Submit the form
      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/caches") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Cache should appear in the list
      await expect(page.locator(`text=${cacheName}`)).toBeVisible();
    });

    test("can create public cache", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("publiccache");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/caches");
      await page.click("text=Create Cache");

      const cacheName = `public-cache-${Date.now()}`;
      await page.fill('input[name="name"]', cacheName);

      // Check public checkbox
      await page.check('input[name="is_public"]');

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/caches") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Should show Public badge
      const row = page.locator(`tr:has-text("${cacheName}")`);
      await expect(row.locator("text=Public")).toBeVisible();
    });

    test("validates cache name format", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("validatecache");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/caches");
      await page.click("text=Create Cache");

      // Try invalid name starting with number
      await page.fill('input[name="name"]', "123invalid");

      // Form validation should prevent submission (pattern attribute)
      const input = page.locator('input[name="name"]');
      await expect(input).toHaveAttribute("pattern", "[a-zA-Z][a-zA-Z0-9_-]*");
    });

    test("regular user with create permission can create cache", async ({ page, context }) => {
      // This test requires a user with can_create_cache permission
      // which needs to be set up via admin or directly in the database
      test.skip();
    });
  });

  test.describe("Cache deletion", () => {
    test("admin can delete a cache", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("deletecache");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // First create a cache
      await page.goto("/ui/caches");
      await page.click("text=Create Cache");

      const cacheName = `delete-me-${Date.now()}`;
      await page.fill('input[name="name"]', cacheName);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/caches") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Verify cache exists
      await expect(page.locator(`text=${cacheName}`)).toBeVisible();

      // Delete it
      page.on("dialog", (dialog) => dialog.accept());

      const row = page.locator(`tr:has-text("${cacheName}")`);
      await row.locator("text=Delete").click();

      // Wait for page reload
      await page.waitForLoadState("networkidle");

      // Cache should be gone
      await expect(page.locator(`text=${cacheName}`)).not.toBeVisible();
    });

    test("deletion requires confirmation", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("confirmdelete");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Create a cache first
      await page.goto("/ui/caches");
      await page.click("text=Create Cache");

      const cacheName = `confirm-delete-${Date.now()}`;
      await page.fill('input[name="name"]', cacheName);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/caches") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Try to delete but cancel
      let dialogDismissed = false;
      page.on("dialog", (dialog) => {
        dialogDismissed = true;
        dialog.dismiss();
      });

      const row = page.locator(`tr:has-text("${cacheName}")`);
      await row.locator("text=Delete").click();

      // Dialog should have been shown
      expect(dialogDismissed).toBe(true);

      // Cache should still exist
      await expect(page.locator(`text=${cacheName}`)).toBeVisible();
    });
  });

  test.describe("Cache table display", () => {
    test("shows cache details in table", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const username = uniqueUsername("tabledetails");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Create a cache
      await page.goto("/ui/caches");
      await page.click("text=Create Cache");

      const cacheName = `detail-cache-${Date.now()}`;
      await page.fill('input[name="name"]', cacheName);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/caches") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Check table headers
      await expect(page.locator("th:has-text('Name')")).toBeVisible();
      await expect(page.locator("th:has-text('Objects')")).toBeVisible();
      await expect(page.locator("th:has-text('Visibility')")).toBeVisible();
      await expect(page.locator("th:has-text('Priority')")).toBeVisible();

      // Check row has expected data
      const row = page.locator(`tr:has-text("${cacheName}")`);
      await expect(row).toBeVisible();
      await expect(row.locator("text=/nix/store")).toBeVisible(); // default store dir
    });
  });
});
