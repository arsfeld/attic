import { test, expect, setupWebAuthn, uniqueUsername } from "./fixtures";

test.describe("Admin Panel", () => {
  test.describe("Access control", () => {
    test("only admins can access admin panel", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      const firstUserBadge = page.locator(".badge-info");

      // Need non-first user (regular user)
      if (await registrationDisabled.isVisible() || await firstUserBadge.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("regularuser");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Try to access admin panel
      await page.goto("/ui/admin/users");

      // Should be forbidden or redirected
      const forbidden = await page.locator("text=Forbidden").isVisible().catch(() => false);
      const redirected = page.url().includes("/ui/login") || page.url().includes("/ui/dashboard");

      expect(forbidden || redirected).toBe(true);
    });

    test("admin link only shown to admins", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const registrationDisabled = page.locator("text=Registration is currently disabled");
      const firstUserBadge = page.locator(".badge-info");

      // Need non-first user
      if (await registrationDisabled.isVisible() || await firstUserBadge.isVisible()) {
        test.skip();
        return;
      }

      const username = uniqueUsername("nonadmin");
      await page.fill("#username", username);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Admin link should not be visible
      await expect(page.locator('a:has-text("Admin")')).not.toBeVisible();
    });
  });

  test.describe("User management", () => {
    test("admin can view users list", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

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

      // Navigate to admin users
      await page.click('a:has-text("Admin")');
      await expect(page).toHaveURL(/\/ui\/admin\/users/);

      // Should show users table
      await expect(page.locator("h1:has-text('Users')")).toBeVisible();
      await expect(page.locator("table")).toBeVisible();

      // Current admin should be in the list
      await expect(page.locator(`td:has-text("${username}")`)).toBeVisible();
    });

    test("admin can create new user", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const adminUsername = uniqueUsername("admin");
      await page.fill("#username", adminUsername);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/admin/users");

      // Click create user
      await page.click("text=Create User");

      // Modal should appear
      const modal = page.locator("#create_user_modal");
      await expect(modal).toBeVisible();

      // Fill user details
      const newUsername = uniqueUsername("newuser");
      await page.fill('input[name="username"]', newUsername);
      await page.fill('input[name="display_name"]', "New User Display");

      // Submit
      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/admin/users") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // New user should appear in the list
      await expect(page.locator(`td:has-text("${newUsername}")`)).toBeVisible();
    });

    test("admin can create admin user", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const adminUsername = uniqueUsername("admin");
      await page.fill("#username", adminUsername);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/admin/users");
      await page.click("text=Create User");

      const newAdminUsername = uniqueUsername("newadmin");
      await page.fill('input[name="username"]', newAdminUsername);
      await page.check('input[name="is_admin"]');

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/admin/users") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Should show Admin badge
      const row = page.locator(`tr:has-text("${newAdminUsername}")`);
      await expect(row.locator(".badge-primary:has-text('Admin')")).toBeVisible();
    });

    test("admin can delete user", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const adminUsername = uniqueUsername("admin");
      await page.fill("#username", adminUsername);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // First create a user to delete
      await page.goto("/ui/admin/users");
      await page.click("text=Create User");

      const deleteUsername = uniqueUsername("todelete");
      await page.fill('input[name="username"]', deleteUsername);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/admin/users") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Verify user exists
      await expect(page.locator(`td:has-text("${deleteUsername}")`)).toBeVisible();

      // Delete user
      page.on("dialog", (dialog) => dialog.accept());

      const row = page.locator(`tr:has-text("${deleteUsername}")`);
      await row.locator("text=Delete").click();

      // Wait for reload
      await page.waitForLoadState("networkidle");

      // User should be gone
      await expect(page.locator(`td:has-text("${deleteUsername}")`)).not.toBeVisible();
    });

    test("admin cannot delete themselves", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const adminUsername = uniqueUsername("selfadmin");
      await page.fill("#username", adminUsername);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/admin/users");

      // Find own row
      const ownRow = page.locator(`tr:has-text("${adminUsername}")`);

      // Delete button should not be present for own user
      await expect(ownRow.locator("text=Delete")).not.toBeVisible();
    });
  });

  test.describe("User detail and permissions", () => {
    test("admin can view user details", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const adminUsername = uniqueUsername("admin");
      await page.fill("#username", adminUsername);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Create a user to view
      await page.goto("/ui/admin/users");
      await page.click("text=Create User");

      const viewUsername = uniqueUsername("viewuser");
      await page.fill('input[name="username"]', viewUsername);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/admin/users") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Click Edit
      const row = page.locator(`tr:has-text("${viewUsername}")`);
      await row.locator("text=Edit").click();

      // Should show user detail page
      await expect(page.locator("text=User Details")).toBeVisible();
      await expect(page.locator(`text=${viewUsername}`)).toBeVisible();
      await expect(page.locator("text=Cache Permissions")).toBeVisible();
    });

    test("admin can add permissions to user", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const adminUsername = uniqueUsername("admin");
      await page.fill("#username", adminUsername);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Create a user
      await page.goto("/ui/admin/users");
      await page.click("text=Create User");

      const permUsername = uniqueUsername("permuser");
      await page.fill('input[name="username"]', permUsername);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/admin/users") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      // Go to user detail
      const row = page.locator(`tr:has-text("${permUsername}")`);
      await row.locator("text=Edit").click();

      // Add permission
      await page.fill('input[name="cache_name"]', "test-cache");
      await page.check('input[name="can_pull"]');
      await page.check('input[name="can_push"]');

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/permissions") && res.status() < 400),
        page.click("text=Add Permission"),
      ]);

      // Permission should appear in the table
      await expect(page.locator("code:has-text('test-cache')")).toBeVisible();
      await expect(page.locator(".badge:has-text('pull')")).toBeVisible();
      await expect(page.locator(".badge:has-text('push')")).toBeVisible();
    });

    test("admin can add wildcard permissions", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const adminUsername = uniqueUsername("admin");
      await page.fill("#username", adminUsername);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/admin/users");
      await page.click("text=Create User");

      const wildcardUser = uniqueUsername("wilduser");
      await page.fill('input[name="username"]', wildcardUser);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/admin/users") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      const row = page.locator(`tr:has-text("${wildcardUser}")`);
      await row.locator("text=Edit").click();

      // Add wildcard permission
      await page.fill('input[name="cache_name"]', "team-*");
      await page.check('input[name="can_pull"]');

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/permissions") && res.status() < 400),
        page.click("text=Add Permission"),
      ]);

      await expect(page.locator("code:has-text('team-*')")).toBeVisible();
    });

    test("admin can remove permissions", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const adminUsername = uniqueUsername("admin");
      await page.fill("#username", adminUsername);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      await page.goto("/ui/admin/users");
      await page.click("text=Create User");

      const removePermUser = uniqueUsername("removeperm");
      await page.fill('input[name="username"]', removePermUser);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/admin/users") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      const row = page.locator(`tr:has-text("${removePermUser}")`);
      await row.locator("text=Edit").click();

      // Add permission
      await page.fill('input[name="cache_name"]', "removable-cache");
      await page.check('input[name="can_pull"]');

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/permissions") && res.status() < 400),
        page.click("text=Add Permission"),
      ]);

      // Verify it exists
      await expect(page.locator("code:has-text('removable-cache')")).toBeVisible();

      // Remove it
      page.on("dialog", (dialog) => dialog.accept());
      await page.click("text=Remove");

      await page.waitForLoadState("networkidle");

      // Should be gone
      await expect(page.locator("code:has-text('removable-cache')")).not.toBeVisible();
    });
  });

  test.describe("Passkeys display", () => {
    test("shows passkey info for registered users", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const adminUsername = uniqueUsername("admin");
      await page.fill("#username", adminUsername);
      await page.fill("#credential_name", "Test Device");

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // View own user detail
      await page.goto("/ui/admin/users");

      const row = page.locator(`tr:has-text("${adminUsername}")`);
      await row.locator("text=Edit").click();

      // Should show passkeys section with the device
      await expect(page.locator("text=Passkeys")).toBeVisible();
      await expect(page.locator("text=Test Device")).toBeVisible();
    });

    test("shows empty state for users without passkeys", async ({ page, context }) => {
      await setupWebAuthn(context, page);
      await page.goto("/ui/register");

      const firstUserBadge = page.locator(".badge-info");
      if (!(await firstUserBadge.isVisible())) {
        test.skip();
        return;
      }

      const adminUsername = uniqueUsername("admin");
      await page.fill("#username", adminUsername);

      await Promise.all([
        page.waitForURL(/\/ui/, { timeout: 15000 }),
        page.click("#register-btn"),
      ]);

      // Create a user without passkey (via admin)
      await page.goto("/ui/admin/users");
      await page.click("text=Create User");

      const noPasskeyUser = uniqueUsername("nopasskey");
      await page.fill('input[name="username"]', noPasskeyUser);

      await Promise.all([
        page.waitForResponse((res) => res.url().includes("/ui/admin/users") && res.status() < 400),
        page.click('button:has-text("Create")'),
      ]);

      const row = page.locator(`tr:has-text("${noPasskeyUser}")`);
      await row.locator("text=Edit").click();

      // Should show empty state for passkeys
      await expect(
        page.locator("text=No passkeys registered")
      ).toBeVisible();
    });
  });
});
