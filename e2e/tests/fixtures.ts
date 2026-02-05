import { test as base, expect, type BrowserContext, type Page } from "@playwright/test";

/**
 * WebAuthn virtual authenticator configuration.
 */
interface VirtualAuthenticator {
  authenticatorId: string;
}

/**
 * Extended test fixtures with WebAuthn support.
 */
export const test = base.extend<{
  authenticator: VirtualAuthenticator;
  authenticatedPage: Page;
}>({
  /**
   * Sets up a virtual authenticator for WebAuthn testing.
   * This allows testing passkey registration and authentication without a physical device.
   */
  authenticator: async ({ context }, use) => {
    const cdpSession = await context.newCDPSession(await context.newPage());

    // Enable WebAuthn testing
    await cdpSession.send("WebAuthn.enable");

    // Add a virtual authenticator that simulates a platform authenticator (like Touch ID)
    const { authenticatorId } = await cdpSession.send(
      "WebAuthn.addVirtualAuthenticator",
      {
        options: {
          protocol: "ctap2",
          transport: "internal",
          hasResidentKey: true,
          hasUserVerification: true,
          isUserVerified: true,
        },
      }
    );

    await use({ authenticatorId });

    // Cleanup
    await cdpSession.send("WebAuthn.removeVirtualAuthenticator", {
      authenticatorId,
    });
  },

  /**
   * A page with WebAuthn enabled for the entire test.
   */
  authenticatedPage: async ({ context, authenticator }, use) => {
    const page = await context.newPage();

    // Enable WebAuthn on this page's CDP session
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

    await use(page);
  },
});

export { expect };

/**
 * Helper to set up WebAuthn on a specific page.
 */
export async function setupWebAuthn(context: BrowserContext, page: Page): Promise<string> {
  const cdpSession = await context.newCDPSession(page);
  await cdpSession.send("WebAuthn.enable");

  const { authenticatorId } = await cdpSession.send(
    "WebAuthn.addVirtualAuthenticator",
    {
      options: {
        protocol: "ctap2",
        transport: "internal",
        hasResidentKey: true,
        hasUserVerification: true,
        isUserVerified: true,
      },
    }
  );

  return authenticatorId;
}

/**
 * Generate a unique username for test isolation.
 */
export function uniqueUsername(prefix: string = "testuser"): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).substring(7)}`;
}

/**
 * Wait for navigation after form submission.
 */
export async function waitForNavigation(page: Page, action: () => Promise<void>): Promise<void> {
  await Promise.all([
    page.waitForURL(/\/ui/, { timeout: 10000 }),
    action(),
  ]);
}
