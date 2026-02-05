//! WebAuthn (passkey) integration for the web UI.
//!
//! This module handles the cryptographic challenge-response flow for passkey
//! registration and authentication.

use dashmap::DashMap;
use url::Url;
use webauthn_rs::prelude::*;

/// WebAuthn state holding the Webauthn instance and in-memory challenge storage.
pub struct WebAuthnState {
    /// The WebAuthn instance.
    webauthn: Webauthn,

    /// In-memory storage for registration challenges.
    /// Key: username, Value: (PasskeyRegistration, expiry timestamp)
    reg_challenges: DashMap<String, (PasskeyRegistration, i64)>,

    /// In-memory storage for authentication challenges.
    /// Key: username, Value: (PasskeyAuthentication, expiry timestamp)
    auth_challenges: DashMap<String, (PasskeyAuthentication, i64)>,
}

impl WebAuthnState {
    /// Creates a new WebAuthn state.
    ///
    /// # Arguments
    /// * `rp_id` - The Relying Party ID (usually the domain, e.g., "cache.example.com")
    /// * `rp_origin` - The Relying Party origin URL (e.g., "https://cache.example.com")
    pub fn new(rp_id: &str, rp_origin: &str) -> Result<Self, WebauthnError> {
        let rp_origin_url = Url::parse(rp_origin).map_err(|_| WebauthnError::Configuration)?;

        let builder = WebauthnBuilder::new(rp_id, &rp_origin_url)?.rp_name("Attic Binary Cache");

        let webauthn = builder.build()?;

        Ok(Self {
            webauthn,
            reg_challenges: DashMap::new(),
            auth_challenges: DashMap::new(),
        })
    }

    /// Starts passkey registration for a user.
    ///
    /// Returns the challenge to send to the browser.
    pub fn start_registration(
        &self,
        user_id: Uuid,
        username: &str,
        display_name: &str,
        exclude_credentials: Vec<CredentialID>,
    ) -> Result<CreationChallengeResponse, WebauthnError> {
        // Clean up expired challenges
        self.cleanup_expired();

        let (challenge, reg_state) = self.webauthn.start_passkey_registration(
            user_id,
            username,
            display_name,
            Some(exclude_credentials),
        )?;

        // Store the registration state (expires in 60 seconds)
        let expiry = chrono::Utc::now().timestamp() + 60;
        self.reg_challenges
            .insert(username.to_string(), (reg_state, expiry));

        Ok(challenge)
    }

    /// Finishes passkey registration.
    ///
    /// Validates the response from the browser and returns the passkey.
    pub fn finish_registration(
        &self,
        username: &str,
        response: &RegisterPublicKeyCredential,
    ) -> Result<Passkey, WebauthnError> {
        let (reg_state, _expiry) = self
            .reg_challenges
            .remove(username)
            .map(|(_, v)| v)
            .ok_or(WebauthnError::ChallengeNotFound)?;

        self.webauthn
            .finish_passkey_registration(response, &reg_state)
    }

    /// Starts passkey authentication for a user.
    ///
    /// Returns the challenge to send to the browser.
    pub fn start_authentication(
        &self,
        username: &str,
        credentials: Vec<Passkey>,
    ) -> Result<RequestChallengeResponse, WebauthnError> {
        // Clean up expired challenges
        self.cleanup_expired();

        if credentials.is_empty() {
            return Err(WebauthnError::CredentialNotFound);
        }

        let (challenge, auth_state) = self.webauthn.start_passkey_authentication(&credentials)?;

        // Store the authentication state (expires in 60 seconds)
        let expiry = chrono::Utc::now().timestamp() + 60;
        self.auth_challenges
            .insert(username.to_string(), (auth_state, expiry));

        Ok(challenge)
    }

    /// Finishes passkey authentication.
    ///
    /// Validates the response from the browser and returns the authentication result.
    pub fn finish_authentication(
        &self,
        username: &str,
        response: &PublicKeyCredential,
    ) -> Result<AuthenticationResult, WebauthnError> {
        let (auth_state, _expiry) = self
            .auth_challenges
            .remove(username)
            .map(|(_, v)| v)
            .ok_or(WebauthnError::ChallengeNotFound)?;

        self.webauthn
            .finish_passkey_authentication(response, &auth_state)
    }

    /// Cleans up expired challenges.
    fn cleanup_expired(&self) {
        let now = chrono::Utc::now().timestamp();

        self.reg_challenges.retain(|_, (_, expiry)| *expiry > now);
        self.auth_challenges.retain(|_, (_, expiry)| *expiry > now);
    }
}

/// Converts stored credential data to a Passkey for authentication.
///
/// We store the entire serialized Passkey in the public_key column (despite the name).
/// The credential_id column is used for lookups, but the full Passkey is reconstructed
/// from the serialized data.
pub fn credential_to_passkey(
    _credential_id: &[u8],
    serialized_passkey: &[u8],
    _counter: u32,
) -> Result<Passkey, String> {
    // Deserialize the full Passkey struct
    // The "danger-allow-state-serialisation" feature enables serde support
    serde_json::from_slice(serialized_passkey)
        .map_err(|e| format!("Failed to deserialize passkey: {}", e))
}

/// Serializes a Passkey for storage.
///
/// We serialize the entire Passkey struct to preserve all internal state.
pub fn passkey_to_stored_key(passkey: &Passkey) -> Result<Vec<u8>, String> {
    serde_json::to_vec(passkey).map_err(|e| format!("Failed to serialize passkey: {}", e))
}
