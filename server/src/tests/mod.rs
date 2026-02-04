//! Integration tests for the Attic server.
//!
//! These tests use in-process testing with `tower::ServiceExt::oneshot()`
//! for fast execution without needing a TCP server.

mod helpers;

mod api;
mod auth;
mod e2e;
mod storage;
