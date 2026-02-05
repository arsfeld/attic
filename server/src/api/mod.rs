//! HTTP API.

mod binary_cache;
mod v1;
pub mod web_ui;

use axum::{response::Html, routing::get, Router};

use crate::config::WebUiConfig;
use crate::State;

async fn placeholder() -> Html<&'static str> {
    Html(include_str!("placeholder.html"))
}

/// Returns the router with optional web UI.
pub(crate) fn get_router_with_web_ui(web_ui_config: &WebUiConfig, app_state: State) -> Router {
    let mut router = Router::new()
        .route("/", get(placeholder))
        .merge(binary_cache::get_router())
        .merge(v1::get_router());

    // Add web UI routes if enabled
    if let Some(web_ui_router) = web_ui::get_router(web_ui_config, app_state) {
        router = router.merge(web_ui_router);
    }

    router
}
