use std::collections::HashSet;

use axum::extract::{Extension, Json};
use tracing::instrument;

use crate::database::queries;
use crate::error::ServerResult;
use crate::{RequestState, State};
use attic::api::v1::get_missing_paths::{GetMissingPathsRequest, GetMissingPathsResponse};
use attic::nix_store::StorePathHash;

/// Gets information on missing paths in a cache.
///
/// Requires "push" permission as it essentially allows probing
/// of cache contents.
#[instrument(skip_all, fields(payload))]
pub(crate) async fn get_missing_paths(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Json(payload): Json<GetMissingPathsRequest>,
) -> ServerResult<Json<GetMissingPathsResponse>> {
    let database = state.database().await?;
    req_state
        .auth
        .auth_cache(database, &payload.cache, |_, permission| {
            permission.require_push()?;
            Ok(())
        })
        .await?;

    let requested_hashes: HashSet<String> = payload
        .store_path_hashes
        .iter()
        .map(|h| h.as_str().to_owned())
        .collect();

    let requested_hashes_vec: Vec<String> = requested_hashes.iter().cloned().collect();

    let found_hashes: HashSet<String> = queries::find_objects_by_store_path_hashes(
        database,
        payload.cache.as_str(),
        &requested_hashes_vec,
    )
    .await?
    .into_iter()
    .collect();

    // Safety: All requested_hashes are validated `StorePathHash`es.
    // No need to pay the cost of checking again
    #[allow(unsafe_code)]
    let missing_paths = requested_hashes
        .difference(&found_hashes)
        .map(|h| unsafe { StorePathHash::new_unchecked(h.to_string()) })
        .collect();

    Ok(Json(GetMissingPathsResponse { missing_paths }))
}
