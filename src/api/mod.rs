use crate::soroban::sequencer::NonceSequencer;
use axum::extract::FromRef;
use sqlx::{Pool, Postgres};
use std::sync::Arc;

use middleware::DynamicRateLimiter;

pub mod alloc_tracker;
pub mod handlers;
pub mod metrics;
pub mod middleware;
pub mod router;

#[derive(Clone)]
pub struct AppState {
    pub sequencer: Arc<NonceSequencer>,
    pub db_pool: Pool<Postgres>,
    pub rate_limiter: Arc<DynamicRateLimiter>,
}

impl FromRef<AppState> for Arc<NonceSequencer> {
    fn from_ref(state: &AppState) -> Self {
        state.sequencer.clone()
    }
}

impl FromRef<AppState> for Pool<Postgres> {
    fn from_ref(state: &AppState) -> Self {
        state.db_pool.clone()
    }
}

impl FromRef<AppState> for Arc<DynamicRateLimiter> {
    fn from_ref(state: &AppState) -> Self {
        state.rate_limiter.clone()
    }
}
