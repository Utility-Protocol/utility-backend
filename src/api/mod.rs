use crate::api::middleware::DynamicRateLimiter;
use crate::gateway::lock::AdvisoryLock;
use crate::soroban::sequencer::NonceSequencer;
use axum::extract::FromRef;
use sqlx::{Pool, Postgres};
use std::sync::Arc;

pub mod alloc_tracker;
pub mod handlers;
pub mod metrics;
pub mod middleware;
pub mod router;

#[derive(Clone)]
pub struct AppState {
    pub sequencer: Arc<NonceSequencer>,
    pub db_pool: Pool<Postgres>,
    pub advisory_lock: Arc<AdvisoryLock>,
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

impl FromRef<AppState> for Arc<AdvisoryLock> {
    fn from_ref(state: &AppState) -> Self {
        state.advisory_lock.clone()
    }
}

impl FromRef<AppState> for Arc<DynamicRateLimiter> {
    fn from_ref(state: &AppState) -> Self {
        state.rate_limiter.clone()
    }
}
