use crate::api::middleware::{DynamicRateLimiter, TenantRateLimiter};
use crate::gateway::lock::AdvisoryLock;
use crate::soroban::rpc::CircuitBreaker;
use crate::soroban::sequencer::NonceSequencer;
use axum::extract::FromRef;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod alloc_tracker;
pub mod handlers;
pub mod metrics;
pub mod middleware;
pub mod router;
pub mod slo_state;

#[derive(Clone)]
pub struct AppState {
    pub sequencer: Arc<NonceSequencer>,
    pub pool: Pool<Postgres>,
    pub advisory_lock: Arc<AdvisoryLock>,
    pub breaker: Arc<Mutex<CircuitBreaker>>,
    pub rate_limiter: Arc<DynamicRateLimiter>,
    pub tenant_rate_limiter: Arc<TenantRateLimiter>,
}

impl FromRef<AppState> for Arc<NonceSequencer> {
    fn from_ref(state: &AppState) -> Self {
        state.sequencer.clone()
    }
}

impl FromRef<AppState> for Pool<Postgres> {
    fn from_ref(state: &AppState) -> Self {
        state.pool.clone()
    }
}

impl FromRef<AppState> for Arc<AdvisoryLock> {
    fn from_ref(state: &AppState) -> Self {
        state.advisory_lock.clone()
    }
}

impl FromRef<AppState> for Arc<Mutex<CircuitBreaker>> {
    fn from_ref(state: &AppState) -> Self {
        state.breaker.clone()
    }
}

impl FromRef<AppState> for Arc<DynamicRateLimiter> {
    fn from_ref(state: &AppState) -> Self {
        state.rate_limiter.clone()
    }
}

impl FromRef<AppState> for Arc<TenantRateLimiter> {
    fn from_ref(state: &AppState) -> Self {
        state.tenant_rate_limiter.clone()
    }
}
