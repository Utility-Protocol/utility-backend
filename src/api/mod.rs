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

use std::sync::Arc;
use sqlx::PgPool;
use tokio::sync::Mutex;
use crate::soroban::sequencer::NonceSequencer;
use crate::soroban::rpc::CircuitBreaker;

#[derive(Clone)]
pub struct AppState {
    pub sequencer: Arc<NonceSequencer>,
    pub pool: PgPool,
    pub breaker: Arc<Mutex<CircuitBreaker>>,
#[derive(Clone)]
pub struct AppState {
    pub sequencer: Arc<NonceSequencer>,
    pub db_pool: Pool<Postgres>,
    pub advisory_lock: Arc<AdvisoryLock>,
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
