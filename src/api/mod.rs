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
}
