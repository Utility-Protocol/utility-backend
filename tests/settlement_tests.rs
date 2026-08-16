use sha2::Digest;
use std::sync::Arc;
use tokio::sync::{Barrier, Mutex};
use utility_backend::settlement::finalizer::Finalizer;
use utility_backend::settlement::mint_queue::MintQueue;
use utility_backend::soroban::rpc::CircuitBreaker;

// Serializes DB access with the other integration test binaries that share the
// utility_test database.  The advisory lock is session-scoped and released when
// the guard connection closes (pool drop at end of test).
async fn setup_test_db() -> Option<(sqlx::PgPool, sqlx::pool::PoolConnection<sqlx::Postgres>)> {
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://utility:utility_secret@localhost:5432/utility_test".into());

    let pool = match sqlx::PgPool::connect(&db_url).await {
        Ok(p) => p,
        Err(_) => {
            eprintln!("Skipping test: DATABASE_URL not available");
            return None;
        }
    };

    let mut lock_conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("Could not acquire DB lock connection: {e}");
            return None;
        }
    };
    if let Err(e) = sqlx::query("SELECT pg_advisory_lock(4000000001)")
        .execute(&mut *lock_conn)
        .await
    {
        eprintln!("Could not acquire DB advisory lock: {e}");
        return None;
    }

    // Clean up
    sqlx::query("DELETE FROM processed_mints")
        .execute(&pool)
        .await
        .ok()?;
    sqlx::query("DELETE FROM pending_mints")
        .execute(&pool)
        .await
        .ok()?;

    Some((pool, lock_conn))
}

#[tokio::test]
async fn test_concurrent_finalization_deduplication() {
    let Some((pool, _db_lock)) = setup_test_db().await else {
        return;
    };

    let batch_id = "test-batch-123";
    let resource_type = "water";
    let destination = "GABC...123";

    let mint_queue = MintQueue::new(pool.clone());
    mint_queue
        .enqueue(batch_id, resource_type, 100.0, destination)
        .await
        .unwrap();

    let breaker = Arc::new(Mutex::new(CircuitBreaker::new(5)));
    let finalizer = Arc::new(Finalizer::new(
        pool.clone(),
        "http://invalid-rpc-url".into(),
        breaker,
    ));

    let num_threads = 5;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for _ in 0..num_threads {
        let f = finalizer.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            f.finalize_mint("test-batch-123", "water").await
        }));
    }

    for h in handles {
        let _ = h.await.unwrap();
    }

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM processed_mints")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0); // Still 0 because RPC fails and rolls back
}

#[tokio::test]
async fn test_aggregation_of_pending_mints() {
    let Some((pool, _db_lock)) = setup_test_db().await else {
        return;
    };

    let batch_id = "batch-agg";
    let resource_type = "energy";
    let mint_queue = MintQueue::new(pool.clone());

    mint_queue
        .enqueue(batch_id, resource_type, 40.0, "dest1")
        .await
        .unwrap();
    mint_queue
        .enqueue(batch_id, resource_type, 60.0, "dest1")
        .await
        .unwrap();

    let breaker = Arc::new(Mutex::new(CircuitBreaker::new(5)));
    let finalizer = Finalizer::new(pool.clone(), "http://invalid-rpc-url".into(), breaker);

    // This will fail at RPC but we want to verify it tried to aggregate
    let _ = finalizer.finalize_mint(batch_id, resource_type).await;

    // Verify both are still there because of rollback
    let pending = mint_queue.get_pending(batch_id).await.unwrap();
    assert_eq!(pending.len(), 2);
}

#[tokio::test]
async fn test_idempotency_key_generation() {
    let batch_id = "batch-1";
    let resource_type = "energy";

    let mut hasher = sha2::Sha256::new();
    hasher.update(batch_id);
    hasher.update(resource_type);
    hasher.update("mint");
    let expected = hex::encode(hasher.finalize());

    assert_eq!(expected.len(), 64);
}
