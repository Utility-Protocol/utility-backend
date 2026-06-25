//! TimescaleDB-backed storage: workload-priority-partitioned connection pooling.
//!
//! Priority map (issue #52):
//!
//! | Class      | Workload                                            |
//! |------------|-----------------------------------------------------|
//! | `Critical` | settlement finalization, Soroban calls              |
//! | `High`     | tariff evaluation, watermark persistence            |
//! | `Normal`   | telemetry ingestion writes                          |
//! | `Low`      | admin queries, reporting, debugging                 |

pub mod pool;
pub mod pool_partitioned;
pub mod priority;
