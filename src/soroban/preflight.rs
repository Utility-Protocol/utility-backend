use lru::LruCache;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::num::NonZeroUsize;
use std::sync::Mutex;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightResult {
    pub footprint: u64,
    pub instructions: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub recommended_fee: u64,
}

#[derive(Debug, Clone)]
pub struct PreflightConfig {
    pub safety_margin: f64,
    pub max_iterations: u32,
    pub min_leeway_fraction: f64,
    pub max_leeway_fraction: f64,
    pub cache_max_entries: usize,
}

impl Default for PreflightConfig {
    fn default() -> Self {
        Self {
            safety_margin: 0.10,
            max_iterations: 3,
            min_leeway_fraction: 0.10,
            max_leeway_fraction: 0.50,
            cache_max_entries: 10_000,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CacheKey {
    contract_id: String,
    op_hash: [u8; 32],
}

struct PreflightCache {
    inner: Mutex<LruCache<CacheKey, PreflightResult>>,
}

impl PreflightCache {
    fn new(max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(NonZeroUsize::new(max_entries).unwrap())),
        }
    }

    fn get(&self, key: &CacheKey) -> Option<PreflightResult> {
        self.inner.lock().ok()?.get(key).cloned()
    }

    fn put(&self, key: CacheKey, result: PreflightResult) {
        if let Ok(mut cache) = self.inner.lock() {
            cache.put(key, result);
        }
    }
}

lazy_static::lazy_static! {
    static ref PREFLIGHT_CACHE: PreflightCache = PreflightCache::new(PreflightConfig::default().cache_max_entries);
}

fn compute_cache_key(contract_id: &str, operation_xdr: &str) -> CacheKey {
    let mut hasher = Sha256::new();
    hasher.update(contract_id.as_bytes());
    hasher.update(operation_xdr.as_bytes());
    let op_hash = hasher.finalize().into();
    CacheKey {
        contract_id: contract_id.to_string(),
        op_hash,
    }
}

pub async fn run_preflight_raw(
    rpc_url: &str,
    operation_xdr: &str,
    instruction_leeway: u64,
) -> Result<PreflightResult, &'static str> {
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "simulateTransaction",
        "params": {
            "transaction": operation_xdr,
            "resourceConfig": {
                "instructionLeeway": instruction_leeway
            }
        }
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(rpc_url)
        .json(&payload)
        .send()
        .await
        .map_err(|_| "preflight request failed")?;

    let result: PreflightResult = resp.json().await.map_err(|_| "preflight parse failure")?;
    info!(
        footprint = result.footprint,
        instructions = result.instructions,
        "preflight simulation complete"
    );
    Ok(result)
}

pub async fn run_preflight(
    rpc_url: &str,
    contract_id: &str,
    operation_xdr: &str,
    config: &PreflightConfig,
) -> Result<PreflightResult, &'static str> {
    let cache_key = compute_cache_key(contract_id, operation_xdr);

    if let Some(cached) = PREFLIGHT_CACHE.get(&cache_key) {
        info!("preflight cache hit for contract {}", contract_id);
        return Ok(apply_safety_margin(&cached, config.safety_margin));
    }

    let mut best_result: Option<PreflightResult> = None;
    let mut leeway = 1_000_000u64;

    for iteration in 0..config.max_iterations {
        let result = run_preflight_raw(rpc_url, operation_xdr, leeway).await?;

        let actual_instr = result.instructions;
        let headroom = leeway as f64 / actual_instr.max(1) as f64;

        if headroom < config.min_leeway_fraction {
            leeway = (actual_instr as f64 * config.max_leeway_fraction).ceil() as u64;
        } else if headroom > config.max_leeway_fraction && iteration > 0 {
            leeway = (actual_instr as f64 * config.min_leeway_fraction).ceil() as u64;
        }

        best_result = Some(result);

        info!(
            iteration,
            instructions = actual_instr,
            leeway,
            "preflight optimization round"
        );
    }

    let mut final_result = best_result.ok_or("preflight returned no result")?;
    final_result.recommended_fee =
        (final_result.recommended_fee as f64 * (1.0 + config.safety_margin)).ceil() as u64;

    PREFLIGHT_CACHE.put(cache_key, final_result.clone());
    Ok(apply_safety_margin(&final_result, config.safety_margin))
}

fn apply_safety_margin(result: &PreflightResult, margin: f64) -> PreflightResult {
    let numerator = (margin * 100.0 + 100.0).round() as u64;
    let scale = |v: u64| v * numerator;
    PreflightResult {
        footprint: scale(result.footprint).div_ceil(100),
        instructions: scale(result.instructions).div_ceil(100),
        read_bytes: scale(result.read_bytes).div_ceil(100),
        write_bytes: scale(result.write_bytes).div_ceil(100),
        recommended_fee: scale(result.recommended_fee).div_ceil(100),
    }
}

pub fn budget_optimizer(
    initial_estimate: u64,
    success_fn: impl Fn(u64) -> bool,
    max_iterations: u32,
) -> u64 {
    let mut low = (initial_estimate as f64 * 0.5).ceil() as u64;
    let mut high = (initial_estimate as f64 * 2.0).ceil() as u64;
    let mut best = high;

    for _ in 0..max_iterations {
        let mid = low + (high - low) / 2;
        if success_fn(mid) {
            best = mid;
            if mid == low {
                break;
            }
            high = mid;
        } else {
            if mid == high {
                break;
            }
            low = mid.saturating_add(1);
        }
    }

    best
}

pub fn clear_cache() {
    if let Ok(mut cache) = PREFLIGHT_CACHE.inner.lock() {
        cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_safety_margin() {
        let result = PreflightResult {
            footprint: 1000,
            instructions: 500,
            read_bytes: 200,
            write_bytes: 100,
            recommended_fee: 50,
        };
        let padded = apply_safety_margin(&result, 0.10);
        assert_eq!(padded.footprint, 1100);
        assert_eq!(padded.instructions, 550);
        assert_eq!(padded.recommended_fee, 55);
    }

    #[test]
    fn test_apply_safety_margin_rounds_up() {
        let result = PreflightResult {
            footprint: 1,
            instructions: 1,
            read_bytes: 1,
            write_bytes: 1,
            recommended_fee: 1,
        };
        let padded = apply_safety_margin(&result, 0.10);
        assert_eq!(padded.footprint, 2); // ceil(1.1) = 2
    }

    #[test]
    fn test_cache_key_uniqueness() {
        let k1 = compute_cache_key("contract-a", "xdr-data");
        let k2 = compute_cache_key("contract-b", "xdr-data");
        let k3 = compute_cache_key("contract-a", "xdr-data");
        assert_ne!(k1, k2);
        assert_eq!(k1, k3);
    }

    #[test]
    fn test_cache_put_get() {
        let cache = PreflightCache::new(100);
        let key = compute_cache_key("test", "op");
        let result = PreflightResult {
            footprint: 500,
            instructions: 300,
            read_bytes: 100,
            write_bytes: 50,
            recommended_fee: 25,
        };
        cache.put(key.clone(), result.clone());
        let retrieved = cache.get(&key);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().footprint, 500);
    }

    #[test]
    fn test_cache_miss() {
        let cache = PreflightCache::new(100);
        let key = compute_cache_key("unknown", "op");
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_budget_optimizer_converges_to_minimum() {
        // Mock success function: succeeds if budget >= 750
        let success = |budget: u64| budget >= 750;

        let optimum = budget_optimizer(1000, success, 10);
        // 0.5*1000=500, 2.0*1000=2000
        // mid=1250 -> success, best=1250, high=1250
        // mid=875 -> success, best=875, high=875
        // mid=687 -> fail, low=688
        // mid=781 -> success, best=781, high=781
        // mid=734 -> fail, low=735
        // mid=758 -> success, best=758, high=758
        // low=735, high=758, mid=746 -> fail, low=747
        // low=747, high=758, mid=752 -> success, best=752, high=752
        // low=747, high=752, mid=749 -> fail, low=750
        // low=750, high=752, mid=751 -> success, best=751, high=751
        assert!(optimum >= 750);
        assert!(optimum <= 1000);
    }

    #[test]
    fn test_budget_optimizer_always_succeeds() {
        let success = |_budget: u64| true;
        let optimum = budget_optimizer(1000, success, 10);
        assert!((500..=501).contains(&optimum));
    }

    #[test]
    fn test_default_config_is_reasonable() {
        let config = PreflightConfig::default();
        assert!((config.safety_margin - 0.10).abs() < 1e-9);
        assert_eq!(config.max_iterations, 3);
        assert_eq!(config.cache_max_entries, 10_000);
    }
}
