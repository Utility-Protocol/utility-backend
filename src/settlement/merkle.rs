pub const HASH_LEN: usize = 32;
pub const MAX_BATCH_LEAVES: usize = 16_384;
pub const MAX_PROOF_LEAVES: usize = 1 << 20;

pub type Hash = [u8; HASH_LEN];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Leaf {
    pub meter_id: u64,
    pub timestamp_ms: i64,
    pub commodity_type: u8,
    pub scaled_reading: i128,
    pub nonce: u64,
    pub hlc_timestamp: u64,
}

impl Leaf {
    pub fn encode_scale_like(&self) -> [u8; 49] {
        let mut out = [0u8; 49];
        out[0..8].copy_from_slice(&self.meter_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.timestamp_ms.to_le_bytes());
        out[16] = self.commodity_type;
        out[17..33].copy_from_slice(&self.scaled_reading.to_le_bytes());
        out[33..41].copy_from_slice(&self.nonce.to_le_bytes());
        out[41..49].copy_from_slice(&self.hlc_timestamp.to_le_bytes());
        out
    }

    pub fn hash(&self) -> Hash {
        hash_tagged(b"ULB_LEAF_V1", &self.encode_scale_like())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiblingSide {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofStep {
    pub hash: Hash,
    pub side: SiblingSide,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleProof {
    pub leaf_index: usize,
    pub leaf_count: usize,
    pub path: Vec<ProofStep>,
}

#[derive(Debug, Clone)]
pub struct MerkleTree {
    levels: Vec<Vec<Hash>>,
    leaf_count: usize,
}

impl MerkleTree {
    pub fn new(leaves: &[Leaf]) -> Result<Self, &'static str> {
        if leaves.is_empty() {
            return Err("merkle tree requires at least one leaf");
        }
        if leaves.len() > MAX_BATCH_LEAVES {
            return Err("merkle batch exceeds maximum leaf count");
        }

        let mut levels = Vec::new();
        levels.push(leaves.iter().map(Leaf::hash).collect::<Vec<_>>());

        while levels.last().expect("level exists").len() > 1 {
            let current = levels.last().expect("level exists");
            let mut next = Vec::with_capacity(current.len().div_ceil(2));
            for pair in current.chunks(2) {
                let right = if pair.len() == 2 { pair[1] } else { pair[0] };
                next.push(hash_children(&pair[0], &right));
            }
            levels.push(next);
        }

        Ok(Self {
            levels,
            leaf_count: leaves.len(),
        })
    }

    pub fn root(&self) -> Hash {
        self.levels.last().expect("root level exists")[0]
    }

    pub fn leaf_count(&self) -> usize {
        self.leaf_count
    }

    pub fn is_empty(&self) -> bool {
        self.leaf_count == 0
    }

    pub fn prove(&self, leaf_index: usize) -> Option<MerkleProof> {
        if leaf_index >= self.leaf_count {
            return None;
        }

        let mut index = leaf_index;
        let mut path = Vec::with_capacity(self.levels.len().saturating_sub(1));
        for level in &self.levels[..self.levels.len() - 1] {
            let is_right = index % 2 == 1;
            let sibling_index = if is_right { index - 1 } else { index + 1 };
            let sibling_hash = level.get(sibling_index).copied().unwrap_or(level[index]);
            path.push(ProofStep {
                hash: sibling_hash,
                side: if is_right {
                    SiblingSide::Left
                } else {
                    SiblingSide::Right
                },
            });
            index /= 2;
        }

        Some(MerkleProof {
            leaf_index,
            leaf_count: self.leaf_count,
            path,
        })
    }

    pub fn verify(root: Hash, leaf: &Leaf, proof: &MerkleProof) -> bool {
        verify(root, leaf, proof)
    }
}

pub fn verify(root: Hash, leaf: &Leaf, proof: &MerkleProof) -> bool {
    if proof.leaf_count == 0
        || proof.leaf_count > MAX_PROOF_LEAVES
        || proof.leaf_index >= proof.leaf_count
    {
        return false;
    }

    let max_depth = usize::BITS as usize - (proof.leaf_count - 1).leading_zeros() as usize;
    if proof.path.len() != max_depth {
        return false;
    }

    let mut computed = leaf.hash();
    for step in &proof.path {
        computed = match step.side {
            SiblingSide::Left => hash_children(&step.hash, &computed),
            SiblingSide::Right => hash_children(&computed, &step.hash),
        };
    }
    computed == root
}

pub fn hash_children(left: &Hash, right: &Hash) -> Hash {
    let mut bytes = [0u8; 64];
    bytes[..32].copy_from_slice(left);
    bytes[32..].copy_from_slice(right);
    hash_tagged(b"ULB_NODE_V1", &bytes)
}

fn hash_tagged(tag: &[u8], payload: &[u8]) -> Hash {
    let mut input = Vec::with_capacity(tag.len() + payload.len());
    input.extend_from_slice(tag);
    input.extend_from_slice(payload);
    blake2b_256(&input)
}

const BLAKE2B_IV: [u64; 8] = [
    0x6a09e667f3bcc908,
    0xbb67ae8584caa73b,
    0x3c6ef372fe94f82b,
    0xa54ff53a5f1d36f1,
    0x510e527fade682d1,
    0x9b05688c2b3e6c1f,
    0x1f83d9abfb41bd6b,
    0x5be0cd19137e2179,
];
const SIGMA: [[usize; 16]; 12] = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
    [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
    [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
    [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
    [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
    [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
    [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
    [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
    [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
];

fn blake2b_256(input: &[u8]) -> Hash {
    debug_assert!(input.len() <= 128);
    let mut h = BLAKE2B_IV;
    h[0] ^= 0x0101_0020; // digest length 32, key length 0, fanout 1, depth 1
    let mut block = [0u8; 128];
    block[..input.len()].copy_from_slice(input);
    let mut m = [0u64; 16];
    for (i, chunk) in block.chunks_exact(8).enumerate() {
        m[i] = u64::from_le_bytes(chunk.try_into().expect("8-byte chunk"));
    }
    let mut v = [0u64; 16];
    v[..8].copy_from_slice(&h);
    v[8..].copy_from_slice(&BLAKE2B_IV);
    v[12] ^= input.len() as u64;
    v[14] = !v[14];

    macro_rules! g {
        ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $y:expr) => {{
            v[$a] = v[$a].wrapping_add(v[$b]).wrapping_add($x);
            v[$d] = (v[$d] ^ v[$a]).rotate_right(32);
            v[$c] = v[$c].wrapping_add(v[$d]);
            v[$b] = (v[$b] ^ v[$c]).rotate_right(24);
            v[$a] = v[$a].wrapping_add(v[$b]).wrapping_add($y);
            v[$d] = (v[$d] ^ v[$a]).rotate_right(16);
            v[$c] = v[$c].wrapping_add(v[$d]);
            v[$b] = (v[$b] ^ v[$c]).rotate_right(63);
        }};
    }
    for s in SIGMA {
        g!(0, 4, 8, 12, m[s[0]], m[s[1]]);
        g!(1, 5, 9, 13, m[s[2]], m[s[3]]);
        g!(2, 6, 10, 14, m[s[4]], m[s[5]]);
        g!(3, 7, 11, 15, m[s[6]], m[s[7]]);
        g!(0, 5, 10, 15, m[s[8]], m[s[9]]);
        g!(1, 6, 11, 12, m[s[10]], m[s[11]]);
        g!(2, 7, 8, 13, m[s[12]], m[s[13]]);
        g!(3, 4, 9, 14, m[s[14]], m[s[15]]);
    }
    for i in 0..8 {
        h[i] ^= v[i] ^ v[i + 8];
    }
    let mut out = [0u8; HASH_LEN];
    for (chunk, word) in out.chunks_exact_mut(8).zip(h) {
        chunk.copy_from_slice(&word.to_le_bytes());
    }
    out
}
