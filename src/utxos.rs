use crate::blocks::{BlockParser, Options};
use anyhow::Result;
use bitcoin::hashes::Hash;
use bitcoin::{Amount, Block, OutPoint, Transaction, Txid};
use rand::rngs::SmallRng;
use rand::{Error, RngCore, SeedableRng};
use rustc_hash::{FxHashMap, FxHasher};
use scalable_cuckoo_filter::{ScalableCuckooFilter, ScalableCuckooFilterBuilder};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, BufWriter, Read};
use std::iter::Zip;
use std::slice::Iter;
use std::sync::{Arc, Mutex};

type OutPointFilter = ScalableCuckooFilter<ShortOutPoint, FastHasher, FastRng>;
type OutPoints = (Vec<ShortOutPoint>, Vec<ShortOutPoint>);

/// A parser that writes a probabilistic filter of unspent [`ShortOutPoint`]
///
/// We use a [`ScalableCuckooFilter`] which is compact like a bloom filter, but allows us
/// to remove elements and grow/shrink the filter as needed.
/// We use this filter in [`UtxoParser`] to avoid caching UTXOs when tracking input amounts and
/// labeling outputs as [`OutStatus::Spent`] or [`OutStatus::Unspent`]
#[derive(Clone)]
pub struct FilterParser {
    filter: Arc<Mutex<OutPointFilter>>,
}
impl Default for FilterParser {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterParser {
    /// Constructs a new filter with a safe size and false positive rate
    ///
    /// Because we are storing SHA256 hashes from [`Txid`] we use non-cryptographic [`Rng`] and
    /// [`Hasher`] which are optimized for speed without bias.
    pub fn new() -> Self {
        Self {
            filter: Arc::new(Mutex::new(
                ScalableCuckooFilterBuilder::default()
                    .initial_capacity(300_000_000)
                    .false_positive_probability(0.000_000_000_001)
                    .rng(FastRng::default())
                    .hasher(FastHasher::default())
                    .finish(),
            )),
        }
    }

    /// Writes the filter to disk for use in another parser (see [`UtxoParser::new`])
    /// You must call the [`FilterParser::parse`] function first.
    pub fn write(self, output: &str) -> Result<()> {
        let writer = BufWriter::new(File::create(output)?);
        let filter = Arc::try_unwrap(self.filter).unwrap();
        let mut filter = filter.into_inner().unwrap();
        filter.shrink_to_fit();
        postcard::to_io(&filter, writer)?;
        Ok(())
    }
}
impl BlockParser<OutPoints, ()> for FilterParser {
    /// By extracting the [`ShortOutPoint`] here we optimize the memory and computation
    /// required.
    fn extract(&self, block: Block) -> Option<OutPoints> {
        let mut inputs = vec![];
        let mut outputs = vec![];
        for tx in block.txdata.iter() {
            let txid = tx.compute_txid();
            for input in &tx.input {
                inputs.push(ShortOutPoint::new(&input.previous_output));
            }

            for (index, _) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(txid, index as u32);
                outputs.push(ShortOutPoint::new(&outpoint));
            }
        }
        Some((inputs, outputs))
    }

    fn batch(&self, items: Vec<OutPoints>) -> Vec<()> {
        let filter = &mut self.filter.lock().unwrap();

        for (inputs, outputs) in items {
            for output in outputs {
                filter.insert(&output);
            }
            for input in inputs {
                filter.remove(&input);
            }
        }
        vec![]
    }

    /// In order to track UTXO amounts we must process blocks in-order
    fn options() -> Options {
        Options::default().order_output()
    }
}

/// Contains a block that has been parsed with frequently needed UTXO information
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct UtxoBlock {
    /// Underlying parsed block from [`bitcoin::Block`]
    pub block: Block,
    /// Precomputed txids for every transaction
    txids: Vec<Txid>,
    /// Map every tx in this block to the list of input amounts
    input_amounts: FxHashMap<Txid, Vec<Amount>>,
    /// Map every tx in this block to the list of output spent/unspent status
    output_status: FxHashMap<Txid, Vec<OutStatus>>,
}
impl UtxoBlock {
    /// Return all [`Transaction`] with [`Txid`] already calculated
    pub fn transactions(&self) -> Zip<Iter<'_, Transaction>, Iter<'_, Txid>> {
        self.block.txdata.iter().zip(self.txids.iter())
    }

    /// Given a tx in this block, return the in-order list of whether the output was spent/unspent
    pub fn output_status(&self, txid: &Txid) -> &Vec<OutStatus> {
        self.output_status.get(txid).expect("exists")
    }

    /// Given a tx in this block, return the in-order list of the input amounts
    pub fn input_amount(&self, txid: &Txid) -> &Vec<Amount> {
        self.input_amounts.get(txid).expect("exists")
    }
}
/// Indicates what happens to this output
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutStatus {
    /// The output was spent in a later block
    Spent,
    /// The output was never spent in any of the blocks we parsed
    Unspent,
}

/// Parser that tracks unspent outputs and input amounts to produce [`UtxoBlock`]
///
/// * Unspent outputs allow you to determine the current set of spendable UTXOs
/// * Input amounts allow you to calculate mining fees and trace spending patterns
#[derive(Clone)]
pub struct UtxoParser {
    unspent: Arc<Mutex<FxHashMap<ShortOutPoint, Amount>>>,
    filter: Arc<OutPointFilter>,
}
impl UtxoParser {
    /// Create a new parser from a file generated by [`FilterParser`]
    pub fn new(filter_file: &str) -> Result<Self> {
        let mut reader = BufReader::new(File::open(filter_file)?);
        let mut buffer = vec![];
        reader.read_to_end(&mut buffer)?;
        let filter: OutPointFilter = postcard::from_bytes(&buffer)?;

        Ok(Self {
            unspent: Arc::new(Mutex::new(Default::default())),
            filter: Arc::new(filter),
        })
    }
}
impl BlockParser<UtxoBlock, UtxoBlock> for UtxoParser {
    /// We try to perform as much computation as possible here where block order doesn't matter
    /// because we benefit from multithreading.
    fn extract(&self, block: Block) -> Option<UtxoBlock> {
        let txids: Vec<Txid> = block.txdata.iter().map(|tx| tx.compute_txid()).collect();
        let mut output_status = FxHashMap::<Txid, Vec<OutStatus>>::default();

        for (tx, txid) in block.txdata.iter().zip(txids.iter()) {
            let entry = output_status.entry(*txid).or_default();

            for (index, _) in tx.output.iter().enumerate() {
                let outpoint = ShortOutPoint::new(&OutPoint::new(*txid, index as u32));
                if self.filter.contains(&outpoint) {
                    entry.push(OutStatus::Unspent);
                } else {
                    entry.push(OutStatus::Spent);
                }
            }
        }

        Some(UtxoBlock {
            block,
            txids,
            input_amounts: Default::default(),
            output_status,
        })
    }

    fn batch(&self, items: Vec<UtxoBlock>) -> Vec<UtxoBlock> {
        let mut results = vec![];
        let unspent = &mut self.unspent.lock().unwrap();

        for mut block in items {
            let mut input_amounts = FxHashMap::<Txid, Vec<Amount>>::default();
            for (tx, txid) in block.transactions() {
                let statuses = block.output_status(&txid);
                for (status, (index, output)) in statuses.iter().zip(tx.output.iter().enumerate()) {
                    let outpoint = ShortOutPoint::new(&OutPoint::new(*txid, index as u32));
                    // do not cache unspent outputs (or we will use a lot of memory in `self.unspent`
                    if *status == OutStatus::Spent {
                        unspent.insert(outpoint, output.value);
                    }
                }

                for input in &tx.input {
                    let entry = input_amounts.entry(*txid).or_default();
                    let outpoint = ShortOutPoint::new(&input.previous_output);
                    if let Some(amount) = unspent.remove(&outpoint) {
                        entry.push(amount);
                    } else if tx.is_coinbase() {
                        entry.push(Amount::ZERO);
                    } else {
                        panic!("Input amount not found for {:?}", input.previous_output);
                    }
                }
            }
            block.input_amounts = input_amounts;
            results.push(block);
        }
        results
    }

    /// In order to track UTXO amounts we must process blocks in-order
    fn options() -> Options {
        Options::default().order_output()
    }
}

/// Shortened [`OutPoint`] to save memory
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct ShortOutPoint(pub Vec<u8>);
impl ShortOutPoint {
    /// Shorten an existing [`OutPoint`]
    ///
    /// 2 bytes represent far more than the maximum tx outputs (2^16)
    /// 12 byte subset of the txid is unlikely to generate collisions even with 1 billion txs (~6.3e-12)
    pub fn new(outpoint: &OutPoint) -> ShortOutPoint {
        let mut bytes = vec![];
        bytes.extend_from_slice(&outpoint.vout.to_le_bytes()[0..2]);
        bytes.extend_from_slice(&outpoint.txid.as_byte_array()[0..12]);
        ShortOutPoint(bytes)
    }
}

/// [`FxHasher`] doesn't implement [`Debug`]
#[derive(Default, Clone)]
struct FastHasher(FxHasher);
impl Hasher for FastHasher {
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes)
    }
}
impl Debug for FastHasher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("FastHasher")
    }
}

/// [`SmallRng`] doesn't implement [`Default`] required to deserialize
#[derive(Debug)]
struct FastRng(SmallRng);
impl Default for FastRng {
    fn default() -> Self {
        Self(SmallRng::seed_from_u64(0x2c76c58e13b3a812))
    }
}
impl RngCore for FastRng {
    fn next_u32(&mut self) -> u32 {
        self.0.next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.0.next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.0.fill_bytes(dest)
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> std::result::Result<(), Error> {
        self.0.try_fill_bytes(dest)
    }
}

#[cfg(test)]
mod tests {
    use crate::utxos::{FastHasher, FastRng, OutPointFilter, ShortOutPoint};
    use scalable_cuckoo_filter::ScalableCuckooFilterBuilder;

    #[test]
    fn test_filter_serde() {
        let mut filter = ScalableCuckooFilterBuilder::default()
            .initial_capacity(100)
            .false_positive_probability(0.000_000_000_001)
            .rng(FastRng::default())
            .hasher(FastHasher::default())
            .finish();

        filter.insert(&outpoint(0));
        filter.insert(&outpoint(1));
        let bytes = postcard::to_allocvec(&filter).unwrap();
        let deserialized: OutPointFilter = postcard::from_bytes(&bytes).unwrap();

        assert!(deserialized.contains(&outpoint(0)));
        assert!(deserialized.contains(&outpoint(1)));
        assert!(!deserialized.contains(&outpoint(2)));
        assert!(!deserialized.contains(&outpoint(3)));
    }

    fn outpoint(n: u8) -> ShortOutPoint {
        ShortOutPoint(vec![n])
    }
}
