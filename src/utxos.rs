//! The [`FilterParser`] and [`UtxoParser`] parser must be used together in order to track UTXOs
//! as they move through the tx graph, giving you a map between:
//! * [`bitcoin::TxIn`] -> [`Amount`]
//! * [`bitcoin::TxOut`] -> [`OutStatus::Spent`] or [`OutStatus::Unspent`]
//!
//! This allows you to easily compute values such as mining fees and find all unspent txs.
//! The [`FilterParser`] creates a [`ScalableCuckooFilter`] which allows us to track all
//! unspent UTXOs with low memory-overhead similar to a bloom filter.
//!
//! The filter is written to a file which is loaded into memory by [`UtxoParser`], allowing us to
//! track the flow of all transaction amounts from inputs with a far lower memory overhead and no
//! performance hit of using an on-disk lookup table.
//!
//! Example usage:
//!
//! ```no_run
//! use bitcoin_block_parser::*;
//! use bitcoin_block_parser::utxos::*;
//!
//! // Note you only need to write the filter to a file once
//! let parser = FilterParser::new();
//! for _ in parser.parse_dir("/path/to/blocks").unwrap() {}
//! parser.write("filter.bin").unwrap();
//!
//! let parser = UtxoParser::new("filter.bin").unwrap();
//! // for every block
//! for block in parser.parse_dir("/path/to/blocks").unwrap() {
//!   let block = block.unwrap();
//!   // for every transaction
//!   for (tx, txid) in block.transactions() {
//!     let outs = block.output_status(txid).iter().zip(tx.output.iter());
//!     let inputs = block.input_amount(txid).iter().zip(tx.input.iter());
//!     // for every output in the transaction
//!     for (status, output) in outs {
//!       if *status == OutStatus::Unspent {
//!         let script = &output.script_pubkey;
//!         println!("{:?} has {} unspent funds", script, output.value);
//!       }
//!     }
//!     // for every input in the transaction
//!     for (amount, input) in inputs {
//!       let outpoint = &input.previous_output;
//!       println!("{:?} has {} input funds", outpoint, amount);
//!     }
//!   }
//! }
//! ```

use crate::blocks::{BlockParser, Options};
use anyhow::Result;
use bitcoin::hashes::Hash;
use bitcoin::{Amount, Block, OutPoint, Transaction, Txid};
use rand::rngs::SmallRng;
use rand::{Error, RngCore, SeedableRng};
use rustc_hash::{FxHashMap, FxHasher};
use scalable_cuckoo_filter::{ScalableCuckooFilter, ScalableCuckooFilterBuilder};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, BufWriter, Read};
use std::iter::Zip;
use std::slice::Iter;
use std::sync::{Arc, Mutex};

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
    /// Constructs a new filter parser
    pub fn new() -> Self {
        Self {
            filter: Arc::new(Mutex::new(OutPointFilter::new(300_000_000))),
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
impl BlockParser<OutPoints> for FilterParser {
    // By extracting the [`ShortOutPoint`] here we optimize the memory and computation
    // required.
    fn extract(&self, block: Block) -> Vec<OutPoints> {
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
        vec![(inputs, outputs)]
    }

    // Access the `filter` lock in batch to reduce contention
    fn batch(&self, items: Vec<OutPoints>) -> Vec<OutPoints> {
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

/// Parser that tracks unspent outputs and input amounts to produce [`UtxoBlock`]
///
/// See the [module docs](crate::utxos) for example usage.
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
impl BlockParser<UtxoBlock> for UtxoParser {
    // We try to perform as much computation as possible here where block order doesn't matter
    // because we benefit from multithreading.
    fn extract(&self, block: Block) -> Vec<UtxoBlock> {
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

        vec![UtxoBlock {
            block,
            txids,
            input_amounts: Default::default(),
            output_status,
        }]
    }

    fn batch(&self, items: Vec<UtxoBlock>) -> Vec<UtxoBlock> {
        let mut results = vec![];
        let unspent = &mut self.unspent.lock().unwrap();

        for mut block in items {
            let mut input_amounts = FxHashMap::<Txid, Vec<Amount>>::default();
            for (tx, txid) in block.transactions() {
                let statuses = block.output_status(txid);
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
    ///
    /// We reduce the number of threads to reduce memory usage.
    /// If you run into memory issues try lowering `num_threads` further or the `buffer_size`
    fn options() -> Options {
        Options::default().order_output().num_threads(64)
    }
}

/// Wrapper for a filter that implements [`Serialize`] and [`Send`]
#[derive(Serialize, Deserialize, Debug)]
pub struct OutPointFilter(ScalableCuckooFilter<ShortOutPoint, FastHasher, FastRng>);
impl OutPointFilter {
    /// Create a filter with a reasonable size and false positive rate
    ///
    /// Because we are storing SHA256 hashes from [`Txid`] we use non-cryptographic [`rand::Rng`] and
    /// [`Hasher`] which are optimized for speed without bias.
    pub fn new(initial_capacity: usize) -> Self {
        Self(
            ScalableCuckooFilterBuilder::default()
                .initial_capacity(initial_capacity)
                .false_positive_probability(0.000_000_000_001)
                .rng(FastRng::default())
                .hasher(FastHasher::default())
                .finish(),
        )
    }

    /// See [`ScalableCuckooFilter::contains`]
    pub fn contains(&self, outpoint: &ShortOutPoint) -> bool {
        self.0.contains(outpoint)
    }

    /// See [`ScalableCuckooFilter::insert`]
    pub fn insert(&mut self, outpoint: &ShortOutPoint) {
        self.0.insert(outpoint);
    }

    /// See [`ScalableCuckooFilter::remove`]
    pub fn remove(&mut self, outpoint: &ShortOutPoint) {
        self.0.remove(outpoint);
    }

    /// See [`ScalableCuckooFilter::shrink_to_fit`]
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }
}

/// Shortened [`OutPoint`] to save memory
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct ShortOutPoint(pub Vec<u8>);
impl ShortOutPoint {
    /// Shorten an existing [`OutPoint`]
    ///
    /// - 2 bytes represent far more than the maximum tx outputs (2^16)
    /// - 12 byte subset of the txid is unlikely to generate collisions even with 1 billion txs (~6.3e-12)
    pub fn new(outpoint: &OutPoint) -> ShortOutPoint {
        let mut bytes = vec![];
        bytes.extend_from_slice(&outpoint.vout.to_le_bytes()[0..2]);
        bytes.extend_from_slice(&outpoint.txid.as_byte_array()[0..12]);
        ShortOutPoint(bytes)
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
    use crate::utxos::{OutPointFilter, ShortOutPoint};

    #[test]
    fn test_filter_serde() {
        let mut filter = OutPointFilter::new(100);

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
