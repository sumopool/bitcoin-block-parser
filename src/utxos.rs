use crate::blocks::{BlockParser2, Options};
use crate::OutStatus;
use anyhow::{anyhow, Result};
use bitcoin::hashes::Hash;
use bitcoin::{Amount, Block, OutPoint, Transaction, Txid};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rustc_hash::FxHashMap;
use scalable_cuckoo_filter::{DefaultHasher, ScalableCuckooFilter, ScalableCuckooFilterBuilder};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::iter::Zip;
use std::ops::Deref;
use std::slice::Iter;
use std::sync::{Arc, Mutex};

type CuckooFilterMutex = Arc<Mutex<ScalableCuckooFilter<ShortOutPoint, DefaultHasher, StdRng>>>;
type OutPoints = (Vec<ShortOutPoint>, Vec<ShortOutPoint>);

#[derive(Clone)]
pub struct FilterParser {
    filter: CuckooFilterMutex,
}
impl FilterParser {
    pub fn new() -> Self {
        Self {
            filter: Arc::new(Mutex::new(
                ScalableCuckooFilterBuilder::default()
                    .initial_capacity(300_000_000)
                    .false_positive_probability(0.000_000_000_001)
                    .rng(StdRng::seed_from_u64(0))
                    .finish(),
            )),
        }
    }

    pub fn write(self, output: &str) -> Result<()> {
        let writer = BufWriter::new(File::create(output)?);
        let mut filter = Arc::try_unwrap(self.filter).unwrap();
        let mut filter = filter.into_inner().unwrap();
        filter.shrink_to_fit();
        postcard::to_io(&filter, writer)?;
        Ok(())
    }
}
impl BlockParser2<OutPoints, ()> for FilterParser {
    fn extract(&self, block: Block) -> Option<OutPoints> {
        let mut inputs = vec![];
        let mut outputs = vec![];
        for tx in block.txdata.iter() {
            let txid = tx.compute_txid();
            for input in &tx.input {
                inputs.push(truncate(&input.previous_output));
            }

            for (index, _) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(txid, index as u32);
                outputs.push(truncate(&outpoint));
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
}

/// Contains a block that has been parsed and additional UTXO information
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct UtxoBlock {
    /// Underlying parsed block from `bitcoin::Block`
    pub block: Block,
    /// Precomputed txids for every transaction
    txids: Vec<Txid>,
    /// Map every tx in this block to the list of input amounts
    input_amounts: BTreeMap<Txid, Vec<Amount>>,
    /// Map every tx in this block to the list of output spent/unspent status
    output_status: BTreeMap<Txid, Vec<OutStatus>>,
}
// impl UtxoBlock {
//     /// Return all `Transaction` with `Txid` already calculated
//     pub fn transactions(&self) -> Zip<Iter<'_, Transaction>, Iter<'_, Txid>> {
//         self.block.txdata.iter().zip(self.txids.iter())
//     }
//
//     /// Given a tx in this block, return the in-order list of whether the output was spent/unspent
//     pub fn output_status(&self, txid: &Txid) -> Result<&Vec<OutStatus>> {
//         self.output_status.get(txid).expect("tx")
//     }
//
//     /// Given a tx in this block, return the in-order list of the input amounts
//     pub fn input_amount(&self, txid: &Txid) -> Result<&Vec<Amount>> {
//         self.input_amounts.get(txid).ok_or(anyhow!(
//             "Input amount not found, try calling parse_i() or parse_io()"
//         ))
//     }
// }

#[derive(Clone)]
pub struct UtxoParser {
    unspent: Arc<Mutex<FxHashMap<ShortOutPoint, Amount>>>,
    filter: CuckooFilterMutex,
}
impl BlockParser2<Block, UtxoBlock> for UtxoParser {
    fn extract(&self, block: Block) -> Option<Block> {
        Some(block)
    }

    fn batch(&self, items: Vec<Block>) -> Vec<UtxoBlock> {
        todo!()
    }
}

#[derive(Hash, Debug)]
pub struct ShortOutPoint(Vec<u8>);

/// Shorten an `OutPoint` to save memory
///
/// 2 bytes represent far more than the maximum tx outputs (2^16)
/// 12 byte subset of the txid is unlikely to generate collisions even with 1 billion txs (~6.3e-12)
fn truncate(outpoint: &OutPoint) -> ShortOutPoint {
    let mut bytes = vec![];
    bytes.extend_from_slice(&outpoint.vout.to_le_bytes()[0..2]);
    bytes.extend_from_slice(&outpoint.txid.as_byte_array()[0..12]);
    ShortOutPoint(bytes)
}
