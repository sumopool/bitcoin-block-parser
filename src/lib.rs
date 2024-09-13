use anyhow::Result;
use anyhow::{anyhow, bail};
use bitcoin::block::Header;
use bitcoin::consensus::Decodable;
use bitcoin::hashes::Hash;
use bitcoin::{Amount, Block, BlockHash, OutPoint, Transaction, Txid};
use rustc_hash::FxHashMap;
use scalable_cuckoo_filter::ScalableCuckooFilter;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read};
use std::iter::Zip;
use std::path::{Path, PathBuf};
use std::slice::Iter;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, SyncSender};
use std::time::Instant;
use std::{fs, thread};
use threadpool::ThreadPool;

/// 100 prevents too many file handles from being open
const NUM_FILE_THREADS: usize = 100;
/// How many blocks we can buffer in our multithreaded channel
const BLOCK_BUFFER: usize = 100;
/// How much to allocate for the filter size (will auto-scale anway)
const APPROX_UNSPENT_TXS: usize = 300_000_000;
/// Keep probability of collision 1e-12
const FILTER_COLLISION_PROB: f64 = 0.000_000_000_001;
/// 12 byte subset of the txid is unlikely to generate collisions even with 1 billion txs (~6.3e-12)
const SHORT_TXID_BYTES: usize = 12;
/// How many blocks to parse before printing a progress log
const LOG_BLOCKS: u32 = 10_000;

/// The size of the block header in bytes
const HEADER_SIZE: usize = 80;
/// Before the header are 4 magic bytes and 4 bytes that indicate the block size
const PRE_HEADER_SIZE: usize = 8;

type UnspentFilter = ScalableCuckooFilter<OutPoint>;
type ResultBlock = Result<ParsedBlock>;

/// Contains a block that has been parsed and additional metadata we have derived about it
#[derive(Clone, Debug)]
pub struct ParsedBlock {
    pub block: Block,
    /// Precomputed txids for every transaction
    txids: Vec<Txid>,
    /// Map every tx in this block to the list of input amounts
    input_amounts: BTreeMap<Txid, Vec<Amount>>,
    /// Map every tx in this block to the list of output spent/unspent status
    output_status: BTreeMap<Txid, Vec<OutStatus>>,
}
impl ParsedBlock {
    /// Return all `Transaction` with `Txid` already calculated
    pub fn transactions(&self) -> Zip<Iter<'_, Transaction>, Iter<'_, Txid>> {
        self.block.txdata.iter().zip(self.txids.iter())
    }

    /// Given a tx in this block, return the in-order list of whether the output was spent/unspent
    /// Currently not checked at compile-time since the multithreading makes the implementation difficult
    pub fn output_status(&self, txid: &Txid) -> Result<&Vec<OutStatus>> {
        self.output_status.get(txid).ok_or(anyhow!("Output status not found, try calling parse_o() or parse_io()"))
    }

    /// Given a tx in this block, return the in-order list of the input amounts
    /// Currently not checked at compile-time since the multithreading makes the implementation difficult
    pub fn input_amount(&self, txid: &Txid) -> Result<&Vec<Amount>> {
        self.input_amounts.get(txid).ok_or(anyhow!("Input amount not found, try calling parse_i() or parse_io()"))
    }
}

/// Indicates what happens to this output
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutStatus {
    /// The output was spent in a later block
    Spent,
    /// The output was never spent in any of the blocks we parsed
    Unspent
}

/// Contains all the block parsing functions
pub struct BlockParser {
    locations: Vec<BlockLocation>
}

impl BlockParser {
    /// Create a new parser, allowing users to select a range or re-order blocks
    pub fn new(locations: &[BlockLocation]) -> Self { Self { locations: locations.to_vec() } }

    /// Parses all the blocks without providing any of the input/output metadata
    pub fn parse(&self)  -> Receiver<ResultBlock> {
        self.parse_ordered(None)
    }

    /// Parses the blocks and tracks the input amounts
    pub fn parse_i(&self)  -> Receiver<ResultBlock> {
        self.parse_txin_amounts(None)
    }

    /// Parses the blocks and tracks if outputs are spent/unspent
    pub fn parse_o(&self, filter_file: &str)  -> Receiver<ResultBlock> {
        self.parse_ordered(Some(filter_file.to_string()))
    }

    /// Parses the blocks and tracks the input amounts and if outputs are spent/unspent
    pub fn parse_io(&self, filter_file: &str)  -> Receiver<ResultBlock> {
        self.parse_txin_amounts(Some(filter_file.to_string()))
    }

    /// Writes a filter that contains all unspent `OutPoint`
    ///
    /// We use `ScalableCuckooFilter` which is a probabilistic set membership data structure
    /// similar to a bloom filter except it supports removes which makes creation faster/easier.
    pub fn write_filter(&self, output: &str) -> Result<()> {
        self.check_genesis()?;
        let writer = BufWriter::new(File::create(output)?);
        let mut filter = ScalableCuckooFilter::new(APPROX_UNSPENT_TXS, FILTER_COLLISION_PROB);
        for parsed in self.parse_ordered(None) {
            let parsed = parsed?;
            for (tx, txid) in parsed.transactions() {
                for (index, _) in tx.output.iter().enumerate() {
                    let outpoint = OutPoint::new(*txid, index as u32);
                    filter.insert(&outpoint);
                }

                for input in &tx.input {
                    filter.remove(&input.previous_output);
                }
            }
        }
        filter.shrink_to_fit();
        println!("Wrote {} filter with {} items", output, filter.len());
        postcard::to_io(&filter, writer)?;
        Ok(())
    }

    /// Parses the blocks, returning them in the order they were passed in.
    fn parse_ordered(&self, filter_file: Option<String>) -> Receiver<ResultBlock> {
        let (tx_blocks, rx_blocks) = mpsc::sync_channel(BLOCK_BUFFER);
        let pool = ThreadPool::new(NUM_FILE_THREADS);

        // Spawns threads for parsing blocks from disk in parallel (current bottleneck is the I/O here)
        for (index, location) in self.locations.clone().into_iter().enumerate() {
            let tx = tx_blocks.clone();
            pool.execute(move || {
                let _ = tx.send((index as u32, Self::parse_block(location)));
            });
        }
        drop(tx_blocks);

        // Spawn a thread that will order the blocks
        let (tx, rx) = mpsc::sync_channel(BLOCK_BUFFER);
        thread::spawn(move || {
            if let Err(e) = Self::parse_ordered_helper(filter_file, tx.clone(), rx_blocks) {
                let _ = tx.send(Err(e));
            }
        });
        rx
    }

    fn parse_ordered_helper(filter_file: Option<String>,
                            tx: SyncSender<ResultBlock>,
                            rx: Receiver<(u32, ResultBlock)>) -> Result<()> {
        let filter = match filter_file {
            None => None,
            Some(file) => {
                let mut reader = BufReader::new(File::open(file)?);
                let mut buffer = vec![];
                reader.read_to_end(&mut buffer)?;
                let filter: UnspentFilter = postcard::from_bytes(&buffer)?;
                Some(filter)
            }
        };

        let mut current_index = 0;
        let mut unordered = FxHashMap::default();
        let start = Instant::now();
        for (index, block) in rx {
            unordered.insert(index, block);

            // Get all the ordered blocks
            while let Some(parsed) = unordered.remove(&current_index) {
                current_index += 1;
                let _ = tx.send(Self::update_outputs(parsed, &filter));

                if current_index % LOG_BLOCKS == 0 {
                    let elapsed = (Instant::now() - start).as_secs();
                    print!("{}0K blocks parsed,", current_index / LOG_BLOCKS);
                    println!(" {}m{}s elapsed", elapsed / 60, elapsed % 60);
                }
            }
        }
        println!("Parsed {} total blocks", current_index);
        Ok(())
    }

    fn update_outputs(parsed: ResultBlock, filter: &Option<UnspentFilter>) -> ResultBlock {
        if let Some(filter) = &filter {
            let mut output_status: BTreeMap<Txid, Vec<OutStatus>> = BTreeMap::new();
            let mut parsed = parsed?;
            for (tx, txid) in parsed.transactions() {
                for (index, _) in tx.output.iter().enumerate() {
                    let entry = output_status.entry(*txid).or_default();
                    if filter.contains(&OutPoint::new(*txid, index as u32)) {
                        entry.push(OutStatus::Unspent);
                    } else {
                        entry.push(OutStatus::Spent);
                    }
                }
            }
            parsed.output_status = output_status;
            return Ok(parsed);
        }
        parsed
    }

    /// Parses the amounts for `TxIn` using an in-memory cache.
    fn parse_txin_amounts(&self, filter_file: Option<String>) -> Receiver<ResultBlock> {
        let (tx, rx) = mpsc::sync_channel(BLOCK_BUFFER);
        let rx_blocks = self.parse_ordered(filter_file);
        if let Err(e) = self.check_genesis() {
            let _ = tx.send(Err(e));
        }

        thread::spawn(move || {
            if let Err(e) = Self::parse_txin_amounts_helper(tx.clone(), rx_blocks) {
                let _ = tx.send(Err(e));
            }
        });
        rx
    }

    fn parse_txin_amounts_helper(tx: SyncSender<ResultBlock>, rx: Receiver<ResultBlock>) -> Result<()> {
        let mut outpoints = FxHashMap::default();

        for parsed in rx {
            let mut parsed = parsed?;
            let mut input_amounts: BTreeMap<Txid, Vec<Amount>> = BTreeMap::new();

            for (tx, txid) in parsed.transactions() {
                for (index, output) in tx.output.iter().enumerate() {
                    let outpoint = OutPoint::new(*txid, index as u32);
                    // cache the output amount (if it's not an unspent output)
                    match parsed.output_status(&txid).ok().and_then(|status| status.get(index)) {
                        Some(OutStatus::Unspent) => {},
                        _ => { outpoints.insert(Self::truncate(&outpoint), output.value); }
                    }
                }

                for input in &tx.input {
                    let entry = input_amounts.entry(*txid).or_default();
                    if let Some(amount) = outpoints.remove(&Self::truncate(&input.previous_output)) {
                        entry.push(amount);
                    } else if tx.is_coinbase() {
                        entry.push(Amount::ZERO);
                    } else {
                        bail!("Input amount not found for {:?}", input.previous_output);
                    }
                }
            }

            parsed.input_amounts = input_amounts;
            let _ = tx.send(Ok(parsed));
        }
        Ok(())
    }

    /// Check we start the genesis block (required for some algorithms)
    fn check_genesis(&self) -> Result<()> {
        match self.locations.first() {
            Some(first) if first.prev == BlockHash::all_zeros() => Ok(()),
            _ => bail!("Calling this function must start at the genesis block")
        }
    }

    /// Parses a block from a `BlockLocation` into a `ParsedBlock`
    fn parse_block(location: BlockLocation) -> ResultBlock {
        let mut reader = BufReader::new(File::open(&location.path)?);
        reader.seek_relative(location.offset as i64)?;
        let block = Block::consensus_decode(&mut reader)?;
        Ok(ParsedBlock {
            txids: block.txdata.iter().map(|tx| tx.compute_txid()).collect(),
            block,
            input_amounts: Default::default(),
            output_status: Default::default(),
        })
    }

    /// Truncate an `OutPoint` to save memory
    ///
    /// 2 bytes represent far more than the maximum tx outputs (2^16)
    /// 12 byte subset of the txid is unlikely to generate collisions even with 1 billion txs (~6.3e-12)
    fn truncate(outpoint: &OutPoint) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend_from_slice(&outpoint.vout.to_le_bytes()[0..2]);
        bytes.extend_from_slice(&outpoint.txid.as_byte_array()[0..SHORT_TXID_BYTES]);
        bytes
    }
}

/// Points to the on-disk location where a block starts (and the header ends)
#[derive(Clone, Debug)]
pub struct BlockLocation {
    /// Byte offset from the beginning of the file
    pub offset: usize,
    /// Prev block hash (used for ordering the blocks)
    pub prev: BlockHash,
    /// This header's block hash
    pub hash: BlockHash,
    /// Path of the BLK file
    pub path: PathBuf,
}

impl BlockLocation {
    /// Parses the headers from the `blocks_dir` returning the `BlockLocation` in height order,
    /// starting from the genesis block.  Takes a few seconds to run.
    pub fn parse(blocks_dir: &str) -> Result<Vec<BlockLocation>> {
        let (tx, rx) = mpsc::channel();
        let pool = ThreadPool::new(NUM_FILE_THREADS);

        // Read headers from every BLK file in a new thread
        for path in Self::blk_files(blocks_dir)? {
            let path = path.clone();
            let tx = tx.clone();
            pool.execute(move || {
                let results = Self::parse_headers_file(path);
                let _ = tx.send(results);
            });
        }
        drop(tx);

        // Receive all the headers from spawned threads
        let mut locations: HashMap<BlockHash, BlockLocation> = HashMap::new();
        let mut collisions: Vec<BlockLocation> = vec![];
        for received in rx {
            for header in received? {
                if let Some(collision) = locations.insert(header.prev, header) {
                    collisions.push(collision)
                }
            }
        }

        // Resolve reorgs and order the headers by block height
        for collision in collisions {
            Self::resolve_collisions(&mut locations, collision);
        }
        Ok(Self::order_headers(locations))
    }

    /// Parses headers from a BLK file
    fn parse_headers_file(path: PathBuf) -> Result<Vec<BlockLocation>> {
        let buffer_size = PRE_HEADER_SIZE + HEADER_SIZE;
        let mut reader = BufReader::with_capacity(buffer_size, File::open(&path)?);
        let mut offset = 0;
        // First 8 bytes are 4 magic bytes and 4 bytes that indicate the block size
        let mut buffer = vec![0; PRE_HEADER_SIZE];
        let mut headers = vec![];

        while reader.read_exact(&mut buffer).is_ok() {
            offset += buffer.len();
            if let Ok(header) = Header::consensus_decode(&mut reader) {
                headers.push(BlockLocation {
                    offset,
                    prev: header.prev_blockhash,
                    hash: header.block_hash(),
                    path: path.clone(),
                });
                // Get the size of the next block
                let size = u32::from_le_bytes(buffer[4..].try_into()?) as usize;
                // Subtract the number of bytes in the block header we consumed
                reader.seek_relative((size - HEADER_SIZE) as i64)?;
                offset += size;
            }
        }
        Ok(headers)
    }

    /// Returns the list of all BLK files in the dir
    fn blk_files(dir: &str) -> Result<Vec<PathBuf>> {
        let read_dir = fs::read_dir(Path::new(&dir))?;
        let mut files = vec![];

        for file in read_dir {
            let file = file?;
            let name = file.file_name().into_string().expect("Could not parse filename");
            if name.starts_with("blk") {
                files.push(file.path())
            }
        }

        if files.is_empty() {
            bail!("No BLK files found in dir {:?}", dir);
        }

        Ok(files)
    }

    /// In case of reorgs we need to resolve to the longest chain
    fn resolve_collisions(headers: &mut HashMap<BlockHash, BlockLocation>, collision: BlockLocation) {
        let existing = headers.get(&collision.prev).expect("exists");
        let mut e_hash = &existing.hash;
        let mut c_hash = &collision.hash;

        while let (Some(e), Some(c)) = (headers.get(e_hash), headers.get(c_hash)) {
            e_hash = &e.hash;
            c_hash = &c.hash;
        }

        // In case collision is the longest, update the blocks map
        if headers.contains_key(c_hash) {
            headers.insert(collision.prev, collision);
        }
    }

    /// Puts the headers into the correct order by block height (using the hashes)
    fn order_headers(mut headers: HashMap<BlockHash, BlockLocation>) -> Vec<BlockLocation> {
        let mut ordered_headers = vec![];
        // Genesis block starts with prev = all_zeros
        let mut next_hash = BlockHash::all_zeros();

        while let Some(index) = headers.remove(&next_hash) {
            next_hash = index.hash;
            ordered_headers.push(index);
        }

        ordered_headers
    }
}