//! Contains [`BlockParser`] for parsing bitcoin [`Block`] from the `blocks` directory.

use crate::headers::ParsedHeader;
use crate::xor::XorReader;
use crate::HeaderParser;
use anyhow::Result;
use bitcoin::consensus::Decodable;
use bitcoin::{Block, Transaction};
use crossbeam_channel::{bounded, Receiver, Sender};
use log::info;
use std::cmp::min;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use threadpool::ThreadPool;

/// Multithreaded parser for [`bitcoin::Block`].
///
/// # Examples
/// Call `parse()` to run a `Fn(Block) -> T` that returns a [`ParserIterator<T>`].  The `Fn` closure
/// runs on multiple threads.
/// ```no_run
/// use bitcoin_block_parser::blocks::*;
///
/// let parser = BlockParser::new("/home/user/.bitcoin/blocks/").unwrap();
/// let iterator = parser.parse(|block| block.total_size() as u64);
/// println!("Total blockchain size: {}", iterator.sum::<u64>());
/// ```
///
/// You can call `block_range()` to constrain the block range and `ordered()` to ensure the
/// iterator returns blocks in height order:
/// ```no_run
/// use bitcoin_block_parser::blocks::*;
///
/// let parser = BlockParser::new("/home/user/.bitcoin/blocks/").unwrap();
/// let iterator = parser
///     .block_range(100_000, 100_010)
///     .parse(|block| block.block_hash())
///     .ordered();
///
/// println!("In-order block hashes from 100,000 to 100,010:");
/// for block_hash in iterator {
///     println!("{}", block_hash);
/// }
/// ```
#[derive(Clone, Debug)]
pub struct BlockParser {
    /// The parsed headers used for locating the blocks
    headers: Vec<ParsedHeader>,
    /// A logger for reporting on the progress of the parsing
    logger: ParserLogger,
    /// Options that can have an effect on memory and cpu performance
    options: ParserOptions,
    /// The block height range to start at
    start_height: usize,
    /// The block height range to end at
    end_height: usize,
}

impl BlockParser {
    /// Creates a new parser given the `blocks` directory where the `*.blk` files are located.
    ///
    /// - Returns an `Err` if unable to parse the `blk` files.
    /// - You can [specify the blocks directory](https://en.bitcoin.it/wiki/Data_directory) when
    ///   running `bitcoind`.
    pub fn new(blocks_dir: &str) -> Result<Self> {
        Self::new_with_opts(blocks_dir, ParserOptions::default())
    }

    /// Creates a parser with custom [`ParserOptions`].
    pub fn new_with_opts(blocks_dir: &str, options: ParserOptions) -> Result<Self> {
        let headers = HeaderParser::parse(blocks_dir)?;
        Ok(Self {
            headers,
            logger: ParserLogger::new(),
            options,
            start_height: 0,
            end_height: usize::MAX,
        })
    }

    /// Sets the *inclusive* range of block heights to parse.
    ///
    /// * `start_height` - must be less than the total number of blocks, `0` will start at the
    ///    genesis block.
    /// * `end_height` - the height to end at, [`usize::MAX`] will stop at the last block
    ///    available.
    pub fn block_range(mut self, start_height: usize, end_height: usize) -> Self {
        self.start_height = start_height;
        self.end_height = end_height;
        self
    }

    /// Parse all [`bitcoin::Block`] into type `T` and return a [`ParserIterator<T>`].  Results will
    /// be in random order due to multithreading.
    ///
    /// * `extract` - a closure that runs on multiple threads.  For best performance perform as much
    ///    computation and data reduction here as possible.
    pub fn parse<T: Send + 'static>(
        &self,
        extract: impl Fn(Block) -> T + Clone + Send + 'static,
    ) -> ParserIterator<T> {
        let end_height = min(self.end_height, self.headers.len() - 1);
        let header_range = self.headers[self.start_height..=end_height].to_vec();
        let pool = ThreadPool::new(self.options.num_threads);
        let (tx, rx) = bounded(self.options.channel_size);

        for (index, header) in header_range.into_iter().enumerate() {
            let logger = self.logger.clone();
            let tx = tx.clone();
            let extract = extract.clone();
            let start_height = self.start_height;
            pool.execute(move || {
                let extract = match Self::parse_block(&header) {
                    Ok(block) => extract(block),
                    // Panic here because a blk file is corrupted, nothing else to do
                    e => panic!("Error reading {:?} - {:?}", header.path, e),
                };
                let height = start_height + index;
                let _ = tx.send((height, extract));
                logger.increment();
            });
        }
        ParserIterator {
            rx,
            options: self.options.clone(),
            start_height: self.start_height,
        }
    }

    /// Helper function for reading a block from the filesystem given the header.
    fn parse_block(header: &ParsedHeader) -> Result<Block> {
        let reader = BufReader::new(File::open(&header.path)?);
        let mut reader = BufReader::new(XorReader::new(reader, header.xor_mask));
        reader.seek_relative(header.offset as i64)?;
        Ok(Block {
            header: header.inner,
            txdata: Vec::<Transaction>::consensus_decode_from_finite_reader(&mut reader)?,
        })
    }
}

/// Options that affect the performance of [`BlockParser`] and [`ParserIterator`].
///
/// Generally changing these will be unnessary unless you really need to tune performance.
#[derive(Clone, Debug)]
pub struct ParserOptions {
    /// How many items will be parsed in each [`ParserIterator::pipeline`] batch.
    pub pipeline_size: usize,
    /// The size of all [`crossbeam_channel::bounded`] channels that communicate between threads.
    pub channel_size: usize,
    /// The number of threads that will be spawned when running a multithreaded function.
    pub num_threads: usize,
}

impl Default for ParserOptions {
    /// Returns sane defaults that will be optimal for most workloads.
    fn default() -> Self {
        Self {
            pipeline_size: 1,
            channel_size: 100,
            num_threads: 64,
        }
    }
}

/// Iterator returned from [`BlockParser::parse`] that allows for advanced transformations.
pub struct ParserIterator<T> {
    /// The receiver coming from a previous transformation step.  `usize` is the block height.
    rx: Receiver<(usize, T)>,
    /// Options for tuning performance.
    options: ParserOptions,
    /// The block height the parser started at.
    start_height: usize,
}

impl<A: Send + 'static> ParserIterator<A> {
    /// Create a new iterator from an existing one, given a new receiver.
    fn create<T>(&self, rx: Receiver<(usize, T)>) -> ParserIterator<T> {
        ParserIterator::<T> {
            rx,
            options: self.options.clone(),
            start_height: self.start_height,
        }
    }

    /// Orders the results by block height, can be called for a small increase in
    /// memory and runtime.
    ///
    /// # Example
    /// Using the `ordered` function to get the first 10 block hashes in-height order:
    /// ```no_run
    /// use bitcoin_block_parser::blocks::*;
    /// use bitcoin::BlockHash;
    ///
    /// let parser = BlockParser::new("/home/user/.bitcoin/blocks/").unwrap();
    /// let ordered: ParserIterator<BlockHash> = parser.parse(|block| block.block_hash());
    /// let first_10: Vec<BlockHash> = ordered.take(10).collect();
    /// ```
    pub fn ordered(&self) -> ParserIterator<A> {
        let (tx, rx) = bounded(self.options.channel_size);
        let parser = self.create(rx);
        let rx_a = self.rx.clone();
        let start_height = self.start_height;

        thread::spawn(move || {
            let mut current_height = start_height;
            let mut unordered: HashMap<usize, A> = HashMap::default();

            for (height, a) in rx_a {
                unordered.insert(height, a);
                while let Some(ordered) = unordered.remove(&current_height) {
                    let _ = tx.send((current_height, ordered));
                    current_height += 1;
                }
            }
        });
        parser
    }

    /// Perform a map function using multiple threads.
    /// * Useful if you need to perform an additional map after [`BlockParser::parse`].
    /// * More performant than calling [`Iterator::map`] on the [`ParserIterator`].
    /// * Returns results in random order.
    pub fn map_parallel<B: Send + 'static>(
        &self,
        function: impl Fn(A) -> B + Clone + Send + 'static,
    ) -> ParserIterator<B> {
        let pool = ThreadPool::new(self.options.num_threads);
        let (tx_b, rx_b) = bounded(self.options.pipeline_size * self.options.num_threads);

        for _ in 0..self.options.num_threads {
            let tx_b = tx_b.clone();
            let rx_a = self.rx.clone();
            let function = function.clone();
            pool.execute(move || {
                for (height, a) in rx_a {
                    let _ = tx_b.send((height, function(a)));
                }
            });
        }

        self.create(rx_b)
    }

    /// Pipelines allow you to perform two functions on the same batch of blocks.
    /// Useful when you want multithreaded performance while processing blocks in-order.
    ///
    /// # Example
    /// For example if calculating the size difference between consecutive blocks you could use the
    /// following code:
    /// ```no_run
    /// use bitcoin_block_parser::blocks::*;
    /// use std::collections::HashMap;
    /// use std::convert::identity;
    /// use bitcoin::BlockHash;
    /// use bitcoin::hashes::Hash;
    ///
    /// let parser = BlockParser::new("/home/user/.bitcoin/blocks/").unwrap();
    /// let mut block_sizes: HashMap<BlockHash, isize> = HashMap::new();
    /// let mut differences = vec![];
    /// // Initial block size for the genesis block
    /// block_sizes.insert(BlockHash::all_zeros(), 0);
    ///
    /// for block in parser.parse(identity).ordered() {
    ///     // Store this block's size in the shared state
    ///     let block_size = block.total_size() as isize;
    ///     block_sizes.insert(block.block_hash(), block_size);
    ///     // Look up the previous size to compute the difference
    ///     let prev_block_hash = block.header.prev_blockhash;
    ///     let prev_size = block_sizes.remove(&prev_block_hash);
    ///     differences.push(block_size - prev_size.unwrap());
    /// }
    ///
    /// let max_difference = differences.into_iter().max().unwrap();
    /// println!("Maximum increase in block size: {}", max_difference);
    /// ```
    ///
    /// The previous code runs on a single thread.  If we want to leverage multithreading we can use
    /// `pipeline` functions for a large speed-up:
    /// ```no_run
    /// use bitcoin_block_parser::blocks::*;
    /// use dashmap::DashMap;
    /// use std::convert::identity;
    /// use std::sync::Arc;
    /// use bitcoin::BlockHash;
    /// use bitcoin::hashes::Hash;
    ///
    /// let parser = BlockParser::new("/home/user/.bitcoin/blocks/").unwrap();
    /// // State shared across all threads
    /// let block_sizes: Arc<DashMap<BlockHash, isize>> = Arc::new(DashMap::new());
    /// let blocksizes_clone = block_sizes.clone();
    /// // Initial block size for the genesis block
    /// block_sizes.insert(BlockHash::all_zeros(), 0);
    ///
    /// let iterator = parser.parse(identity).ordered().pipeline_fn(
    ///     move |block| {
    ///         // Store this block's size in the shared state
    ///         let block_size = block.total_size() as isize;
    ///         block_sizes.insert(block.block_hash(), block_size);
    ///         (block.header.prev_blockhash, block_size)
    ///     },
    ///     move |(prev_block_hash, block_size)| {
    ///         // Look up the previous size to compute the difference
    ///         let prev_size = blocksizes_clone.remove(&prev_block_hash);
    ///         block_size - prev_size.unwrap().1
    ///     },
    /// );
    ///
    /// let max_difference = iterator.max().unwrap();
    /// println!("Maximum increase in block size: {}", max_difference);
    /// ```
    pub fn pipeline_fn<B: Send + 'static, C: Send + 'static>(
        &self,
        f1: impl Fn(A) -> B + Clone + Send + 'static,
        f2: impl Fn(B) -> C + Clone + Send + 'static,
    ) -> ParserIterator<C> {
        let pipeline = PipelineClosure { f1, f2 };
        self.pipeline(&pipeline)
    }

    /// Runs [`ParserIterator::pipeline_fn`] functions by implementing a [`Pipeline`] trait for
    /// convenience / cleaner code.
    pub fn pipeline<B: Send + 'static, C: Send + 'static>(
        &self,
        pipeline: &(impl Pipeline<A, B, C> + Clone + Send + 'static),
    ) -> ParserIterator<C> {
        let pool_a = ThreadPool::new(self.options.num_threads);
        let pool_b = ThreadPool::new(self.options.num_threads);
        let rx_a = self.rx.clone();
        let opts = self.options.clone();
        let pipeline = pipeline.clone();
        let (tx_b, rx_b) = bounded(self.options.pipeline_size * self.options.num_threads);
        let (tx_c, rx_c) = bounded(self.options.pipeline_size * self.options.num_threads);
        let run = Arc::new(AtomicBool::new(true));

        thread::spawn(move || {
            while run.load(Ordering::Relaxed) {
                let p1 = pipeline.clone();
                let p2 = pipeline.clone();
                Self::run_pipeline(&opts, &pool_a, &run, &rx_a, &tx_b, &move |a| p1.first(a));
                pool_a.join();
                pipeline.between();
                Self::run_pipeline(&opts, &pool_b, &run, &rx_b, &tx_c, &move |b| p2.second(b));
            }
        });

        self.create(rx_c)
    }

    /// Helper for running the pipeline functions on multiple threads.
    fn run_pipeline<X: Send + 'static, Y: Send + 'static>(
        options: &ParserOptions,
        pool: &ThreadPool,
        running: &Arc<AtomicBool>,
        rx: &Receiver<(usize, X)>,
        tx: &Sender<(usize, Y)>,
        function: &(impl Fn(X) -> Y + Clone + Send + 'static),
    ) {
        // Spawns `num_threads` and run `function` on a `pipeline_size` # of items
        for _ in 0..options.num_threads {
            let running = running.clone();
            let tx = tx.clone();
            let rx = rx.clone();
            let function = function.clone();
            let pipeline_size = options.pipeline_size;
            pool.execute(move || {
                for _ in 0..pipeline_size {
                    match rx.recv() {
                        Ok((height, x)) => {
                            let _ = tx.send((height, function(x)));
                        }
                        Err(_) => {
                            // Signal to the pipeline thread that we have consumed all input
                            running.store(false, Ordering::Relaxed);
                        }
                    };
                }
            });
        }
    }
}

impl<T> Iterator for ParserIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.iter().map(|(_, t)| t).next()
    }
}

/// Implement this trait for calling [`ParserIterator::pipeline`].
pub trait Pipeline<A, B, C> {
    /// Transforms a batch of inputs in parallel
    fn first(&self, a: A) -> B;

    /// Runs once the batch in `first()` has finished completely.
    fn between(&self) {}

    /// Transforms the same batch processed in `first()` in parallel
    fn second(&self, b: B) -> C;
}

/// Helper for turning closures into a pipeline trait.
#[derive(Clone)]
struct PipelineClosure<F1, F2> {
    f1: F1,
    f2: F2,
}

impl<F1, F2, A, B, C> Pipeline<A, B, C> for PipelineClosure<F1, F2>
where
    F1: Fn(A) -> B,
    F2: Fn(B) -> C,
{
    fn first(&self, a: A) -> B {
        (self.f1)(a)
    }

    fn second(&self, b: B) -> C {
        (self.f2)(b)
    }
}

/// Logs the progress of the parsing every 10K blocks in a thread-safe manner.
#[derive(Clone, Debug)]
struct ParserLogger {
    num_parsed: Arc<AtomicUsize>,
    start: Instant,
    log_at: usize,
}

impl ParserLogger {
    fn new() -> Self {
        Self {
            num_parsed: Arc::new(Default::default()),
            start: Instant::now(),
            log_at: 10_000,
        }
    }

    fn increment(&self) {
        let num = self.num_parsed.fetch_add(1, Ordering::Relaxed);

        if num == 0 {
            info!("Starting to parse blocks...");
        } else if num % self.log_at == 0 {
            let elapsed = (Instant::now() - self.start).as_secs();
            let blocks = format!("{}K blocks parsed,", num / 1000);
            info!("{} {}m{}s elapsed", blocks, elapsed / 60, elapsed % 60);
        }
    }
}
