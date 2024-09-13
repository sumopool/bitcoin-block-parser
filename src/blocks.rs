//! The [`BlockParser`] trait allows you to implement a custom parser or use one of the predefined ones.
//!
//! Imagine you want to take the highest transaction by [`Txid`] from the first 600K blocks and sum
//! their output [`Amount`].
//!
//! You can use the [`DefaultParser`] to simply iterate over the blocks:
//! ```
//! use bitcoin_block_parser::*;
//!
//! let mut headers = HeaderParser::parse("/path/to/blocks")?;
//! let mut amount = bitcoin::Amount::ZERO;
//!     for block in DefaultParser.parse(&headers[..600_000]) {
//!         let txs = block?.txdata;
//!         let max = txs.iter().max_by_key(|tx| tx.compute_txid());
//!         for output in max.unwrap().output {
//!             amount += output.value;
//!         }
//!     }
//!     println!("Sum of txids: {}", amount);
//! ```
//!
//! If you wish to take advantage of multithreading you can implement your own parser.  This example
//! uses ~2.5x less memory and time since both [`BlockParser::extract`] and [`BlockParser::batch`] run on multiple threads.
//! ```
//! use bitcoin::*;
//! use bitcoin_block_parser::*;
//!
//! #[derive(Clone)]
//! struct AmountParser;
//! impl BlockParser<Transaction, Amount> for AmountParser {
//!     fn extract(&self, block: Block) -> Option<Transaction> {
//!         block.txdata.into_iter().max_by_key(|tx| tx.compute_txid())
//!     }
//!
//!     fn batch(&self, items: Vec<Transaction>) -> Vec<Amount> {
//!         let outputs = items.into_iter().flat_map(|tx| tx.output);
//!         vec![outputs.map(|output| output.value).sum()]
//!     }
//! }
//!
//! let mut headers = HeaderParser::parse("/path/to/blocks")?;
//! let receiver = AmountParser.parse(&headers[..600_000]);
//! let amounts: anyhow::Result<Vec<Amount>> = receiver.iter().collect();
//! println!("Sum of txids: {}", amounts?.into_iter().sum::<Amount>());
//! ```
//!
//! You can also cache data within your [`BlockParser`] by using an [`Arc`], for instance:
//! ```
//! use std::sync::Arc;
//! use std::sync::atomic::*;
//! use bitcoin::*;
//! use bitcoin_block_parser::*;
//!
//! #[derive(Clone, Default)]
//! // Note that we can cache shared state within an Arc<_>
//! struct AmountParser(Arc<AtomicU64>);
//! impl BlockParser<Transaction, Amount> for AmountParser {
//!     fn extract(&self, block: bitcoin::Block) -> Option<Transaction> {
//!         block.txdata.into_iter().max_by_key(|tx| tx.compute_txid())
//!     }
//!
//!     fn batch(&self, items: Vec<Transaction>) -> Vec<Amount> {
//!         let outputs = items.into_iter().flat_map(|tx| tx.output);
//!         let sum = outputs.map(|output| output.value.to_sat()).sum();
//!         self.0.fetch_add(sum, Ordering::Relaxed);
//!         vec![]
//!     }
//! }
//!
//! let mut headers = HeaderParser::parse("/path/to/blocks")?;
//! let parser = AmountParser::default();
//! for _ in parser.parse(&headers[..600_000]) {}
//! let sum = Amount::from_sat(parser.0.fetch_add(0, Ordering::Relaxed));
//! println!("Sum of txids: {}", sum);
//! ```

use crate::headers::ParsedHeader;
use anyhow::Result;
use bitcoin::consensus::Decodable;
use bitcoin::{Amount, Block, Transaction, Txid};
use crossbeam_channel::{bounded, Receiver, Sender};
use rustc_hash::FxHashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use threadpool::ThreadPool;

/// Implement this trait to create a custom [`Block`] parser.
pub trait BlockParser<B: Send + 'static, C: Send + 'static>: Clone + Send + 'static {
    /// Extracts the data you need from the block.
    ///
    /// If you can keep [`B`] small or return [`None`] you will gain memory/speed performance.
    /// Always runs on blocks out-of-order using multiple threads so put compute-heavy code in here.
    fn extract(&self, block: Block) -> Option<B>;

    /// Runs on batches of the [`BlockParser::extract`] to return the final results.
    ///
    /// We batch to reduce contention in the case the parser needs to modify shared state by
    /// locking an `Arc<Mutex<_>>`.
    /// Use [`Options::batch_size`] if you need to tune the number of the `items`.
    fn batch(&self, items: Vec<B>) -> Vec<C>;

    /// The default [`Options`] that this parser will use.
    ///
    /// Generally you will want to call `BlockParser::options` if you need to modify the default
    /// options, rather than constructing your options from scratch.
    fn options() -> Options {
        Options::default()
    }

    /// Parse all the blocks represented by the headers.
    fn parse(&self, headers: &[ParsedHeader]) -> Receiver<Result<C>> {
        self.parse_with_opts(headers, Self::options())
    }

    /// Parse all the blocks represented by the headers, ensuring the [`C`] results are returned
    /// in the same order the [`ParsedHeader`] were passed in.
    ///
    /// Note that by ordering the results [`BlockParser::batch`] will run on a single thread instead
    /// of multiple which could affect performance.
    fn parse_ordered(&self, headers: &[ParsedHeader]) -> Receiver<Result<C>> {
        self.parse_with_opts(headers, Self::options().order_output())
    }

    /// Allows users to pass in custom [`Options`] in case they need to reduce memory usage or
    /// otherwise tune performance for their system.
    fn parse_with_opts(&self, headers: &[ParsedHeader], opts: Options) -> Receiver<Result<C>> {
        // Create the batches of headers
        let mut batched: Vec<Vec<ParsedHeader>> = vec![vec![]];
        for header in headers.to_vec().into_iter() {
            let last = batched.last_mut().unwrap();
            last.push(header);
            if last.len() == opts.batch_size {
                batched.push(vec![]);
            }
        }

        // Run the extract function on multiple threads
        let start = Instant::now();
        let num_parsed = Arc::new(AtomicUsize::new(0));
        let (tx_b, rx_b) = bounded::<(usize, Result<Vec<B>>)>(opts.channel_buffer_size);
        let pool_extract = ThreadPool::new(opts.num_threads);
        for (index, headers) in batched.to_vec().into_iter().enumerate() {
            let tx_b = tx_b.clone();
            let parser = self.clone();
            let num_parsed = num_parsed.clone();
            pool_extract.execute(move || {
                let mut batch_b = vec![];
                for header in headers {
                    let block = parse_block(header);
                    increment_log(&num_parsed, start, opts.log_at);
                    if let Some(b) = block.map(|block| parser.extract(block)).transpose() {
                        batch_b.push(b);
                    }
                }
                let result: Result<Vec<B>> = batch_b.into_iter().collect();
                let _ = tx_b.send((index, result));
            });
        }

        if opts.order_output {
            // Spawn a single thread to ensure the output is in order
            let (tx_c, rx_c) = bounded::<Result<C>>(opts.channel_buffer_size);
            let parser = self.clone();
            thread::spawn(move || {
                let mut current_index = 0;
                let mut unordered = FxHashMap::default();

                for (index, b) in rx_b {
                    unordered.insert(index, b);

                    while let Some(ordered) = unordered.remove(&current_index) {
                        current_index += 1;
                        parser.send_batch(&tx_c, ordered);
                    }
                }
            });
            rx_c
        } else {
            // Spawn multiple threads in the case we don't care about the output order
            let pool_batch = ThreadPool::new(opts.num_threads);
            let (tx_c, rx_c) = bounded::<Result<C>>(opts.channel_buffer_size);
            for _ in 0..opts.num_threads {
                let tx_c = tx_c.clone();
                let rx_b = rx_b.clone();
                let parser = self.clone();
                pool_batch.execute(move || {
                    for (_, batch) in rx_b {
                        parser.send_batch(&tx_c, batch);
                    }
                });
            }
            rx_c
        }
    }

    /// Helper function for sending batch results in a channel
    fn send_batch(&self, tx_c: &Sender<Result<C>>, batch: Result<Vec<B>>) {
        let results = match batch.map(|b| self.batch(b)) {
            Ok(c) => c.into_iter().map(|c| Ok(c)).collect(),
            Err(e) => vec![Err(e)],
        };
        for result in results {
            let _ = tx_c.send(result);
        }
    }
}

/// Increments the number of blocks parsed, reporting the progress in a thread-safe manner
fn increment_log(num_parsed: &Arc<AtomicUsize>, start: Instant, log_at: usize) {
    let num = num_parsed.fetch_add(1, Ordering::Relaxed) + 1;

    if num % log_at == 0 {
        let elapsed = (Instant::now() - start).as_secs();
        print!("{}K blocks parsed,", num / 1000);
        println!(" {}m{}s elapsed", elapsed / 60, elapsed % 60);
    }
}

/// Parses a block from a `ParsedHeader` into a `bitcoin::Block`
fn parse_block(header: ParsedHeader) -> Result<Block> {
    let mut reader = BufReader::new(File::open(&header.path)?);
    reader.seek_relative(header.offset as i64)?;
    Ok(Block {
        header: header.inner,
        txdata: Vec::<Transaction>::consensus_decode_from_finite_reader(&mut reader)?,
    })
}

/// Parser that returns [`Block`] for users that don't want to implement a custom [`BlockParser`]
#[derive(Clone, Debug)]
pub struct DefaultParser;
impl BlockParser<Block, Block> for DefaultParser {
    fn extract(&self, block: Block) -> Option<Block> {
        Some(block)
    }

    fn batch(&self, items: Vec<Block>) -> Vec<Block> {
        items
    }

    fn options() -> Options {
        // since we do no batch processing, set batch_size to 1
        Options::default().batch_size(1)
    }
}

/// Options to tune the performance of the parser, generally you can stick to the defaults unless
/// you run into memory issues.
pub struct Options {
    order_output: bool,
    num_threads: usize,
    batch_size: usize,
    channel_buffer_size: usize,
    log_at: usize,
}
/// Defaults that should be close to optimal for most parsers
///
/// `num_threads: 128` should be enough for most systems regardless of disk speed
/// `batch_size: 10` improves batch performance without using too much memory
/// `channel_buffer_size: 100` increasing beyond this usually just increases memory usage
/// `log_at: 10_000` will produce logs every few seconds without spamming output
impl Default for Options {
    fn default() -> Self {
        Self {
            order_output: false,
            num_threads: 128,
            batch_size: 10,
            channel_buffer_size: 100,
            log_at: 10_000,
        }
    }
}
impl Options {
    /// Ensures that the output of the [`BlockParser::parse`] function will be in block height order.
    ///
    /// Some algorithms require [`BlockParser::batch`] to process blocks in-order, however this
    /// requires running that function in a single thread.
    /// `BlockParser::extract` will still run multithreaded out-of-order.
    pub fn order_output(mut self) -> Self {
        self.order_output = true;
        self
    }

    /// Set the number of threads to handle the processing steps.
    ///
    /// Typically limited by disk I/O and the number of threads your system can handle,
    /// increasing it generally improves speed at the cost of memory usage.
    pub fn num_threads(mut self, n: usize) -> Self {
        assert!(n > 0);
        self.num_threads = n;
        self
    }

    /// Number of items passed into [`BlockParser::batch`].
    ///
    /// If you need to access shared state through an `Arc<Mutex<_>>` a bigger batch size can
    /// improve performance, at the cost of more memory depending on the size of [`BlockParser::extract`]
    pub fn batch_size(mut self, n: usize) -> Self {
        assert!(n > 0);
        self.batch_size = n;
        self
    }

    /// Set the number of size of the buffers used between channels.
    ///
    /// Doesn't have a significant impact on speed/memory so long as it's set high enough.
    pub fn channel_buffer_size(mut self, n: usize) -> Self {
        assert!(n > 0);
        self.channel_buffer_size = n;
        self
    }

    /// Set how many blocks to parse before printing a log message out.
    ///
    /// To disable logging, simply set `log_at` to `usize::MAX`, required to be at least 1K
    pub fn log_at(mut self, n: usize) -> Self {
        assert!(n >= 1000);
        self.log_at = n;
        self
    }
}