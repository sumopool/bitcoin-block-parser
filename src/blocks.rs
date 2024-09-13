use crate::headers::ParsedHeader;
use anyhow::{Error, Result};
use bitcoin::consensus::Decodable;
use bitcoin::{Block, Transaction};
use crossbeam_channel::{bounded, Receiver, Sender};
use rustc_hash::FxHashMap;
use std::fs::File;
use std::io::{BufReader, Seek};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use threadpool::ThreadPool;

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
    /// Ensures that the output of the `parse` function will be in block height order.
    ///
    /// Some algorithms require `BlockParser::batch` to process blocks in-order, however this
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

    /// Number of items passed into `BlockParser::batch`.
    ///
    /// If you need to access shared state through an `Arc<Mutex<_>>` a bigger batch size can
    /// improve performance, at the cost of more memory depending on the size of `BlockParser::extract`
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

type IndexedReceiver<T> = Receiver<(usize, Result<Vec<T>>)>;

pub trait BlockParser2<B: Send + 'static, C: Send + 'static>: Clone + Send + 'static {
    fn extract(&self, block: Block) -> Option<B>;

    fn batch(&self, items: Vec<B>) -> Vec<C>;

    fn options() -> Options {
        Options::default()
    }

    fn parse(&self, headers: &[ParsedHeader]) -> Receiver<Result<C>> {
        self.parse_with_opts(headers, Self::options())
    }

    fn parse_ordered(&self, headers: &[ParsedHeader]) -> Receiver<Result<C>> {
        self.parse_with_opts(headers, Self::options().order_output())
    }

    fn parse_with_opts(&self, headers: &[ParsedHeader], opts: Options) -> Receiver<Result<C>> {
        let mut batched: Vec<Vec<ParsedHeader>> = vec![vec![]];
        for header in headers.to_vec().into_iter() {
            let last = batched.last_mut().unwrap();
            last.push(header);
            if last.len() == opts.batch_size {
                batched.push(vec![]);
            }
        }

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

#[derive(Clone, Debug)]
pub struct DefaultParser;
impl BlockParser2<Block, Block> for DefaultParser {
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
