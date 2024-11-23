use anyhow::Result;
use bitcoin::hashes::Hash;
use bitcoin::{Amount, Block, BlockHash, Txid};
use bitcoin_block_parser::blocks::{BlockParser, ParserIterator, Pipeline};
use bitcoin_block_parser::utxos::{OutputStatus, UtxoParser};
use clap::{Parser, ValueEnum};
use dashmap::DashMap;
use std::cmp::max;
use std::collections::HashMap;
use std::convert::identity;
use std::str::FromStr;
use std::sync::Arc;

const BLOCKS_TO_PARSE: usize = 800_000;

/// Example program that can perform self-tests and benchmarks
#[derive(Parser, Debug)]
struct Args {
    /// Input directory containing BLK files
    #[arg(short, long)]
    blocks_dir: String,

    /// File to write to for the filter
    #[arg(short, long, default_value = "filter.bin")]
    filter_file: String,

    /// Which of the functions to run
    #[arg(short, long)]
    run: Function,
}

/// Types of functions we can run
#[derive(ValueEnum, Clone, Debug)]
enum Function {
    Parse,
    ParseNoop,
    MapParallel,
    Ordered,
    NoPipelineFn,
    PipelineFn,
    Pipeline,
    UtxoParse,
    CreateFilter,
    LoadFilter,
    Test,
}

fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args = Args::parse();

    fn block_parser(blocks_dir: &str) -> Result<BlockParser> {
        Ok(BlockParser::new(blocks_dir)?.block_range(0, BLOCKS_TO_PARSE))
    }

    fn utxo_parser(blocks_dir: &str) -> Result<UtxoParser> {
        Ok(UtxoParser::new(blocks_dir)?.block_range_end(BLOCKS_TO_PARSE))
    }

    match args.run {
        Function::Parse => parse(block_parser(&args.blocks_dir)?),
        Function::ParseNoop => parse_noop(block_parser(&args.blocks_dir)?),
        Function::MapParallel => map_parallel(block_parser(&args.blocks_dir)?),
        Function::Ordered => ordered(block_parser(&args.blocks_dir)?),
        Function::NoPipelineFn => no_pipeline_fn(block_parser(&args.blocks_dir)?),
        Function::PipelineFn => pipeline_fn(block_parser(&args.blocks_dir)?),
        Function::Pipeline => pipeline(block_parser(&args.blocks_dir)?),
        Function::UtxoParse => utxo_parse(utxo_parser(&args.blocks_dir)?),
        Function::CreateFilter => create_filter(utxo_parser(&args.blocks_dir)?, &args.filter_file)?,
        Function::LoadFilter => load_filter(utxo_parser(&args.blocks_dir)?, &args.filter_file)?,
        Function::Test => test(args)?,
    }
    Ok(())
}

fn parse(parser: BlockParser) {
    let iterator = parser.parse(|block| block.total_size() as u64);
    println!("Total blockchain size: {}", iterator.sum::<u64>());
}

fn parse_noop(parser: BlockParser) {
    let mut blockchain_size = 0;
    for block in parser.parse(identity) {
        blockchain_size += block.total_size() as u64;
    }
    println!("Total blockchain size: {}", blockchain_size);
}

fn map_parallel(parser: BlockParser) {
    let blocks: ParserIterator<Block> = parser.parse(identity);
    let sizes: ParserIterator<u64> = blocks.map_parallel(|block| block.total_size() as u64);
    println!("Total blockchain size: {}", sizes.sum::<u64>());
}

fn ordered(parser: BlockParser) {
    let iter: ParserIterator<String> = parser.parse(|block| block.block_hash().to_string());
    let in_order = iter.ordered().collect::<Vec<_>>();
    println!("Ordered block hashes: \n{}", in_order[0..100].join("\n"));
}

fn no_pipeline_fn(parser: BlockParser) {
    let mut block_sizes: HashMap<BlockHash, isize> = HashMap::new();
    let mut differences = vec![];
    // Initial block size for the genesis block
    block_sizes.insert(BlockHash::all_zeros(), 0);

    for block in parser.parse(identity).ordered() {
        // Store this block's size in the shared state
        let block_size = block.total_size() as isize;
        block_sizes.insert(block.block_hash(), block_size);
        // Look up the previous size to compute the difference
        let prev_block_hash = block.header.prev_blockhash;
        let prev_size = block_sizes.remove(&prev_block_hash);
        differences.push(block_size - prev_size.unwrap());
    }

    let max_difference = differences.into_iter().max().unwrap();
    println!("Maximum increase in block size: {}", max_difference);
}

fn pipeline_fn(parser: BlockParser) {
    let block_sizes: Arc<DashMap<BlockHash, isize>> = Arc::new(DashMap::new());
    let blocksizes_clone = block_sizes.clone();
    // Initial block size for the genesis block
    block_sizes.insert(BlockHash::all_zeros(), 0);

    let iterator = parser.parse(identity).ordered().pipeline_fn(
        move |block: Block| {
            // Store this block's size in the shared state
            let block_size = block.total_size() as isize;
            block_sizes.insert(block.block_hash(), block_size);
            (block.header.prev_blockhash, block_size)
        },
        move |(prev_block_hash, block_size)| {
            // Look up the previous size to compute the difference
            let prev_size = blocksizes_clone.remove(&prev_block_hash);
            block_size - prev_size.unwrap().1
        },
    );

    let max_difference = iterator.max().unwrap();
    println!("Maximum increase in block size: {}", max_difference);
}

fn pipeline(parser: BlockParser) {
    let pipeline = BlockSizePipeline::default();
    pipeline.0.insert(BlockHash::all_zeros(), 0);
    let iter: ParserIterator<isize> = parser.parse(identity).ordered().pipeline(&pipeline);
    println!("Maximum increase in block sizes: {}", iter.max().unwrap());
}

#[derive(Clone, Default)]
struct BlockSizePipeline(Arc<DashMap<BlockHash, usize>>);
impl Pipeline<Block, (BlockHash, usize), isize> for BlockSizePipeline {
    fn first(&self, block: Block) -> (BlockHash, usize) {
        self.0.insert(block.block_hash(), block.total_size());
        (block.header.prev_blockhash, block.total_size())
    }

    fn second(&self, value: (BlockHash, usize)) -> isize {
        let (prev_blockhash, size) = value;
        let (_, prev_size) = self.0.remove(&prev_blockhash).unwrap();
        size as isize - prev_size as isize
    }
}

fn utxo_parse(parser: UtxoParser) {
    let fees = parser.parse().map_parallel(|block| {
        let mut max_mining_fee = Amount::ZERO;
        for tx in block.txdata.into_iter() {
            // For every transaction sum up the input and output amounts
            let inputs: Amount = tx.input().map(|(_, amount)| *amount).sum();
            let outputs: Amount = tx.output().map(|(out, _)| out.value).sum();
            if !tx.transaction.is_coinbase() {
                // Subtract outputs amount from inputs amount to get the fee
                max_mining_fee = max(inputs - outputs, max_mining_fee);
            }
        }
        max_mining_fee
    });
    println!("Maximum mining fee: {}", fees.max().unwrap());
}

fn create_filter(parser: UtxoParser, filter_file: &str) -> Result<()> {
    parser.create_filter(filter_file)?;
    Ok(())
}

fn load_filter(parser: UtxoParser, filter_file: &str) -> Result<()> {
    let blocks = parser.load_filter(filter_file)?.parse();
    let amounts = blocks.map_parallel(|block| {
        let mut max_unspent_tx = Amount::ZERO;
        for tx in block.txdata.into_iter() {
            for (output, status) in tx.output() {
                if status == &OutputStatus::Unspent {
                    max_unspent_tx = max(output.value, max_unspent_tx);
                }
            }
        }
        max_unspent_tx
    });
    println!("Maximum unspent output: {}", amounts.max().unwrap());
    Ok(())
}

/// Integration test based off of real mainchain data
fn test(args: Args) -> Result<()> {
    println!("\nTesting write_filter");
    let parser = UtxoParser::new(&args.blocks_dir)?
        .block_range_end(151_000)
        .create_filter(&args.filter_file)?
        .load_filter(&args.filter_file)?
        .parse();

    println!("\nTesting UtxoParser");
    let test_block =
        BlockHash::from_str("00000000000008df4269884f1d3bfc2aed3ea747292abb89be3dc3faa8c5d26f")
            .unwrap();
    let test_txid1 =
        Txid::from_str("062ed26778b8d0794c269029ee7b1d56b4ecaa379048b21298bf6d35876d00c4").unwrap();
    let test_txid2 =
        Txid::from_str("cf2cc1897eb061e2406e644ecee3c26ee64cfadcc626890438c3d058511c9094").unwrap();

    for block in parser {
        for tx in block.txdata {
            // Verify spent UTXO #2 tx here https://mempool.space/tx/062ed26778b8d0794c269029ee7b1d56b4ecaa379048b21298bf6d35876d00c4
            if tx.txid == test_txid1 {
                let statuses: Vec<_> = tx.output().map(|(_, status)| *status).collect();
                let mut real_status = vec![OutputStatus::Unspent; 3];
                real_status[2] = OutputStatus::Spent;
                assert_eq!(block.header.block_hash(), test_block);
                assert_eq!(statuses, real_status);
            }
            // Verify tx amounts here https://mempool.space/tx/cf2cc1897eb061e2406e644ecee3c26ee64cfadcc626890438c3d058511c9094
            if tx.txid == test_txid2 {
                let amounts: Vec<_> = tx.input().map(|(_, amount)| *amount).collect();
                let real_amounts = vec![
                    Amount::from_sat(56892597),
                    Amount::from_sat(274000000),
                    Amount::from_sat(248000000),
                    Amount::from_sat(14832476),
                    Amount::from_sat(48506744443),
                ];
                assert_eq!(amounts, real_amounts);
            }
        }
    }
    println!("\nTest successful!");
    Ok(())
}
