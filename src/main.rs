use anyhow::Result;
use bitcoin::{Amount, BlockHash, Txid};
use bitcoin_block_parser::blocks::{BlockParser, DefaultParser};
use bitcoin_block_parser::headers::HeaderParser;
use bitcoin_block_parser::utxos::{FilterParser, OutStatus, UtxoParser};
use clap::{Parser, ValueEnum};
use std::str::FromStr;

/// Example program that can perform self-tests and benchmarks
#[derive(Parser, Debug)]
struct Args {
    /// Input directory containing BLK files
    #[arg(short, long)]
    input: String,

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
    /// Runs a self-test
    Test,
    /// [`FilterParser`] benchmark writing a filter
    Filter,
    /// [`UtxoParser`] benchmark parsing the UTXOs
    Utxo,
    /// [`DefaultParser::parse`] benchmark for all blocks unordered
    Unordered,
    /// [`DefaultParser::parse_ordered`] benchmark for all blocks ordered
    Ordered,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut headers = HeaderParser::parse(&args.input)?;
    headers.truncate(850_000);

    match args.run {
        Function::Test => test(args)?,
        Function::Filter => {
            let parser = FilterParser::new();
            for _ in parser.parse(&headers) {}
            parser.write(&args.filter_file)?;
        }
        Function::Utxo => {
            let parser = UtxoParser::new(&args.filter_file)?;
            for block in parser.parse(&headers) {
                let block = block?;
                for (tx, txid) in block.transactions() {
                    assert_eq!(tx.output.len(), block.output_status(txid).len());
                    assert_eq!(tx.input.len(), block.input_amount(txid).len());
                }
            }
        }
        Function::Unordered => for _ in DefaultParser.parse(&headers) {},
        Function::Ordered => for _ in DefaultParser.parse_ordered(&headers) {},
    }
    Ok(())
}

/// Integration test based off of real mainchain data
fn test(args: Args) -> Result<()> {
    // Create a filter if one doesn't yet exist
    let mut headers = HeaderParser::parse(&args.input)?;
    // Truncate so we can capture the spends, but also run faster
    headers.truncate(151_000);

    let parser = FilterParser::new();
    println!("\nTesting write_filter");
    for _ in parser.parse(&headers) {}
    parser.write(&args.filter_file)?;

    println!("\nTesting UtxoParser");
    let test_block =
        BlockHash::from_str("00000000000008df4269884f1d3bfc2aed3ea747292abb89be3dc3faa8c5d26f")?;
    let test_txid1 =
        Txid::from_str("062ed26778b8d0794c269029ee7b1d56b4ecaa379048b21298bf6d35876d00c4")?;
    let test_txid2 =
        Txid::from_str("cf2cc1897eb061e2406e644ecee3c26ee64cfadcc626890438c3d058511c9094")?;

    let parser = UtxoParser::new(&args.filter_file)?;
    for block in parser.parse(&headers) {
        let block = block?;
        for (_, txid) in block.transactions() {
            // Verify spent UTXO #2 tx here https://mempool.space/tx/062ed26778b8d0794c269029ee7b1d56b4ecaa379048b21298bf6d35876d00c4
            if *txid == test_txid1 {
                let mut status = vec![OutStatus::Unspent; 3];
                status[2] = OutStatus::Spent;
                assert_eq!(block.block.block_hash(), test_block);
                assert_eq!(*block.output_status(txid), status);
            }
            // Verify tx amounts here https://mempool.space/tx/cf2cc1897eb061e2406e644ecee3c26ee64cfadcc626890438c3d058511c9094
            if *txid == test_txid2 {
                let amounts = vec![
                    sat(56892597),
                    sat(274000000),
                    sat(248000000),
                    sat(14832476),
                    sat(48506744443),
                ];
                assert_eq!(*block.input_amount(txid), amounts);
            }
        }
    }
    println!("Test successful!");
    Ok(())
}

fn sat(amt: u64) -> Amount {
    Amount::from_sat(amt)
}
