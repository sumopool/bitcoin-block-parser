use anyhow::Result;
use bitcoin::{Amount, BlockHash, Txid};
use bitcoin_block_parser::{BlockLocation, BlockParser, OutStatus};
use clap::{Parser, ValueEnum};
use std::path::Path;
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
    WriteFilter,
    Parse,
    ParseO,
    ParseI,
    ParseIO,
    Test,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut locations = BlockLocation::parse(&args.input)?;
    locations.truncate(300_000);
    let parser = BlockParser::new(&locations);
    match args.run {
        Function::WriteFilter => {
            parser.write_filter(&args.filter_file)?;
        }
        Function::Parse => {
            for parsed in parser.parse() {
                let parsed = parsed?;
                for (_, _) in parsed.transactions() {}
            }
        }
        Function::ParseI => {
            for parsed in parser.parse_i() {
                let parsed = parsed?;
                for (tx, txid) in parsed.transactions() {
                    assert_eq!(tx.input.len(), parsed.input_amount(txid)?.len());
                }
            }
        }
        Function::ParseO => {
            for parsed in parser.parse_o(&args.filter_file) {
                let parsed = parsed?;
                for (tx, txid) in parsed.transactions() {
                    assert_eq!(tx.output.len(), parsed.output_status(txid)?.len());
                }
            }
        }
        Function::ParseIO => {
            for parsed in parser.parse_io(&args.filter_file) {
                let parsed = parsed?;
                for (tx, txid) in parsed.transactions() {
                    assert_eq!(tx.output.len(), parsed.output_status(txid)?.len());
                    assert_eq!(tx.input.len(), parsed.input_amount(txid)?.len());
                }
            }
        }
        Function::Test => test(&args.input, &args.filter_file)?,
    }
    Ok(())
}

/// Integration test based off of real mainchain data
fn test(blocks_dir: &str, filter_file: &str) -> Result<()> {
    // Create a filter if one doesn't yet exist
    let mut locations = BlockLocation::parse(blocks_dir)?;
    let parser = BlockParser::new(&locations);
    if !Path::new(filter_file).exists() {
        println!("\nTesting write_filter");
        parser.write_filter(filter_file)?;
    }

    // Truncate blocks so we can run the tests quickly
    locations.truncate(150_000);

    println!("\nTesting parse_o");
    let test_block =
        BlockHash::from_str("0000000000000aff9f826ed90c99e0aeb673e22494e830fecde4113a6bc264af")?;
    let test_txid =
        Txid::from_str("3045a3b13be965622fb5d1652a810e39dca28913f495604f99de6ffb9f71f587")?;
    let parser = BlockParser::new(&locations);
    for parsed in parser.parse_o(filter_file) {
        let parsed = parsed?;
        for (_, txid) in parsed.transactions() {
            if *txid == test_txid {
                let mut status = vec![OutStatus::Spent; 7];
                status[2] = OutStatus::Unspent;
                assert_eq!(parsed.block.block_hash(), test_block);
                assert_eq!(*parsed.output_status(txid)?, status);
            }
        }
    }

    println!("\nTesting parse_i");
    let test_txid =
        Txid::from_str("cf2cc1897eb061e2406e644ecee3c26ee64cfadcc626890438c3d058511c9094")?;
    let parser = BlockParser::new(&locations);
    let amounts = vec![
        sat(56892597),
        sat(274000000),
        sat(248000000),
        sat(14832476),
        sat(48506744443),
    ];
    for parsed in parser.parse_i() {
        let parsed = parsed?;
        for (_, txid) in parsed.transactions() {
            if *txid == test_txid {
                assert_eq!(*parsed.input_amount(txid)?, amounts);
            }
        }
    }

    println!("\nTesting parse_io");
    for parsed in parser.parse_io(filter_file) {
        let parsed = parsed?;
        for (_, txid) in parsed.transactions() {
            if *txid == test_txid {
                assert_eq!(*parsed.input_amount(txid)?, amounts);
            }
        }
    }

    Ok(())
}

fn sat(amt: u64) -> Amount {
    Amount::from_sat(amt)
}

#[allow(dead_code, unused_variables)]
fn example() -> Result<()> {
    // Load all the block locations from the headers of the block files
    let locations = BlockLocation::parse("/home/user/.bitcoin/blocks")?;
    // Create a parser from a slice of the first 100K blocks
    let parser = BlockParser::new(&locations[0..100_000]);
    // Iterates over all the blocks in height order
    for parsed in parser.parse() {
        // Do whatever you want with the parsed block here
        parsed?.block.check_witness_commitment();
    }

    // `parse_i()` provides the input amounts for all transactions
    for parsed in parser.parse_i() {
        // Any parsing errors will be returned here
        let parsed = parsed.expect("parsing failed");
        // Iterate over all transactions (txid is computed by the parser)
        for (tx, txid) in parsed.transactions() {
            // Iterate over all the inputs in the transaction
            let mut total_amount: Amount = Amount::ZERO;
            for (amount, input) in parsed.input_amount(txid)?.iter().zip(tx.input.iter()) {
                // Do whatever you want with the input amount
                total_amount += *amount;
            }
        }
    }

    // We have to write filter before getting any output information
    parser.write_filter("filter.bin")?;
    // `parse_o()` provides whether the output was spent or unspent for all transactions
    for parsed in parser.parse_o("filter.bin") {
        // Any parsing errors will be returned here
        let parsed = parsed.expect("parsing failed");
        // Iterate over all transactions (txid is computed by the parser)
        for (tx, txid) in parsed.transactions() {
            // Iterate over all the outputs in the transaction
            for (status, output) in parsed.output_status(txid)?.iter().zip(tx.output.iter()) {
                // Do whatever you want with the output status
                match status {
                    OutStatus::Unspent => {}
                    OutStatus::Spent => {}
                }
            }
        }
    }

    // If we already wrote the filter file we don't need to write it again for the same blocks
    for parsed in parser.parse_io("filter.bin") {
        // Do whatever you want with output status and input amounts
    }

    Ok(())
}
