# Bitcoin Block Parser

[![Crates.io][crates-badge]][crates-url]
[![Docs][docs-badge]][docs-url]
[![MIT licensed][mit-badge]][mit-url]

[crates-badge]: https://img.shields.io/crates/v/bitcoin-block-parser.svg
[crates-url]: https://crates.io/crates/bitcoin-block-parser
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/sumopool/bitcoin-block-parser/blob/master/LICENSE
[docs-badge]: https://img.shields.io/docsrs/bitcoin-block-parser
[docs-url]: https://docs.rs/bitcoin-block-parser

Blazing fast parser for bitcoin `blocks` data with input amount and output spend tracking.

⚠️ The API is still evolving and should not be considered stable until release `1.0.0`

## Features
- Parses blocks into the [Rust bitcoin](https://github.com/rust-bitcoin/rust-bitcoin) [`Block`](bitcoin::Block) format for easier manipulation
- Can track if any [`TxOut`](bitcoin::TxOut) is spent or unspent for calculations on the UTXO set
- Can track the [`TxOut`](bitcoin::Amount) of every [`TxIn`](bitcoin::TxIn) for calculating metrics such as fee rates
- Multithreaded in-memory parsing provides fast block parsing performance

## Requirements / Benchmarks
- You must be running a [non-pruning](https://bitcoin.org/en/full-node#reduce-storage) bitcoin node (this is the default configuration)
- We recommend using fast storage (e.g. NVMe) and a multithreaded CPU for best performance
- See benchmarks below to understand how much RAM you may need:

| Function                                              | Time    | Memory  |
|-------------------------------------------------------|---------|---------|
| `BlockParser::parse()`<br/>Parses blocks              | 2m 55s  | 0.9 GB  | 
| `UtxoParser::create_filter()`<br/>Create a new filter | 15m 12s | 2.6 GB  | 
| `UtxoParser::parse()`<br/>Parse with existing filter  | 18m 30s | 12.1 GB |

Our benchmarks were run on NVMe storage with a 32-thread processor on **800,000** blocks.

## Quick Usage
See [`BlockParser`](blocks::BlockParser) for details on how to parse blocks:
```rust
use bitcoin_block_parser::*;

// Initialize a logger (if you want to monitor parsing progress)
env_logger::builder().filter_level(log::LevelFilter::Info).init();

// Parse all blocks in the directory and map them to total_size
let parser = BlockParser::new("/home/user/.bitcoin/blocks/").unwrap();
for size in parser.parse(|block| block.total_size()) {
  // Do something with the block sizes
}
```

See [`UtxoParser`](utxos::UtxoParser) for details on how to track inputs and outputs:
```rust
use bitcoin_block_parser::*;

// Load a filter file or create a new one for tracking output status
let parser = UtxoParser::new("/home/user/.bitcoin/blocks/", "filter.bin");
for txdata in parser.parse(|block| block.txdata).unwrap() {
  for tx in txdata {
    for (output, status) in tx.output() {
      // Do something with the output status
    }
    for (input, output) in tx.input() {
      // Do something with TxOut that are used in the inputs
    }
  }
}
```