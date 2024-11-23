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

Fast optimized parser for bitcoin `blocks` with input amount and output status tracking.

⚠️ The API is still evolving and should not be considered stable until release `1.0.0`

## Features
- Parses blocks into the Rust bitcoin [`Block`](bitcoin::Block) format for easier manipulation
- Can track if any [`TxOut`](bitcoin::TxOut) is spent or unspent for calculations on the UTXO set
- Can track the [`Amount`](bitcoin::Amount) of every [`TxIn`](bitcoin::TxIn) for calculations such as fee rates
- Multithreaded in-memory parsing provides the fastest block parsing performance

## Requirements / Benchmarks
- You must be running a [non-pruning](https://bitcoin.org/en/full-node#reduce-storage) bitcoin node (this is the default configuration)
- We recommend using fast storage (e.g. NVMe) and a multithreaded CPU for best performance
- See benchmarks below to understand how much RAM you may need:

| Function                          | Purpose                              | Time    | Memory  |
|-----------------------------------|--------------------------------------|---------|---------|
| BlockParser::parse()              | Parses blocks                        | 2m 55s  | 0.9 GB  | 
| UtxoParser::parse()               | Tracks input amounts (no filter)     | 7m 41s  | 25.0 GB | 
| UtxoParser::create_filter()       | Creates new filter                   | 16m 09s | 5.6 GB  |
| UtxoParser::load_filter().parse() | Tracks input & outputs (with filter) | 7m 46s  | 11.8 GB |

Our benchmarks were run on NVMe storage with a 32-thread processor on **800,000** blocks.

## Quick Usage
See [`BlockParser`](blocks::BlockParser) for details on how to parse blocks:
```rust
use bitcoin_block_parser::*;

// Initialize a logger (if you want to monitor parsing progress)
env_logger::builder().filter_level(log::LevelFilter::Info).init();

// Parse all blocks in the directory and map them to a value
let parser = BlockParser::new("/home/user/.bitcoin/blocks/").unwrap();
for size in parser.parse(|block| block.total_size()) {
  // Do something with the block sizes
}
```

See [`UtxoParser`](utxos::UtxoParser) for details on how to track inputs and outputs:
```rust
use bitcoin_block_parser::*;

use crate::*;

let parser = UtxoParser::new("/home/user/.bitcoin/blocks/").unwrap();
let blocks = parser.load_or_create_filter("filter.bin").unwrap().parse();
for block in blocks {
  for tx in block.txdata {
    for (output, status) in tx.output() {
      // Do something with the output status
    }
    for (input, amount) in tx.input() {
      // Do something with the input amounts
    }
  }
}
```