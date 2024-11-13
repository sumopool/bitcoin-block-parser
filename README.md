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

Fast optimized parser for the bitcoin `blocks` data with UTXO tracking.

⚠️ The API is still evolving and should not be considered stable until release `1.0.0`

## Features
- Parses blocks into the Rust bitcoin [`Block`](bitcoin::Block) format for easier manipulation
- Can track whether any `TxOut` in a `Transaction` is spent or unspent
- Can track the `Amount` of every `TxIn` for calculating metrics such as fee rates
- Or implement your own custom multithreaded [`BlockParser`](crate::BlockParser)
- Uses many optimizations to provide the best block parsing performance

## Requirements / Benchmarks
- You must be running a [non-pruning](https://bitcoin.org/en/full-node#reduce-storage) bitcoin node (this is the default configuration)
- You should look at the table below to understand how much RAM you need (tunable through `Options`)
- We recommend using fast storage and a multithreaded CPU for best performance

Our benchmarks were run on NVMe storage with a 32-thread processor on **850,000** blocks:

| Function       | Time    | Memory  |
|----------------|---------|---------|
| ParallelParser | 3m 29s  | 2.1 GB  | 
| InOrderParser  | 5m 42s  | 2.9 GB  | 
| FilterParser   | 16m 58s | 9.4 GB  |
| UtxoParser     | 42m 12s | 17.2 GB |

## Quick Usage
- To parse blocks pass in the `blocks` directory of your bitcoin node and call [`DefaultParser::parse_dir`](blocks::DefaultParser::parse_dir)
- If your algorithm requires the blocks to be processed in-order use [`InOrderParser::parse_dir`](blocks::InOrderParser::parse_dir)
- For advanced usage and implementing your own block parser see the [`blocks`](crate::blocks) module docs
- To track amounts through the transaction graph see the [`utxos`](crate::utxos) module docs

```rust
use bitcoin_block_parser::*;

// Initialize a logger (if you want to monitor parsing progress)
env_logger::builder().filter_level(log::LevelFilter::Info).init();

// Parse all blocks in the directory and map them to a value
let results = ParallelParser.parse_dir("/home/user/.bitcoin/blocks", |block| {
    block.total_size()
}).unwrap();

// Iterate over the results to do whatever you want
for result in results {
    println!("Block size {}", result.unwrap());
}
```
