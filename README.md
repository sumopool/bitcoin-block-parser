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

## Features
- Parses blocks into the Rust bitcoin [`Block`](bitcoin::Block) format for easier manipulation
- Tracks whether any `TxOut` in a `Transaction` is spent or unspent
- Tracks the `Amount` of every `TxIn` for calculating metrics such as fee rates
- Uses memory-optimizations, multithreading, and filters to
  provide the best performance

## Requirements / Benchmarks
- You must be running a [non-pruning](https://bitcoin.org/en/full-node#reduce-storage) bitcoin node (this is the default configuration)
- You should look at the table below to understand how much RAM you need (tunable through `Options`)
- We recommend using fast storage and a multithreaded CPU for best performance

Our benchmarks were run on NVMe storage with a 32-thread processor on **850,000** blocks:

| Function       | Time   | Memory  |
|----------------|--------|---------|
| DefaultParser  | 5 min  | 3.5 GB  | 
| FilterParser   | 17 min | 9.3 GB  |
| UtxoParser     | 39 min | 17.5 GB |

## Quick Usage
- To parse blocks pass in the `blocks` directory of your bitcoin node and call [parse()](DefaultParser::parse)
- If your algorithm requires the blocks to be processed in-order use [parse_ordered()](DefaultParser::parse_ordered)
- For examples of how to write custom parsers see the [blocks](crate::blocks) and [utxos](crate::utxos) module docs

```rust
use bitcoin_block_parser::*;

// Load all the block locations from the headers of the block files
let mut headers = HeaderParser::parse("/home/user/.bitcoin/blocks")?;
// Iterates over all the first 600k blocks
for block in DefaultParser.parse(&headers[..600_000]) {
  // Do whatever you want with the parsed block here
  parsed?.block.check_witness_commitment();
}
```
