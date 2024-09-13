# Bitcoin Block Parser

Fast optimized parser for the bitcoin `blocks` data with UTXO tracking.

## Features
- Parses blocks into the [Rust bitcoin crate](https://crates.io/crates/bitcoin) `Block` format for easier manipulation
- Tracks whether any `TxOut` in a `Transaction` is spent or unspent
- Tracks the `Amount` of every `TxIn` for calculating metrics such as fee rates
- Uses memory-optimizations, multithreading, and [cuckoo filters](https://en.wikipedia.org/wiki/Cuckoo_filter) to
  provide the best performance

## Requirements / Benchmarks
- You must be running a [non-pruning](https://bitcoin.org/en/full-node#reduce-storage) bitcoin node (this is the default configuration)
- You should look at the table below to understand how much RAM you need (increases with # of blocks)
- We recommend using fast storage and a multithreaded CPU for best performance

Our benchmarks were run on NVMe storage with a 32-thread processor on **850,000** blocks:

| Function       | Time   | Memory  |
|----------------|--------|---------|
| parse()        | 6 min  | 2.5 GB  |
| parse_o()      | 21 min | 3.4 GB  |
| parse_i()      | 47 min | 35.4 GB |
| parse_io()     | 47 min | 10.9 GB |
| write_filter() | 23 min | 6.6 GB  |

Beware of running out-of-memory when using `parse_i()` because unspent UTXOs are stored in RAM instead of a filter.

## Usage
To parse blocks pass in the `blocks` directory of your bitcoin node and call `BlockParser::parse()`
```rust
// Load all the block locations from the headers of the block files
let locations = BlockLocation::parse("/home/user/.bitcoin/blocks")?;
// Create a parser from a slice of the first 100K blocks
let parser = BlockParser::new(&locations[0..100_000]);
// Iterates over all the blocks in height order
for parsed in parser.parse() {
    // Do whatever you want with the parsed block here
    parsed?.block.check_witness_commitment();
}
```

If you need the input `Amount` for every transaction you can use `BlockParser::parse_i()`
```rust
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
```

If you need to know if a transaction output was spent or unspent use `BlockParser::parse_o()`
```rust
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
                OutStatus::Unspent => {},
                OutStatus::Spent => {}
            }
        }
    }
}
```

If you want both input amounts and output status use `BlockParser::parse_io()`
```rust
// If we already wrote the filter file we don't need to write it again for the same blocks
for parsed in parser.parse_io("filter.bin") {
    // Do whatever you want with output status and input amounts
}
```