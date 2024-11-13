#![cfg_attr(not(doctest), doc = include_str!("../README.md"))]
#![warn(missing_docs)]
#![allow(rustdoc::redundant_explicit_links)]

pub mod blocks;
#[cfg(feature = "examples")]
pub mod examples;
pub mod headers;
#[cfg(feature = "utxo")]
pub mod utxos;
pub mod xor;

pub use blocks::BlockParser;
pub use blocks::InOrderParser;
pub use blocks::ParallelParser;
pub use headers::HeaderParser;
