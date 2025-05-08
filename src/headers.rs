//! Used to parse the [`bitcoin::block::Header`] from the `blocks` directory to order and locate
//! every block for later parsing.

use crate::xor::{XorReader, XOR_MASK_LEN};
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use bitcoin::block::Header;
use bitcoin::consensus::Decodable;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use log::info;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use threadpool::ThreadPool;

/// Before the header are 4 magic bytes and 4 bytes that indicate the block size
const PRE_HEADER_SIZE: usize = 8;

/// Points to the on-disk location where a block starts (and the header ends)
#[derive(Clone, Debug)]
pub struct ParsedHeader {
    /// Consensus parsed `bitcoin::Header`
    pub inner: Header,
    /// Byte offset from the beginning of the file
    pub offset: usize,
    /// This header's block hash
    pub hash: BlockHash,
    /// Path of the BLK file
    pub path: PathBuf,
    /// XOR mask of the BLK file
    pub xor_mask: Option<[u8; XOR_MASK_LEN]>,
}
/// You can [specify the blocks directory](https://en.bitcoin.it/wiki/Data_directory) when
/// running `bitcoind`.

/// Fast multithreaded parser of [`ParsedHeader`] from the blocks directory
pub struct HeaderParser;
impl HeaderParser {
    /// Parses the headers from the bitcoin `blocks` directory returning [`ParsedHeader`] in height order,
    /// starting from the genesis block.
    /// - Returns an `Err` if the directory contains invalid `.blk` files.
    /// - Takes a few seconds to run.
    pub fn parse(blocks_dir: &str) -> Result<Vec<ParsedHeader>> {
        info!("Reading headers from {}", blocks_dir);
        let xor_mask = Self::read_xor_mask(blocks_dir)?;
        let (tx, rx) = mpsc::channel();
        let pool = ThreadPool::new(64);

        // Read headers from every BLK file in a new thread
        for path in Self::blk_files(blocks_dir)? {
            let path = path.clone();
            let tx = tx.clone();
            pool.execute(move || {
                let results = Self::parse_headers_file(path, xor_mask);
                let _ = tx.send(results);
            });
        }
        drop(tx);

        // Receive all the headers from spawned threads
        let mut locations = HashMap::default();
        let mut collisions: Vec<ParsedHeader> = vec![];
        for received in rx {
            for header in received? {
                if let Some(collision) = locations.insert(header.inner.prev_blockhash, header) {
                    collisions.push(collision)
                }
            }
        }

        // Resolve reorgs and order the headers by block height
        for collision in collisions {
            Self::resolve_collisions(&mut locations, collision);
        }
        let ordered = Self::order_headers(locations);
        info!("Finished reading {} headers", ordered.len());
        if ordered.len() == 0 {
            bail!("Read 0 Headers. Is blk000000.dat missing?");
        }
        Ok(ordered)
    }

    /// Parses headers from a BLK file
    fn parse_headers_file(
        path: PathBuf,
        xor_mask: Option<[u8; XOR_MASK_LEN]>,
    ) -> Result<Vec<ParsedHeader>> {
        let buffer_size = PRE_HEADER_SIZE + Header::SIZE;
        let reader = XorReader::new(
            File::open(&path).context(format!("Could not open: {}", path.display().to_string()))?,
            xor_mask,
        );
        let mut reader = BufReader::with_capacity(buffer_size, reader);

        let mut offset = 0;
        // First 8 bytes are 4 magic bytes and 4 bytes that indicate the block size
        let mut buffer = vec![0; PRE_HEADER_SIZE];
        let mut headers = vec![];

        while reader.read_exact(&mut buffer).is_ok() {
            offset += buffer.len();
            if let Ok(header) = Header::consensus_decode(&mut reader) {
                headers.push(ParsedHeader {
                    inner: header,
                    offset: offset + Header::SIZE,
                    hash: header.block_hash(),
                    path: path.clone(),
                    xor_mask,
                });
                // Get the size of the next block
                let size = u32::from_le_bytes(buffer[4..].try_into()?) as usize;
                // Seek to the next block, subtracting the block header bytes we parsed
                reader.seek_relative(size.saturating_sub(Header::SIZE) as i64)?;
                offset += size;
            }
        }
        Ok(headers)
    }

    /// Returns the list of all BLK files in the dir
    fn blk_files(dir: &str) -> Result<Vec<PathBuf>> {
        let read_dir = fs::read_dir(Path::new(&dir)).context(dir.to_string())?;
        let mut files = vec![];

        for file in read_dir {
            let file = file?;
            let name = file.file_name().to_string_lossy().to_string();
            if name.starts_with("blk") {
                files.push(file.path())
            }
        }

        if files.is_empty() {
            bail!("No BLK files found in dir {:?}", dir);
        }

        Ok(files)
    }

    /// Reads the block XOR mask. If no `xor.dat` file is present,
    /// use all-zeroed array to perform an XOR no-op.
    fn read_xor_mask<P: AsRef<Path>>(dir: P) -> Result<Option<[u8; XOR_MASK_LEN]>> {
        let path = dir.as_ref().join("xor.dat");
        if !path.exists() {
            return Ok(None);
        }
        let mut file = File::open(path)?;
        let mut buf = [0_u8; XOR_MASK_LEN];
        file.read_exact(&mut buf)?;
        Ok(Some(buf))
    }

    /// In case of reorgs we need to resolve to the longest chain
    fn resolve_collisions(headers: &mut HashMap<BlockHash, ParsedHeader>, collision: ParsedHeader) {
        let existing = headers
            .get(&collision.inner.prev_blockhash)
            .expect("Missing previous blockhash (corrupted blocks)");
        let mut e_hash = &existing.hash;
        let mut c_hash = &collision.hash;

        while let (Some(e), Some(c)) = (headers.get(e_hash), headers.get(c_hash)) {
            e_hash = &e.hash;
            c_hash = &c.hash;
        }

        // In case collision is the longest, update the blocks map
        if headers.contains_key(c_hash) {
            headers.insert(collision.inner.prev_blockhash, collision);
        }
    }

    /// Puts the headers into the correct order by block height (using the hashes)
    fn order_headers(mut headers: HashMap<BlockHash, ParsedHeader>) -> Vec<ParsedHeader> {
        let mut ordered_headers = vec![];
        // Genesis block starts with prev = all_zeros
        let mut next_hash = BlockHash::all_zeros();

        while let Some(index) = headers.remove(&next_hash) {
            next_hash = index.hash;
            ordered_headers.push(index);
        }

        ordered_headers
    }
}
