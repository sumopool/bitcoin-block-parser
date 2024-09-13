use crate::NUM_FILE_THREADS;
use anyhow::bail;
use anyhow::Result;
use bitcoin::block::Header;
use bitcoin::consensus::Decodable;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rustc_hash::FxHashMap;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read, Seek};
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
}

pub struct HeaderParser;
impl HeaderParser {
    /// Parses the headers from the `blocks_dir` returning the `ParsedHeader` in height order,
    /// starting from the genesis block.  Takes a few seconds to run.
    pub fn parse(blocks_dir: &str) -> Result<Vec<ParsedHeader>> {
        let (tx, rx) = mpsc::channel();
        let pool = ThreadPool::new(NUM_FILE_THREADS);

        // Read headers from every BLK file in a new thread
        for path in Self::blk_files(blocks_dir)? {
            let path = path.clone();
            let tx = tx.clone();
            pool.execute(move || {
                let results = Self::parse_headers_file(path);
                let _ = tx.send(results);
            });
        }
        drop(tx);

        // Receive all the headers from spawned threads
        let mut locations = FxHashMap::default();
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
        Ok(Self::order_headers(locations))
    }

    /// Parses headers from a BLK file
    fn parse_headers_file(path: PathBuf) -> anyhow::Result<Vec<ParsedHeader>> {
        let buffer_size = PRE_HEADER_SIZE + Header::SIZE;
        let mut reader = BufReader::with_capacity(buffer_size, File::open(&path)?);
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
    fn blk_files(dir: &str) -> anyhow::Result<Vec<PathBuf>> {
        let read_dir = fs::read_dir(Path::new(&dir))?;
        let mut files = vec![];

        for file in read_dir {
            let file = file?;
            let name = file
                .file_name()
                .into_string()
                .expect("Could not parse filename");
            if name.starts_with("blk") {
                files.push(file.path())
            }
        }

        if files.is_empty() {
            bail!("No BLK files found in dir {:?}", dir);
        }

        Ok(files)
    }

    /// In case of reorgs we need to resolve to the longest chain
    fn resolve_collisions(
        headers: &mut FxHashMap<BlockHash, ParsedHeader>,
        collision: ParsedHeader,
    ) {
        let existing = headers
            .get(&collision.inner.prev_blockhash)
            .expect("exists");
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
    fn order_headers(mut headers: FxHashMap<BlockHash, ParsedHeader>) -> Vec<ParsedHeader> {
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
