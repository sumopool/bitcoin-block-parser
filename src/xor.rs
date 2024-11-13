//! Handles XOR'd Bitcoin-core block data.
//!
//! - https://github.com/bitcoin/bitcoin/pull/28052

use std::io::{Read, Seek, SeekFrom};

/// XOR mask length. It's the length of file `blocks/xor.dat`.
pub const XOR_MASK_LEN: usize = 8;

/// Transparent reader for XOR'd blk*.dat files.
pub struct XorReader<R: Read> {
    /// Inner reader.
    inner: R,
    /// Stream position. This is expected to be synchronous with `Seek::stream_position`,
    /// but without a syscall to fetch it.
    pos: u64,
    /// XOR mask.
    mask: [u8; XOR_MASK_LEN],
}

impl<R: Read> XorReader<R> {
    /// Create a reader wrapper that performs XOR on reads.
    pub fn new(reader: R, xor_mask: [u8; XOR_MASK_LEN]) -> Self {
        Self {
            inner: reader,
            pos: 0,
            mask: xor_mask,
        }
    }
}

impl<R: Read> Read for XorReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let size = self.inner.read(buf)?;
        for x in &mut buf[..size] {
            *x ^= self.mask[(self.pos % self.mask.len() as u64) as usize];
            self.pos += 1;
        }
        Ok(size)
    }
}

impl<R: Seek + Read> Seek for XorReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let result = self.inner.seek(pos);
        // Just use a syscall to update the current position.
        self.pos = self.inner.stream_position()?;
        result
    }
}
