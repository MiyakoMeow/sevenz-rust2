use std::{io::Read, path::PathBuf};

use async_fs as afs;
use async_io::block_on;
use futures_lite::AsyncReadExt;

pub(crate) struct LazyFileReader {
    path: PathBuf,
    reader: Option<afs::File>,
    end: bool,
}

impl LazyFileReader {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            reader: None,
            end: false,
        }
    }
}

impl Read for LazyFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.end {
            return Ok(0);
        }
        if self.reader.is_none() {
            self.reader = Some(block_on(afs::File::open(&self.path))?);
        }
        let n = block_on(self.reader.as_mut().unwrap().read(buf))?;
        if n == 0 {
            self.end = true;
            self.reader = None;
        }
        Ok(n)
    }
}
