use std::{future::Future, path::PathBuf, pin::Pin};

use async_fs as afs;
use futures::io::AsyncRead;

pub(crate) struct LazyFileReader {
    path: PathBuf,
    reader: Option<afs::File>,
    opening: Option<Pin<Box<dyn Future<Output = std::io::Result<afs::File>>>>>,
    end: bool,
}

impl LazyFileReader {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            reader: None,
            opening: None,
            end: false,
        }
    }
}

impl AsyncRead for LazyFileReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.end {
            return std::task::Poll::Ready(Ok(0));
        }
        if self.reader.is_none() {
            if self.opening.is_none() {
                let fut = afs::File::open(self.path.clone());
                self.opening = Some(Box::pin(fut));
            }
            let fut = self.opening.as_mut().unwrap();
            match fut.as_mut().poll(_cx) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Ok(f)) => {
                    self.reader = Some(f);
                    self.opening = None;
                }
                std::task::Poll::Ready(Err(e)) => {
                    self.opening = None;
                    return std::task::Poll::Ready(Err(e));
                }
            }
        }
        let poll = std::pin::Pin::new(self.reader.as_mut().unwrap()).poll_read(_cx, buf);
        match poll {
            std::task::Poll::Ready(Ok(n)) => {
                if n == 0 {
                    self.end = true;
                    self.reader = None;
                }
                std::task::Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
}
