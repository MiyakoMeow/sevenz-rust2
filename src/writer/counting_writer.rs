use futures::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use std::{
    cell::Cell,
    io::{Result, Seek, SeekFrom, Write},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

pub(crate) struct CountingWriter<W> {
    inner: W,
    counting: Rc<Cell<usize>>,
    written_bytes: usize,
}

impl<W> CountingWriter<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self {
            inner,
            counting: Rc::new(Cell::new(0)),
            written_bytes: 0,
        }
    }

    pub(crate) fn counting(&self) -> Rc<Cell<usize>> {
        Rc::clone(&self.counting)
    }
}

impl<W: AsyncWrite + Unpin> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let len = async_io::block_on(AsyncWriteExt::write(&mut self.inner, buf))?;
        self.written_bytes += len;
        self.counting.set(self.written_bytes);
        Ok(len)
    }

    fn flush(&mut self) -> Result<()> {
        async_io::block_on(AsyncWriteExt::flush(&mut self.inner))
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for CountingWriter<W> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(len)) = &poll {
            this.written_bytes += *len;
            this.counting.set(this.written_bytes);
        }
        poll
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_close(cx)
    }
}

impl<W: AsyncSeek + Unpin> Seek for CountingWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        async_io::block_on(AsyncSeekExt::seek(
            &mut self.inner,
            match pos {
                SeekFrom::Start(n) => futures::io::SeekFrom::Start(n),
                SeekFrom::End(i) => futures::io::SeekFrom::End(i),
                SeekFrom::Current(i) => futures::io::SeekFrom::Current(i),
            },
        ))
    }
}
