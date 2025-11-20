use futures::io::Cursor;
#[cfg(feature = "compress")]
use std::collections::VecDeque;

use crate::Error;
use async_compression::futures::bufread::Lz4Decoder as AsyncLz4Decoder;
#[cfg(feature = "compress")]
use async_compression::futures::write::Lz4Encoder as AsyncLz4Encoder;
use futures::io::BufReader as AsyncBufReader;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Magic bytes of a skippable frame as used in LZ4 by zstdmt.
const SKIPPABLE_FRAME_MAGIC: u32 = 0x184D2A50;

/// Custom decoder to support the custom format first implemented by zstdmt, which allows to have
/// optional skippable frames.
pub(crate) struct Lz4Decoder<R: AsyncRead + Unpin> {
    inner: Option<AsyncLz4Decoder<AsyncBufReader<InnerReader<R>>>>,
}

impl<R: AsyncRead + Unpin> Lz4Decoder<R> {
    pub(crate) async fn new(mut input: R) -> Result<Self, Error> {
        let mut header = [0u8; 12];
        let header_read = match AsyncReadExt::read(&mut input, &mut header).await {
            Ok(n) if n >= 4 => n,
            Ok(_) => return Err(Error::other("Input too short")),
            Err(e) => return Err(e.into()),
        };

        let magic_value = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);

        let inner_reader = if magic_value == SKIPPABLE_FRAME_MAGIC && header_read >= 12 {
            let skippable_size = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
            if skippable_size != 4 {
                return Err(Error::other("Invalid lz4 skippable frame size"));
            }

            let compressed_size =
                u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

            InnerReader::new_skippable(input, compressed_size)
        } else {
            InnerReader::new_standard(input, header[..header_read].to_vec())
        };

        let bufread = AsyncBufReader::new(inner_reader);
        let decoder = AsyncLz4Decoder::new(bufread);

        Ok(Lz4Decoder {
            inner: Some(decoder),
        })
    }
}

impl<R: AsyncRead + Unpin> futures::io::AsyncRead for Lz4Decoder<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if let Some(inner) = &mut self.inner {
            let mut pin_inner = std::pin::Pin::new(inner);
            match pin_inner.as_mut().poll_read(cx, buf) {
                std::task::Poll::Ready(Ok(0)) => {
                    let inner_reader: &mut InnerReader<R> = {
                        let bufreader: &mut AsyncBufReader<InnerReader<R>> =
                            pin_inner.get_mut().get_mut();
                        bufreader.get_mut()
                    };
                    if inner_reader.read_next_frame_header()? {
                        let reader = std::mem::replace(inner_reader, InnerReader::empty());
                        let bufread: AsyncBufReader<InnerReader<R>> = AsyncBufReader::new(reader);
                        let mut deencoder = AsyncLz4Decoder::new(bufread);
                        let poll = std::pin::Pin::new(&mut deencoder).poll_read(cx, buf);
                        self.inner = Some(deencoder);
                        poll
                    } else {
                        self.inner = None;
                        std::task::Poll::Ready(Ok(0))
                    }
                }
                other => other,
            }
        } else {
            std::task::Poll::Ready(Ok(0))
        }
    }
}

enum InnerReader<R: AsyncRead + Unpin> {
    Empty,
    Standard {
        reader: R,
        header_buffer: Cursor<Vec<u8>>,
        header_finished: bool,
    },
    Skippable {
        reader: R,
        remaining_in_frame: u32,
        frame_finished: bool,
    },
}

impl<R: AsyncRead + Unpin> InnerReader<R> {
    fn empty() -> Self {
        InnerReader::Empty
    }

    fn new_standard(reader: R, header: Vec<u8>) -> Self {
        InnerReader::Standard {
            reader,
            header_buffer: Cursor::new(header),
            header_finished: false,
        }
    }

    fn new_skippable(reader: R, remaining_in_frame: u32) -> Self {
        InnerReader::Skippable {
            reader,
            remaining_in_frame,
            frame_finished: false,
        }
    }

    fn read_next_frame_header(&mut self) -> std::io::Result<bool> {
        match self {
            InnerReader::Empty => Ok(false),
            InnerReader::Standard { .. } => Ok(false),
            InnerReader::Skippable {
                reader,
                remaining_in_frame,
                frame_finished,
            } => {
                if !*frame_finished {
                    return Ok(false);
                }
                let mut buf4 = [0u8; 4];
                match async_io::block_on(AsyncReadExt::read_exact(reader, &mut buf4)) {
                    Ok(_) => {
                        let magic = u32::from_le_bytes(buf4);
                        if magic != SKIPPABLE_FRAME_MAGIC {
                            return Ok(false);
                        }

                        async_io::block_on(AsyncReadExt::read_exact(reader, &mut buf4))?;
                        let skippable_size = u32::from_le_bytes(buf4);
                        if skippable_size != 4 {
                            return Ok(false);
                        }

                        async_io::block_on(AsyncReadExt::read_exact(reader, &mut buf4))?;
                        let compressed_size = u32::from_le_bytes(buf4);

                        *remaining_in_frame = compressed_size;
                        *frame_finished = false;

                        Ok(true)
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
                    Err(e) => Err(e),
                }
            }
        }
    }
}

impl<R: AsyncRead + Unpin> futures::io::AsyncRead for InnerReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut *self {
            InnerReader::Empty => std::task::Poll::Ready(Ok(0)),
            InnerReader::Standard {
                reader,
                header_buffer,
                header_finished,
            } => {
                if !*header_finished {
                    let poll = std::pin::Pin::new(header_buffer).poll_read(cx, buf);
                    if let std::task::Poll::Ready(Ok(bytes_read)) = poll {
                        if bytes_read > 0 {
                            return std::task::Poll::Ready(Ok(bytes_read));
                        }
                        *header_finished = true;
                    } else {
                        return poll;
                    }
                }
                std::pin::Pin::new(reader).poll_read(cx, buf)
            }
            InnerReader::Skippable {
                reader,
                remaining_in_frame,
                frame_finished,
            } => {
                if *frame_finished || *remaining_in_frame == 0 {
                    return std::task::Poll::Ready(Ok(0));
                }
                let bytes_to_read = std::cmp::min(*remaining_in_frame as usize, buf.len());
                let poll = std::pin::Pin::new(reader).poll_read(cx, &mut buf[..bytes_to_read]);
                if let std::task::Poll::Ready(Ok(bytes_read)) = poll {
                    if bytes_read == 0 {
                        *frame_finished = true;
                        return std::task::Poll::Ready(Ok(0));
                    }
                    *remaining_in_frame -= bytes_read as u32;
                    if *remaining_in_frame == 0 {
                        *frame_finished = true;
                    }
                    std::task::Poll::Ready(Ok(bytes_read))
                } else {
                    poll
                }
            }
        }
    }
}

/// Custom encoder to support the custom format first implemented by zstdmt, which allows to have
/// optional skippable frames.
#[cfg(feature = "compress")]
pub(crate) struct Lz4Encoder<W: AsyncWrite + Unpin> {
    inner: InnerWriter<W>,
}

#[cfg(feature = "compress")]
enum InnerWriter<W: AsyncWrite + Unpin> {
    Standard(AsyncLz4Encoder<W>),
    Framed {
        writer: W,
        compressor: Option<AsyncLz4Encoder<futures::io::Cursor<Vec<u8>>>>,
        frame_size: usize,
        uncompressed_bytes_in_frame: usize,
        pending_frames: VecDeque<Vec<u8>>,
        pending_offset: usize,
    },
}

#[cfg(feature = "compress")]
impl<W: AsyncWrite + Unpin> Lz4Encoder<W> {
    pub(crate) fn new(writer: W, frame_size: usize) -> Result<Self, Error> {
        let inner = if frame_size == 0 {
            let encoder = AsyncLz4Encoder::new(writer);
            InnerWriter::Standard(encoder)
        } else {
            let cursor = futures::io::Cursor::new(Vec::with_capacity(frame_size));
            let compressor = Some(AsyncLz4Encoder::new(cursor));
            InnerWriter::Framed {
                writer,
                compressor,
                frame_size,
                uncompressed_bytes_in_frame: 0,
                pending_frames: VecDeque::new(),
                pending_offset: 0,
            }
        };

        Ok(Self { inner })
    }

    fn build_frame_bytes(compressed_data: &[u8]) -> Vec<u8> {
        if compressed_data.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(12 + compressed_data.len());
        out.extend_from_slice(&SKIPPABLE_FRAME_MAGIC.to_le_bytes());
        out.extend_from_slice(&(4u32).to_le_bytes());
        out.extend_from_slice(&(compressed_data.len() as u32).to_le_bytes());
        out.extend_from_slice(compressed_data);
        out
    }

    pub fn finish(self) -> std::io::Result<W> {
        match self.inner {
            InnerWriter::Standard(mut encoder) => {
                async_io::block_on(AsyncWriteExt::flush(&mut encoder))?;
                Ok(encoder.into_inner())
            }
            InnerWriter::Framed {
                mut writer,
                mut compressor,
                frame_size: _,
                uncompressed_bytes_in_frame: _,
                mut pending_frames,
                pending_offset: _,
            } => {
                if let Some(mut comp) = compressor.take() {
                    async_io::block_on(comp.close())?;
                    let cursor = comp.into_inner();
                    let data = cursor.into_inner();
                    if !data.is_empty() {
                        let frame = Self::build_frame_bytes(&data);
                        pending_frames.push_back(frame);
                    }
                }
                while let Some(frame) = pending_frames.pop_front() {
                    async_io::block_on(AsyncWriteExt::write_all(&mut writer, &frame))?;
                }
                Ok(writer)
            }
        }
    }
}

#[cfg(feature = "compress")]
impl<W: AsyncWrite + Unpin> futures::io::AsyncWrite for Lz4Encoder<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut self.inner {
            InnerWriter::Standard(encoder) => {
                let mut pin = std::pin::Pin::new(encoder);
                pin.as_mut().poll_write(cx, buf)
            }
            InnerWriter::Framed {
                writer,
                compressor,
                frame_size,
                uncompressed_bytes_in_frame,
                pending_frames,
                pending_offset,
            } => {
                if let Some(front) = pending_frames.front_mut() {
                    if *pending_offset < front.len() {
                        match std::pin::Pin::new(&mut *writer)
                            .poll_write(cx, &front[*pending_offset..])
                        {
                            std::task::Poll::Ready(Ok(w)) => {
                                if w == 0 {
                                    return std::task::Poll::Ready(Ok(0));
                                }
                                *pending_offset += w;
                                if *pending_offset >= front.len() {
                                    pending_frames.pop_front();
                                    *pending_offset = 0;
                                }
                            }
                            std::task::Poll::Ready(Err(e)) => {
                                return std::task::Poll::Ready(Err(e));
                            }
                            std::task::Poll::Pending => {}
                        }
                    }
                }

                if buf.is_empty() {
                    return std::task::Poll::Ready(Ok(0));
                }

                let cap = *frame_size - *uncompressed_bytes_in_frame;
                let to_write = std::cmp::min(buf.len(), cap);
                if to_write == 0 {
                    let mut comp = compressor.take().expect("no compressor set");
                    let mut pin = std::pin::Pin::new(&mut comp);
                    match pin.as_mut().poll_close(cx) {
                        std::task::Poll::Ready(Ok(())) => {
                            let cursor = comp.into_inner();
                            let data = cursor.into_inner();
                            let frame = Self::build_frame_bytes(&data);
                            if !frame.is_empty() {
                                pending_frames.push_back(frame);
                            }
                            let new_cursor =
                                futures::io::Cursor::new(Vec::with_capacity(*frame_size));
                            *compressor = Some(AsyncLz4Encoder::new(new_cursor));
                            *uncompressed_bytes_in_frame = 0;
                            std::task::Poll::Pending
                        }
                        std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
                        std::task::Poll::Pending => std::task::Poll::Pending,
                    }
                } else {
                    let comp = compressor.as_mut().expect("no compressor set");
                    let mut pin = std::pin::Pin::new(comp);
                    match pin.as_mut().poll_write(cx, &buf[..to_write]) {
                        std::task::Poll::Ready(Ok(n)) => {
                            *uncompressed_bytes_in_frame += n;
                            if *uncompressed_bytes_in_frame >= *frame_size {
                                let mut comp2 = compressor.take().expect("no compressor set");
                                let mut pin2 = std::pin::Pin::new(&mut comp2);
                                match pin2.as_mut().poll_close(cx) {
                                    std::task::Poll::Ready(Ok(())) => {
                                        let cursor = comp2.into_inner();
                                        let data = cursor.into_inner();
                                        let frame = Self::build_frame_bytes(&data);
                                        if !frame.is_empty() {
                                            pending_frames.push_back(frame);
                                        }
                                        let new_cursor = futures::io::Cursor::new(
                                            Vec::with_capacity(*frame_size),
                                        );
                                        *compressor = Some(AsyncLz4Encoder::new(new_cursor));
                                        *uncompressed_bytes_in_frame = 0;
                                    }
                                    std::task::Poll::Ready(Err(e)) => {
                                        return std::task::Poll::Ready(Err(e));
                                    }
                                    std::task::Poll::Pending => return std::task::Poll::Pending,
                                }
                            }
                            std::task::Poll::Ready(Ok(n))
                        }
                        std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
                        std::task::Poll::Pending => std::task::Poll::Pending,
                    }
                }
            }
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.inner {
            InnerWriter::Standard(encoder) => {
                let mut pin = std::pin::Pin::new(encoder);
                pin.as_mut().poll_flush(cx)
            }
            InnerWriter::Framed {
                writer,
                compressor,
                frame_size,
                uncompressed_bytes_in_frame,
                pending_frames,
                pending_offset,
            } => {
                if *uncompressed_bytes_in_frame > 0 {
                    let mut comp = compressor.take().expect("no compressor set");
                    let mut pin = std::pin::Pin::new(&mut comp);
                    match pin.as_mut().poll_close(cx) {
                        std::task::Poll::Ready(Ok(())) => {
                            let cursor = comp.into_inner();
                            let data = cursor.into_inner();
                            let frame = Self::build_frame_bytes(&data);
                            if !frame.is_empty() {
                                pending_frames.push_back(frame);
                            }
                            let new_cursor =
                                futures::io::Cursor::new(Vec::with_capacity(*frame_size));
                            *compressor = Some(AsyncLz4Encoder::new(new_cursor));
                            *uncompressed_bytes_in_frame = 0;
                        }
                        std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }

                while let Some(front) = pending_frames.front_mut() {
                    if *pending_offset >= front.len() {
                        pending_frames.pop_front();
                        *pending_offset = 0;
                        continue;
                    }
                    match std::pin::Pin::new(&mut *writer).poll_write(cx, &front[*pending_offset..])
                    {
                        std::task::Poll::Ready(Ok(w)) => {
                            if w == 0 {
                                return std::task::Poll::Pending;
                            }
                            *pending_offset += w;
                            if *pending_offset >= front.len() {
                                pending_frames.pop_front();
                                *pending_offset = 0;
                            }
                        }
                        std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }

                let mut pin = std::pin::Pin::new(&mut *writer);
                pin.as_mut().poll_flush(cx)
            }
        }
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.inner {
            InnerWriter::Standard(encoder) => {
                let mut pin = std::pin::Pin::new(encoder);
                pin.as_mut().poll_close(cx)
            }
            InnerWriter::Framed {
                writer,
                compressor,
                frame_size,
                uncompressed_bytes_in_frame,
                pending_frames,
                pending_offset,
            } => {
                if *uncompressed_bytes_in_frame > 0 {
                    let mut comp = compressor.take().expect("no compressor set");
                    let mut pin = std::pin::Pin::new(&mut comp);
                    match pin.as_mut().poll_close(cx) {
                        std::task::Poll::Ready(Ok(())) => {
                            let cursor = comp.into_inner();
                            let data = cursor.into_inner();
                            let frame = Self::build_frame_bytes(&data);
                            if !frame.is_empty() {
                                pending_frames.push_back(frame);
                            }
                            let new_cursor =
                                futures::io::Cursor::new(Vec::with_capacity(*frame_size));
                            *compressor = Some(AsyncLz4Encoder::new(new_cursor));
                            *uncompressed_bytes_in_frame = 0;
                        }
                        std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }

                while let Some(front) = pending_frames.front_mut() {
                    if *pending_offset >= front.len() {
                        pending_frames.pop_front();
                        *pending_offset = 0;
                        continue;
                    }
                    match std::pin::Pin::new(&mut *writer).poll_write(cx, &front[*pending_offset..])
                    {
                        std::task::Poll::Ready(Ok(w)) => {
                            if w == 0 {
                                return std::task::Poll::Pending;
                            }
                            *pending_offset += w;
                            if *pending_offset >= front.len() {
                                pending_frames.pop_front();
                                *pending_offset = 0;
                            }
                        }
                        std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }

                let mut pin = std::pin::Pin::new(&mut *writer);
                pin.as_mut().poll_close(cx)
            }
        }
    }
}
