use futures::io::Cursor;
#[cfg(feature = "compress")]
use std::collections::VecDeque;
#[cfg(feature = "compress")]
use std::io;

use crate::Error;
use async_compression::futures::bufread::BrotliDecoder as AsyncBrotliDecoder;
#[cfg(feature = "compress")]
use async_compression::futures::write::BrotliEncoder as AsyncBrotliEncoder;
use futures::io::AsyncRead;
#[cfg(feature = "compress")]
use futures::io::AsyncWrite;
use futures::io::BufReader as AsyncBufReader;

/// Magic bytes of a skippable frame format as used in brotli by zstdmt.
const SKIPPABLE_FRAME_MAGIC: u32 = 0x184D2A50;
/// "BR" in little-endian
const BROTLI_MAGIC: u16 = 0x5242;
#[cfg(feature = "compress")]
const HINT_UNIT_SIZE: usize = 65536;

/// Custom decoder to support the custom format first implemented by zstdmt, which allows to have
/// optional skippable frames.
pub(crate) struct BrotliDecoder<R: AsyncRead + Unpin> {
    inner: Option<AsyncBrotliDecoder<AsyncBufReader<InnerReader<R>>>>,
    buffer_size: usize,
}

impl<R: AsyncRead + Unpin> BrotliDecoder<R> {
    pub(crate) fn new(mut input: R, buffer_size: usize) -> Result<Self, Error> {
        let mut header = [0u8; 16];
        let header_read =
            match async_io::block_on(futures::io::AsyncReadExt::read(&mut input, &mut header)) {
                Ok(n) if n >= 4 => n,
                Ok(_) => return Err(Error::other("Input too short")),
                Err(e) => return Err(e.into()),
            };

        let magic_value = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);

        let inner_reader = if magic_value == SKIPPABLE_FRAME_MAGIC && header_read >= 16 {
            let skippable_size = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
            if skippable_size != 8 {
                return Err(Error::other("Invalid brotli skippable frame size"));
            }

            let compressed_size =
                u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

            let brotli_magic_value = u16::from_le_bytes([header[12], header[13]]);
            if brotli_magic_value != BROTLI_MAGIC {
                return Err(Error::other("Invalid brotli magic value"));
            }

            InnerReader::new_skippable(input, compressed_size)
        } else {
            InnerReader::new_standard(input, header[..header_read].to_vec())
        };

        let bufread = AsyncBufReader::with_capacity(buffer_size, inner_reader);
        let decompressor = AsyncBrotliDecoder::new(bufread);

        Ok(BrotliDecoder {
            inner: Some(decompressor),
            buffer_size,
        })
    }
}

impl<R: AsyncRead + Unpin> futures::io::AsyncRead for BrotliDecoder<R> {
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
                        let bufread: AsyncBufReader<InnerReader<R>> =
                            AsyncBufReader::with_capacity(self.buffer_size, reader);
                        let mut decompressor = AsyncBrotliDecoder::new(bufread);
                        let poll = std::pin::Pin::new(&mut decompressor).poll_read(cx, buf);
                        self.inner = Some(decompressor);
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

    fn read_next_frame_header(&mut self) -> io::Result<bool> {
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
                match async_io::block_on(futures::io::AsyncReadExt::read_exact(reader, &mut buf4)) {
                    Ok(_) => {
                        let magic = u32::from_le_bytes(buf4);
                        if magic != SKIPPABLE_FRAME_MAGIC {
                            return Ok(false);
                        }

                        async_io::block_on(futures::io::AsyncReadExt::read_exact(
                            reader, &mut buf4,
                        ))?;
                        let skippable_size = u32::from_le_bytes(buf4);
                        if skippable_size != 8 {
                            return Ok(false);
                        }

                        async_io::block_on(futures::io::AsyncReadExt::read_exact(
                            reader, &mut buf4,
                        ))?;
                        let compressed_size = u32::from_le_bytes(buf4);

                        let mut buf2 = [0u8; 2];
                        async_io::block_on(futures::io::AsyncReadExt::read_exact(
                            reader, &mut buf2,
                        ))?;
                        let brotli_magic = u16::from_le_bytes(buf2);
                        if brotli_magic != BROTLI_MAGIC {
                            return Ok(false);
                        }

                        async_io::block_on(futures::io::AsyncReadExt::read_exact(
                            reader, &mut buf2,
                        ))?;
                        let _uncompressed_hint = u16::from_le_bytes(buf2);

                        *remaining_in_frame = compressed_size;
                        *frame_finished = false;

                        Ok(true)
                    }
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
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
pub(crate) struct BrotliEncoder<W: AsyncWrite + Unpin> {
    inner: InnerWriter<W>,
    quality: u32,
}

#[cfg(feature = "compress")]
enum InnerWriter<W: AsyncWrite + Unpin> {
    Standard(AsyncBrotliEncoder<W>),
    Framed {
        writer: W,
        compressor: Option<AsyncBrotliEncoder<futures::io::Cursor<Vec<u8>>>>,
        frame_size: usize,
        uncompressed_bytes_in_frame: usize,
        pending_frames: VecDeque<Vec<u8>>,
        pending_offset: usize,
    },
}

#[cfg(feature = "compress")]
impl<W: AsyncWrite + Unpin> BrotliEncoder<W> {
    pub(crate) fn new(writer: W, quality: u32, frame_size: usize) -> Result<Self, Error> {
        let inner = if frame_size == 0 {
            let compressor = AsyncBrotliEncoder::with_quality(
                writer,
                async_compression::Level::Precise(quality as i32),
            );
            InnerWriter::Standard(compressor)
        } else {
            let cursor = futures::io::Cursor::new(Vec::with_capacity(frame_size));
            let compressor = Some(AsyncBrotliEncoder::with_quality(
                cursor,
                async_compression::Level::Precise(quality as i32),
            ));
            InnerWriter::Framed {
                writer,
                compressor,
                frame_size,
                uncompressed_bytes_in_frame: 0,
                pending_frames: VecDeque::new(),
                pending_offset: 0,
            }
        };

        Ok(Self { inner, quality })
    }

    #[cfg(feature = "compress")]
    fn build_frame_bytes(compressed_data: &[u8], uncompressed_bytes: usize) -> Vec<u8> {
        if compressed_data.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(12 + 2 + 2 + compressed_data.len());
        out.extend_from_slice(&SKIPPABLE_FRAME_MAGIC.to_le_bytes());
        out.extend_from_slice(&(8u32).to_le_bytes());
        out.extend_from_slice(&(compressed_data.len() as u32).to_le_bytes());
        out.extend_from_slice(&BROTLI_MAGIC.to_le_bytes());
        let hint_value = uncompressed_bytes.div_ceil(HINT_UNIT_SIZE);
        let hint_value = if hint_value > usize::from(u16::MAX) {
            u16::MAX
        } else {
            hint_value as u16
        };
        out.extend_from_slice(&hint_value.to_le_bytes());
        out.extend_from_slice(compressed_data);
        out
    }
}

#[cfg(feature = "compress")]
impl<W: AsyncWrite + Unpin> futures::io::AsyncWrite for BrotliEncoder<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let quality = self.quality;
        match &mut self.inner {
            InnerWriter::Standard(compressor) => {
                let mut pin = std::pin::Pin::new(compressor);
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
                            let frame =
                                Self::build_frame_bytes(&data, *uncompressed_bytes_in_frame);
                            if !frame.is_empty() {
                                pending_frames.push_back(frame);
                            }
                            let new_cursor =
                                futures::io::Cursor::new(Vec::with_capacity(*frame_size));
                            *compressor = Some(AsyncBrotliEncoder::with_quality(
                                new_cursor,
                                async_compression::Level::Precise(quality as i32),
                            ));
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
                                        let frame = Self::build_frame_bytes(
                                            &data,
                                            *uncompressed_bytes_in_frame,
                                        );
                                        if !frame.is_empty() {
                                            pending_frames.push_back(frame);
                                        }
                                        let new_cursor = futures::io::Cursor::new(
                                            Vec::with_capacity(*frame_size),
                                        );
                                        *compressor = Some(AsyncBrotliEncoder::with_quality(
                                            new_cursor,
                                            async_compression::Level::Precise(quality as i32),
                                        ));
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
        let quality = self.quality;
        match &mut self.inner {
            InnerWriter::Standard(compressor) => {
                let mut pin = std::pin::Pin::new(compressor);
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
                            let frame =
                                Self::build_frame_bytes(&data, *uncompressed_bytes_in_frame);
                            if !frame.is_empty() {
                                pending_frames.push_back(frame);
                            }
                            let new_cursor =
                                futures::io::Cursor::new(Vec::with_capacity(*frame_size));
                            *compressor = Some(AsyncBrotliEncoder::with_quality(
                                new_cursor,
                                async_compression::Level::Precise(quality as i32),
                            ));
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
        let quality = self.quality;
        match &mut self.inner {
            InnerWriter::Standard(compressor) => {
                let mut pin = std::pin::Pin::new(compressor);
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
                            let frame =
                                Self::build_frame_bytes(&data, *uncompressed_bytes_in_frame);
                            if !frame.is_empty() {
                                pending_frames.push_back(frame);
                            }
                            let new_cursor =
                                futures::io::Cursor::new(Vec::with_capacity(*frame_size));
                            *compressor = Some(AsyncBrotliEncoder::with_quality(
                                new_cursor,
                                async_compression::Level::Precise(quality as i32),
                            ));
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
