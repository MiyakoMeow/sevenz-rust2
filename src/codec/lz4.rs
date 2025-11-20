use futures::io::Cursor;
#[cfg(feature = "compress")]
use std::io::Write;

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
    pub(crate) fn new(mut input: R) -> Result<Self, Error> {
        let mut header = [0u8; 12];
        let header_read = match async_io::block_on(AsyncReadExt::read(&mut input, &mut header)) {
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
        frame_size: usize,
        compressed_data: Vec<u8>,
        uncompressed_data: Vec<u8>,
        uncompressed_data_size: usize,
    },
}

#[cfg(feature = "compress")]
impl<W: AsyncWrite + Unpin> Lz4Encoder<W> {
    pub(crate) fn new(writer: W, frame_size: usize) -> Result<Self, Error> {
        let inner = if frame_size == 0 {
            let encoder = AsyncLz4Encoder::new(writer);
            InnerWriter::Standard(encoder)
        } else {
            InnerWriter::Framed {
                writer,
                frame_size,
                compressed_data: Vec::with_capacity(frame_size),
                uncompressed_data: vec![0; frame_size],
                uncompressed_data_size: 0,
            }
        };

        Ok(Self { inner })
    }

    fn write_frame(
        writer: &mut W,
        compressed_data: &mut Vec<u8>,
        uncompressed_data: &[u8],
    ) -> std::io::Result<()> {
        if uncompressed_data.is_empty() {
            return Ok(());
        }
        compressed_data.clear();

        let cursor = futures::io::Cursor::new(Vec::new());
        let mut enc = AsyncLz4Encoder::new(cursor);
        async_io::block_on(AsyncWriteExt::write_all(&mut enc, uncompressed_data))?;
        async_io::block_on(enc.close())?;
        let cursor = enc.into_inner();
        let data = cursor.into_inner();

        if data.is_empty() {
            return Ok(());
        }

        let mut hdr = [0u8; 12];
        hdr[0..4].copy_from_slice(&SKIPPABLE_FRAME_MAGIC.to_le_bytes());
        hdr[4..8].copy_from_slice(&(4u32).to_le_bytes());
        hdr[8..12].copy_from_slice(&(data.len() as u32).to_le_bytes());
        async_io::block_on(AsyncWriteExt::write_all(writer, &hdr))?;
        async_io::block_on(AsyncWriteExt::write_all(writer, data.as_slice()))?;

        Ok(())
    }

    pub fn finish(self) -> std::io::Result<W> {
        match self.inner {
            InnerWriter::Standard(mut encoder) => {
                async_io::block_on(AsyncWriteExt::flush(&mut encoder))?;
                Ok(encoder.into_inner())
            }
            InnerWriter::Framed {
                mut writer,
                mut compressed_data,
                uncompressed_data,
                uncompressed_data_size,
                ..
            } => {
                Self::write_frame(
                    &mut writer,
                    &mut compressed_data,
                    &uncompressed_data[..uncompressed_data_size],
                )?;
                Ok(writer)
            }
        }
    }
}

#[cfg(feature = "compress")]
impl<W: AsyncWrite + Unpin> Write for Lz4Encoder<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match &mut self.inner {
            InnerWriter::Standard(encoder) => {
                async_io::block_on(AsyncWriteExt::write(encoder, buf))
            }
            InnerWriter::Framed {
                writer,
                frame_size,
                compressed_data,
                uncompressed_data,
                uncompressed_data_size,
            } => {
                let mut bytes_consumed = 0;
                let total_bytes = buf.len();

                while bytes_consumed < total_bytes {
                    let available_space = *frame_size - *uncompressed_data_size;
                    let bytes_to_copy =
                        std::cmp::min(total_bytes - bytes_consumed, available_space);

                    uncompressed_data
                        [*uncompressed_data_size..*uncompressed_data_size + bytes_to_copy]
                        .copy_from_slice(&buf[bytes_consumed..bytes_consumed + bytes_to_copy]);

                    *uncompressed_data_size += bytes_to_copy;
                    bytes_consumed += bytes_to_copy;

                    if *uncompressed_data_size >= *frame_size {
                        Self::write_frame(
                            writer,
                            compressed_data,
                            &uncompressed_data[..*uncompressed_data_size],
                        )?;
                        *uncompressed_data_size = 0;
                    }
                }

                Ok(total_bytes)
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.inner {
            InnerWriter::Standard(encoder) => async_io::block_on(AsyncWriteExt::flush(encoder)),
            InnerWriter::Framed {
                writer,
                compressed_data,
                uncompressed_data,
                uncompressed_data_size,
                ..
            } => {
                Self::write_frame(
                    writer,
                    compressed_data,
                    &uncompressed_data[..*uncompressed_data_size],
                )?;
                *uncompressed_data_size = 0;
                async_io::block_on(AsyncWriteExt::flush(writer))
            }
        }
    }
}
