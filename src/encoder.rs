use std::{
    pin::Pin,
    task::{Context, Poll},
};

// AsyncWrite is imported below together with AsyncWriteExt
use lzma_rust2::{
    Lzma2Writer, Lzma2WriterMt,
    filter::{bcj::BcjWriter, delta::DeltaWriter},
};

#[cfg(feature = "brotli")]
use crate::codec::brotli::BrotliEncoder;
#[cfg(feature = "lz4")]
use crate::codec::lz4::Lz4Encoder;
#[cfg(feature = "brotli")]
use crate::encoder_options::BrotliOptions;
#[cfg(feature = "bzip2")]
use crate::encoder_options::Bzip2Options;
#[cfg(feature = "deflate")]
use crate::encoder_options::DeflateOptions;
#[cfg(feature = "lz4")]
use crate::encoder_options::Lz4Options;
#[cfg(feature = "ppmd")]
use crate::encoder_options::PpmdOptions;
#[cfg(feature = "zstd")]
use crate::encoder_options::ZstandardOptions;
#[cfg(feature = "aes256")]
use crate::encryption::Aes256Sha256Encoder;
use crate::{
    Error,
    archive::{EncoderConfiguration, EncoderMethod},
    encoder_options::{DeltaOptions, EncoderOptions, Lzma2Options, LzmaOptions},
    writer::CountingWriter,
};
#[cfg(any(feature = "deflate", feature = "bzip2", feature = "zstd"))]
use async_compression::Level;
#[cfg(any(feature = "deflate", feature = "bzip2", feature = "zstd"))]
use async_compression::futures::write::BzEncoder as AsyncBzip2Encoder;
#[cfg(feature = "deflate")]
use async_compression::futures::write::DeflateEncoder as AsyncDeflateEncoder;
use async_compression::futures::write::LzmaEncoder as AsyncLzmaEncoder;
#[cfg(feature = "zstd")]
use async_compression::futures::write::ZstdEncoder as AsyncZstdEncoder;
use futures::io::{AsyncWrite, AsyncWriteExt};

pub(crate) enum Encoder<W: AsyncWrite + Unpin> {
    Copy(CountingWriter<W>),
    Bcj(Option<Box<BcjWriter<CountingWriter<W>>>>),
    Delta(Box<DeltaWriter<CountingWriter<W>>>),
    Lzma(Option<Box<LzmaEnc<W>>>),
    Lzma2(Option<Box<Lzma2Writer<CountingWriter<W>>>>),
    Lzma2Mt(Option<Box<Lzma2WriterMt<CountingWriter<W>>>>),
    #[cfg(feature = "ppmd")]
    Ppmd(Option<Box<ppmd_rust::Ppmd7Encoder<CountingWriter<W>>>>),
    #[cfg(feature = "brotli")]
    Brotli(Box<BrotliEncoder<CountingWriter<W>>>),
    #[cfg(feature = "bzip2")]
    Bzip2(Option<Box<AsyncBzip2Encoder<CountingWriter<W>>>>),
    #[cfg(feature = "deflate")]
    Deflate(Option<Box<AsyncDeflateEncoder<CountingWriter<W>>>>),
    #[cfg(feature = "lz4")]
    Lz4(Option<Box<Lz4Encoder<CountingWriter<W>>>>),
    #[cfg(feature = "zstd")]
    Zstd(Option<Box<AsyncZstdEncoder<CountingWriter<W>>>>),
    #[cfg(feature = "aes256")]
    Aes(Box<Aes256Sha256Encoder<CountingWriter<W>>>),
}
type LzmaEnc<W> = AsyncLzmaEncoder<StripLzmaHeaderWrite<CountingWriter<W>>>;

pub(crate) struct StripLzmaHeaderWrite<W> {
    inner: W,
    offset: usize,
}

impl<W> StripLzmaHeaderWrite<W> {
    fn new(inner: W) -> Self {
        Self { inner, offset: 0 }
    }
    fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for StripLzmaHeaderWrite<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut to_send = buf;
        if self.offset < 13 {
            let skip = 13 - self.offset;
            if to_send.len() <= skip {
                self.offset += to_send.len();
                return Poll::Ready(Ok(to_send.len()));
            } else {
                to_send = &to_send[skip..];
                self.offset = 13;
            }
        }
        Pin::new(&mut self.inner).poll_write(cx, to_send)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Encoder<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Encoder::Copy(w) => Pin::new(w).poll_write(cx, buf),
            Encoder::Delta(w) => Poll::Ready(std::io::Write::write(w.as_mut(), buf)),
            Encoder::Bcj(w) => match buf.is_empty() {
                true => {
                    let writer = w.take().unwrap();
                    let mut inner = writer.finish()?;
                    let _ = async_io::block_on(AsyncWriteExt::write(&mut inner, buf));
                    Poll::Ready(Ok(0))
                }
                false => Poll::Ready(std::io::Write::write(w.as_mut().unwrap().as_mut(), buf)),
            },
            Encoder::Lzma(w) => match buf.is_empty() {
                true => {
                    let mut writer = w.take().unwrap();
                    match Pin::new(&mut *writer).as_mut().poll_close(cx) {
                        Poll::Ready(Ok(())) => {
                            let strip = writer.into_inner();
                            let mut inner = strip.into_inner();
                            let _ = Pin::new(&mut inner).poll_write(cx, buf);
                            Poll::Ready(Ok(0))
                        }
                        Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                        Poll::Pending => Poll::Pending,
                    }
                }
                false => Pin::new(w.as_mut().unwrap().as_mut()).poll_write(cx, buf),
            },
            Encoder::Lzma2(w) => match buf.is_empty() {
                true => {
                    let writer = w.take().unwrap();
                    let mut inner = writer.finish()?;
                    let _ = Pin::new(&mut inner).poll_write(cx, buf);
                    Poll::Ready(Ok(0))
                }
                false => Poll::Ready(std::io::Write::write(w.as_mut().unwrap().as_mut(), buf)),
            },
            Encoder::Lzma2Mt(w) => match buf.is_empty() {
                true => {
                    let writer = w.take().unwrap();
                    let mut inner = writer.finish()?;
                    let _ = Pin::new(&mut inner).poll_write(cx, buf);
                    Poll::Ready(Ok(0))
                }
                false => Poll::Ready(std::io::Write::write(w.as_mut().unwrap().as_mut(), buf)),
            },
            #[cfg(feature = "ppmd")]
            Encoder::Ppmd(w) => match buf.is_empty() {
                true => {
                    let writer = w.take().unwrap();
                    let mut inner = writer.finish(false)?;
                    let _ = Pin::new(&mut inner).poll_write(cx, buf);
                    Poll::Ready(Ok(0))
                }
                false => Poll::Ready(std::io::Write::write(w.as_mut().unwrap().as_mut(), buf)),
            },
            #[cfg(feature = "brotli")]
            Encoder::Brotli(w) => Pin::new(w.as_mut()).poll_write(cx, buf),
            #[cfg(feature = "bzip2")]
            Encoder::Bzip2(w) => match buf.is_empty() {
                true => {
                    let mut writer = w.take().unwrap();
                    match Pin::new(&mut *writer).as_mut().poll_close(cx) {
                        Poll::Ready(Ok(())) => {
                            let mut inner = writer.into_inner();
                            let _ = Pin::new(&mut inner).poll_write(cx, buf);
                            Poll::Ready(Ok(0))
                        }
                        Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                        Poll::Pending => Poll::Pending,
                    }
                }
                false => Pin::new(w.as_mut().unwrap().as_mut()).poll_write(cx, buf),
            },
            #[cfg(feature = "deflate")]
            Encoder::Deflate(w) => match buf.is_empty() {
                true => {
                    let mut writer = w.take().unwrap();
                    match Pin::new(&mut *writer).as_mut().poll_close(cx) {
                        Poll::Ready(Ok(())) => {
                            let mut inner = writer.into_inner();
                            let _ = Pin::new(&mut inner).poll_write(cx, buf);
                            Poll::Ready(Ok(0))
                        }
                        Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                        Poll::Pending => Poll::Pending,
                    }
                }
                false => Pin::new(w.as_mut().unwrap().as_mut()).poll_write(cx, buf),
            },
            #[cfg(feature = "lz4")]
            Encoder::Lz4(w) => match buf.is_empty() {
                true => {
                    let writer = w.take().unwrap();
                    let mut inner = writer.finish()?;
                    let _ = Pin::new(&mut inner).poll_write(cx, buf);
                    Poll::Ready(Ok(0))
                }
                false => Pin::new(w.as_mut().unwrap().as_mut()).poll_write(cx, buf),
            },
            #[cfg(feature = "zstd")]
            Encoder::Zstd(w) => match buf.is_empty() {
                true => {
                    let mut writer = w.take().unwrap();
                    match Pin::new(&mut *writer).as_mut().poll_close(cx) {
                        Poll::Ready(Ok(())) => {
                            let mut inner = writer.into_inner();
                            let _ = Pin::new(&mut inner).poll_write(cx, buf);
                            Poll::Ready(Ok(0))
                        }
                        Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                        Poll::Pending => Poll::Pending,
                    }
                }
                false => Pin::new(w.as_mut().unwrap().as_mut()).poll_write(cx, buf),
            },
            #[cfg(feature = "aes256")]
            Encoder::Aes(w) => Poll::Ready(std::io::Write::write(w.as_mut(), buf)),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Encoder::Copy(w) => Pin::new(w).poll_flush(cx),
            Encoder::Bcj(w) => Poll::Ready(std::io::Write::flush(w.as_mut().unwrap().as_mut())),
            Encoder::Delta(w) => Poll::Ready(std::io::Write::flush(w.as_mut())),
            Encoder::Lzma(w) => Pin::new(w.as_mut().unwrap().as_mut()).poll_flush(cx),
            Encoder::Lzma2(w) => Poll::Ready(std::io::Write::flush(w.as_mut().unwrap().as_mut())),
            Encoder::Lzma2Mt(w) => Poll::Ready(std::io::Write::flush(w.as_mut().unwrap().as_mut())),
            #[cfg(feature = "brotli")]
            Encoder::Brotli(w) => Pin::new(w.as_mut()).poll_flush(cx),
            #[cfg(feature = "ppmd")]
            Encoder::Ppmd(w) => Poll::Ready(std::io::Write::flush(w.as_mut().unwrap().as_mut())),
            #[cfg(feature = "bzip2")]
            Encoder::Bzip2(w) => Pin::new(w.as_mut().unwrap().as_mut()).poll_flush(cx),
            #[cfg(feature = "deflate")]
            Encoder::Deflate(w) => Pin::new(w.as_mut().unwrap().as_mut()).poll_flush(cx),
            #[cfg(feature = "lz4")]
            Encoder::Lz4(w) => Pin::new(w.as_mut().unwrap().as_mut()).poll_flush(cx),
            #[cfg(feature = "zstd")]
            Encoder::Zstd(w) => Pin::new(w.as_mut().unwrap().as_mut()).poll_flush(cx),
            #[cfg(feature = "aes256")]
            Encoder::Aes(w) => Poll::Ready(std::io::Write::flush(w.as_mut())),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Encoder::Copy(w) => Pin::new(w).poll_close(cx),
            Encoder::Delta(_w) => Poll::Ready(Ok(())),
            Encoder::Bcj(w) => {
                let writer = w.take().unwrap();
                let mut inner = writer.finish()?;
                let _ = Pin::new(&mut inner).poll_write(cx, &[]);
                Poll::Ready(Ok(()))
            }
            Encoder::Lzma(w) => Pin::new(w.as_mut().unwrap().as_mut()).poll_close(cx),
            Encoder::Lzma2(w) => {
                let writer = w.take().unwrap();
                let _inner = writer.finish()?;
                Poll::Ready(Ok(()))
            }
            Encoder::Lzma2Mt(w) => {
                let writer = w.take().unwrap();
                let _inner = writer.finish()?;
                Poll::Ready(Ok(()))
            }
            #[cfg(feature = "brotli")]
            Encoder::Brotli(w) => Pin::new(w.as_mut()).poll_close(cx),
            #[cfg(feature = "ppmd")]
            Encoder::Ppmd(w) => {
                let writer = w.take().unwrap();
                let _inner = writer.finish(false)?;
                Poll::Ready(Ok(()))
            }
            #[cfg(feature = "bzip2")]
            Encoder::Bzip2(w) => Pin::new(w.as_mut().unwrap().as_mut()).poll_close(cx),
            #[cfg(feature = "deflate")]
            Encoder::Deflate(w) => Pin::new(w.as_mut().unwrap().as_mut()).poll_close(cx),
            #[cfg(feature = "lz4")]
            Encoder::Lz4(w) => {
                let writer = w.take().unwrap();
                let _inner = writer.finish()?;
                Poll::Ready(Ok(()))
            }
            #[cfg(feature = "zstd")]
            Encoder::Zstd(w) => Pin::new(w.as_mut().unwrap().as_mut()).poll_close(cx),
            #[cfg(feature = "aes256")]
            Encoder::Aes(w) => {
                let _ = std::io::Write::write(w.as_mut(), &[])?;
                Poll::Ready(Ok(()))
            }
        }
    }
}

pub(crate) fn add_encoder<W: AsyncWrite + Unpin>(
    input: CountingWriter<W>,
    method_config: &EncoderConfiguration,
) -> Result<Encoder<W>, Error> {
    let method = method_config.method;

    match method.id() {
        EncoderMethod::ID_COPY => Ok(Encoder::Copy(input)),
        EncoderMethod::ID_DELTA => {
            let options = match method_config.options {
                Some(EncoderOptions::Delta(options)) => options,
                _ => DeltaOptions::default(),
            };
            let dw = DeltaWriter::new(input, options.0 as usize);
            Ok(Encoder::Delta(Box::new(dw)))
        }
        EncoderMethod::ID_BCJ_X86 => Ok(Encoder::Bcj(Some(Box::new(BcjWriter::new_x86(input, 0))))),
        EncoderMethod::ID_BCJ_ARM => Ok(Encoder::Bcj(Some(Box::new(BcjWriter::new_arm(input, 0))))),
        EncoderMethod::ID_BCJ_ARM_THUMB => Ok(Encoder::Bcj(Some(Box::new(
            BcjWriter::new_arm_thumb(input, 0),
        )))),
        EncoderMethod::ID_BCJ_ARM64 => {
            Ok(Encoder::Bcj(Some(Box::new(BcjWriter::new_arm64(input, 0)))))
        }
        EncoderMethod::ID_BCJ_IA64 => {
            Ok(Encoder::Bcj(Some(Box::new(BcjWriter::new_ia64(input, 0)))))
        }
        EncoderMethod::ID_BCJ_SPARC => {
            Ok(Encoder::Bcj(Some(Box::new(BcjWriter::new_sparc(input, 0)))))
        }
        EncoderMethod::ID_BCJ_PPC => Ok(Encoder::Bcj(Some(Box::new(BcjWriter::new_ppc(input, 0))))),
        EncoderMethod::ID_BCJ_RISCV => {
            Ok(Encoder::Bcj(Some(Box::new(BcjWriter::new_riscv(input, 0)))))
        }
        EncoderMethod::ID_LZMA => {
            let _options = match &method_config.options {
                Some(EncoderOptions::Lzma(options)) => options.clone(),
                _ => LzmaOptions::default(),
            };
            let strip = StripLzmaHeaderWrite::new(input);
            let enc = AsyncLzmaEncoder::new(strip);
            Ok(Encoder::Lzma(Some(Box::new(enc))))
        }
        EncoderMethod::ID_LZMA2 => {
            let lzma2_options = match &method_config.options {
                Some(EncoderOptions::Lzma2(options)) => options.clone(),
                _ => Lzma2Options::default(),
            };

            let encoder = match lzma2_options.threads {
                0 | 1 => Encoder::Lzma2(Some(Box::new(Lzma2Writer::new(
                    input,
                    lzma2_options.options,
                )))),
                _ => {
                    let threads = lzma2_options.threads;
                    Encoder::Lzma2Mt(Some(Box::new(Lzma2WriterMt::new(
                        input,
                        lzma2_options.options,
                        threads,
                    )?)))
                }
            };

            Ok(encoder)
        }
        #[cfg(feature = "ppmd")]
        EncoderMethod::ID_PPMD => {
            let options = match method_config.options {
                Some(EncoderOptions::Ppmd(options)) => options,
                _ => PpmdOptions::default(),
            };

            let ppmd_encoder =
                ppmd_rust::Ppmd7Encoder::new(input, options.order, options.memory_size)
                    .map_err(|err| Error::other(err.to_string()))?;

            Ok(Encoder::Ppmd(Some(Box::new(ppmd_encoder))))
        }
        #[cfg(feature = "brotli")]
        EncoderMethod::ID_BROTLI => {
            let options = match method_config.options {
                Some(EncoderOptions::Brotli(options)) => options,
                _ => BrotliOptions::default(),
            };

            let brotli_encoder = BrotliEncoder::new(
                input,
                options.quality,
                options.window,
                options.skippable_frame_size as usize,
            )?;

            Ok(Encoder::Brotli(Box::new(brotli_encoder)))
        }
        #[cfg(feature = "bzip2")]
        EncoderMethod::ID_BZIP2 => {
            let options = match method_config.options {
                Some(EncoderOptions::Bzip2(options)) => options,
                _ => Bzip2Options::default(),
            };
            let level = Level::Precise(options.0 as i32);
            let bzip2_encoder = AsyncBzip2Encoder::with_quality(input, level);
            Ok(Encoder::Bzip2(Some(Box::new(bzip2_encoder))))
        }
        #[cfg(feature = "deflate")]
        EncoderMethod::ID_DEFLATE => {
            let options = match method_config.options {
                Some(EncoderOptions::Deflate(options)) => options,
                _ => DeflateOptions::default(),
            };
            let level = Level::Precise(options.0 as i32);
            let deflate_encoder = AsyncDeflateEncoder::with_quality(input, level);
            Ok(Encoder::Deflate(Some(Box::new(deflate_encoder))))
        }
        #[cfg(feature = "lz4")]
        EncoderMethod::ID_LZ4 => {
            let options = match method_config.options.as_ref() {
                Some(EncoderOptions::Lz4(options)) => *options,
                _ => Lz4Options::default(),
            };

            let lz4_encoder = Lz4Encoder::new(input, options.skippable_frame_size as usize)?;

            Ok(Encoder::Lz4(Some(Box::new(lz4_encoder))))
        }
        #[cfg(feature = "zstd")]
        EncoderMethod::ID_ZSTD => {
            let options = match method_config.options.as_ref() {
                Some(EncoderOptions::Zstd(options)) => *options,
                _ => ZstandardOptions::default(),
            };
            let level = Level::Precise(options.0 as i32);
            let zstd_encoder = AsyncZstdEncoder::with_quality(input, level);
            Ok(Encoder::Zstd(Some(Box::new(zstd_encoder))))
        }
        #[cfg(feature = "aes256")]
        EncoderMethod::ID_AES256_SHA256 => {
            let options = match method_config.options.as_ref() {
                Some(EncoderOptions::Aes(p)) => p,
                _ => return Err(Error::PasswordRequired),
            };
            Ok(Encoder::Aes(Box::new(Aes256Sha256Encoder::new(
                input, options,
            )?)))
        }
        _ => Err(Error::UnsupportedCompressionMethod(
            method.name().to_string(),
        )),
    }
}

pub(crate) fn get_options_as_properties<'a>(
    method: EncoderMethod,
    options: Option<&EncoderOptions>,
    out: &'a mut [u8],
) -> &'a [u8] {
    match method.id() {
        EncoderMethod::ID_DELTA => {
            let options = match options {
                Some(EncoderOptions::Delta(options)) => *options,
                _ => DeltaOptions::default(),
            };

            out[0] = options.0.saturating_sub(1) as u8;
            &out[0..1]
        }
        EncoderMethod::ID_LZMA2 => {
            let options = match options {
                Some(EncoderOptions::Lzma2(options)) => options,
                _ => &Lzma2Options::default(),
            };
            let dict_size = options.options.lzma_options.dict_size;
            let lead = dict_size.leading_zeros();
            let second_bit = (dict_size >> (30u32.wrapping_sub(lead))).wrapping_sub(2);
            let prop = (19u32.wrapping_sub(lead) * 2 + second_bit) as u8;
            out[0] = prop;
            &out[0..1]
        }
        EncoderMethod::ID_LZMA => {
            let options = match options {
                Some(EncoderOptions::Lzma(options)) => options,
                _ => &LzmaOptions::default(),
            };
            let dict_size = options.0.dict_size;
            out[0] = options.0.get_props();
            out[1..5].copy_from_slice(dict_size.to_le_bytes().as_ref());
            &out[0..5]
        }
        #[cfg(feature = "ppmd")]
        EncoderMethod::ID_PPMD => {
            let options = match options {
                Some(EncoderOptions::Ppmd(options)) => *options,
                _ => PpmdOptions::default(),
            };

            out[0] = options.order as u8;
            out[1..5].copy_from_slice(&options.memory_size.to_le_bytes());
            &out[0..5]
        }
        #[cfg(feature = "brotli")]
        EncoderMethod::ID_BROTLI => {
            let version_major = 1;
            let version_minor = 0;
            let options = match options {
                Some(EncoderOptions::Brotli(options)) => *options,
                _ => BrotliOptions::default(),
            };

            out[0] = version_major;
            out[1] = version_minor;
            out[2] = options.quality as u8;
            &out[0..3]
        }
        #[cfg(feature = "lz4")]
        EncoderMethod::ID_LZ4 => {
            // Since we use lz4_flex, we only support one compression level
            // and set the version to 1.0 for best compatibility.
            out[0] = 1; // Major version
            out[1] = 0; // Minor version
            out[2] = 3; // Fast compression
            &out[0..3]
        }
        #[cfg(feature = "zstd")]
        EncoderMethod::ID_ZSTD => {
            let options = match options {
                Some(EncoderOptions::Zstd(options)) => *options,
                _ => ZstandardOptions::default(),
            };

            out[0] = 1; // Zstd major version
            out[1] = 0; // Zstd minor version
            out[2] = options.0 as u8;
            &out[0..3]
        }
        #[cfg(feature = "aes256")]
        EncoderMethod::ID_AES256_SHA256 => {
            let options = match options.as_ref() {
                Some(EncoderOptions::Aes(p)) => p,
                _ => return &[],
            };
            options.write_properties(out);
            &out[..34]
        }
        _ => &[],
    }
}
