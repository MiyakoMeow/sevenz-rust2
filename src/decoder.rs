use std::{
    io::{self, Read},
    pin::Pin,
};

#[cfg(feature = "bzip2")]
use async_compression::futures::bufread::BzDecoder as AsyncBzip2Decoder;
#[cfg(feature = "deflate")]
use async_compression::futures::bufread::DeflateDecoder as AsyncDeflateDecoder;
use async_compression::futures::bufread::LzmaDecoder as AsyncLzmaDecoder;
#[cfg(feature = "zstd")]
use async_compression::futures::bufread::ZstdDecoder as AsyncZstdDecoder;
use futures::io::{AsyncRead, AsyncReadExt, BufReader, Cursor};
use lzma_rust2::{
    Lzma2Reader, Lzma2ReaderMt,
    filter::{bcj::BcjReader, delta::DeltaReader},
    lzma2_get_memory_usage,
};
#[cfg(feature = "ppmd")]
use ppmd_rust::{
    PPMD7_MAX_MEM_SIZE, PPMD7_MAX_ORDER, PPMD7_MIN_MEM_SIZE, PPMD7_MIN_ORDER, Ppmd7Decoder,
};

#[cfg(feature = "brotli")]
use crate::codec::brotli::BrotliDecoder;
#[cfg(feature = "lz4")]
use crate::codec::lz4::Lz4Decoder;
#[cfg(feature = "aes256")]
use crate::encryption::Aes256Sha256Decoder;
use crate::{
    Password, archive::EncoderMethod, block::Coder, error::Error,
    util::decompress::AsyncReadSeekAsStd,
};

type LzmaBuf<R> = BufReader<futures::io::Chain<Cursor<Vec<u8>>, R>>;

/// 归档数据解码器枚举，根据编码方法选择相应的异步解码器。
///
/// 所有变体均实现 `AsyncRead`，可在统一的读取管线中以异步方式解码数据。
pub enum Decoder<R: AsyncRead + Unpin> {
    /// 原样读取，不进行任何解码处理。
    Copy(R),
    /// LZMA 算法解码器。
    Lzma(Box<AsyncLzmaDecoder<LzmaBuf<R>>>),
    /// LZMA2 算法解码器（单线程）。
    Lzma2(Box<AsyncStdRead<Lzma2Reader<AsyncReadSeekAsStd<R>>>>),
    /// LZMA2 算法解码器（多线程）。
    Lzma2Mt(Box<AsyncStdRead<Lzma2ReaderMt<AsyncReadSeekAsStd<R>>>>),
    #[cfg(feature = "ppmd")]
    /// PPMd 算法解码器。
    Ppmd(Box<AsyncStdRead<Ppmd7Decoder<AsyncReadSeekAsStd<R>>>>),
    /// BCJ 分支转换过滤器解码器（用于可执行指令流）。
    Bcj(Box<AsyncStdRead<BcjReader<AsyncReadSeekAsStd<R>>>>),
    /// Delta 过滤器解码器（小幅差值编码）。
    Delta(Box<AsyncStdRead<DeltaReader<AsyncReadSeekAsStd<R>>>>),
    #[cfg(feature = "brotli")]
    /// Brotli 算法解码器。
    Brotli(Box<BrotliDecoder<R>>),
    #[cfg(feature = "bzip2")]
    /// bzip2 算法解码器。
    Bzip2(Box<AsyncBzip2Decoder<BufReader<R>>>),
    #[cfg(feature = "deflate")]
    /// deflate 算法解码器。
    Deflate(Box<AsyncDeflateDecoder<BufReader<R>>>),
    #[cfg(feature = "lz4")]
    /// LZ4 算法解码器。
    Lz4(Box<Lz4Decoder<R>>),
    #[cfg(feature = "zstd")]
    /// Zstd 算法解码器。
    Zstd(Box<AsyncZstdDecoder<BufReader<R>>>),
    #[cfg(feature = "aes256")]
    /// AES-256-SHA256 加密数据流解码器。
    Aes256Sha256(Box<AsyncStdRead<Aes256Sha256Decoder<AsyncReadSeekAsStd<R>>>>),
}

impl<R: AsyncRead + Unpin> AsyncRead for Decoder<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut *self {
            Decoder::Copy(r) => Pin::new(r).poll_read(cx, buf),
            Decoder::Lzma(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            Decoder::Lzma2(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            Decoder::Lzma2Mt(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            #[cfg(feature = "ppmd")]
            Decoder::Ppmd(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            Decoder::Bcj(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            Decoder::Delta(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            #[cfg(feature = "brotli")]
            Decoder::Brotli(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            #[cfg(feature = "bzip2")]
            Decoder::Bzip2(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            #[cfg(feature = "deflate")]
            Decoder::Deflate(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            #[cfg(feature = "lz4")]
            Decoder::Lz4(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            #[cfg(feature = "zstd")]
            Decoder::Zstd(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
            #[cfg(feature = "aes256")]
            Decoder::Aes256Sha256(r) => Pin::new(r.as_mut()).poll_read(cx, buf),
        }
    }
}

impl<R: AsyncRead + Unpin> Read for Decoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Decoder::Copy(r) => async_io::block_on(AsyncReadExt::read(r, buf)),
            Decoder::Lzma(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            Decoder::Lzma2(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            Decoder::Lzma2Mt(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            #[cfg(feature = "ppmd")]
            Decoder::Ppmd(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            Decoder::Bcj(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            Decoder::Delta(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            #[cfg(feature = "brotli")]
            Decoder::Brotli(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            #[cfg(feature = "bzip2")]
            Decoder::Bzip2(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            #[cfg(feature = "deflate")]
            Decoder::Deflate(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            #[cfg(feature = "lz4")]
            Decoder::Lz4(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            #[cfg(feature = "zstd")]
            Decoder::Zstd(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
            #[cfg(feature = "aes256")]
            Decoder::Aes256Sha256(r) => async_io::block_on(AsyncReadExt::read(r.as_mut(), buf)),
        }
    }
}

pub(crate) struct AsyncStdRead<D> {
    inner: D,
}

impl<D> AsyncStdRead<D> {
    pub(crate) fn new(inner: D) -> Self {
        Self { inner }
    }
}

impl<D: Read + Unpin> AsyncRead for AsyncStdRead<D> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(self.inner.read(buf))
    }
}

pub fn add_decoder<I: AsyncRead + Unpin>(
    input: I,
    uncompressed_len: usize,
    coder: &Coder,
    #[allow(unused)] password: &Password,
    max_mem_limit_kb: usize,
    threads: u32,
) -> Result<Decoder<I>, Error> {
    let method = EncoderMethod::by_id(coder.encoder_method_id());
    let method = if let Some(m) = method {
        m
    } else {
        return Err(Error::UnsupportedCompressionMethod(format!(
            "{:?}",
            coder.encoder_method_id()
        )));
    };
    match method.id() {
        EncoderMethod::ID_COPY => Ok(Decoder::Copy(input)),
        EncoderMethod::ID_LZMA => {
            let dict_size = get_lzma_dic_size(coder)?;
            if coder.properties.is_empty() {
                return Err(Error::Other("LZMA properties too short".into()));
            }

            let mut header = Vec::with_capacity(13);
            header.push(coder.properties[0]);
            header.extend_from_slice(&dict_size.to_le_bytes());
            header.extend_from_slice(&(uncompressed_len as u64).to_le_bytes());

            let header_cursor = Cursor::new(header);
            let chained = AsyncReadExt::chain(header_cursor, input);
            let bufread = BufReader::new(chained);
            let de = AsyncLzmaDecoder::new(bufread);
            Ok(Decoder::Lzma(Box::new(de)))
        }
        EncoderMethod::ID_LZMA2 => {
            let dic_size = get_lzma2_dic_size(coder)?;
            let mem_size = lzma2_get_memory_usage(dic_size) as usize;
            if mem_size > max_mem_limit_kb {
                return Err(Error::MaxMemLimited {
                    max_kb: max_mem_limit_kb,
                    actaul_kb: mem_size,
                });
            }

            let std_in = AsyncReadSeekAsStd::new(input);
            let lz = if threads < 2 {
                Decoder::Lzma2(Box::new(AsyncStdRead::new(Lzma2Reader::new(
                    std_in, dic_size, None,
                ))))
            } else {
                Decoder::Lzma2Mt(Box::new(AsyncStdRead::new(Lzma2ReaderMt::new(
                    std_in, dic_size, None, threads,
                ))))
            };

            Ok(lz)
        }
        #[cfg(feature = "ppmd")]
        EncoderMethod::ID_PPMD => {
            let (order, memory_size) = get_ppmd_order_memory_size(coder, max_mem_limit_kb)?;
            let std_in = AsyncReadSeekAsStd::new(input);
            let ppmd = Ppmd7Decoder::new(std_in, order, memory_size)
                .map_err(|err| Error::other(err.to_string()))?;
            Ok(Decoder::Ppmd(Box::new(AsyncStdRead::new(ppmd))))
        }
        #[cfg(feature = "brotli")]
        EncoderMethod::ID_BROTLI => {
            let de = BrotliDecoder::new(input, 4096)?;
            Ok(Decoder::Brotli(Box::new(de)))
        }
        #[cfg(feature = "bzip2")]
        EncoderMethod::ID_BZIP2 => {
            let br = BufReader::new(input);
            let de = AsyncBzip2Decoder::new(br);
            Ok(Decoder::Bzip2(Box::new(de)))
        }
        #[cfg(feature = "deflate")]
        EncoderMethod::ID_DEFLATE => {
            let br = BufReader::new(input);
            let de = AsyncDeflateDecoder::new(br);
            Ok(Decoder::Deflate(Box::new(de)))
        }
        #[cfg(feature = "lz4")]
        EncoderMethod::ID_LZ4 => {
            let de = Lz4Decoder::new(input)?;
            Ok(Decoder::Lz4(Box::new(de)))
        }
        #[cfg(feature = "zstd")]
        EncoderMethod::ID_ZSTD => {
            let br = BufReader::new(input);
            let zs = AsyncZstdDecoder::new(br);
            Ok(Decoder::Zstd(Box::new(zs)))
        }
        EncoderMethod::ID_BCJ_X86 => {
            let std_in = AsyncReadSeekAsStd::new(input);
            let de = BcjReader::new_x86(std_in, 0);
            Ok(Decoder::Bcj(Box::new(AsyncStdRead::new(de))))
        }
        EncoderMethod::ID_BCJ_ARM => {
            let std_in = AsyncReadSeekAsStd::new(input);
            let de = BcjReader::new_arm(std_in, 0);
            Ok(Decoder::Bcj(Box::new(AsyncStdRead::new(de))))
        }
        EncoderMethod::ID_BCJ_ARM64 => {
            let std_in = AsyncReadSeekAsStd::new(input);
            let de = BcjReader::new_arm64(std_in, 0);
            Ok(Decoder::Bcj(Box::new(AsyncStdRead::new(de))))
        }
        EncoderMethod::ID_BCJ_ARM_THUMB => {
            let std_in = AsyncReadSeekAsStd::new(input);
            let de = BcjReader::new_arm_thumb(std_in, 0);
            Ok(Decoder::Bcj(Box::new(AsyncStdRead::new(de))))
        }
        EncoderMethod::ID_BCJ_PPC => {
            let std_in = AsyncReadSeekAsStd::new(input);
            let de = BcjReader::new_ppc(std_in, 0);
            Ok(Decoder::Bcj(Box::new(AsyncStdRead::new(de))))
        }
        EncoderMethod::ID_BCJ_IA64 => {
            let std_in = AsyncReadSeekAsStd::new(input);
            let de = BcjReader::new_ia64(std_in, 0);
            Ok(Decoder::Bcj(Box::new(AsyncStdRead::new(de))))
        }
        EncoderMethod::ID_BCJ_SPARC => {
            let std_in = AsyncReadSeekAsStd::new(input);
            let de = BcjReader::new_sparc(std_in, 0);
            Ok(Decoder::Bcj(Box::new(AsyncStdRead::new(de))))
        }
        EncoderMethod::ID_BCJ_RISCV => {
            let std_in = AsyncReadSeekAsStd::new(input);
            let de = BcjReader::new_riscv(std_in, 0);
            Ok(Decoder::Bcj(Box::new(AsyncStdRead::new(de))))
        }
        EncoderMethod::ID_DELTA => {
            let d = if coder.properties.is_empty() {
                1
            } else {
                coder.properties[0].wrapping_add(1)
            };
            let std_in = AsyncReadSeekAsStd::new(input);
            let de = DeltaReader::new(std_in, d as usize);
            Ok(Decoder::Delta(Box::new(AsyncStdRead::new(de))))
        }
        #[cfg(feature = "aes256")]
        EncoderMethod::ID_AES256_SHA256 => {
            if password.is_empty() {
                return Err(Error::PasswordRequired);
            }
            let std_in = AsyncReadSeekAsStd::new(input);
            let de = Aes256Sha256Decoder::new(std_in, &coder.properties, password)?;
            Ok(Decoder::Aes256Sha256(Box::new(AsyncStdRead::new(de))))
        }
        _ => Err(Error::UnsupportedCompressionMethod(
            method.name().to_string(),
        )),
    }
}

#[cfg(feature = "ppmd")]
fn get_ppmd_order_memory_size(coder: &Coder, max_mem_limit_kb: usize) -> Result<(u32, u32), Error> {
    if coder.properties.len() < 5 {
        return Err(Error::other("PPMD properties too short"));
    }
    let order = coder.properties[0] as u32;
    let memory_size = u32::from_le_bytes([
        coder.properties[1],
        coder.properties[2],
        coder.properties[3],
        coder.properties[4],
    ]);

    if order < PPMD7_MIN_ORDER {
        return Err(Error::other("PPMD order smaller than PPMD7_MIN_ORDER"));
    }

    if order > PPMD7_MAX_ORDER {
        return Err(Error::other("PPMD order larger than PPMD7_MAX_ORDER"));
    }

    if memory_size < PPMD7_MIN_MEM_SIZE {
        return Err(Error::other(
            "PPMD memory size smaller than PPMD7_MIN_MEM_SIZE",
        ));
    }

    if memory_size > PPMD7_MAX_MEM_SIZE {
        return Err(Error::other(
            "PPMD memory size larger than PPMD7_MAX_MEM_SIZE",
        ));
    }

    if memory_size as usize > max_mem_limit_kb {
        return Err(Error::MaxMemLimited {
            max_kb: max_mem_limit_kb,
            actaul_kb: memory_size as usize,
        });
    }

    Ok((order, memory_size))
}

fn get_lzma2_dic_size(coder: &Coder) -> Result<u32, Error> {
    if coder.properties.is_empty() {
        return Err(Error::other("LZMA2 properties too short"));
    }
    let dict_size_bits = 0xFF & coder.properties[0] as u32;
    if (dict_size_bits & (!0x3F)) != 0 {
        return Err(Error::other("Unsupported LZMA2 property bits"));
    }
    if dict_size_bits > 40 {
        return Err(Error::other("Dictionary larger than 4GiB maximum size"));
    }
    if dict_size_bits == 40 {
        return Ok(0xFFFFFFFF);
    }
    let size = (2 | (dict_size_bits & 0x1)) << (dict_size_bits / 2 + 11);
    Ok(size)
}

fn get_lzma_dic_size(coder: &Coder) -> io::Result<u32> {
    if coder.properties.len() < 5 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "LZMA properties too short",
        ));
    }
    let arr: [u8; 4] = coder.properties[1..5].try_into().map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "LZMA properties too short")
    })?;
    Ok(u32::from_le_bytes(arr))
}
