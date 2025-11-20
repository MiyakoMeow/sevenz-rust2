use std::pin::Pin;
use std::task::{Context, Poll};

use futures::io::{AsyncRead, AsyncWrite};

#[cfg(feature = "compress")]
use aes::cipher::BlockEncryptMut;
use aes::{
    Aes256,
    cipher::{BlockDecryptMut, KeyIvInit, generic_array::GenericArray},
};
use sha2::Digest;

use crate::Password;
#[cfg(feature = "compress")]
use crate::encoder_options::AesEncoderOptions;

type Aes256CbcDec = cbc::Decryptor<Aes256>;

#[cfg(feature = "compress")]
type Aes256CbcEnc = cbc::Encryptor<Aes256>;

pub(crate) struct Aes256Sha256Decoder<R> {
    cipher: Cipher,
    input: R,
    done: bool,
    obuffer: Vec<u8>,
    ostart: usize,
    ofinish: usize,
    pos: usize,
}

impl<R: AsyncRead + Unpin> Aes256Sha256Decoder<R> {
    pub(crate) fn new(
        input: R,
        properties: &[u8],
        password: &Password,
    ) -> Result<Self, crate::Error> {
        let cipher = Cipher::from_properties(properties, password.as_slice())?;
        Ok(Self {
            input,
            cipher,
            done: false,
            obuffer: Default::default(),
            ostart: 0,
            ofinish: 0,
            pos: 0,
        })
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for Aes256Sha256Decoder<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.ostart < self.ofinish {
            let available = self.ofinish - self.ostart;
            let n = available.min(buf.len());
            buf[..n].copy_from_slice(&self.obuffer[self.ostart..self.ostart + n]);
            self.ostart += n;
            self.pos += n;
            return Poll::Ready(Ok(n));
        }

        if self.done {
            return Poll::Ready(Ok(0));
        }

        self.ofinish = 0;
        self.ostart = 0;
        self.obuffer.clear();

        let mut ibuffer = [0u8; 512];
        match Pin::new(&mut self.input).poll_read(cx, &mut ibuffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(readin)) => {
                if readin == 0 {
                    self.done = true;
                    let mut out_local = Vec::new();
                    let n = self.cipher.do_final(&mut out_local)?;
                    self.obuffer = out_local;
                    self.ofinish = n;
                } else {
                    let mut out_local = Vec::new();
                    let n = self.cipher.update(&mut ibuffer[..readin], &mut out_local)?;
                    self.obuffer = out_local;
                    self.ofinish = n;
                }

                if self.ofinish == 0 {
                    if self.done {
                        Poll::Ready(Ok(0))
                    } else {
                        Poll::Pending
                    }
                } else {
                    let count = self.ofinish.min(buf.len());
                    buf[..count].copy_from_slice(&self.obuffer[..count]);
                    self.ostart = count;
                    self.pos += count;
                    Poll::Ready(Ok(count))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }
}

fn get_aes_key(properties: &[u8], password: &[u8]) -> Result<([u8; 32], [u8; 16]), crate::Error> {
    if properties.len() < 2 {
        return Err(crate::Error::other("AES256 properties too shart"));
    }
    let b0 = properties[0];
    let num_cycles_power = b0 & 63;
    let b1 = properties[1];
    let iv_size = (((b0 >> 6) & 1) + (b1 & 15)) as usize;
    let salt_size = (((b0 >> 7) & 1) + (b1 >> 4)) as usize;
    if 2 + salt_size + iv_size > properties.len() {
        return Err(crate::Error::other("Salt size + IV size too long"));
    }
    let mut salt = vec![0u8; salt_size];
    salt.copy_from_slice(&properties[2..(2 + salt_size)]);
    let mut iv = [0u8; 16];
    iv[0..iv_size].copy_from_slice(&properties[(2 + salt_size)..(2 + salt_size + iv_size)]);
    if password.is_empty() {
        return Err(crate::Error::PasswordRequired);
    }
    let aes_key = if num_cycles_power == 0x3F {
        let mut aes_key = [0u8; 32];
        aes_key.copy_from_slice(&salt[..salt_size]);
        let n = password.len().min(aes_key.len() - salt_size);
        aes_key[salt_size..n + salt_size].copy_from_slice(&password[0..n]);
        aes_key
    } else {
        let mut sha = sha2::Sha256::default();
        let mut extra = [0u8; 8];
        for _ in 0..(1u32 << num_cycles_power) {
            sha.update(&salt);
            sha.update(password);
            sha.update(extra);
            for item in &mut extra {
                *item = item.wrapping_add(1);
                if *item != 0 {
                    break;
                }
            }
        }
        sha.finalize().into()
    };
    Ok((aes_key, iv))
}

struct Cipher {
    dec: Aes256CbcDec,
    buf: Vec<u8>,
}

impl Cipher {
    fn from_properties(properties: &[u8], password: &[u8]) -> Result<Self, crate::Error> {
        let (aes_key, iv) = get_aes_key(properties, password)?;
        Ok(Self {
            dec: Aes256CbcDec::new(&GenericArray::from(aes_key), &iv.into()),
            buf: Default::default(),
        })
    }

    fn update(&mut self, mut data: &mut [u8], output: &mut Vec<u8>) -> std::io::Result<usize> {
        let mut n = 0;
        if !self.buf.is_empty() {
            assert!(self.buf.len() < 16);
            let end = 16 - self.buf.len();
            self.buf.extend_from_slice(&data[..end]);
            data = &mut data[end..];
            let block = GenericArray::from_mut_slice(&mut self.buf);
            self.dec.decrypt_block_mut(block);
            let out = block.as_slice();
            output.extend_from_slice(out);
            n += out.len();
            self.buf.clear();
        }

        for a in data.chunks_mut(16) {
            if a.len() < 16 {
                self.buf.extend_from_slice(a);
                break;
            }
            let block = GenericArray::from_mut_slice(a);
            self.dec.decrypt_block_mut(block);
            let out = block.as_slice();
            output.extend_from_slice(out);
            n += out.len();
        }
        Ok(n)
    }

    fn do_final(&mut self, output: &mut Vec<u8>) -> std::io::Result<usize> {
        if self.buf.is_empty() {
            output.clear();
            Ok(0)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "IllegalBlockSize",
            ))
        }
    }
}

#[cfg(feature = "compress")]
pub(crate) struct Aes256Sha256Encoder<W> {
    output: W,
    enc: Aes256CbcEnc,
    buffer: Vec<u8>,
    out_buf: Vec<u8>,
    out_pos: usize,
    finished: bool,
    write_size: u32,
}

#[cfg(feature = "compress")]
impl<W> Aes256Sha256Encoder<W> {
    pub(crate) fn new(output: W, options: &AesEncoderOptions) -> Result<Self, crate::Error> {
        let (key, iv) = crate::encryption::aes::get_aes_key(
            &options.properties(),
            options.password.as_slice(),
        )?;

        Ok(Self {
            output,
            enc: Aes256CbcEnc::new(&GenericArray::from(key), &iv.into()),
            buffer: Default::default(),
            out_buf: Default::default(),
            out_pos: 0,
            finished: false,
            write_size: 0,
        })
    }
}

#[cfg(feature = "compress")]
#[cfg(feature = "compress")]
impl<W: AsyncWrite + Unpin> AsyncWrite for Aes256Sha256Encoder<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.out_pos < self.out_buf.len() {
            let buf = std::mem::take(&mut self.out_buf);
            let mut pos = self.out_pos;
            let slice = &buf[pos..];
            match Pin::new(&mut self.output).poll_write(cx, slice) {
                Poll::Pending => {
                    self.out_buf = buf;
                    self.out_pos = pos;
                    return Poll::Pending;
                }
                Poll::Ready(Ok(w)) => {
                    pos += w;
                    self.write_size += w as u32;
                    if pos < buf.len() {
                        self.out_buf = buf;
                        self.out_pos = pos;
                        return Poll::Pending;
                    }
                    self.out_pos = 0;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }
        if self.finished && !buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if buf.is_empty() {
            self.finished = true;
            if !self.buffer.is_empty() {
                let mut block = [0u8; 16];
                block[..self.buffer.len()].copy_from_slice(&self.buffer);
                {
                    let b = GenericArray::from_mut_slice(&mut block);
                    self.enc.encrypt_block_mut(b);
                }
                self.buffer.clear();
                self.out_buf.extend_from_slice(&block);
            }
            if self.out_pos < self.out_buf.len() {
                let buf2 = std::mem::take(&mut self.out_buf);
                let mut pos2 = self.out_pos;
                let slice = &buf2[pos2..];
                match Pin::new(&mut self.output).poll_write(cx, slice) {
                    Poll::Pending => {
                        self.out_buf = buf2;
                        self.out_pos = pos2;
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(w)) => {
                        pos2 += w;
                        self.write_size += w as u32;
                        if pos2 < buf2.len() {
                            self.out_buf = buf2;
                            self.out_pos = pos2;
                            return Poll::Pending;
                        }
                        self.out_pos = 0;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                }
            }
            return Poll::Ready(Ok(0));
        }
        let len = buf.len();
        let mut out = Vec::new();
        if !self.buffer.is_empty() {
            let blen = self.buffer.len();
            let need = 16 - blen;
            if buf.len() >= need {
                let mut block = [0u8; 16];
                block[..blen].copy_from_slice(&self.buffer);
                block[blen..].copy_from_slice(&buf[..need]);
                {
                    let b = GenericArray::from_mut_slice(&mut block);
                    self.enc.encrypt_block_mut(b);
                }
                out.extend_from_slice(&block);
                self.buffer.clear();
                for chunk in buf[need..].chunks(16) {
                    if chunk.len() < 16 {
                        self.buffer.extend_from_slice(chunk);
                        break;
                    }
                    let mut block = [0u8; 16];
                    block.copy_from_slice(chunk);
                    {
                        let b = GenericArray::from_mut_slice(&mut block);
                        self.enc.encrypt_block_mut(b);
                    }
                    out.extend_from_slice(&block);
                }
            } else {
                self.buffer.extend_from_slice(buf);
                return Poll::Ready(Ok(len));
            }
        } else {
            for chunk in buf.chunks(16) {
                if chunk.len() < 16 {
                    self.buffer.extend_from_slice(chunk);
                    break;
                }
                let mut block = [0u8; 16];
                block.copy_from_slice(chunk);
                {
                    let b = GenericArray::from_mut_slice(&mut block);
                    self.enc.encrypt_block_mut(b);
                }
                out.extend_from_slice(&block);
            }
        }
        if out.is_empty() {
            return Poll::Ready(Ok(len));
        }
        self.out_buf.extend_from_slice(&out);
        let buf = std::mem::take(&mut self.out_buf);
        let mut pos = self.out_pos;
        let slice = &buf[pos..];
        match Pin::new(&mut self.output).poll_write(cx, slice) {
            Poll::Pending => {
                self.out_buf = buf;
                self.out_pos = pos;
                Poll::Pending
            }
            Poll::Ready(Ok(w)) => {
                pos += w;
                self.write_size += w as u32;
                if pos == buf.len() {
                    self.out_pos = 0;
                } else {
                    self.out_buf = buf;
                    self.out_pos = pos;
                }
                Poll::Ready(Ok(len))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.finished && !self.buffer.is_empty() {
            let mut block = [0u8; 16];
            block[..self.buffer.len()].copy_from_slice(&self.buffer);
            {
                let b = GenericArray::from_mut_slice(&mut block);
                self.enc.encrypt_block_mut(b);
            }
            self.buffer.clear();
            self.out_buf.extend_from_slice(&block);
        }
        if self.out_pos < self.out_buf.len() {
            let buf = std::mem::take(&mut self.out_buf);
            let mut pos = self.out_pos;
            let slice = &buf[pos..];
            match Pin::new(&mut self.output).poll_write(cx, slice) {
                Poll::Pending => {
                    self.out_buf = buf;
                    self.out_pos = pos;
                    return Poll::Pending;
                }
                Poll::Ready(Ok(w)) => {
                    pos += w;
                    self.write_size += w as u32;
                    if pos == buf.len() {
                        self.out_pos = 0;
                    } else {
                        self.out_buf = buf;
                        self.out_pos = pos;
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }
        Pin::new(&mut self.output).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.finished = true;
        match self.as_mut().poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Pin::new(&mut self.output).poll_close(cx),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }
}

#[cfg(all(test, feature = "compress"))]
mod tests {
    use futures::io::{AsyncReadExt, AsyncWriteExt, Cursor};

    use super::*;

    #[test]
    fn test_aes_codec() {
        let mut encoded = vec![];
        let writer = Cursor::new(&mut encoded);
        let password: Password = "1234".into();
        let options = AesEncoderOptions::new(password.clone());
        let mut enc = Aes256Sha256Encoder::new(writer, &options).unwrap();
        let original = include_bytes!("aes.rs");
        smol::block_on(AsyncWriteExt::write_all(&mut enc, original)).unwrap();
        let _ = smol::block_on(AsyncWriteExt::write(&mut enc, &[])).unwrap();

        let cursor = Cursor::new(&encoded[..]);
        let mut dec = Aes256Sha256Decoder::new(cursor, &options.properties(), &password).unwrap();

        let mut decoded = vec![];
        async_io::block_on(AsyncReadExt::read_to_end(&mut dec, &mut decoded)).unwrap();
        assert_eq!(&decoded[..original.len()], &original[..]);
    }
}
