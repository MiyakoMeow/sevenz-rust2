use std::io::{Read, Seek, SeekFrom, Write};

use async_io::block_on;
use futures::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite};

use js_sys::*;
use wasm_bindgen::prelude::*;

use crate::*;

/// Decompresses a 7z archive in WebAssembly environment.
///
/// This function is specifically designed for WASM targets and uses JavaScript interop
/// to handle the decompression process with a callback function.
///
/// # Arguments
/// * `src` - Uint8Array containing the compressed archive data
/// * `pwd` - Password string for encrypted archives (use empty string for unencrypted)
/// * `f` - JavaScript callback function to handle extracted entries
/// 在 WASM 环境中从 `Uint8Array` 解压 7z 数据，并对每个条目调用回调。
///
/// `f` 的签名应为 `(name: string, data: Uint8Array) => void`。
#[wasm_bindgen]
pub fn decompress(src: Uint8Array, pwd: &str, f: &Function) -> Result<(), String> {
    let mut src_reader = Uint8ArrayStream::new(src);
    let pos =
        block_on(AsyncSeekExt::stream_position(&mut src_reader)).map_err(|e| e.to_string())?;
    block_on(AsyncSeekExt::seek(
        &mut src_reader,
        futures::io::SeekFrom::Start(pos),
    ))
    .map_err(|e| e.to_string())?;
    let mut seven =
        block_on(ArchiveReader::new(src_reader, Password::from(pwd))).map_err(|e| e.to_string())?;
    block_on(seven.for_each_entries_async(|entry, reader| {
        if !entry.is_directory() {
            let path = entry.name();

            if entry.size() > 0 {
                let mut writer = Vec::new();
                block_on(AsyncReadExt::read_to_end(reader, &mut writer))
                    .map_err(|e| e.to_string())?;
                let _ = f.call2(
                    &JsValue::NULL,
                    &JsValue::from(path),
                    &Uint8Array::from(&writer[..]),
                );
            }
        }
        Ok(true)
    }))
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// Compresses multiple entries into a 7z archive in WebAssembly environment.
///
/// This function creates a compressed archive from multiple file entries,
/// designed specifically for WASM targets.
///
/// # Arguments
/// * `entries` - Vector of JavaScript strings representing file names/paths
/// * `datas` - Vector of Uint8Arrays containing the file data corresponding to entries
/// 在 WASM 环境中将若干 `Uint8Array` 按名称压缩为 7z 数据并返回。
///
/// `entries` 与 `datas` 长度应一致，分别表示文件名与对应内容。
#[wasm_bindgen]
pub fn compress(entries: Vec<JsString>, datas: Vec<Uint8Array>) -> Result<Uint8Array, String> {
    let mut sz = block_on(ArchiveWriter::new(Uint8ArrayStream::new(
        Uint8Array::new_with_length(0),
    )))
    .map_err(|e| e.to_string())?;
    let reader: Vec<SourceReader<_>> = datas
        .into_iter()
        .map(Uint8ArrayStream::new)
        .map(SourceReader::new)
        .collect();
    let entries = entries
        .into_iter()
        .map(|name| ArchiveEntry {
            name: name.into(),
            has_stream: true,
            ..Default::default()
        })
        .collect();

    block_on(sz.push_archive_entries(entries, reader)).map_err(|e| e.to_string())?;
    let out = block_on(sz.finish()).map_err(|e| e.to_string())?;
    Ok(out.into_inner().data)
}

struct Uint8ArrayStream {
    data: Uint8Array,
    pos: usize,
}

impl Seek for Uint8ArrayStream {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(n) => {
                self.pos = n as usize;
            }
            SeekFrom::End(i) => {
                let posi = self.data.length() as i64 + i;
                if posi < 0 {
                    self.pos = 0;
                } else if posi >= self.data.length() as i64 {
                    self.pos = self.data.length() as usize;
                } else {
                    self.pos = posi as usize;
                }
            }
            SeekFrom::Current(i) => {
                if i != 0 {
                    let posi = self.pos as i64 + i;
                    if posi < 0 {
                        self.pos = 0;
                    } else if posi >= self.data.length() as i64 {
                        self.pos = self.data.length() as usize;
                    } else {
                        self.pos = posi as usize;
                    }
                }
            }
        }
        Ok(self.pos as u64)
    }
}

impl Uint8ArrayStream {
    fn new(data: Uint8Array) -> Self {
        Self { data, pos: 0 }
    }
}

impl Read for Uint8ArrayStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let end = (self.pos + buf.len()).min(self.data.length() as usize);
        let len = end - self.pos;
        if len == 0 {
            return Ok(0);
        }
        self.data
            .slice(self.pos as u32, end as u32)
            .copy_to(&mut buf[..len]);
        self.pos = end;
        Ok(len)
    }
}

impl Write for Uint8ArrayStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let end = self.pos + buf.len();
        let cur_len = self.data.length() as usize;
        if end > cur_len {
            let new_len = end.max(cur_len * 2).max(1);
            let mut new_data = Uint8Array::new_with_length(new_len as u32);
            new_data.set(&self.data, 0);
            self.data = new_data;
        }
        let target = self.data.subarray(self.pos as u32, end as u32);
        target.copy_from(buf);
        self.pos = end;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl AsyncRead for Uint8ArrayStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let end = (self.pos + buf.len()).min(self.data.length() as usize);
        let len = end - self.pos;
        if len == 0 {
            return std::task::Poll::Ready(Ok(0));
        }
        self.data
            .slice(self.pos as u32, end as u32)
            .copy_to(&mut buf[..len]);
        self.pos = end;
        std::task::Poll::Ready(Ok(len))
    }
}

impl AsyncSeek for Uint8ArrayStream {
    fn poll_seek(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        pos: futures::io::SeekFrom,
    ) -> std::task::Poll<std::io::Result<u64>> {
        match pos {
            futures::io::SeekFrom::Start(n) => {
                self.pos = n as usize;
            }
            futures::io::SeekFrom::End(i) => {
                let posi = self.data.length() as i64 + i;
                if posi < 0 {
                    self.pos = 0;
                } else if posi >= self.data.length() as i64 {
                    self.pos = self.data.length() as usize;
                } else {
                    self.pos = posi as usize;
                }
            }
            futures::io::SeekFrom::Current(i) => {
                if i != 0 {
                    let posi = self.pos as i64 + i;
                    if posi < 0 {
                        self.pos = 0;
                    } else if posi >= self.data.length() as i64 {
                        self.pos = self.data.length() as usize;
                    } else {
                        self.pos = posi as usize;
                    }
                }
            }
        }
        std::task::Poll::Ready(Ok(self.pos as u64))
    }
}

impl AsyncWrite for Uint8ArrayStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let end = self.pos + buf.len();
        let cur_len = self.data.length() as usize;
        if end > cur_len {
            let new_len = end.max(cur_len * 2).max(1);
            let mut new_data = Uint8Array::new_with_length(new_len as u32);
            new_data.set(&self.data, 0);
            self.data = new_data;
        }
        let target = self.data.subarray(self.pos as u32, end as u32);
        target.copy_from(buf);
        self.pos = end;
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
