use std::path::{Path, PathBuf};

use async_fs as afs;
use futures::io::{AllowStdIo, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom};
use std::pin::Pin;

use crate::{Error, Password, *};

struct AsyncReadSeekAsStd<R: AsyncRead + AsyncSeek + Unpin> {
    inner: R,
}

impl<R: AsyncRead + AsyncSeek + Unpin> AsyncReadSeekAsStd<R> {
    fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> Read for AsyncReadSeekAsStd<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        async_io::block_on(AsyncReadExt::read(&mut self.inner, buf))
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> Seek for AsyncReadSeekAsStd<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
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

/// Decompresses an archive file to a destination directory.
///
/// This is a convenience function for decompressing archive files directly from the filesystem.
///
/// # Arguments
/// * `src_path` - Path to the source archive file
/// * `dest` - Path to the destination directory where files will be extracted
pub async fn decompress_file(
    src_path: impl AsRef<Path>,
    dest: impl AsRef<Path>,
) -> Result<(), Error> {
    let dest_path = dest.as_ref().to_path_buf();
    decompress_path_impl_async(
        src_path.as_ref(),
        dest_path,
        Password::empty(),
        |entry, reader, dest| Box::pin(default_entry_extract_fn_async(entry, reader, dest)),
    )
    .await
}

/// Decompresses an archive file to a destination directory with a custom extraction function.
///
/// The extraction function is called for each entry in the archive, allowing custom handling
/// of individual files and directories during extraction.
///
/// # Arguments
/// * `src_path` - Path to the source archive file
/// * `dest` - Path to the destination directory where files will be extracted
/// * `extract_fn` - Custom function to handle each archive entry during extraction
pub async fn decompress_file_with_extract_fn(
    src_path: impl AsRef<Path>,
    dest: impl AsRef<Path>,
    mut extract_fn: impl for<'a> FnMut(
        &'a ArchiveEntry,
        &'a mut (dyn futures::io::AsyncRead + Unpin + 'a),
        &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Error>> + 'a>>,
) -> Result<(), Error> {
    decompress_path_impl_async(
        src_path.as_ref(),
        dest.as_ref().to_path_buf(),
        Password::empty(),
        move |entry, reader, path| extract_fn(entry, reader, path),
    )
    .await
}

/// Decompresses an archive from a reader to a destination directory.
///
/// # Arguments
/// * `src_reader` - Reader containing the archive data
/// * `dest` - Path to the destination directory where files will be extracted
pub async fn decompress<R: AsyncRead + AsyncSeek + Unpin>(
    mut src_reader: R,
    dest: impl AsRef<Path>,
) -> Result<(), Error> {
    let pos = AsyncSeekExt::stream_position(&mut src_reader).await?;
    AsyncSeekExt::seek(&mut src_reader, futures::io::SeekFrom::Start(pos)).await?;
    let reader_std = AsyncReadSeekAsStd::new(src_reader);
    decompress_impl_async(
        reader_std,
        dest,
        Password::empty(),
        |entry, reader, dest| Box::pin(default_entry_extract_fn_async(entry, reader, dest)),
    )
    .await
}

/// Decompresses an archive from a reader to a destination directory with a custom extraction function.
///
/// This provides the most flexibility, allowing both custom input sources and custom extraction logic.
///
/// # Arguments
/// * `src_reader` - Reader containing the archive data
/// * `dest` - Path to the destination directory where files will be extracted
/// * `extract_fn` - Custom function to handle each archive entry during extraction
#[cfg(not(target_arch = "wasm32"))]
pub async fn decompress_with_extract_fn<R: AsyncRead + AsyncSeek + Unpin>(
    mut src_reader: R,
    dest: impl AsRef<Path>,
    extract_fn: impl for<'a> FnMut(
        &'a ArchiveEntry,
        &'a mut (dyn futures::io::AsyncRead + Unpin + 'a),
        &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Error>> + 'a>>,
) -> Result<(), Error> {
    let pos = AsyncSeekExt::stream_position(&mut src_reader).await?;
    AsyncSeekExt::seek(&mut src_reader, futures::io::SeekFrom::Start(pos)).await?;
    let reader_std = AsyncReadSeekAsStd::new(src_reader);
    decompress_impl_async(reader_std, dest, Password::empty(), extract_fn).await
}

/// Decompresses an encrypted archive file with the given password.
///
/// # Arguments
/// * `src_path` - Path to the encrypted source archive file
/// * `dest` - Path to the destination directory where files will be extracted
/// * `password` - Password to decrypt the archive
#[cfg(all(feature = "aes256", not(target_arch = "wasm32")))]
pub async fn decompress_file_with_password(
    src_path: impl AsRef<Path>,
    dest: impl AsRef<Path>,
    password: Password,
) -> Result<(), Error> {
    let dest_path = dest.as_ref().to_path_buf();
    decompress_path_impl_async(
        src_path.as_ref(),
        dest_path,
        password,
        |entry, reader, dest| Box::pin(default_entry_extract_fn_async(entry, reader, dest)),
    )
    .await
}

/// Decompresses an encrypted archive from a reader with the given password.
///
/// # Arguments
/// * `src_reader` - Reader containing the encrypted archive data
/// * `dest` - Path to the destination directory where files will be extracted
/// * `password` - Password to decrypt the archive
#[cfg(all(feature = "aes256", not(target_arch = "wasm32")))]
pub async fn decompress_with_password<R: AsyncRead + AsyncSeek + Unpin>(
    mut src_reader: R,
    dest: impl AsRef<Path>,
    password: Password,
) -> Result<(), Error> {
    let pos = AsyncSeekExt::stream_position(&mut src_reader).await?;
    AsyncSeekExt::seek(&mut src_reader, futures::io::SeekFrom::Start(pos)).await?;
    let reader_std = AsyncReadSeekAsStd::new(src_reader);
    decompress_impl_async(reader_std, dest, password, |entry, reader, dest| {
        Box::pin(default_entry_extract_fn_async(entry, reader, dest))
    })
    .await
}

/// Decompresses an encrypted archive from a reader with a custom extraction function and password.
///
/// This provides maximum flexibility for encrypted archives, allowing custom input sources,
/// custom extraction logic, and password decryption.
///
/// # Arguments
/// * `src_reader` - Reader containing the encrypted archive data
/// * `dest` - Path to the destination directory where files will be extracted
/// * `password` - Password to decrypt the archive
/// * `extract_fn` - Custom function to handle each archive entry during extraction
#[cfg(all(feature = "aes256", not(target_arch = "wasm32")))]
pub async fn decompress_with_extract_fn_and_password<R: AsyncRead + AsyncSeek + Unpin>(
    mut src_reader: R,
    dest: impl AsRef<Path>,
    password: Password,
    extract_fn: impl for<'a> FnMut(
        &'a ArchiveEntry,
        &'a mut (dyn futures::io::AsyncRead + Unpin + 'a),
        &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Error>> + 'a>>,
) -> Result<(), Error> {
    let pos = AsyncSeekExt::stream_position(&mut src_reader).await?;
    AsyncSeekExt::seek(&mut src_reader, futures::io::SeekFrom::Start(pos)).await?;
    let reader_std = AsyncReadSeekAsStd::new(src_reader);
    decompress_impl_async(reader_std, dest, password, extract_fn).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn decompress_impl_async<R: Read + Seek>(
    mut src_reader: R,
    dest: impl AsRef<Path>,
    password: Password,
    mut extract_fn: impl for<'a> FnMut(
        &'a ArchiveEntry,
        &'a mut (dyn futures::io::AsyncRead + Unpin + 'a),
        &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Error>> + 'a>>,
) -> Result<(), Error> {
    use std::io::SeekFrom;

    let pos = src_reader.stream_position()?;
    src_reader.seek(SeekFrom::Start(pos))?;
    let mut seven = ArchiveReader::new(src_reader, password)?;
    let dest = PathBuf::from(dest.as_ref());
    if !dest.exists() {
        afs::create_dir_all(&dest).await?;
    }
    seven.for_each_entries(|entry, reader| {
        let dest_path = dest.join(entry.name());
        let mut ar = AllowStdIo::new(reader);
        async_io::block_on(extract_fn(entry, &mut ar, dest_path.as_path()))
    })?;

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn decompress_path_impl_async(
    src_path: &Path,
    dest: PathBuf,
    password: Password,
    mut extract_fn: impl for<'a> FnMut(
        &'a ArchiveEntry,
        &'a mut (dyn futures::io::AsyncRead + Unpin + 'a),
        &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Error>> + 'a>>,
) -> Result<(), Error> {
    let mut seven = ArchiveReader::open_async(src_path, password).await?;
    if !dest.exists() {
        afs::create_dir_all(&dest).await?;
    }
    seven.for_each_entries(|entry, reader| {
        let dest_path = dest.join(entry.name());
        let mut ar = AllowStdIo::new(reader);
        async_io::block_on(extract_fn(entry, &mut ar, dest_path.as_path()))
    })?;
    Ok(())
}

/// Default extraction function that handles standard file and directory extraction.
///
/// # Arguments
/// * `entry` - Archive entry being processed
/// * `reader` - Reader for the entry's data
/// * `dest` - Destination path for the entry
#[cfg(not(target_arch = "wasm32"))]
pub async fn default_entry_extract_fn_async(
    entry: &ArchiveEntry,
    reader: &mut (dyn futures::io::AsyncRead + Unpin),
    dest: &Path,
) -> Result<bool, Error> {
    if entry.is_directory() {
        let dir = dest.to_path_buf();
        afs::create_dir_all(&dir).await?;
    } else {
        let path = dest.to_path_buf();
        if let Some(p) = path.parent() {
            if !p.exists() {
                afs::create_dir_all(p).await?;
            }
        }
        if entry.size() > 0 {
            let mut data = Vec::new();
            AsyncReadExt::read_to_end(reader, &mut data)
                .await
                .map_err(|e| Error::io_msg(e, "read entry data"))?;
            afs::write(&path, &data).await?;
        } else {
            afs::File::create(&path).await?;
        }
    }

    Ok(true)
}
