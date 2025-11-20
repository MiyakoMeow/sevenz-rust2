//! 7z Compressor helper functions

use std::path::{Path, PathBuf};

use async_fs as afs;
use futures::io::{AsyncSeek, AsyncWrite};
use futures_lite::StreamExt;

#[cfg(feature = "aes256")]
use crate::encoder_options::AesEncoderOptions;
use crate::{ArchiveEntry, ArchiveWriter, EncoderMethod, Error, Password, writer::LazyFileReader};

/// Compresses a source file or directory to a destination writer.
///
/// # Arguments
/// * `src` - Path to the source file or directory to compress
/// * `dest` - Writer that implements `AsyncWrite + AsyncSeek + Unpin`
pub async fn compress<W: AsyncWrite + AsyncSeek + Unpin>(
    src: impl AsRef<Path>,
    dest: W,
) -> Result<W, Error> {
    let mut archive_writer = ArchiveWriter::new(dest).await?;
    let parent = if src.as_ref().is_dir() {
        src.as_ref()
    } else {
        src.as_ref().parent().unwrap_or(src.as_ref())
    };
    compress_path(src.as_ref(), parent, &mut archive_writer).await?;
    let out = archive_writer.finish().await?;
    Ok(out)
}

/// Compresses a source file or directory to a destination writer with password encryption.
///
/// # Arguments
/// * `src` - Path to the source file or directory to compress
/// * `dest` - Writer that implements `AsyncWrite + AsyncSeek + Unpin`
/// * `password` - Password to encrypt the archive with
#[cfg(feature = "aes256")]
pub async fn compress_encrypted<W: AsyncWrite + AsyncSeek + Unpin>(
    src: impl AsRef<Path>,
    dest: W,
    password: Password,
) -> Result<W, Error> {
    let mut archive_writer = ArchiveWriter::new(dest).await?;
    if !password.is_empty() {
        archive_writer.set_content_methods(vec![
            AesEncoderOptions::new(password).into(),
            EncoderMethod::LZMA2.into(),
        ]);
    }
    let parent = if src.as_ref().is_dir() {
        src.as_ref()
    } else {
        src.as_ref().parent().unwrap_or(src.as_ref())
    };
    compress_path(src.as_ref(), parent, &mut archive_writer).await?;
    let out = archive_writer.finish().await?;
    Ok(out)
}

/// Compresses a source file or directory to a destination file path.
///
/// This is a convenience function that handles file creation automatically.
///
/// # Arguments
/// * `src` - Path to the source file or directory to compress
/// * `dest` - Path where the compressed archive will be created
pub async fn compress_to_path(src: impl AsRef<Path>, dest: impl AsRef<Path>) -> Result<(), Error> {
    if let Some(path) = dest.as_ref().parent() {
        if !path.exists() {
            afs::create_dir_all(path)
                .await
                .map_err(|e| Error::io_msg(e, format!("Create dir failed:{:?}", dest.as_ref())))?;
        }
    }
    let cursor = futures::io::Cursor::new(Vec::<u8>::new());
    let cursor = compress(src, cursor).await?;
    let data = cursor.into_inner();
    afs::write(dest.as_ref(), data).await?;
    Ok(())
}

/// Compresses a source file or directory to a destination file path with password encryption.
///
/// This is a convenience function that handles file creation automatically.
///
/// # Arguments
/// * `src` - Path to the source file or directory to compress
/// * `dest` - Path where the encrypted compressed archive will be created
/// * `password` - Password to encrypt the archive with
#[cfg(feature = "aes256")]
pub async fn compress_to_path_encrypted(
    src: impl AsRef<Path>,
    dest: impl AsRef<Path>,
    password: Password,
) -> Result<(), Error> {
    if let Some(path) = dest.as_ref().parent() {
        if !path.exists() {
            afs::create_dir_all(path)
                .await
                .map_err(|e| Error::io_msg(e, format!("Create dir failed:{:?}", dest.as_ref())))?;
        }
    }
    let cursor = futures::io::Cursor::new(Vec::<u8>::new());
    let cursor = compress_encrypted(src, cursor, password).await?;
    let data = cursor.into_inner();
    afs::write(dest.as_ref(), data).await?;
    Ok(())
}

async fn compress_path<W: AsyncWrite + AsyncSeek + Unpin, P: AsRef<Path>>(
    src: P,
    root: &Path,
    archive_writer: &mut ArchiveWriter<W>,
) -> Result<(), Error> {
    let mut stack: Vec<PathBuf> = vec![src.as_ref().to_path_buf()];
    while let Some(path) = stack.pop() {
        let entry_name = path
            .strip_prefix(root)
            .map_err(|e| Error::other(e.to_string()))?
            .to_string_lossy()
            .to_string();
        let entry = ArchiveEntry::from_path(path.as_path(), entry_name);
        let meta = afs::metadata(&path)
            .await
            .map_err(|e| Error::io_msg(e, "error metadata"))?;
        if meta.is_dir() {
            archive_writer
                .push_archive_entry::<&[u8]>(entry, None)
                .await?;
            let mut rd = afs::read_dir(&path)
                .await
                .map_err(|e| Error::io_msg(e, "error read dir"))?;
            while let Some(res) = rd.next().await {
                let dir = res.map_err(|e| Error::io_msg(e, "error read dir entry"))?;
                let ftype = dir
                    .file_type()
                    .await
                    .map_err(|e| Error::io_msg(e, "error file type"))?;
                if ftype.is_dir() || ftype.is_file() {
                    stack.push(dir.path());
                }
            }
        } else {
            archive_writer
                .push_archive_entry::<crate::writer::SourceReader<crate::writer::LazyFileReader>>(
                    entry,
                    Some(LazyFileReader::new(path.clone()).into()),
                )
                .await?;
        }
    }
    Ok(())
}

impl<W: AsyncWrite + AsyncSeek + Unpin> ArchiveWriter<W> {
    /// Adds a source path to the compression builder with a filter function using solid compression.
    ///
    /// The filter function allows selective inclusion of files based on their paths.
    /// Files are compressed using solid compression for better compression ratios.
    ///
    /// # Arguments
    /// * `path` - Path to add to the compression
    /// * `filter` - Function that returns `true` for paths that should be included
    pub async fn push_source_path<Fut>(
        &mut self,
        path: impl AsRef<Path>,
        mut filter: impl FnMut(&Path) -> Fut,
    ) -> Result<(), Error>
    where
        Fut: std::future::Future<Output = bool>,
    {
        encode_path(true, &path, self, &mut filter).await?;
        Ok(())
    }

    /// Adds a source path to the compression builder with a filter function using non-solid compression.
    ///
    /// Non-solid compression allows individual file extraction without decompressing the entire archive,
    /// but typically results in larger archive sizes compared to solid compression.
    ///
    /// # Arguments
    /// * `path` - Path to add to the compression
    /// * `filter` - Function that returns `true` for paths that should be included
    pub async fn push_source_path_non_solid<Fut>(
        &mut self,
        path: impl AsRef<Path>,
        mut filter: impl FnMut(&Path) -> Fut,
    ) -> Result<(), Error>
    where
        Fut: std::future::Future<Output = bool>,
    {
        encode_path(false, &path, self, &mut filter).await?;
        Ok(())
    }
}

async fn collect_file_paths<Fut>(
    src: impl AsRef<Path>,
    paths: &mut Vec<PathBuf>,
    filter: &mut impl FnMut(&Path) -> Fut,
) -> std::io::Result<()>
where
    Fut: std::future::Future<Output = bool>,
{
    let mut stack: Vec<PathBuf> = vec![src.as_ref().to_path_buf()];
    while let Some(path) = stack.pop() {
        if !filter(&path).await {
            continue;
        }
        let meta = afs::metadata(&path).await?;
        if meta.is_dir() {
            let mut rd = afs::read_dir(&path).await?;
            while let Some(res) = rd.next().await {
                let dir = res?;
                let ftype = dir.file_type().await?;
                if ftype.is_file() || ftype.is_dir() {
                    stack.push(dir.path());
                }
            }
        } else {
            paths.push(path);
        }
    }
    Ok(())
}

const MAX_BLOCK_SIZE: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB

async fn encode_path<W: AsyncWrite + AsyncSeek + Unpin, Fut>(
    solid: bool,
    src: impl AsRef<Path>,
    zip: &mut ArchiveWriter<W>,
    filter: &mut impl FnMut(&Path) -> Fut,
) -> Result<(), Error>
where
    Fut: std::future::Future<Output = bool>,
{
    let mut entries = Vec::new();
    let mut paths = Vec::new();
    collect_file_paths(&src, &mut paths, filter)
        .await
        .map_err(|e| {
            Error::io_msg(
                e,
                format!("Failed to collect entries from path:{:?}", src.as_ref()),
            )
        })?;

    if !solid {
        for ele in paths.into_iter() {
            let name = extract_file_name(&src, &ele)?;

            zip.push_archive_entry::<crate::writer::SourceReader<crate::writer::LazyFileReader>>(
                ArchiveEntry::from_path(ele.as_path(), name),
                Some(LazyFileReader::new(ele.clone()).into()),
            )
            .await?;
        }
        return Ok(());
    }
    let mut files = Vec::new();
    let mut file_size = 0;
    for ele in paths.into_iter() {
        let size = afs::metadata(&ele).await?.len();
        let name = extract_file_name(&src, &ele)?;

        if size >= MAX_BLOCK_SIZE {
            zip.push_archive_entry::<crate::writer::SourceReader<crate::writer::LazyFileReader>>(
                ArchiveEntry::from_path(ele.as_path(), name),
                Some(LazyFileReader::new(ele.clone()).into()),
            )
            .await?;
            continue;
        }
        if file_size + size >= MAX_BLOCK_SIZE {
            zip.push_archive_entries(entries, files).await?;
            entries = Vec::new();
            files = Vec::new();
            file_size = 0;
        }
        file_size += size;
        entries.push(ArchiveEntry::from_path(ele.as_path(), name));
        files.push(LazyFileReader::new(ele).into());
    }
    if !entries.is_empty() {
        zip.push_archive_entries(entries, files).await?;
    }

    Ok(())
}

fn extract_file_name(src: &impl AsRef<Path>, ele: &PathBuf) -> Result<String, Error> {
    if ele == src.as_ref() {
        // Single file case: use just the filename.
        Ok(ele
            .file_name()
            .ok_or_else(|| {
                Error::io_msg(
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid filename"),
                    format!("Failed to get filename from {ele:?}"),
                )
            })?
            .to_string_lossy()
            .to_string())
    } else {
        // Directory case: remove path.
        Ok(ele.strip_prefix(src).unwrap().to_string_lossy().to_string())
    }
}
