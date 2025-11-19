use std::io::{Read, Seek};
use std::path::{Path, PathBuf};

use async_fs as afs;
use async_io::block_on;

use crate::{Error, Password, *};

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
        default_entry_extract_fn_async,
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
    mut extract_fn: impl FnMut(&ArchiveEntry, &mut dyn Read, &PathBuf) -> Result<bool, Error>,
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
pub async fn decompress<R: Read + Seek>(
    src_reader: R,
    dest: impl AsRef<Path>,
) -> Result<(), Error> {
    decompress_impl_async(
        src_reader,
        dest,
        Password::empty(),
        default_entry_extract_fn_async,
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
pub async fn decompress_with_extract_fn<R: Read + Seek>(
    src_reader: R,
    dest: impl AsRef<Path>,
    extract_fn: impl FnMut(&ArchiveEntry, &mut dyn Read, &PathBuf) -> Result<bool, Error>,
) -> Result<(), Error> {
    decompress_impl_async(src_reader, dest, Password::empty(), extract_fn).await
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
        default_entry_extract_fn_async,
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
pub async fn decompress_with_password<R: Read + Seek>(
    src_reader: R,
    dest: impl AsRef<Path>,
    password: Password,
) -> Result<(), Error> {
    decompress_impl_async(src_reader, dest, password, default_entry_extract_fn_async).await
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
pub async fn decompress_with_extract_fn_and_password<R: Read + Seek>(
    src_reader: R,
    dest: impl AsRef<Path>,
    password: Password,
    extract_fn: impl FnMut(&ArchiveEntry, &mut dyn Read, &PathBuf) -> Result<bool, Error>,
) -> Result<(), Error> {
    decompress_impl_async(src_reader, dest, password, extract_fn).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn decompress_impl_async<R: Read + Seek>(
    mut src_reader: R,
    dest: impl AsRef<Path>,
    password: Password,
    mut extract_fn: impl FnMut(&ArchiveEntry, &mut dyn Read, &PathBuf) -> Result<bool, Error>,
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
        extract_fn(entry, reader, &dest_path)
    })?;

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn decompress_path_impl_async(
    src_path: &Path,
    dest: PathBuf,
    password: Password,
    mut extract_fn: impl FnMut(&ArchiveEntry, &mut dyn Read, &PathBuf) -> Result<bool, Error>,
) -> Result<(), Error> {
    let mut seven = ArchiveReader::open(src_path, password)?;
    if !dest.exists() {
        afs::create_dir_all(&dest).await?;
    }
    seven.for_each_entries(|entry, reader| {
        let dest_path = dest.join(entry.name());
        extract_fn(entry, reader, &dest_path)
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
pub fn default_entry_extract_fn_async(
    entry: &ArchiveEntry,
    reader: &mut dyn Read,
    dest: &PathBuf,
) -> Result<bool, Error> {
    if entry.is_directory() {
        let dir = dest.clone();
        block_on(afs::create_dir_all(&dir))?;
    } else {
        let path = dest.clone();
        if let Some(p) = path.parent() {
            if !p.exists() {
                block_on(afs::create_dir_all(p))?;
            }
        }
        if entry.size() > 0 {
            let mut data = Vec::new();
            std::io::Read::read_to_end(reader, &mut data)
                .map_err(|e| Error::io_msg(e, "read entry data"))?;
            block_on(afs::write(&path, &data))?;
        } else {
            block_on(afs::File::create(&path))?;
        }
    }

    Ok(true)
}
