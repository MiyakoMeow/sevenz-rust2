#[cfg(feature = "compress")]
use sevenz_rust2::*;
#[cfg(feature = "compress")]
use tempfile::*;

#[cfg(feature = "compress")]
#[test]
fn compress_multi_files_solid() {
    let temp_dir = tempdir().unwrap();
    let folder = temp_dir.path().join("folder");
    std::fs::create_dir(&folder).unwrap();
    let mut files = Vec::with_capacity(100);
    let mut contents = Vec::with_capacity(100);
    for i in 1..=10000 {
        let name = format!("file{i}.txt");
        let content = format!("file{i} with content");
        std::fs::write(folder.join(&name), &content).unwrap();
        files.push(name);
        contents.push(content);
    }
    let dest = temp_dir.path().join("folder.7z");

    let mut sz = ArchiveWriter::new(std::io::Cursor::new(Vec::<u8>::new())).unwrap();
    sz.push_source_path(&folder, |_| true).unwrap();
    let cursor = sz.finish().expect("compress ok");
    let data = cursor.into_inner();
    smol::block_on(async_fs::write(&dest, data)).unwrap();

    let decompress_dest = temp_dir.path().join("decompress");
    smol::block_on(decompress_file(dest, &decompress_dest)).expect("decompress ok");
    assert!(decompress_dest.exists());
    for i in 0..files.len() {
        let name = &files[i];
        let content = &contents[i];
        let decompress_file = decompress_dest.join(name);
        assert!(decompress_file.exists());
        assert_eq!(&std::fs::read_to_string(&decompress_file).unwrap(), content);
    }
}

#[cfg(feature = "compress")]
#[test]
fn compress_multi_files_mix_solid_and_non_solid() {
    let temp_dir = tempdir().unwrap();
    let folder = temp_dir.path().join("folder");
    std::fs::create_dir(&folder).unwrap();
    let mut files = Vec::with_capacity(100);
    let mut contents = Vec::with_capacity(100);
    for i in 1..=100 {
        let name = format!("file{i}.txt");
        let content = format!("file{i} with content");
        std::fs::write(folder.join(&name), &content).unwrap();
        files.push(name);
        contents.push(content);
    }
    let dest = temp_dir.path().join("folder.7z");

    let mut sz = ArchiveWriter::new(std::io::Cursor::new(Vec::<u8>::new())).unwrap();

    // solid compression
    sz.push_source_path(&folder, |_| true).unwrap();

    // non solid compression
    for i in 101..=200 {
        let name = format!("file{i}.txt");
        let content = format!("file{i} with content");
        std::fs::write(folder.join(&name), &content).unwrap();
        files.push(name.clone());
        contents.push(content);

        let src = folder.join(&name);
        let data = smol::block_on(async_fs::read(&src)).unwrap();
        sz.push_archive_entry(
            ArchiveEntry::from_path(&src, name),
            Some(std::io::Cursor::new(data)),
        )
        .expect("ok");
    }

    let cursor = sz.finish().expect("compress ok");
    let data = cursor.into_inner();
    smol::block_on(async_fs::write(&dest, data)).unwrap();

    let decompress_dest = temp_dir.path().join("decompress");
    smol::block_on(decompress_file(dest, &decompress_dest)).expect("decompress ok");
    assert!(decompress_dest.exists());
    for i in 0..files.len() {
        let name = &files[i];
        let content = &contents[i];
        let decompress_file = decompress_dest.join(name);
        assert!(decompress_file.exists());
        assert_eq!(&std::fs::read_to_string(&decompress_file).unwrap(), content);
    }
}
