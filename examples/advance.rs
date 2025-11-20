use std::{collections::HashMap, env::temp_dir, time::Instant};

use rand::Rng;
use sevenz_rust2::*;

fn main() {
    let temp_dir = temp_dir();
    let src = temp_dir.join("compress/advance");
    if src.exists() {
        let _ = smol::block_on(async_fs::remove_dir_all(&src));
    }
    let _ = smol::block_on(async_fs::create_dir_all(&src));
    let file_count = 100;
    let mut contents = HashMap::with_capacity(file_count);
    let mut unpack_size = 0;
    // generate random content files
    {
        for i in 0..file_count {
            let c = gen_random_contents(rand::rng().random_range(1024..10240));
            unpack_size += c.len();
            contents.insert(format!("file{i}.txt"), c);
        }
        for (filename, content) in contents.iter() {
            let _ = smol::block_on(async_fs::write(src.join(filename), content));
        }
    }
    let dest = temp_dir.join("compress/compress.7z");

    let time = Instant::now();

    // start to compress
    #[cfg(feature = "aes256")]
    {
        smol::block_on(sevenz_rust2::compress_to_path_encrypted(
            &src,
            &dest,
            Password::new("sevenz-rust"),
        ))
        .expect("compress ok");
    }
    #[cfg(not(feature = "aes256"))]
    {
        smol::block_on(sevenz_rust2::compress_to_path(&src, &dest)).expect("compress ok");
    }
    println!("compress took {:?}/{:?}", time.elapsed(), dest);
    if src.exists() {
        let _ = smol::block_on(async_fs::remove_dir_all(&src));
    }
    assert!(dest.exists());
    let m = smol::block_on(async_fs::metadata(&dest)).unwrap();
    println!("src  file len:{unpack_size:?}");
    println!("dest file len:{:?}", m.len());
    println!("ratio:{:?}", m.len() as f64 / unpack_size as f64);

    // start to decompress
    let mut sz = smol::block_on(ArchiveReader::open(&dest, Password::new("sevenz-rust")))
        .expect("create reader ok");
    assert_eq!(contents.len(), sz.archive().files.len());
    assert_eq!(1, sz.archive().blocks.len());
    let names: Vec<String> = sz
        .archive()
        .files
        .iter()
        .filter(|f| !f.is_directory)
        .map(|f| f.name().to_string())
        .collect();
    for name in names {
        let data = smol::block_on(sz.read_file(&name)).expect("read file ok");
        let content = String::from_utf8(data).unwrap();
        assert_eq!(content, contents[&name].to_string());
    }
    let _ = smol::block_on(async_fs::remove_file(dest));
}

fn gen_random_contents(len: usize) -> String {
    let mut s = String::with_capacity(len);
    let mut rng = rand::rng();
    for _ in 0..len {
        let ch = rng.random_range('A'..='Z');
        s.push(ch);
    }
    s
}
