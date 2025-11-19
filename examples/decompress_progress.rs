use async_fs as afs;
use futures::io::{AsyncReadExt, Cursor};
use sevenz_rust2::Password;
use std::path::PathBuf;

fn main() {
    let total_size = {
        let sz = smol::block_on(sevenz_rust2::ArchiveReader::open_async(
            "examples/data/sample.7z",
            Password::from("pass"),
        ))
        .unwrap();
        sz.archive()
            .files
            .iter()
            .filter(|e| e.has_stream())
            .map(|e| e.size())
            .sum::<u64>()
    };
    let mut uncompressed_size = 0usize;
    let dest = PathBuf::from("examples/data/sample");

    smol::block_on(async {
        let data = afs::read("examples/data/sample.7z").await.unwrap();
        sevenz_rust2::decompress_with_extract_fn_and_password(
            Cursor::new(data),
            &dest,
            Password::from("pass"),
            |entry, reader, dest| {
                Box::pin(async move {
                    let path = dest.join(entry.name());
                    if let Some(parent) = path.parent() {
                        async_fs::create_dir_all(parent).await.unwrap();
                    }
                    let mut buf = [0u8; 8192];
                    let mut data = Vec::new();
                    loop {
                        let n = AsyncReadExt::read(reader, &mut buf).await?;
                        if n == 0 {
                            break;
                        }
                        data.extend_from_slice(&buf[..n]);
                        uncompressed_size += n;
                        println!(
                            "progress:{:.2}%",
                            (uncompressed_size as f64 / total_size as f64) * 100f64
                        );
                    }
                    async_fs::write(&path, &data).await.unwrap();
                    Ok(true)
                })
            },
        )
        .await
    })
    .unwrap();
}
