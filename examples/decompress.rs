use std::time::Instant;

use async_fs as afs;
use futures::io::Cursor;
use sevenz_rust2::default_entry_extract_fn_async;

fn main() {
    let instant = Instant::now();
    smol::block_on(async {
        let data = afs::read("examples/data/sample.7z").await.unwrap();
        sevenz_rust2::decompress_with_extract_fn_and_password(
            Cursor::new(data),
            "examples/data/sample",
            "pass".into(),
            |entry, reader, dest| {
                Box::pin(async move {
                    println!("start extract {}", entry.name());
                    let r = default_entry_extract_fn_async(entry, reader, dest).await;
                    println!("complete extract {}", entry.name());
                    r
                })
            },
        )
        .await
    })
    .expect("complete");
    println!("decompress done:{:?}", instant.elapsed());
}
