use std::time::Instant;

use async_fs as afs;
use sevenz_rust2::default_entry_extract_fn_async;

fn main() {
    let instant = Instant::now();
    smol::block_on(async {
        let data = afs::read("examples/data/sample.7z").await.unwrap();
        sevenz_rust2::decompress_with_extract_fn_and_password(
            std::io::Cursor::new(data),
            "examples/data/sample",
            "pass".into(),
            |entry, reader, dest| {
                println!("start extract {}", entry.name());
                let r = default_entry_extract_fn_async(entry, reader, dest);
                println!("complete extract {}", entry.name());
                r
            },
        )
        .await
    })
    .expect("complete");
    println!("decompress done:{:?}", instant.elapsed());
}
