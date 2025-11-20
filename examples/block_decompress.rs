use async_fs as afs;
use std::path::PathBuf;

use sevenz_rust2::{Archive, BlockDecoder, Password};

fn main() {
    let password = Password::empty();
    let archive = smol::block_on(Archive::open_with_password_async(
        "examples/data/sample.7z",
        &password,
    ))
    .unwrap();
    let data = smol::block_on(afs::read("examples/data/sample.7z")).unwrap();
    let mut cursor = futures::io::Cursor::new(data);
    let block_count = archive.blocks.len();
    let my_file_name = "7zFormat.txt";

    for block_index in 0..block_count {
        let forder_dec = BlockDecoder::new(1, block_index, &archive, &password, &mut cursor);

        if !forder_dec
            .entries()
            .iter()
            .any(|entry| entry.name() == my_file_name)
        {
            // skip the folder if it does not contain the file we want
            continue;
        }
        let dest = PathBuf::from("examples/data/sample_mt/");

        smol::block_on(forder_dec.for_each_entries_async(&mut |entry, reader| {
            if entry.name() == my_file_name {
                let dest = dest.join(entry.name());
                Box::pin(async move {
                    sevenz_rust2::default_entry_extract_fn_async(entry, reader, &dest).await?;
                    Ok(true)
                })
            } else {
                Box::pin(async move {
                    let mut buf = Vec::new();
                    futures::io::AsyncReadExt::read_to_end(reader, &mut buf).await?;
                    Ok(true)
                })
            }
        }))
        .expect("ok");
    }
}
