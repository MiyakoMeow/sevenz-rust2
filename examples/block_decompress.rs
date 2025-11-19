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
    let mut cursor = std::io::Cursor::new(data);
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

        forder_dec
            .for_each_entries(&mut |entry, reader| {
                if entry.name() == my_file_name {
                    //only extract the file we want
                    let dest = dest.join(entry.name());
                    sevenz_rust2::default_entry_extract_fn_async(entry, reader, &dest)?;
                } else {
                    //skip other files
                    std::io::copy(reader, &mut std::io::sink())?;
                }
                Ok(true)
            })
            .expect("ok");
    }
}
