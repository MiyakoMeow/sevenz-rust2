use std::{env, time::Instant};

use async_fs as afs;
use sevenz_rust2::{ArchiveReader, ArchiveWriter, Password};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!(
            "Usage: {} [--solid] [-o output.7z] <file1> [file2] ...",
            args[0]
        );
        eprintln!("  --solid: Create a solid archive (all files compressed together)");
        eprintln!("  -o <filename>: Specify output filename (default: output.7z)");
        std::process::exit(1);
    }

    let mut solid = false;
    let mut output_path = String::from("output.7z");
    let mut file_paths = Vec::new();
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "--solid" => {
                solid = true;
                i += 1;
            }
            "-o" => {
                if i + 1 >= args.len() {
                    eprintln!("Error: -o option requires an output filename");
                    std::process::exit(1);
                }
                output_path = args[i + 1].clone();
                i += 2;
            }
            _ => {
                file_paths.push(args[i].clone());
                i += 1;
            }
        }
    }

    if file_paths.is_empty() {
        eprintln!("Error: No files specified");
        std::process::exit(1);
    }

    println!(
        "Creating {} archive: {output_path}",
        if solid { "solid" } else { "non-solid" }
    );

    let now = Instant::now();

    let mut writer = smol::block_on(ArchiveWriter::new(futures::io::Cursor::new(
        Vec::<u8>::new(),
    )))
    .unwrap_or_else(|error| panic!("Failed to create archive '{output_path}': {error}"));

    if solid {
        for file_path in &file_paths {
            smol::block_on(writer.push_source_path(file_path, |_| async { true }))
                .expect("Failed to push source path");
            println!("Added path: {file_path}");
        }
    } else {
        for file_path in &file_paths {
            smol::block_on(writer.push_source_path_non_solid(file_path, |_| async { true }))
                .expect("Failed to push source path");
            println!("Added path: {file_path}");
        }
    }

    let cursor = smol::block_on(writer.finish()).expect("Failed to finalize archive");
    let data = cursor.into_inner();
    smol::block_on(afs::write(&output_path, data))
        .unwrap_or_else(|error| panic!("Failed to write output file '{output_path}': {error}"));

    let _archive_reader =
        smol::block_on(ArchiveReader::open_async(&output_path, Password::empty()))
            .unwrap_or_else(|error| panic!("Failed to open output file '{output_path}': {error}"));

    println!("Archive created: {output_path}");
    println!("Compress done: {:?}", now.elapsed());
}
