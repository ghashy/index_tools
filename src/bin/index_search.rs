// ───── Current Crate Imports ────────────────────────────────────────────── //

use std::{
    collections::HashMap,
    fs::{self, File},
    io::prelude::*,
};

use clap::Parser;
use fingertips::prelude::*;
use ring::digest::{Context, SHA256};

// ───── Body ─────────────────────────────────────────────────────────────── //

/// Search terms in index.dat file.
#[derive(Default, Parser, Debug)]
#[clap(version, about)]
struct Arguments {
    /// Terms to search in index divided by space symbol.
    #[clap(short, long, value_parser, num_args = 1.., value_delimiter = ' ')]
    terms: Vec<String>,
    /// Path to directory with documents.
    #[clap(short, long)]
    doc_dir: String,
    /// Path to index.dat file.
    #[clap(short, long)]
    index_file: String,
}

fn run(args: Arguments) -> std::io::Result<()> {
    let index = IndexFileReader::get_index_from_file(args.index_file)?;

    // Collect all files paths and hashes
    let paths = fs::read_dir(args.doc_dir)?;
    let mut files = HashMap::new();
    for path in paths.into_iter().flatten() {
        let mut f = File::open(path.path())?;
        let mut text = String::new();
        match f.read_to_string(&mut text) {
            Ok(_) => {}
            Err(_) => {
                continue;
            }
        }

        // Hashing
        let mut context = Context::new(&SHA256);
        context.update(text.as_bytes());
        let digest = context.finish();
        let hash = digest.as_ref(); // has 32 bytes length

        files.insert(
            Vec::from(&hash[..]),
            path.file_name().into_string().unwrap(),
        );
    }

    display(files, index, args.terms);

    Ok(())
}

fn display(
    files: HashMap<Vec<u8>, String>,
    index: ParsedIndex,
    terms: Vec<String>,
) {
    println!("Word count in entire index: {}\n", index.word_count);
    for term in terms {
        let term_lower = term.to_lowercase();
        if let Some(entry) = index.map.get(&term_lower) {
            println!(
                "Term \"{}\" was found in {} documents:",
                term,
                entry.len()
            );

            for (doc, offsets) in entry {
                println!(
                    "\t Document: {}",
                    files.get(&doc.hash).unwrap_or(&"Unknown".to_string())
                );
                for offset in offsets {
                    println!("\t Offset: {}", offset);
                }
            }
        }
    }
}

fn main() {
    let args = Arguments::parse();
    match run(args) {
        Ok(_) => {}
        Err(e) => println!("Error: {}", e),
    }
}
