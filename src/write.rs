use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufWriter, SeekFrom};
use std::path::PathBuf;

use byteorder::{LittleEndian, WriteBytesExt};

// ───── Current Crate Imports ────────────────────────────────────────────── //

use crate::index::{Hit, InMemoryIndex};
use crate::tmp::TmpDir;

// ───── Body ─────────────────────────────────────────────────────────────── //

/// Writer for saving an index to a binary file.
///
/// The first 8 bytes of the index file contain the offset of the table of
/// contents, in bytes. Then come the main entries, all stored back-to-back
/// with no particular metadata.
pub struct IndexFileWriter {
    /// The number of bytes written so far.
    offset: u64,
    /// The open file we're writing to.
    writer: BufWriter<File>,
    /// The table of contents for this file.
    contents_buf: Vec<u8>,
}

impl IndexFileWriter {
    pub fn new(mut f: BufWriter<File>) -> io::Result<IndexFileWriter> {
        const HEADER_SIZE: u64 = 8;
        f.write_u64::<LittleEndian>(0)?;
        Ok(IndexFileWriter {
            offset: HEADER_SIZE,
            writer: f,
            contents_buf: vec![],
        })
    }

    pub fn write_data(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf)?;
        self.offset += buf.len() as u64;
        Ok(())
    }

    pub fn write_contents_entry(
        &mut self,
        term: String,
        doc_count: u32,
        offset: u64,
        nbytes: u64,
    ) {
        self.contents_buf.write_u64::<LittleEndian>(offset).unwrap();
        self.contents_buf.write_u64::<LittleEndian>(nbytes).unwrap();
        self.contents_buf
            .write_u32::<LittleEndian>(doc_count)
            .unwrap();
        let bytes = term.bytes();
        self.contents_buf
            .write_u32::<LittleEndian>(bytes.len() as u32)
            .unwrap();
        self.contents_buf.extend(bytes);
    }

    /// Finish writing the index file and close it
    pub fn finish(mut self) -> io::Result<()> {
        let table_contents_start = self.offset;
        self.writer.write_all(&self.contents_buf)?;
        println!(
            "{} bytes data, {}, bytes total",
            table_contents_start,
            table_contents_start + self.contents_buf.len() as u64
        );
        self.writer.seek(SeekFrom::Start(0))?;
        self.writer
            .write_u64::<LittleEndian>(table_contents_start)?;
        Ok(())
    }
}

pub fn write_index_to_tmp_file(
    index: InMemoryIndex,
    tmp_dir: &mut TmpDir,
) -> io::Result<PathBuf> {
    let (filename, f) = tmp_dir.create()?;
    let mut writer = IndexFileWriter::new(f)?;

    // The merge algorighm requires the entries within each file to be
    // sorted by term. Sort before writing anything.
    let mut index_as_vec: Vec<(String, Vec<Hit>)> =
        index.map.into_iter().collect();
    index_as_vec.sort_by(|&(ref a, _), &(ref b, _)| a.cmp(b));

    for (term, hits) in index_as_vec {
        let doc_count = hits.len() as u32;
        let start = writer.offset;
        for buffer in hits {
            writer.write_data(&buffer)?;
        }
        let stop = writer.offset;
        writer.write_contents_entry(term, doc_count, start, stop - start);
    }

    writer.finish()?;
    println!("Wrote file {:?}", filename);
    Ok(filename)
}
