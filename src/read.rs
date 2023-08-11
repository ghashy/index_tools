//! Reading index files linearly from disk, a capability needed for merging
//! index files.

use byteorder::{LittleEndian, ReadBytesExt};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{self, BufReader, SeekFrom};
use std::path::Path;

// ───── Current Crate Imports ────────────────────────────────────────────── //

use crate::index::{Doc, Hit, Offsets};
use crate::prelude::{InMemoryIndex, ParsedIndex};
use crate::write::IndexFileWriter;
use crate::HASH_LENGTH;

// ───── Body ─────────────────────────────────────────────────────────────── //

/// An `IndexFileReader` does a single linear pass over an index file from
/// beginning to end. Needless to say, this is not how an index is normally
/// used! It is used only when merging multiple index files.
///
/// The only way to advance through the file is to use the `.move_entry_to()`
/// method.
#[derive(Debug)]
pub struct IndexFileReader {
    /// Reader that reads the actual index data.
    ///
    /// We have two readers. The index data is most of the file. There's also a
    /// table of contents, stored separately at the end. We have to read them
    /// in tandem, so we open the file twice.
    data: BufReader<File>,
    /// Reader that reads the table of contents. (Since this table is stored at
    /// the end of the file, we have to begin by `seek`ing to it; see the code
    /// in `IndexFileReader::open_and_delete`).
    table_of_contents: BufReader<File>,
    /// The next entry in the table of contents, if any; or `None` if we've
    /// reached the end of the table. `IndexFileReader` always reads ahead one
    /// entry in the contents and stores it here.
    next: Option<Entry>,
}

/// An entry in the table of contents of an index file.
///
/// Each entry in the table of contents is small. It consists of a string, the
/// `term`; summary information about that term, as used in the corpus (`df`);
/// and a pointer to bulkier data that tells more (`offset` and `nbytes`).
#[derive(Debug)]
pub struct Entry {
    /// The term is a word that appears in one or more documents in the corpus.
    /// The index file contains information about the documents that use this
    /// word.
    pub term: String,
    /// Total number of documents in the corpus that contain this term.
    pub doc_count: u32,
    /// Offset of the index data for this term from the beginning of the file,
    /// in bytes.
    pub offset: u64,
    /// Length of the index data for this term, in bytes.
    pub nbytes: u64,
}

impl IndexFileReader {
    /// Open an index file to read it from beginning to end.
    ///
    /// This deletes the file, which may not work properly on Windows. Patches
    /// welcome! On Unix, it works like this: the file immediately disappears
    /// from its directory, but it'll still take up space on disk until the
    /// file is closed, which normally happens when the `IndexFileReader` is
    /// dropped.
    pub fn open_and_delete<P: AsRef<Path>>(
        filename: P,
    ) -> io::Result<IndexFileReader> {
        let filename = filename.as_ref();
        let mut data_raw = File::open(filename)?;

        // Read the file header.
        let table_contents_offset = data_raw.read_u64::<LittleEndian>()?;
        println!(
            "Opened {}, table of contents starts at {}",
            filename.display(),
            table_contents_offset
        );

        // Open again so we have two read heads;
        // move the contents read head to its starting position.
        // Set up buffering.
        let mut table_contents_raw = File::open(filename)?;
        table_contents_raw.seek(SeekFrom::Start(table_contents_offset))?;
        let data = BufReader::new(data_raw);
        let mut table = BufReader::new(table_contents_raw);

        // We always read ahead one entry, so load the first entry right away.
        let first = IndexFileReader::read_entry(&mut table)?;

        println!("Removing file: {}", filename.display());
        fs::remove_file(filename)?; // YOLO

        Ok(IndexFileReader {
            data,
            table_of_contents: table,
            next: first,
        })
    }

    /// Read and parse index from binary file to a user-friendly format.
    pub fn get_index_from_file<P: AsRef<Path>>(
        filename: P,
    ) -> io::Result<ParsedIndex> {
        let filename = filename.as_ref();
        let mut f = File::open(filename)?;

        // Read the file header.
        let table_contents_offset = f.read_u64::<LittleEndian>()?;
        println!(
            "Opened {}, table of contents starts at {}",
            filename.display(),
            table_contents_offset
        );

        // Open again so we have two read heads;
        // move the contents read head to its starting position.
        // Set up buffering.
        let mut table_contents_raw = File::open(filename)?;
        table_contents_raw.seek(SeekFrom::Start(table_contents_offset))?;

        // Data - reader over beginning of the index, 8 bytes skipped (u64 with
        // offset information).
        // Table - table of contents section.
        let mut data = BufReader::new(f);
        let mut table = BufReader::new(table_contents_raw);

        // It will be our `HashMap` with term : DocEntry pairs.
        let mut map = HashMap::new();
        let mut word_count = 0;

        loop {
            // Offset from beginning of the binary file.
            let offset = match table.read_u64::<LittleEndian>() {
                Ok(v) => v,
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        panic!("Wrong table format");
                    }
                }
            };
            // Length in bytes of our term's data.
            let nbytes = table.read_u64::<LittleEndian>()?;
            // Amount of documents where our term occurs.
            let doc_count = table.read_u32::<LittleEndian>()?;
            // Length of term in bytes
            let term_length = table.read_u32::<LittleEndian>()?;
            // Get term
            let mut term = vec![0; term_length as usize];
            table.read_exact(&mut term)?;
            let term = String::from_utf8(term).unwrap();

            word_count += 1;

            // Seek to our term's data first byte
            data.seek(SeekFrom::Start(offset))?;

            let mut hits_raw = vec![0; nbytes as usize];
            data.read_exact(&mut hits_raw)?;

            // This entry is multiple docs and offsets which corresponds to
            // one term.
            let mut entry: HashMap<Doc, Offsets> = HashMap::new();

            let reader = &mut hits_raw[..].as_ref();

            for _ in 0..doc_count {
                // Firsly we read hash, and create `Doc` object.
                let hash = &mut [0; HASH_LENGTH];
                reader.read_exact(&mut hash[..])?;
                let doc = Doc::new(&hash[..]);

                // How much offsets in this document existing.
                let offsets_count = reader.read_u32::<LittleEndian>()?;
                let mut offsets = vec![];

                // Read all offsets.
                for _ in 0..offsets_count {
                    let word_offset = reader.read_u32::<LittleEndian>()?;
                    offsets.push(word_offset);
                }
                // Push doc and offsets to entry
                entry.insert(doc, offsets);
            }
            // Insert entry for term
            map.insert(term, entry);
        }

        Ok(ParsedIndex { word_count, map })
    }

    /// Borrow a reference to the next entry in the table of contents.
    /// (Since we always read ahead one entry, this method can't fail).
    ///
    /// Returns `None` if we've reached the end of the file.
    pub fn peek(&self) -> Option<&Entry> {
        self.next.as_ref()
    }

    /// True if the next entry is for the given term.
    pub fn is_at(&self, term: &str) -> bool {
        match self.next {
            Some(ref e) => e.term == term,
            None => false,
        }
    }

    pub fn move_entry_to(
        &mut self,
        out: &mut IndexFileWriter,
    ) -> io::Result<()> {
        // This block limits the scope of borrowing `self.next` (for`e`),
        // because after this block is over we'll want to assign to `self.next`.
        {
            let e = self.next.as_ref().expect("no entry to move");
            if e.nbytes > usize::MAX as u64 {
                // This can only happen on 32-bit platforms.
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Computer's archutecture do not
                    allow to hold such big index entry",
                ));
            }
            let mut buf = Vec::with_capacity(e.nbytes as usize);
            buf.resize(e.nbytes as usize, 0);
            self.data.read_exact(&mut buf)?;
            out.write_data(&buf)?;
        }

        self.next = Self::read_entry(&mut self.table_of_contents)?;
        Ok(())
    }
}

impl IndexFileReader {
    /// Read the next entry from the table of contents.
    ///
    /// Returns `Ok(None)` if we have reached the end of the file.
    fn read_entry(f: &mut BufReader<File>) -> io::Result<Option<Entry>> {
        // If the first read here fails with `Undexpected Eof`,
        // that's considered a success, with no entry read.
        let offset = match f.read_u64::<LittleEndian>() {
            Ok(value) => value,
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                } else {
                    return Err(e);
                }
            }
        };

        let nbytes = f.read_u64::<LittleEndian>()?;
        let doc_count = f.read_u32::<LittleEndian>()?;
        let term_len = f.read_u32::<LittleEndian>()? as usize;
        let mut bytes = Vec::with_capacity(term_len);
        bytes.resize(term_len, 0);
        f.read_exact(&mut bytes)?;
        let term = match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unicode fail",
                ))
            }
        };

        Ok(Some(Entry {
            term,
            doc_count,
            offset,
            nbytes,
        }))
    }
}
