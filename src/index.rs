//! In-memory indexes.
//!
//! The first step in building the index is to index documents in memory.
//! `InMemoryIndex` can be used to do that, up to the size of the machine's
//! memory.

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::{collections::HashMap, path::PathBuf};

use crate::HASH_LENGTH;

// ───── Body ─────────────────────────────────────────────────────────────── //

/// Break a string into words.
fn tokenize(text: &str) -> Vec<&str> {
    text.split(|ch: char| !ch.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect()
}

/// A `Hit` indicates that a particular document contains some term, how many
/// times it appears, and at what offsets (that is, the word count, from the
/// beginning of the document, of each place where the term appears).
///
/// The buffer contains all the hit data in binary form, little-endian. The
/// first u32 of the data is the document id. The remaining [u32] are offsets.
pub type Hit = Vec<u8>;

/// An in-memory index.
///
/// Of course, a real index for a large corpus of documets wont' fit in memory.
/// But apart from memory constraints, this is everything you need to answer
/// simple search queries. And you can use the `read`, `write` and `merge`
/// modules to save an in-memory index to disk and merge it with other indices,
/// producing a large index.
#[derive(Debug)]
pub struct InMemoryIndex {
    /// The total number of words in the indexed documents.
    pub word_count: usize,
    /// For every term that appears in the index, the list of all search hits
    /// for that term (i.e. which documents contain that term, and where).
    ///
    /// It's possible for an index to be "sorted by document id", which means
    /// that for every `Vec<Hit>` in this map, the `Hit` elements all have
    /// distinct document ids (the first u32) and the `Hit`s are arranged by
    /// document id in increasing order. This is handy for some algorithms you
    /// might want to run on the index, so we preserve this property wherever
    /// possible.
    pub map: HashMap<String, Vec<Hit>>,
}

impl InMemoryIndex {
    /// Create a new, empty index.
    pub fn new() -> InMemoryIndex {
        InMemoryIndex {
            word_count: 0,
            map: HashMap::new(),
        }
    }

    /// Index a single document.
    ///
    /// The resulting index contains exactly on one `Hit` per term.
    pub fn from_single_document(
        document_hash: &[u8],
        text: String,
    ) -> InMemoryIndex {
        let mut index = InMemoryIndex::new();

        let text = text.to_lowercase();
        let tokens = tokenize(&text);
        for (i, token) in tokens.iter().enumerate() {
            let vec_with_hits =
                index.map.entry(token.to_string()).or_insert_with(|| {
                    let mut hits = Vec::with_capacity(4 + 4); // 4 bytes + 4 bytes; u32 is 4 bytes
                                                              // document_hash has length of 32 bytes
                    for byte in document_hash {
                        hits.write_u8(*byte).unwrap(); // Write doc hash to hit
                    }
                    // Write place for offsets count
                    hits.write_u32::<LittleEndian>(0).unwrap();
                    vec![hits]
                });
            vec_with_hits[0]
                .write_u32::<LittleEndian>(i as u32) // Write word offset to hit
                .unwrap();

            // Update offsets count
            let offsets_count = (&vec_with_hits[0]
                [HASH_LENGTH..HASH_LENGTH + 4])
                .read_u32::<LittleEndian>()
                .unwrap()
                + 1;
            let offsets_count = offsets_count.to_le_bytes();
            for (idx, byte) in vec_with_hits[0][HASH_LENGTH..HASH_LENGTH + 4]
                .iter_mut()
                .enumerate()
            {
                *byte = offsets_count[idx];
            }

            index.word_count += 1;
        }
        index
    }

    /// Add all search hits from `other` to this index.
    ///
    /// If both `*self` and `other` are sorted by document id, and all document
    /// ids in `other` are greater than every document id in `*self`, then
    /// `*self` remain sorted by document id after merging.
    pub fn merge(&mut self, other: InMemoryIndex) {
        for (term, hits) in other.map {
            self.map.entry(term).or_insert_with(|| vec![]).extend(hits);
        }
        self.word_count += other.word_count
    }

    /// True if this index contains no data.
    pub fn is_empty(&self) -> bool {
        self.word_count == 0
    }

    /// True if this index is large enough that we should dump it to disk
    /// rather than keep adding more data to it.
    pub fn is_large(&self) -> bool {
        //This depends on how much memory your computer has, of course.
        const REASONABLE_SIZE: usize = 100_000_000;
        self.word_count > REASONABLE_SIZE
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct Doc {
    pub hash: Vec<u8>,
}

impl std::hash::Hash for Doc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for n in &self.hash {
            state.write_u8(*n);
        }
    }
}

impl Doc {
    pub fn new(hash: &[u8]) -> Self {
        Doc { hash: hash.into() }
    }
}

pub type Offsets = Vec<u32>;

pub type DocEntry = HashMap<Doc, Offsets>;

#[derive(Debug)]
pub struct ParsedIndex {
    pub word_count: usize,
    pub map: HashMap<String, DocEntry>,
}
