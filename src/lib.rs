//! `fingertips` creates an inverted index for a set of text files.
//!
//! Most of the actual work is done by the modules `index`, `read`, `write`,
//! and `merge`. In this file, `main.rs` we put the pieces together in two
//! different ways.
//!
//! *    `run_single_threaded` simply does everything in one thread, in the
//!      the most straightforward possible way.
//!
//! *    Then, we break the work into a five-stage pipeline so that we can run
//!      it on multiple CPUs. `run_pipeline` puts the five stages together.
//!
//! The `main` function at the end handles command-line arguments. It calls one
//! of the two functions above to do the work.

pub(crate) const HASH_LENGTH: usize = 32;

pub mod prelude {
    pub use crate::index::InMemoryIndex;
    pub use crate::index::ParsedIndex;
    pub use crate::merge::FileMerge;
    pub use crate::read::IndexFileReader;
    pub use crate::tmp::TmpDir;
    pub use crate::write::write_index_to_tmp_file;
}

// ───── Submodules ───────────────────────────────────────────────────────── //

pub mod index;
mod merge;
mod read;
mod tmp;
mod write;
