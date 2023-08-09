use std::fs::{self, File};
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};

// ───── Body ─────────────────────────────────────────────────────────────── //

#[derive(Clone)]
pub struct TmpDir {
    dir: PathBuf,
    n: usize,
}

impl TmpDir {
    pub fn new<P: AsRef<Path>>(dir: P) -> TmpDir {
        TmpDir {
            dir: dir.as_ref().to_path_buf(),
            n: 1,
        }
    }

    pub fn create(&mut self) -> io::Result<(PathBuf, BufWriter<File>)> {
        let mut retry = 1;
        loop {
            let filename = self
                .dir
                .join(PathBuf::from(format!("tmp{:08x}.dat", self.n)));
            self.n += 1;
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&filename)
            {
                Ok(f) => return Ok((filename, BufWriter::new(f))),
                Err(e) => {
                    if retry < 10 && e.kind() == io::ErrorKind::AlreadyExists {
                        // keep going
                    } else {
                        return Err(e);
                    }
                }
            }
            retry += 1;
        }
    }
}
