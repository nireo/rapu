use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::path::Path;

type PageNum = u64;
type ByteString = Vec<u8>;

struct Page {
    num: PageNum,
    data: ByteString,
}

struct DataAccessLayer {
    file: File,
    page_size: usize,
}

impl DataAccessLayer {
    pub fn new(path: &Path, page_size: usize) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Ok(Self { file, page_size })
    }

    fn allocate_empty_page(&self) -> Page {
        Page {
            data: ByteString::with_capacity(self.page_size),
            num: 0,
        }
    }

    fn read_page(&mut self, page_num: PageNum) -> std::io::Result<Page> {
        let mut p = self.allocate_empty_page();
        let offset = (page_num as usize) * self.page_size;

        let mut f = BufReader::new(&mut self.file);
        f.seek(SeekFrom::Start(offset as u64))?;

        f.take(self.page_size as u64).read_to_end(&mut p.data)?;

        Ok(p)
    }

    fn write_page(&mut self, p: &Page) -> std::io::Result<()> {
        let offset = (p.num as usize) * self.page_size;
        let mut f = BufWriter::new(&mut self.file);
        f.seek(SeekFrom::Start(offset as u64))?;
        f.write_all(&p.data)?;

        Ok(())
    }
}
