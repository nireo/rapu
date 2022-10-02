use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use std::io::Cursor;

type PageNum = u64;
type ByteString = Vec<u8>;

const META_PAGE_NUM: PageNum = 0;
const PAGE_NUM_SIZE: usize = 8; // page number size in bytes

struct Meta {
    freelist_page: PageNum,
}

impl Meta {
    pub fn new() -> Self {
        Self { freelist_page: 0 }
    }

    pub fn serialize(&self, vc: &mut Vec<u8>) -> std::io::Result<()> {
        vc.write_u64::<LittleEndian>(self.freelist_page)
    }

    pub fn deserialize(&mut self, vc: &[u8]) -> std::io::Result<()> {
        let mut rdr = Cursor::new(vc);
        self.freelist_page = rdr.read_u64::<LittleEndian>()?;

        Ok(())
    }
}

pub struct Freelist {
    max_page: PageNum,

    // prefer using already allocated pages, to prevent allocating uncessary memory.
    released_pages: Vec<PageNum>,
}

impl Freelist {
    pub fn new() -> Self {
        Self {
            max_page: 0,
            released_pages: Vec::new(),
        }
    }

    pub fn next_page(&mut self) -> PageNum {
        if self.released_pages.len() != 0 {
            // we can unwrap, since we know that the lenght is more than 0
            self.released_pages.pop().unwrap()
        } else {
            self.max_page
        }
    }

    pub fn release_page(&mut self, page: PageNum) {
        self.released_pages.push(page)
    }
}

struct Page {
    num: PageNum,
    data: ByteString,
}

struct DataAccessLayer {
    file: File,
    page_size: usize,
    freelist: Freelist,
}

impl DataAccessLayer {
    pub fn new(path: &Path, page_size: usize) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Ok(Self {
            file,
            page_size,
            freelist: Freelist::new(),
        })
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

    fn write_meta(&mut self, meta: &Meta) -> std::io::Result<Page> {
        let mut meta_bytes: Vec<u8> = Vec::new();
        meta.serialize(&mut meta_bytes)?;

        let mut pg = self.allocate_empty_page();
        for i in 0..meta_bytes.len() {
            pg.data[i] = meta_bytes[i].clone()
        }

        self.write_page(&pg)?;
        Ok(pg)
    }

    fn read_meta(&mut self) -> std::io::Result<Meta> {
        let pg = self.read_page(META_PAGE_NUM)?;
        let mut meta = Meta::new();
        meta.deserialize(&pg.data)?;

        Ok(meta)
    }
}
