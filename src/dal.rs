use crate::node::Node;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::Cursor;
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::path::Path;

pub type PageNum = u64;
pub type ByteString = Vec<u8>;

const META_PAGE_NUM: PageNum = 0;
pub const PAGE_NUM_SIZE: usize = 8; // page number size in bytes

#[derive(Clone)]
pub struct Meta {
    pub freelist_page: PageNum,
    pub root: PageNum,
}

impl Meta {
    pub fn new() -> Self {
        Self {
            freelist_page: 0,
            root: 0,
        }
    }

    pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
        let mut vc = Vec::new();
        vc.write_u64::<LittleEndian>(self.root)?;
        vc.write_u64::<LittleEndian>(self.freelist_page)?;

        Ok(vc)
    }

    pub fn deserialize(&mut self, vc: &[u8]) -> std::io::Result<()> {
        let mut rdr = Cursor::new(vc);
        self.root = rdr.read_u64::<LittleEndian>()?;
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
            self.max_page += 1;
            self.max_page
        }
    }

    pub fn release_page(&mut self, page: PageNum) {
        self.released_pages.push(page)
    }

    pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
        let mut data = Vec::new();
        data.write_u16::<LittleEndian>(self.max_page as u16)?;
        data.write_u16::<LittleEndian>(self.released_pages.len() as u16)?;

        for page in self.released_pages.iter() {
            data.write_u64::<LittleEndian>(*page)?;
        }

        Ok(data)
    }

    pub fn deserialize(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let mut rdr = Cursor::new(buf);
        self.max_page = rdr.read_u16::<LittleEndian>()? as u64;
        let page_count = rdr.read_u16::<LittleEndian>()?;

        for _ in 0..page_count {
            self.released_pages.push(rdr.read_u64::<LittleEndian>()?);
        }

        Ok(())
    }
}

pub struct DataAccessLayerConfig {
    page_size: usize,
    min_fill_percent: f32,
    max_fill_percent: f32,
}

impl DataAccessLayerConfig {
    pub fn default() -> Self {
        Self {
            page_size: 4096,
            min_fill_percent: 0.5,
            max_fill_percent: 0.95,
        }
    }
}

pub struct Page {
    pub num: PageNum,
    pub data: ByteString,
}

pub struct DataAccessLayer {
    pub file: File,
    pub page_size: usize,
    pub freelist: Freelist,
    pub meta: Meta,
    pub min_fill_percent: f32,
    pub max_fill_percent: f32,
}

impl DataAccessLayer {
    pub fn new(path: &Path, options: &DataAccessLayerConfig) -> std::io::Result<Self> {
        if path.exists() {
            let file = OpenOptions::new().read(true).write(true).open(path)?;

            let mut dal = Self {
                file,
                page_size: options.page_size,
                min_fill_percent: options.min_fill_percent,
                max_fill_percent: options.max_fill_percent,
                freelist: Freelist::new(),
                meta: Meta::new(),
            };
            dal.meta = dal.read_meta()?;
            dal.freelist = dal.read_freelist()?;

            Ok(dal)
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;

            let mut dal = Self {
                file,
                page_size,
                freelist: Freelist::new(),
                meta: Meta::new(),
            };
            dal.meta.freelist_page = dal.freelist.next_page();
            let meta_clone = dal.meta.clone();
            dal.write_meta(&meta_clone)?;

            Ok(dal)
        }
    }

    pub fn allocate_empty_page(&self) -> Page {
        Page {
            data: vec![0; self.page_size],
            num: 0,
        }
    }

    pub fn read_page(&self, page_num: PageNum) -> std::io::Result<Page> {
        let mut p = self.allocate_empty_page();
        let offset = (page_num as usize) * self.page_size;

        let mut f = BufReader::new(&self.file);
        f.seek(SeekFrom::Start(offset as u64))?;

        f.take(self.page_size as u64).read_to_end(&mut p.data)?;

        Ok(p)
    }

    pub fn write_page(&self, p: &Page) -> std::io::Result<()> {
        let offset = (p.num as usize) * self.page_size;
        let mut f = BufWriter::new(&self.file);
        f.seek(SeekFrom::Start(offset as u64))?;
        f.write_all(&p.data)?;

        Ok(())
    }

    pub fn write_meta(&mut self, meta: &Meta) -> std::io::Result<Page> {
        let meta_bytes = meta.serialize()?;

        let mut pg = self.allocate_empty_page();
        for i in 0..meta_bytes.len() {
            pg.data[i] = meta_bytes[i].clone()
        }

        self.write_page(&pg)?;
        Ok(pg)
    }

    pub fn read_meta(&mut self) -> std::io::Result<Meta> {
        let pg = self.read_page(META_PAGE_NUM)?;
        let mut meta = Meta::new();
        meta.deserialize(&pg.data)?;

        Ok(meta)
    }

    pub fn write_freelist(&mut self) -> std::io::Result<Page> {
        let mut pg = self.allocate_empty_page();
        pg.num = self.meta.freelist_page;
        let buf = self.freelist.serialize()?;

        for i in 0..buf.len() {
            pg.data[i] = buf[i].clone();
        }
        self.write_page(&pg)?;
        self.meta.freelist_page = pg.num;

        Ok(pg)
    }

    pub fn read_freelist(&mut self) -> std::io::Result<Freelist> {
        let pg = self.read_page(self.meta.freelist_page)?;
        let mut freelist = Freelist::new();
        freelist.deserialize(&pg.data)?;

        Ok(freelist)
    }

    pub fn write_node(&mut self, node: &Node) -> std::io::Result<PageNum> {
        let mut pg = self.allocate_empty_page();
        if node.page_num == 0 {
            pg.num = self.freelist.next_page();
        } else {
            pg.num = node.page_num;
        }
        node.serialize(&mut pg.data)?;

        self.write_page(&pg)?;
        Ok(pg.num)
    }

    pub fn delete_node(&mut self, pgnum: PageNum) {
        self.freelist.release_page(pgnum);
    }

    pub fn get_node(&self, pgnum: PageNum) -> std::io::Result<Node> {
        let pg = self.read_page(pgnum)?;
        let mut node = Node::new();
        node.deserialize(&pg.data)?;
        node.page_num = pgnum;

        Ok(node)
    }
}
