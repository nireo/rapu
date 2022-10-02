use crate::dal::{ByteString, DataAccessLayer, PageNum, PAGE_NUM_SIZE};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::io::Read;

pub struct Item {
    pub key: ByteString,
    pub value: ByteString,
}

pub struct Node {
    pub dal: Option<DataAccessLayer>,
    pub page_num: PageNum,
    pub items: Vec<Item>,
    pub children: Vec<PageNum>,
}

impl Item {
    pub fn new(key: ByteString, value: ByteString) -> Self {
        Self { key, value }
    }
}

impl Node {
    pub fn new() -> Self {
        Self {
            dal: None,
            page_num: 0,
            items: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.children.len() == 0
    }

    pub fn deserialize(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let mut rdr = Cursor::new(buf);
        let is_leaf = rdr.read_u8()?;
        let item_count = rdr.read_u16::<LittleEndian>()?;

        for _ in 0..item_count {
            if is_leaf == 0 {
                let pgnum = rdr.read_u64::<LittleEndian>()?;
                self.children.push(pgnum);
            }

            let mut offset = rdr.read_u16::<LittleEndian>()? as usize;
            let klen = buf[offset as usize] as usize;
            offset += 1;

            let key = buf[offset..(klen + offset)].to_vec();
            offset += klen;

            let vlen = buf[offset] as usize;
            offset += 1;

            let value = buf[offset..(vlen + offset)].to_vec();

            self.items.push(Item::new(key, value));
        }

        Ok(())
    }
}
