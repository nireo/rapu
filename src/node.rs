use crate::dal::{ByteString, DataAccessLayer, PageNum, PAGE_NUM_SIZE};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::io::Read;
use std::io::{Error, ErrorKind};

pub struct Item {
    pub key: ByteString,
    pub value: ByteString,
}

pub struct Node {
    pub dal: Option<Box<DataAccessLayer>>,
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

        if is_leaf == 0 {
            self.children.push(rdr.read_u64::<LittleEndian>()?);
        }

        Ok(())
    }

    fn key_in_node(&self, key: &[u8]) -> (bool, usize) {
        for (i, item) in self.items.iter().enumerate() {
            if &*item.key == key {
                return (true, i);
            }

            if &*item.key > key {
                return (false, i);
            }
        }

        (false, self.items.len())
    }

    fn find_key_helper(n: &Node, key: &[u8], dal: &DataAccessLayer) -> std::io::Result<(usize, Node)> {
        let (found, idx) = n.key_in_node(key);
        if found {}

        if n.is_leaf() {
            return Err(Error::new(ErrorKind::Other, "node is leaf."));
        }

        let next = dal.get_node(n.children[idx])?;
        Node::find_key_helper(&next, key, dal)
    }

    pub fn find_key(&self, key: &[u8], dal: &DataAccessLayer) -> std::io::Result<()> {
        Ok(())
    }

    pub fn element_size(&self, i: usize) -> usize {
        self.items[i].key.len() + self.items[i].value.len() + PAGE_NUM_SIZE
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        size += 3;
        for n in self.children.iter() {
            size += self.element_size(n.to_owned() as usize);
        }
        size + PAGE_NUM_SIZE
    }
}