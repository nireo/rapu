use crate::dal::{ByteString, DataAccessLayer, PageNum, PAGE_NUM_SIZE};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cell::RefCell;
use std::io::Cursor;
use std::io::Read;
use std::io::{Error, ErrorKind};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct Item {
    pub key: ByteString,
    pub value: ByteString,
}

#[derive(Debug, Clone)]
pub struct Node {
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

    fn find_key_helper(
        n: &Node,
        key: &[u8],
        dal: &DataAccessLayer,
        ancestor_idxs: &mut Vec<usize>,
    ) -> std::io::Result<(usize, Node)> {
        let (found, idx) = n.key_in_node(key);
        if found {
            return Ok((idx, n.to_owned()));
        }

        if n.is_leaf() {
            return Err(Error::new(ErrorKind::Other, "not found"))
        }

        ancestor_idxs.push(idx);
        let next = dal.get_node(n.children[idx])?;
        Node::find_key_helper(&next, key, dal, ancestor_idxs)
    }

    pub fn find_key(
        &self,
        key: &[u8],
        dal: &DataAccessLayer,
    ) -> std::io::Result<(usize, Node, Vec<usize>)> {
        let mut ancestor_idxs = vec![0usize];
        match Node::find_key_helper(self, key, dal, &mut ancestor_idxs) {
            Ok((idx, node)) => Ok((idx, node, ancestor_idxs)),
            Err(e) => Err(e),
        }
    }

    pub fn element_size(&self, i: usize) -> usize {
        self.items[i].key.len() + self.items[i].value.len() + PAGE_NUM_SIZE
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        size += 3;
        for n in 0..self.items.len() {
            size += self.element_size(n);
        }
        size + PAGE_NUM_SIZE
    }

    pub fn serialize(&self, page_buffer: &mut [u8]) -> std::io::Result<()> {
        let mut left_pos = 0;
        let mut right_pos = page_buffer.len() - 1;

        let mut bitsetvar: u8 = 0;
        if self.is_leaf() {
            bitsetvar = 1;
        }
        page_buffer[left_pos] = bitsetvar;
        left_pos += 1;

        LittleEndian::write_u16(
            &mut page_buffer[left_pos..left_pos + 2],
            self.items.len() as u16,
        );
        left_pos += 2;

        for idx in 0..self.items.len() {
            if !self.is_leaf() {
                LittleEndian::write_u64(
                    &mut page_buffer[left_pos..left_pos + PAGE_NUM_SIZE],
                    self.children[idx],
                );
                left_pos += PAGE_NUM_SIZE;
            }

            let klen = self.items[idx].key.len();
            let vlen = self.items[idx].value.len();

            LittleEndian::write_u16(
                &mut page_buffer[left_pos..left_pos + 2],
                (right_pos - klen - vlen - 2) as u16,
            );
            left_pos += 2;

            right_pos -= vlen;
            page_buffer[right_pos..right_pos + vlen].clone_from_slice(&self.items[idx].value);
            right_pos -= 1;
            page_buffer[right_pos] = vlen as u8;

            right_pos -= klen;
            page_buffer[right_pos..right_pos + klen].clone_from_slice(&self.items[idx].key);
            right_pos -= 1;
            page_buffer[right_pos] = klen as u8;
        }

        if !self.is_leaf() {
            LittleEndian::write_u64(
                &mut page_buffer[left_pos..left_pos + PAGE_NUM_SIZE],
                *self.children.last().unwrap(),
            );
        }

        Ok(())
    }

    pub fn add_item(&mut self, item: Item, insertion_idx: usize) -> usize {
        if self.items.len() == insertion_idx {
            self.items.push(item);
            return insertion_idx;
        }

        self.items[insertion_idx] = item;
        insertion_idx
    }

    pub fn write_node(&mut self, dal: &mut DataAccessLayer) -> std::io::Result<()> {
        let pgnum = dal.write_node(self)?;
        if self.page_num == 0 {
            self.page_num = pgnum;
        }

        Ok(())
    }

    // TODO: make this a bit smarter sometime for now it just works.
    pub fn split(
        &mut self,
        to_split: &mut Node,
        n_to_split_idx: usize,
        dal: &mut DataAccessLayer,
    ) -> std::io::Result<()> {
        let split_idx = dal.get_split_index(to_split)?;
        let middle_item = to_split.items[split_idx].clone();

        let mut node = if to_split.is_leaf() {
            let mut new_node = dal.new_node(to_split.items[(split_idx + 1)..].to_vec(), Vec::new());
            new_node.write_node(dal)?;
            to_split.items.truncate(split_idx);

            new_node
        } else {
            let new_node = dal.new_node(
                to_split.items[(split_idx + 1)..].to_vec(),
                to_split.children[(split_idx + 1)..].to_vec(),
            );
            to_split.items.truncate(split_idx);
            to_split.children.truncate(split_idx);
            new_node
        };
        self.add_item(middle_item, n_to_split_idx);

        if self.children.len() == n_to_split_idx + 1 {
            self.children.push(node.page_num);
        } else {
            // we basically need to flip the vector around. this could probably be done in
            // a smarter way :D.
            let mut new_vec = self.children[..(n_to_split_idx + 1)].to_vec();
            new_vec.push(node.page_num);
            new_vec.extend_from_slice(&self.children[n_to_split_idx..]);

            self.children = new_vec;
        }

        self.write_node(dal)?;
        node.write_node(dal)?;

        Ok(())
    }
}
