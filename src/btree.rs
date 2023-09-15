use byteorder::{LittleEndian, ByteOrder};

const BNODE_NODE: u16 = 1;
const BNODE_LEAF: u16 = 2;
const HEADER: u16 = 4;
const BTREE_PAGE_SIZE: u16 = 4096;
const BTREE_MAX_KEY_SIZE: u16 = 1000;
const BTREE_MAX_VAL_SIZE: u16 = 3000;

struct Node {
    data: Vec<u8>,
}

impl Node {
    fn btype(&self) -> u16 {
        LittleEndian::read_u16(&self.data)
    }

    fn nkeys(&self) -> u16 {
        LittleEndian::read_u16(&self.data[2..4])
    }

    fn set_header(&mut self, btype: u16, nkeys: u16) {
        LittleEndian::write_u16(&mut self.data[0..2], btype);
        LittleEndian::write_u16(&mut self.data[2..4], nkeys);
    }

    fn get_ptr(&self, idx: u16) -> u64 {
        assert!(idx < self.nkeys());
        let pos = (HEADER + idx) as usize;
        LittleEndian::read_u64(&self.data[pos..pos+8])
    }

    fn set_ptr(&mut self, idx: u16, val: u64) {
        assert!(idx < self.nkeys());
        let pos = (HEADER + idx) as usize;
        LittleEndian::write_u64(&mut self.data[pos..pos+8], val);
    }

    fn offset_pos(&self, idx: u16) -> u16 {
        assert!((1 <= idx && idx <= self.nkeys()));
        HEADER + 8 * self.nkeys() + 2*(idx - 1)
    }

    fn get_offset(&self, idx: u16) -> u16 {
        if idx == 0 {
            0
        } else {
            let offset_pos = self.offset_pos(idx) as usize;
            LittleEndian::read_u16(&self.data[offset_pos..offset_pos+2])
        }
    }

    fn set_offset(&mut self, idx: u16, offset: u16) {
        let offset_pos = self.offset_pos(idx) as usize;
        LittleEndian::write_u16(&mut self.data[offset_pos..offset_pos+2], offset)
    }

    fn kv_pos(&self, idx: u16) -> u16 {
        assert!(idx <= self.nkeys());
        HEADER + 8 * self.nkeys() + 2 * self.nkeys() + self.get_offset(idx)
    }

    fn get_key(&self, idx: u16) -> &[u8] {
        assert!(idx < self.nkeys());
        let pos = self.kv_pos(idx) as usize;
        let klen = LittleEndian::read_u16(&self.data[pos..pos+2]) as usize;
        &self.data[pos+4..pos+4+klen]
    }

    fn get_val(&self, idx: u16) -> &[u8] {
        assert!(idx < self.nkeys());
        let pos = self.kv_pos(idx) as usize;
        let klen = LittleEndian::read_u16(&self.data[pos..pos+2]) as usize;
        let vlen = LittleEndian::read_u16(&self.data[pos+2..pos+4]) as usize;
        &self.data[pos+4+klen..pos+4+klen+vlen]
    }
}
