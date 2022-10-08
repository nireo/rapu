use crate::dal::{DataAccessLayer, PageNum};
use crate::node::{Item, Node};

pub struct Collection<'a> {
    name: &'a str,
    root_page: PageNum,
    dal: DataAccessLayer,
}

impl<'a> Collection<'a> {
    pub fn new(name: &'a str, dal: DataAccessLayer) -> Self {
        Self {
            name,
            root_page: dal.meta.root,
            dal,
        }
    }

    pub fn find(&self, key: &[u8]) -> std::io::Result<&Item> {
        let node = self.dal.get_node(self.root_page)?;
        match node.find_key(key, &self.dal) {
            Ok((idx, node, _)) => Ok(&node.items[idx]),
            Err(e) => Err(e),
        }
    }

    pub fn get_nodes(&self, idxs: Vec<usize>) -> std::io::Result<Vec<Node>> {
        let root = self.dal.get_node(self.root_page)?;

        let mut child = root;
        let nodes: Vec<Node> = Vec::new();
        for i in 0..idxs.len() {
            nodes.push(child);
            child = self.dal.get_node(nodes[i].children[idxs[i]])?;
        }

        Ok(nodes)
    }
}
