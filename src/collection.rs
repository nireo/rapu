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

    pub fn find(&self, key: &[u8]) -> std::io::Result<Item> {
        let node = self.dal.get_node(self.root_page)?;
        println!("{:#?}", node);
        match node.find_key(key, &self.dal) {
            Ok((idx, node, _)) => Ok(node.items[idx].clone()),
            Err(e) => Err(e),
        }
    }

    pub fn get_nodes(&self, idxs: &[usize]) -> std::io::Result<Vec<Node>> {
        let root = self.dal.get_node(self.root_page)?;

        let mut child = root;
        let mut nodes: Vec<Node> = Vec::new();
        for i in 0..idxs.len() {
            nodes.push(child);
            child = self.dal.get_node(nodes[i].children[idxs[i]])?;
        }

        Ok(nodes)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let item = Item::new(key.to_vec(), value.to_vec());

        let root_node = if self.root_page == 0 {
            let mut node = self.dal.new_node(vec![item.clone()], Vec::new());
            node.write_node(&mut self.dal)?;
            self.root_page = node.page_num;
            println!("{:#?}", node);

            node
        } else {
            println!("getting node from dal");
            self.dal.get_node(self.root_page)?
        };

        println!("{:#?}", root_node);

        let (idx, mut node, ancestors) = root_node.find_key(key, &self.dal)?;
        if idx < node.items.len() && node.items[idx].key == key {
            node.items[idx] = item
        } else {
            node.add_item(item, idx);
        }

        node.write_node(&mut self.dal)?;

        // rebalance
        let mut ancestor_nodes = self.get_nodes(&ancestors)?;
        for i in (0..ancestor_nodes.len() - 1).rev() {
            // -1 because we want to exclude the root node
            if self.dal.is_over_populated(&ancestor_nodes[i + 1]) {
                // ancestor_nodes[i].split(
                //     &mut ancestor_nodes[i + 1],
                //     ancestors[i + 1],
                //     &mut self.dal,
                // )?;
                // TODO: fix this
                println!("should split");
            }
        }

        if self.dal.is_over_populated(&ancestor_nodes[0]) {
            let mut new_root = self
                .dal
                .new_node(Vec::new(), vec![ancestor_nodes[0].page_num]);
            new_root.split(&mut ancestor_nodes[0], 0usize, &mut self.dal)?;

            new_root.write_node(&mut self.dal)?;
            self.root_page = new_root.page_num;
        }

        Ok(())
    }
}
