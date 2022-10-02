mod dal;
use crate::dal::DataAccessLayer;
use std::path::Path;

fn main() -> std::io::Result<()> {
    {
        let mut dal = DataAccessLayer::new(Path::new("./test.db"), 4096)?;
        let mut pg = dal.allocate_empty_page();
        pg.num = dal.freelist.next_page();
        for (i, b) in b"hello".iter().enumerate() {
            pg.data[i] = *b;
        }

        dal.write_page(&pg)?;
        dal.write_freelist()?;
    }

    {
        let mut dal = DataAccessLayer::new(Path::new("./test.db"), 4096)?;
        let mut pg = dal.allocate_empty_page();
        pg.num = dal.freelist.next_page();

        for (i, b) in b"hello2".iter().enumerate() {
            pg.data[i] = *b;
        }
        dal.write_page(&pg)?;
        let pgnum = dal.freelist.next_page();
        dal.freelist.release_page(pgnum);
        dal.write_freelist()?;
    }

    Ok(())
}
