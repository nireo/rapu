mod collection;
mod dal;
mod node;

use crate::collection::Collection;
use crate::dal::{DataAccessLayer, DataAccessLayerConfig};
use std::path::Path;

fn main() -> std::io::Result<()> {
    let mut options = DataAccessLayerConfig::default();
    options.min_fill_percent = 0.0125;
    options.max_fill_percent = 0.025;

    let dal = DataAccessLayer::new(Path::new("./test.db"), &options)?;
    let mut bucket = Collection::new("test_collection", dal);

    bucket.put("Key1".as_bytes(), "Value1".as_bytes())?;
    bucket.put("Key2".as_bytes(), "Value2".as_bytes())?;
    bucket.put("Key3".as_bytes(), "Value3".as_bytes())?;
    bucket.put("Key4".as_bytes(), "Value4".as_bytes())?;
    bucket.put("Key5".as_bytes(), "Value5".as_bytes())?;
    bucket.put("Key6".as_bytes(), "Value6".as_bytes())?;

    Ok(())
}
