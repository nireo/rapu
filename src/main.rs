mod dal;
use crate::dal::DataAccessLayer;
use std::path::Path;

fn main() -> std::io::Result<()> {
    let dal = DataAccessLayer::new(Path::new("./"), 4096)?;

    Ok(())
}
