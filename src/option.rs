use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Options {
    // database directory
    pub dir_path: PathBuf,

    //data file size
    pub data_file_size: u64,

    // sync writes or not
    pub sync_writes: bool,

    // index type option
    pub index_type: IndexType,
}

#[derive(Debug, Clone)]
pub enum IndexType {
    /// Btree index
    BTree,

    /// SkipList index WIP
    SkipList,
}
