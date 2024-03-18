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

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitkv-rs"),
            data_file_size: 256 * 1024 * 1024, // 256MB
            sync_writes: false,
            index_type: IndexType::BTree,
        }
    }
}
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}

pub struct WriteBatchOptions {
    // max batch number in one batch write
    pub max_batch_num: usize,

    // when commit if sync or not
    pub sync_writes: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 1000,
            sync_writes: true,
        }
    }
}
