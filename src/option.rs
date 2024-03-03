use std::path::PathBuf;

pub struct Options {
    // database directory
    pub dir_path: PathBuf,

    //data file size
    pub data_file_size: u64,

    // sync writes or not
    pub sync_writes: bool,
}
