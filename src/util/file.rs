use std::path::{Path, PathBuf};

// calculate available disk space
pub fn available_disk_space() -> u64 {
  match fs2::available_space(PathBuf::from("/")) {
    Ok(size) => size,
    _ => 0,
  }
}

// calculate the total size of directory in disk
pub fn dir_disk_size<P: AsRef<Path>>(dir_path: P) -> u64 {
  match fs_extra::dir::get_size(dir_path) {
    Ok(size) => size,
    _ => 0,
  }
}
