use std::{
  fs, io,
  path::{Path, PathBuf},
};

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

pub fn copy_dir<P: AsRef<Path>>(src: P, dst: P, exclude: &[&str]) -> io::Result<()> {
  //
  if !dst.as_ref().exists() {
    fs::create_dir_all(&dst)?;
  }

  for dir_entry in fs::read_dir(&src)? {
    let entry = dir_entry?;
    let src_path = entry.path();

    if exclude.iter().any(|&x| src_path.ends_with(x)) {
      continue;
    }

    let dst_path = dst.as_ref().join(src_path.file_name().unwrap());
    if entry.file_type()?.is_dir() {
      copy_dir(src_path, dst_path, exclude)?;
    } else {
      fs::copy(&src_path, &dst_path)?;
    }
  }
  Ok(())
}

#[test]
fn test_available_disk_space() {
  let size = available_disk_space();
  assert!(size > 0);
}
