use std::{fmt::Debug, result};
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum Errors {
  #[error("failed to read from data file")]
  FailedToReadFromDataFile,

  #[error("failed to write to data file")]
  FailedToWriteToDataFile,

  #[error("failed to sync to data file")]
  FailedToSyncToDataFile,

  #[error("failed to open data file error")]
  FailedToOpenDataFile,

  #[error("the key is empty")]
  KeyIsEmpty,

  #[error("memory index failed to update")]
  IndexUpdateFailed,

  #[error("key is not found in database")]
  KeyNotFound,

  #[error("data file is not found in database")]
  DataFileNotFound,

  #[error("database dir path can not be empty")]
  DirPathIsEmpty,

  #[error("database data file size must be greater than 0")]
  DataFileSizeTooSmall,

  #[error("failed to create the database directory")]
  FailedToCreateDatabaseDir,

  #[error("failed to read the database directory")]
  FailedToReadDatabaseDir,

  #[error("database directory may be corrupted")]
  DatabaseDirectoryCorrupted,

  #[error("read data file eof")]
  ReadDataFileEOF,

  #[error("invalid crc value, log record maybe corrupted")]
  InvalidLogRecordCrc,

  #[error("exceed max batch number in one batch write")]
  ExceedMaxBatchNum,

  #[error("merge is in progress, try again later")]
  MergeInProgress,

  #[error("cannot use write batch, seq_no does not exist")]
  UnableToUseWriteBatch,

  #[error("the database directory is used by another process")]
  DatabaseIsUsing,
}

pub type Result<T> = result::Result<T, Errors>;
