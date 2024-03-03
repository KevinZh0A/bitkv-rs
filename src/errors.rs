use std::fmt::Debug;
use std::result;
use thiserror::Error;

#[derive(Error, Debug)]
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
}

pub type Result<T> = result::Result<T, Errors>;
