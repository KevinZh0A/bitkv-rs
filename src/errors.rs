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
}

pub type Result<T> = result::Result<T, Errors>;
