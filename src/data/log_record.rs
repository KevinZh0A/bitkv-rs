#[derive(PartialEq, Eq)]
pub enum LogRecordType {
    // normal putting data
    NORMAL = 1,

    // deleted data, tombstone value
    DELETED = 2,
}
// LogRecord write to data file record
// for it is called log, data writes by appending to datafile, WAL format
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

// data position index info, describes a position data stores
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

// read log_record info from data file, contains its size
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}
