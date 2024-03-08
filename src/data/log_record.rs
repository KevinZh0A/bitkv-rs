use prost::length_delimiter_len;

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
    pub(crate) size: usize,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }

    pub fn get_crc(&mut self) -> u32 {
        todo!()
    }
}

impl LogRecordType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            _ => panic!("unsupported log record type"),
        }
    }
}

// get max log record header length
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}
