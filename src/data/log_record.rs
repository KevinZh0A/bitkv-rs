use bytes::{BufMut, BytesMut};
use prost::{
    encode_length_delimiter, encoding::decode_varint, encoding::encode_varint, length_delimiter_len,
};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum LogRecordType {
    // normal putting data
    Normal = 1,

    // deleted data, tombstone value
    Deleted = 2,

    // transaction finished
    TxnFinished = 3,
}
// LogRecord write to data file record
// for it is called log, data writes by appending to datafile, WAL format
#[derive(Debug)]
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
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

// temp record for transaction
pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecordPos,
}

impl LogRecord {
    // Encode for log record, return bytes and its size
    // +----------+----------------+------------------+---------+-----------+---------+
    // |   Type   |   Key Length   |   Value Length   |   Key   |   Value   |   Crc   |
    // +----------+----------------+------------------+---------+-----------+---------+
    //  1bytes       n(n<=5) bytes     m(m<=5) bytes       x          y        4bytes
    //
    pub fn encode(&self) -> Vec<u8> {
        let (encode_buf, _) = self.encode_and_get_crc();
        encode_buf
    }

    pub fn get_crc(&self) -> u32 {
        let (_, crc_val) = self.encode_and_get_crc();
        crc_val
    }

    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        // init bytes array, store encoded log record
        let mut buf = BytesMut::new();
        buf.reserve(self.encoded_length());

        // write log record type into buffer
        buf.put_u8(self.rec_type as u8);

        // write key length and value length into buffer
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();

        // write key and value into buffer

        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // write crc32 checksum into buffer
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc.clone());

        (buf.to_vec(), crc)
    }

    // get encoded log record length
    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }
}

impl LogRecordPos {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        encode_varint(self.file_id as u64, &mut buf);
        encode_varint(self.offset, &mut buf);
        buf.to_vec()
    }
}

impl LogRecordType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => LogRecordType::Normal,
            2 => LogRecordType::Deleted,
            3 => LogRecordType::TxnFinished,
            _ => panic!("unsupported log record type"),
        }
    }
}

// get max log record header length
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}

// decode LogRecordPos
pub fn decode_log_record_pos(pos: Vec<u8>) -> LogRecordPos {
    let mut buf = BytesMut::new();
    buf.put_slice(&pos);

    let fid = match decode_varint(&mut buf) {
        Ok(fid) => fid,
        Err(e) => panic!("decode log record pos error: {}", e),
    };
    let offset = match decode_varint(&mut buf) {
        Ok(offset) => offset,
        Err(e) => panic!("decode log record pos error: {}", e),
    };
    LogRecordPos {
        file_id: fid as u32,
        offset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode_and_get_crc() {
        // set a normal log record
        let rec1 = LogRecord {
            key: "key-a".as_bytes().to_vec(),
            value: "value-a".as_bytes().to_vec(),
            rec_type: LogRecordType::Normal,
        };
        let enc1 = rec1.encode();
        assert!(enc1.len() > 5);
        assert_eq!(2460538915, rec1.get_crc());

        // set a log record which value is empty
        let rec2 = LogRecord {
            key: "bitkv-rs".as_bytes().to_vec(),
            value: vec![],
            rec_type: LogRecordType::Normal,
        };
        let enc2 = rec2.encode();
        assert!(enc2.len() > 5);
        assert_eq!(3786119330, rec2.get_crc());

        // set a deleted log record
        let rec3 = LogRecord {
            key: "key-b".as_bytes().to_vec(),
            value: "value-b".as_bytes().to_vec(),
            rec_type: LogRecordType::Deleted,
        };
        let enc3 = rec3.encode();
        assert!(enc3.len() > 5);
        assert_eq!(2488525827, rec3.get_crc());
    }
}
