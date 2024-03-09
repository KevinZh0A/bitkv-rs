use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};
use std::{path::PathBuf, sync::Arc};

use super::log_record::{LogRecord, LogRecordType, ReadLogRecord};
use crate::data::log_record::max_log_record_header_size;
use crate::errors::{Errors, Result};
use crate::fio::{new_io_manager, IOManager};

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,      // data file id
    write_off: Arc<RwLock<u64>>, // current write offset, used for recording appending write position
    io_manager: Box<dyn IOManager>, // IO manager interface
}

impl DataFile {
    pub fn new(dir_path: &PathBuf, file_id: u32) -> Result<DataFile> {
        // get filename by file_id and dir_path
        let file_name = get_data_file_name(dir_path, file_id);

        // initialize IO manager
        let io_manager = new_io_manager(&file_name)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    // read log record by offset
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // read header
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(&mut header_buf, offset)?;

        // Retrieve first byte of header, which is the type of log record
        let rec_type = header_buf.get_u8();

        // Retrieve the length of the key and value
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();

        // if key_size and value_size are 0, EOF then return error
        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadDataFileEOF);
        }

        // get actual data size
        let actual_header_size =
            length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;

        // read actual key and value, last 4 bytes is crc32 checksum
        let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4);
        self.io_manager
            .read(&mut kv_buf, offset + actual_header_size as u64)?;

        // construct log record
        let log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
            rec_type: LogRecordType::from_u8(rec_type),
        };

        // advance to last 4 bytes, read crc32 checksum
        kv_buf.advance(key_size + value_size);

        if kv_buf.get_u32() != log_record.get_crc() {
            return Err(Errors::InvalidLogRecordCrc);
        }

        Ok(ReadLogRecord {
            record: log_record,
            size: actual_header_size + key_size + value_size + 4,
        })
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;

        //update write_off
        let mut write_off = self.write_off.write();
        *write_off += n_bytes as u64;

        Ok(n_bytes)
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
}

/// get filename
fn get_data_file_name(dir_path: &PathBuf, file_id: u32) -> PathBuf {
    let name = format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    dir_path.join(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();
        let data_file_res = DataFile::new(&dir_path, 0);
        assert!(data_file_res.is_ok());
        let data_file = data_file_res.unwrap();
        assert_eq!(data_file.get_file_id(), 0);

        let data_file_res2 = DataFile::new(&dir_path, 0);
        assert!(data_file_res2.is_ok());
        let data_file2 = data_file_res2.unwrap();
        assert_eq!(data_file2.get_file_id(), 0);

        let data_file_res3 = DataFile::new(&dir_path, 160);
        assert!(data_file_res3.is_ok());
        let data_file3 = data_file_res3.unwrap();
        assert_eq!(data_file3.get_file_id(), 160);
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();
        let data_file_res = DataFile::new(&dir_path, 2);
        assert!(data_file_res.is_ok());
        let data_file = data_file_res.unwrap();
        assert_eq!(data_file.get_file_id(), 2);

        let write_res1 = data_file.write("aaa".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(3 as usize, write_res1.ok().unwrap());

        let write_res2 = data_file.write("bbb".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(3 as usize, write_res2.ok().unwrap());
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();
        let data_file_res = DataFile::new(&dir_path, 3);
        assert!(data_file_res.is_ok());
        let data_file = data_file_res.unwrap();
        assert_eq!(data_file.get_file_id(), 3);

        let sync_res = data_file.sync();
        assert!(sync_res.is_ok());
    }

    #[test]
    fn test_data_file_read_log_record() {
        let dir_path = std::env::temp_dir();
        let data_file_res = DataFile::new(&dir_path, 600);
        assert!(data_file_res.is_ok());
        let data_file = data_file_res.unwrap();
        assert_eq!(data_file.get_file_id(), 600);

        let enc1 = LogRecord {
            key: "key-a".as_bytes().to_vec(),
            value: "value-a".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let buf1 = enc1.encode();
        let write_res1: std::prelude::v1::Result<usize, Errors> = data_file.write(&buf1);
        assert!(write_res1.is_ok());

        // read from offset 0
        let read_res1 = data_file.read_log_record(0);
        assert!(read_res1.is_ok());
        let read_enc1 = read_res1.ok().unwrap();
        assert_eq!(enc1.key, read_enc1.record.key);
        assert_eq!(enc1.value, read_enc1.record.value);
        assert_eq!(enc1.rec_type, read_enc1.record.rec_type);

        // multiple log records
        let enc2 = LogRecord {
            key: "key-b".as_bytes().to_vec(),
            value: "value-b".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc3 = LogRecord {
            key: "key-c".as_bytes().to_vec(),
            value: "value-c".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };

        // Read from current write offset
        let buf2 = enc2.encode();
        let buf3 = enc3.encode();

        let write_res2 = data_file.write(&buf2);
        assert!(write_res2.is_ok());
        let write_res3 = data_file.write(&buf3);

        let read_res2 = data_file.read_log_record(19);
        assert!(read_res2.is_ok());
        let read_enc2 = read_res2.ok().unwrap();
        assert_eq!(enc2.key, read_enc2.record.key);
        assert_eq!(enc2.value, read_enc2.record.value);
        assert_eq!(enc2.rec_type, read_enc2.record.rec_type);

        let read_res3 = data_file.read_log_record(19 + read_enc2.size as u64);
        assert!(read_res3.is_ok());
        let read_enc3 = read_res3.ok().unwrap();
        assert_eq!(enc3.key, read_enc3.record.key);
        assert_eq!(enc3.value, read_enc3.record.value);
        assert_eq!(enc3.rec_type, read_enc3.record.rec_type);

        // read record type deleted
        let enc4 = LogRecord {
            key: "key-d".as_bytes().to_vec(),
            value: "value-d".as_bytes().to_vec(),
            rec_type: LogRecordType::DELETED,
        };

        let buf4 = enc4.encode();
        assert!(write_res3.is_ok());
        let write_res4: std::prelude::v1::Result<usize, Errors> = data_file.write(&buf4);
        assert!(write_res4.is_ok());

        let read_res4 =
            data_file.read_log_record(19 + read_enc2.size as u64 + read_enc3.size as u64);
        assert!(read_res4.is_ok());
        let read_enc4 = read_res4.ok().unwrap();
        assert_eq!(enc4.key, read_enc4.record.key);
        assert_eq!(enc4.value, read_enc4.record.value);
        assert_eq!(enc4.rec_type, read_enc4.record.rec_type);
    }
}
