
use std::io::{Read, Seek, SeekFrom};

use num_traits::FromBytes;

use crate::{page::Page, page_header::read_page_header, parsing_error::ParsingError, sqlite_header::{SqliteHeader, read_sqlite_header}};

pub(crate) fn offset_range<'a>(buffer: &'a [u8], offset: &mut usize, size: usize) -> &'a [u8] {
    let begin_index = *offset;
    let end_index = *offset + size;
    *offset += size;
    return &buffer[begin_index..end_index]
}

pub(crate) fn get_num_from_be<'a, T>(offset: &mut usize, bytes: &'a[u8]) -> Result<T, ParsingError>
where
    T: FromBytes,
    <T as FromBytes>::Bytes: Sized,
    <T as FromBytes>::Bytes: 'a + TryFrom<&'a[u8]>,
    <<T as FromBytes>::Bytes as TryFrom<&'a [u8]>>::Error: Into<ParsingError>
{
    let bytes: &'a[u8] = offset_range(bytes, offset, size_of::<T>());
    let result = bytes.try_into();
    result.map(|value| FromBytes::from_be_bytes(&value)).map_err(|err: <<T as FromBytes>::Bytes as TryFrom<&'a [u8]>>::Error| err.into())
}

pub struct SqliteReader {
    file: std::fs::File,
    buffer: Vec<u8>,
    pub header: SqliteHeader,
}

impl SqliteReader {
    pub fn new(path: &str) -> Result<Self, ParsingError> {
        let mut file = std::fs::File::open(path)?;
        
        let header = read_sqlite_header(&mut file)?;
        
        return Ok(SqliteReader { file: file, buffer: vec![0; header.page_size as usize], header: header })
    }
    
    
    pub fn read_page(&mut self, page: u64) -> Result<Page, ParsingError> {
        self.file.seek(SeekFrom::Start((page - 1) * self.header.page_size as u64))?;
        self.file.read_exact(&mut self.buffer)?;
        let mut offset: usize = if page == 1 {
            100
        } else {
            0
        };
        let page_header = read_page_header(&mut offset, &self.buffer)?;
        return Ok(Page {
            page_header: page_header,
            page: self.buffer[(offset)..].to_vec(),
            page_start: offset,
        })
    }
}
