use std::{fs::File, io::Read};

use crate::{parsing_error::ParsingError, reader::{get_num_from_be, offset_range}};

pub struct SqliteHeader {
    pub page_size: u16,
    pub file_format_write_version: u8,
    pub file_format_read_version: u8,
    pub reserved_space: u8,
    pub max_payload_fraction: u8,
    pub min_payload_fraction: u8,
    pub leaf_payload_fraction: u8,
    pub file_change_counter: u32,
    pub database_size_in_pages: u32,
    pub first_freelist_trunk_page: u32,
    pub total_freelist_pages: u32,
    pub schema_cookie: u32,
    pub schema_format_number: u32,
    pub default_page_cache_size: u32,
    pub largest_root_btree_page_number: u32,
    pub text_encoding: u32,
    pub user_version: u32,
    pub incremental_vacuum_mode: u32,
    pub application_id: u32,
    pub reserved_for_expansion: [u8; 20],
    pub version_valid_for_number: u32,
    pub sqlite_version_number: u32,
}



impl SqliteHeader {
    pub fn from_bytes(buffer: &[u8; 100]) -> Result<Self, ParsingError> {
        let mut offset = 0;
        if offset_range(buffer, &mut offset, 16) != b"SQLite format 3\0" {
            return Err(ParsingError::InvalidHeaderString)
        };
    
        return Ok(SqliteHeader {
            page_size: get_num_from_be(&mut offset, buffer)?,
            file_format_write_version: get_num_from_be(&mut offset, buffer)?,
            file_format_read_version: get_num_from_be(&mut offset, buffer)?,
            reserved_space: get_num_from_be(&mut offset, buffer)?,
            max_payload_fraction: get_num_from_be(&mut offset, buffer)?,
            min_payload_fraction: get_num_from_be(&mut offset, buffer)?,
            leaf_payload_fraction: get_num_from_be(&mut offset, buffer)?,
            file_change_counter: get_num_from_be(&mut offset, buffer)?,
            database_size_in_pages: get_num_from_be(&mut offset, buffer)?,
            first_freelist_trunk_page: get_num_from_be(&mut offset, buffer)?,
            total_freelist_pages: get_num_from_be(&mut offset, buffer)?,
            schema_cookie: get_num_from_be(&mut offset, buffer)?,
            schema_format_number: get_num_from_be(&mut offset, buffer)?,
            default_page_cache_size: get_num_from_be(&mut offset, buffer)?,
            largest_root_btree_page_number: get_num_from_be(&mut offset, buffer)?,
            text_encoding: get_num_from_be(&mut offset, buffer)?,
            user_version: get_num_from_be(&mut offset, buffer)?,
            incremental_vacuum_mode: get_num_from_be(&mut offset, buffer)?,
            application_id: get_num_from_be(&mut offset, buffer)?,
            reserved_for_expansion: offset_range(buffer, &mut offset, 20).try_into()?,
            version_valid_for_number: get_num_from_be(&mut offset, buffer)?,
            sqlite_version_number: get_num_from_be(&mut offset, buffer)?,
            
        })
    }
}

pub(crate) fn read_sqlite_header(file: &mut File) -> Result<SqliteHeader, ParsingError> {
    let mut buffer = [0u8; 100];
    file.read_exact(&mut buffer)?;
    SqliteHeader::from_bytes(&buffer)
}
