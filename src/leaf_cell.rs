
use crate::{parsing_error::ParsingError};

#[derive(Clone, Copy, Debug)]
pub enum SerialType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    Double,
    False,
    True,
    Unused,
    Blob(usize),
    String(usize)
}




impl SerialType {
    pub fn from_varint(value: i128) -> Result<SerialType, ParsingError> {
        let value = match value {
            0 => Self::Null,
            1 => Self::I8,
            2 => Self::I16,
            3 => Self::I24,
            4 => Self::I32,
            5 => Self::I48,
            6 => Self::I64,
            7 => Self::Double,
            8 => Self::False,
            9 => Self::True,
            10 | 11 => Self::Unused,
            n if n >= 12 && n % 2 == 0 => Self::Blob(((n - 12) / 2) as usize),
            n if n >= 13 && n % 2 != 0 => Self::String(((n - 13) / 2) as usize),
            _ => return Err(ParsingError::InvalidVarint)        
        };
        return Ok(value);
    }
    
    pub fn size(&self) -> usize {
        return match self {
            SerialType::Null => 0,
            SerialType::I8 => 1,
            SerialType::I16 => 2,
            SerialType::I24 => 3,
            SerialType::I32 => 4,
            SerialType::I48 => 6,
            SerialType::I64 => 8,
            SerialType::Double => 8,
            SerialType::False => 0,
            SerialType::True => 0,
            SerialType::Unused => 0,
            SerialType::Blob(size) => *size,
            SerialType::String(size) => *size,
        }
    }
    
    pub fn parse_value(&self, bytes: &[u8]) -> Result<String, ParsingError> {
        match self {
            SerialType::Null => Ok("NULL".to_string()),
            SerialType::I8 => Ok(format!("{}", i8::from_be_bytes(bytes[0..1].try_into()?))),
            SerialType::I16 => Ok(format!("{}", i16::from_be_bytes(bytes[0..2].try_into()?))),
            SerialType::I24 => Ok(format!("{}", "unimplemented")),
            SerialType::I32 => Ok(format!("{}", i32::from_be_bytes(bytes[0..4].try_into()?))),
            SerialType::I48 => Ok(format!("{}", "unimplemented")),
            SerialType::I64 => Ok(format!("{}", i64::from_be_bytes(bytes[0..8].try_into()?))),
            SerialType::Double => Ok(format!("{}", f64::from_be_bytes(bytes[0..8].try_into()?))),
            SerialType::False => Ok("false".to_string()),
            SerialType::True => Ok("true".to_string()),
            SerialType::Unused => unreachable!(),
            SerialType::Blob(_) => unimplemented!(),
            SerialType::String(size) => Ok(format!("{}", String::from_utf8_lossy(&bytes[0..*size]).to_string())),
        }
    }
    
    pub fn parse_value_cmp(&self, bytes: &[u8]) -> Result<String, ParsingError> {
        match self {
            SerialType::Null => Ok("NULL".to_string()),
            SerialType::I8 => Ok(format!("{}", i8::from_be_bytes(bytes[0..1].try_into()?))),
            SerialType::I16 => Ok(format!("{}", i16::from_be_bytes(bytes[0..2].try_into()?))),
            SerialType::I24 => Ok(format!("{}", format!("{}", "unimplemented"))),
            SerialType::I32 => Ok(format!("{}", i32::from_be_bytes(bytes[0..4].try_into()?))),
            SerialType::I48 => Ok(format!("{}", format!("{}", "unimplemented"))),
            SerialType::I64 => Ok(format!("{}", i64::from_be_bytes(bytes[0..8].try_into()?))),
            SerialType::Double => Ok(format!("{}", f64::from_be_bytes(bytes[0..8].try_into()?))),
            SerialType::False => Ok("false".to_string()),
            SerialType::True => Ok("true".to_string()),
            SerialType::Unused => unreachable!(),
            SerialType::Blob(_) => unimplemented!(),
            SerialType::String(size) => Ok(format!("\"{}\"", String::from_utf8_lossy(&bytes[0..*size]))),
        }
    }
}

#[derive(Clone)]
pub struct LazyLeafCell {
    pub record_size: i128,
    pub rowid: i128,
    pub records_begin: usize,
    pub record_types: Vec<SerialType>,
}

impl LazyLeafCell {
    
    pub fn get_column_offset(&self, column: usize) -> usize {
        return self.record_types[0..column].iter().map(|value| value.size()).sum::<usize>() + self.records_begin
    }
    
    pub fn get_column_size(&self, column: usize) -> usize {
        return self.record_types[column].size()
    }
    
    pub fn get_column_type(&self, column: usize) -> SerialType {
        return self.record_types[column]
    }
    

    pub fn get_column(&self, page_bytes: &[u8], column: usize) -> Result<String, ParsingError> {
        let column_offset = self.get_column_offset(column);
        let column_size = self.get_column_size(column);
        let column_type = self.get_column_type(column);
        let begin_index = column_offset;
        let end_index = begin_index + column_size;
        column_type.parse_value(&page_bytes[begin_index..end_index])
    }
    
    pub fn get_column_cmp(&self, page_bytes: &[u8], column: usize) -> Result<String, ParsingError> {
        let column_offset = self.get_column_offset(column);
        let column_size = self.get_column_size(column);
        let column_type = self.get_column_type(column);
        let begin_index = column_offset;
        let end_index = begin_index + column_size;
        column_type.parse_value_cmp(&page_bytes[begin_index..end_index])
    }
}
