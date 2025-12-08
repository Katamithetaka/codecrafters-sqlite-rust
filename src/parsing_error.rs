use std::{array::TryFromSliceError, fmt::Display, io};

#[derive(Debug)]
pub enum ParsingError {
    IoError(io::Error),
    SliceConversionError(TryFromSliceError),
    InvalidHeaderString,
    InvalidPageType,
    InvalidVarint
}

impl std::error::Error for ParsingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParsingError::IoError(error) => Some(error),
            ParsingError::SliceConversionError(try_from_slice_error) => Some(try_from_slice_error),
            ParsingError::InvalidHeaderString => None,
            ParsingError::InvalidPageType => None,
            ParsingError::InvalidVarint => None
        }
    }


    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

impl Display for ParsingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParsingError::IoError(error) => f.write_fmt(format_args!("IoError {error}")),
            ParsingError::SliceConversionError(try_from_slice_error) => f.write_fmt(format_args!("Slice Error {try_from_slice_error}")),
            ParsingError::InvalidHeaderString => f.write_str("Invalid header string for sqlite file"),
            ParsingError::InvalidPageType => f.write_str("Invalid page type"),
            ParsingError::InvalidVarint => f.write_str("Error while parsing a varint"),
        }
    }
}

impl From<io::Error> for ParsingError {
    fn from(value: io::Error) -> Self {
        return ParsingError::IoError(value)
    }
}

impl From<TryFromSliceError> for ParsingError {
    fn from(value: TryFromSliceError) -> Self {
        return ParsingError::SliceConversionError(value)
    }
}
