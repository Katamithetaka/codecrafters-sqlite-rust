/*
 Cells use variable-length integers, also called "varints." Here's how they work:
 
 Varints are in big-endian.
 
 A varint is 1-9 bytes in length.
 
 For varints that are 1-8 bytes in length:
 
 The first 0-7 bytes have a most significant bit (MSB) that is set.
 The final byte has an MSB that is clear.
 To reconstruct the integer, drop the MSB of each byte (so that each byte is only 7 bits long).
 For varints that are 9 bytes in length:
 
 The first 8 bytes have an MSB that is set.
 To reconstruct the integer, drop the MSB of the first 8 bytes. (Do not drop the MSB of the 9th byte.)
 */

use crate::parsing_error::ParsingError;

type Varint = i128;

pub fn is_msb_set(number: u8) -> bool {
    return number & 0b1000_0000 != 0
}

pub fn parse_varint(offset: &mut usize, buffer: &[u8]) -> Result<Varint, ParsingError> {
    let mut result: Varint = 0;
    for i in 0..9 {
        if i < 8 {
            result = (result << 7) | (buffer[*offset+i] & 0b0111_1111) as i128;
            if !is_msb_set(buffer[*offset+i]) {
                *offset += i+1;
                return Ok(result);
            }
        }
        else {
            result = (result << 8) | (buffer[*offset+i]) as i128;
            *offset += 9;
            return Ok(result);
        }
    };
    
    return Err(ParsingError::InvalidVarint)
}
