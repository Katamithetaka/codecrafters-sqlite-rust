use crate::{leaf_cell::{LazyLeafCell, SerialType}, page::Page, parsing_error::ParsingError, reader::SqliteReader, varint::parse_varint};


pub fn parse_leaf_cell_lazy(bytes: &[u8], cell_offset: usize) -> Result<LazyLeafCell, ParsingError> {
    let mut offset = cell_offset;
    let record_size = parse_varint(&mut offset, bytes)?;
    let rowid = parse_varint(&mut offset, bytes)?;
    let start_offset = offset;
    let record_header_size = parse_varint(&mut offset, bytes)?;
    let mut serial_types = vec![];
    while (offset - start_offset) < record_header_size as usize {
        serial_types.push(SerialType::from_varint(parse_varint(&mut offset, bytes)?)?);
    };
    
    
    
    return Ok(LazyLeafCell {
        record_size,
        rowid,
        records_begin: offset,
        record_types: serial_types,
    })
}

pub fn get_cells_lazy(page: &Page, _sqlite_reader: &mut SqliteReader) -> Result<Vec<LazyLeafCell>, ParsingError> {
    match page.page_header.page_type {
        crate::page_header::BtreePageType::InteriorIndexPage => panic!("This method shouldn't be used for index cells"),
        crate::page_header::BtreePageType::InteriorTablePage => unimplemented!(),
        crate::page_header::BtreePageType::LeafIndexPage => panic!("This method shouldn't be used for index cells"),
        crate::page_header::BtreePageType::LeafTablePage => {
            let cell_array = page.parse_cell_pointer_array();
            cell_array.iter().map(|cell| parse_leaf_cell_lazy(&page.page, *cell as usize - page.page_start)).collect()
        },
    }
}
