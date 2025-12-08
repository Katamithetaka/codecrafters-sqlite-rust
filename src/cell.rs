use crate::{
    interior_cell::InteriorCell,
    leaf_cell::{LazyLeafCell, SerialType},
    page::Page,
    parsing_error::ParsingError,
    reader::{SqliteReader, get_num_from_be},
    varint::parse_varint,
};
use std::rc::Rc;

pub fn parse_leaf_cell_lazy(
    bytes: &[u8],
    cell_offset: usize,
    page_data: Rc<[u8]>,
) -> Result<LazyLeafCell, ParsingError> {
    let mut offset = cell_offset;
    let record_size = parse_varint(&mut offset, bytes)?;
    let rowid = parse_varint(&mut offset, bytes)?;
    let start_offset = offset;
    let record_header_size = parse_varint(&mut offset, bytes)?;
    let mut serial_types = vec![];
    while (offset - start_offset) < record_header_size as usize {
        serial_types.push(SerialType::from_varint(parse_varint(&mut offset, bytes)?)?);
    }

    return Ok(LazyLeafCell {
        record_size,
        rowid,
        records_begin: offset,
        record_types: serial_types,
        page_data,
    });
}

pub fn parse_interior_cell(bytes: &[u8], cell_offset: usize) -> Result<InteriorCell, ParsingError> {
    let mut offset = cell_offset;
    let page_number = get_num_from_be(&mut offset, bytes)?;
    let row_id = parse_varint(&mut offset, bytes)?;
    return Ok(InteriorCell {
        page_number,
        rowid: row_id,
    });
}

pub fn get_cells_lazy(
    page: &Page,
    reader: &mut SqliteReader,
) -> Result<Vec<LazyLeafCell>, ParsingError> {
    match page.page_header.page_type {
        crate::page_header::BtreePageType::InteriorIndexPage => {
            panic!("This method shouldn't be used for index cells")
        }
        crate::page_header::BtreePageType::InteriorTablePage => {
            let right_most_page = page
                .page_header
                .rightmost_pointer
                .ok_or(ParsingError::InvalidPageType)?;
            let cell_array = page.parse_cell_pointer_array();
            let cells: Result<Vec<InteriorCell>, ParsingError> = cell_array
                .iter()
                .map(|cell| parse_interior_cell(&page.page, *cell as usize))
                .collect();
            let mut page_numbers: Vec<u32> = cells?.iter().map(|cells| cells.page_number).collect();
            page_numbers.push(right_most_page);
            let v = page_numbers
                .iter()
                .flat_map(|page_number| {
                    let result = reader.read_page(*page_number as u64);
                    result.map(|page| get_cells_lazy(&page, reader))
                })
                .collect::<Result<Vec<_>, _>>()?;

            return Ok(v.into_iter().flatten().collect());
        }
        crate::page_header::BtreePageType::LeafIndexPage => {
            panic!("This method shouldn't be used for index cells")
        }
        crate::page_header::BtreePageType::LeafTablePage => {
            let cell_array = page.parse_cell_pointer_array();
            cell_array
                .iter()
                .map(|cell| parse_leaf_cell_lazy(&page.page, *cell as usize, Rc::clone(&page.page)))
                .collect()
        }
    }
}
