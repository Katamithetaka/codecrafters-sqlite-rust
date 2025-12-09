use crate::{
    interior_cell::InteriorCell,
    leaf_cell::{LazyLeafCell, SerialType},
    page::Page,
    parsing_error::ParsingError,
    reader::{SqliteReader, get_num_from_be},
    select_builder::{Op, WhereColumn, compare},
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

pub fn parse_index_leaf_cell(
    bytes: &[u8],
    cell_offset: usize,
    page_data: Rc<[u8]>,
) -> Result<LazyLeafCell, ParsingError> {
    let mut offset = cell_offset;
    let record_size = parse_varint(&mut offset, bytes)?;
    let start_offset = offset;
    let record_header_size = parse_varint(&mut offset, bytes)?;
    let mut serial_types = vec![];
    while (offset - start_offset) < record_header_size as usize {
        serial_types.push(SerialType::from_varint(parse_varint(&mut offset, bytes)?)?);
    }
    let records_begin = offset;

    let mut row_id = LazyLeafCell {
        record_size,
        rowid: 0,
        records_begin: records_begin,
        record_types: serial_types.clone(),
        page_data,
    };
    
    row_id.rowid = i128::from_str_radix(row_id.get_column(serial_types.len()-1)?.as_str(), 10).map_err(|_| ParsingError::InvalidVarint)?;
    
    // for i in 0..serial_types.len() {
    //     eprintln!("{:?}", row_id.get_column(i))
    // };
    
    return Ok(row_id);
}

pub fn parse_index_interior_cell(
    bytes: &[u8],
    cell_offset: usize,
    page_data: Rc<[u8]>
) -> Result<(u32, LazyLeafCell), ParsingError>  {
    let mut offset = cell_offset;
    let page_number = get_num_from_be(&mut offset, bytes)?;
    Ok((
        page_number,
        parse_index_leaf_cell(bytes, offset, page_data)?,
    ))
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

pub fn binary_search_interior_table_page(
    page: &Page,
    cell_array: &[u16],
    reader: &mut SqliteReader,
    rowids: &[i128],
) -> Result<Vec<LazyLeafCell>, ParsingError> {
    if rowids.is_empty() || cell_array.is_empty() {
        return Ok(vec![]);
    }

    // Parse all cells
    let cells: Result<Vec<InteriorCell>, ParsingError> = cell_array
        .iter()
        .map(|cell| parse_interior_cell(&page.page, *cell as usize))
        .collect();
    let cells = cells?;
    
    let mut results = vec![];
    
    // Interior B-tree structure:
    // Cell i's left_child contains the range between cell i-1 and cell i
    // Cell 0's left_child contains all rowids < cell 0
    // Rightmost pointer (not in cell_array) contains rowids >= last cell
    
    for (i, cell) in cells.iter().enumerate() {
        let prev_rowid = if i == 0 {
            None
        } else {
            Some(cells[i - 1].rowid)
        };
        
        let matching_rowids: Vec<i128> = rowids
            .iter()
            .filter(|&&rowid| {
                let above_prev = prev_rowid.map_or(true, |prev| rowid >= prev);
                let below_curr = rowid < cell.rowid;
                above_prev && below_curr
            })
            .copied()
            .collect();
        
        if !matching_rowids.is_empty() {
            let child_page = reader.read_page(cell.page_number as u64)?;
            results.append(&mut binary_search_cells_lazy(&child_page, reader, &matching_rowids)?);
        }
    }
    
    return Ok(results);
}

pub fn binary_search_leaf_page(
    page: &Page,
    cell_array: &[u16],
    _reader: &mut SqliteReader,
    rowids: &[i128],
) -> Result<Vec<LazyLeafCell>, ParsingError> {
    if rowids.is_empty() || cell_array.is_empty() {
        return Ok(vec![]);
    }

    // For leaf pages, just search all cells linearly
    // This is simpler and more reliable than binary search
    let results: Vec<LazyLeafCell> = cell_array
        .into_iter()
        .map(|cell| parse_leaf_cell_lazy(&page.page, *cell as usize, Rc::clone(&page.page)))
        .filter(|cell| cell.as_ref().is_ok_and(|cell| rowids.contains(&cell.rowid)))
        .collect::<Result<Vec<_>, _>>()?;
    
    return Ok(results);
}

pub fn binary_search_cells_lazy(
    page: &Page,
    reader: &mut SqliteReader,
    rowids: &[i128],
) -> Result<Vec<LazyLeafCell>, ParsingError> {

    match page.page_header.page_type {
        crate::page_header::BtreePageType::InteriorIndexPage => {
            panic!("This method shouldn't be used for index cells")
        }
        crate::page_header::BtreePageType::InteriorTablePage => {
            let right_most_page = page
                .page_header
                .rightmost_pointer
                .map(|root_page| reader.read_page(root_page as u64))
                .ok_or(ParsingError::InvalidPageType)??;

            let cell_array = page.parse_cell_pointer_array();

            if cell_array.is_empty() {
                return binary_search_cells_lazy(&right_most_page, reader, rowids);
            }


            let last_cell = cell_array
                .last()
                .map(|cell| parse_interior_cell(&page.page, *cell as usize))
                .transpose()?
                .unwrap();


            let rowids_filtered: Vec<_> = rowids
                .iter()
                .filter(|rowid| **rowid <= last_cell.rowid)
                .copied()
                .collect();

            if rowids_filtered.is_empty() {
                return binary_search_cells_lazy(&right_most_page, reader, rowids);
            }

            let mut return_val =
                binary_search_interior_table_page(page, &cell_array, reader, &rowids_filtered)?;
            
            let rightmost_rowids: Vec<_> = rowids
                .iter()
                .filter(|rowid| **rowid >= last_cell.rowid)
                .copied()
                .collect();
            
            if !rightmost_rowids.is_empty() {
                return_val.append(&mut binary_search_cells_lazy(
                    &right_most_page,
                    reader,
                    &rightmost_rowids,
                )?);
            }

            return Ok(return_val);
        }
        crate::page_header::BtreePageType::LeafIndexPage => {
            panic!("This method shouldn't be used for index cells")
        }
        crate::page_header::BtreePageType::LeafTablePage => {
            let cell_array = page.parse_cell_pointer_array();

            let first_cell = cell_array
                .first()
                .map(|cell| parse_leaf_cell_lazy(&page.page, *cell as usize, Rc::clone(&page.page)))
                .transpose()?
                .unwrap();
            let last_cell = cell_array
                .last()
                .map(|cell| parse_leaf_cell_lazy(&page.page, *cell as usize, Rc::clone(&page.page)))
                .transpose()?
                .unwrap();

            let rowids_filtered: Vec<_> = rowids
                .iter()
                .filter(|rowid| **rowid >= first_cell.rowid && **rowid <= last_cell.rowid)
                .copied()
                .collect();


            if rowids_filtered.is_empty() {
                return Ok(vec![]);
            }

            let result = binary_search_leaf_page(page, &cell_array, reader, &rowids_filtered)?;

            return Ok(result);
        }
    }
}

pub fn index_search(
    page: &Page,
    reader: &mut SqliteReader,
    column: WhereColumn,
    value: String,
    op: Op,
) -> Result<Vec<i128>, ParsingError> {

    match page.page_header.page_type {
        crate::page_header::BtreePageType::InteriorIndexPage => {
            let cell_array = page.parse_cell_pointer_array();

            // If there are no cells, follow the rightmost pointer
            if cell_array.is_empty() {
                let right_most_page = page
                    .page_header
                    .rightmost_pointer
                    .ok_or(ParsingError::InvalidPageType)?;
                let page = reader.read_page(right_most_page as u64)?;
                return index_search(&page, reader, column, value, op);
            }

            // Parse all interior index cells: (left_child_page, key_cell)
            let parsed: Vec<(u32, LazyLeafCell)> = cell_array
                .iter()
                .map(|cell| {
                    parse_index_interior_cell(&page.page, *cell as usize, Rc::clone(&page.page))
                })
                .collect::<Result<_, ParsingError>>()?;

            let mut results: Vec<i128> = Vec::new();

            // For equality searches, we need to find the specific child page that could contain the value
            // Interior index structure: each cell has a left child page and a separator key
            // Left child contains keys < separator
            // Rightmost pointer contains keys >= last separator
            
            match op {
                Op::Eq => {
                    // Find which child page should contain the search value
                    
                    for i in 0..parsed.len() {
                        let (left_page, key_cell) = &parsed[i];
                        let key_cmp = match column {
                            WhereColumn::Column(col) => key_cell.get_column_cmp(col)?,
                            WhereColumn::RowId => key_cell.rowid.to_string(),
                        };
                        
                        // Check if the separator key matches
                        if compare(&key_cmp, &value, Op::Eq) {
                            results.push(key_cell.rowid);
                            // For equality, also search left child for duplicate keys with smaller rowids
                            let child_page = reader.read_page(*left_page as u64)?;
                            results.append(&mut index_search(&child_page, reader, column.clone(), value.clone(), op)?);
                            // Don't break - continue to check rightmost pointer for more duplicates
                        }
                        // If search value < separator, it must be in the left child
                        else if compare(&value, &key_cmp, Op::Lt) {
                            let child_page = reader.read_page(*left_page as u64)?;
                            results.append(&mut index_search(&child_page, reader, column.clone(), value.clone(), op)?);
                            break;
                        }
                    }
                    
                    // Always check the rightmost pointer for equality searches (may have more duplicates)
                    if let Some(right_most_page) = page.page_header.rightmost_pointer {
                        let right_page = reader.read_page(right_most_page as u64)?;
                        results.append(&mut index_search(&right_page, reader, column.clone(), value.clone(), op)?);
                    }
                }
                _ => {
                    // For range queries, we may need to traverse multiple children
                    for i in 0..parsed.len() {
                        let (left_page, key_cell) = &parsed[i];
                        let key_cmp = match column {
                            WhereColumn::Column(col) => key_cell.get_column_cmp(col)?,
                            WhereColumn::RowId => key_cell.rowid.to_string(),
                        };
                        
                        // Check if separator matches the condition
                        if compare(&key_cmp, &value, op) {
                            results.push(key_cell.rowid);
                        }
                        
                        // Decide if we should traverse the left child
                        let should_traverse_left = match op {
                            Op::Lt => compare(&key_cmp, &value, Op::Gt),
                            Op::LtEq => compare(&key_cmp, &value, Op::GtEq),
                            Op::Gt => compare(&key_cmp, &value, Op::Gt),
                            Op::GtEq => compare(&key_cmp, &value, Op::GtEq),
                            Op::Eq => unreachable!(),
                        };
                        
                        if should_traverse_left {
                            let child_page = reader.read_page(*left_page as u64)?;
                            results.append(&mut index_search(&child_page, reader, column.clone(), value.clone(), op)?);
                        }
                    }
                    
                    // Check if we should traverse the rightmost pointer
                    if let Some(right_most_page) = page.page_header.rightmost_pointer {
                        let should_traverse_right = if let Some((_, last_key_cell)) = parsed.last() {
                            let last_key = match column {
                                WhereColumn::Column(col) => last_key_cell.get_column_cmp(col)?,
                                WhereColumn::RowId => last_key_cell.rowid.to_string(),
                            };
                            match op {
                                Op::Lt => true, // might have values < search value
                                Op::LtEq => true,
                                Op::Gt => compare(&last_key, &value, Op::Lt), // only if last_key < search value
                                Op::GtEq => compare(&last_key, &value, Op::LtEq),
                                Op::Eq => unreachable!(),
                            }
                        } else {
                            true
                        };
                        
                        if should_traverse_right {
                            let right_page = reader.read_page(right_most_page as u64)?;
                            results.append(&mut index_search(&right_page, reader, column, value, op)?);
                        }
                    }
                }
            }

            return Ok(results);
        },
        crate::page_header::BtreePageType::InteriorTablePage => {
            panic!("This method shouldn't be used for table cells")
        }
        crate::page_header::BtreePageType::LeafIndexPage => {
            let cell_array = page.parse_cell_pointer_array();
            let index_cells: Vec<_> = cell_array
                .iter()
                .map(|cell| {
                    parse_index_leaf_cell(&page.page, *cell as usize, Rc::clone(&page.page))
                })
                .collect::<Result<Vec<_>, ParsingError>>()?;
            
            let results: Vec<i128> = index_cells
                .iter()
                .filter(|cell| {
                    let cell_value = match column {
                        WhereColumn::Column(column) => cell.get_column_cmp(column).unwrap(),
                        WhereColumn::RowId => cell.rowid.to_string(),
                    };
                    compare(&cell_value, &value, op)
                })
                .map(|cell| cell.rowid)
                .collect();
            
            Ok(results)
        }
        crate::page_header::BtreePageType::LeafTablePage => {
            panic!("This method shouldn't be used for table cells")
        }
    }
}
