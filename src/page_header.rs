use crate::{parsing_error::ParsingError, reader::get_num_from_be};

#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum BtreePageType {
    InteriorIndexPage = 0x02,
    InteriorTablePage = 0x05,
    LeafIndexPage = 0x0a,
    LeafTablePage = 0x0d
}

impl TryFrom<u8> for BtreePageType {
    type Error = ParsingError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let interior_index_page = Self::InteriorIndexPage as u8;
        let interior_table_page = Self::InteriorTablePage as u8;
        let leaf_index_page = Self::LeafIndexPage as u8;
        let leaf_table_page = Self::LeafTablePage as u8;
        
        if value == interior_index_page {
            return Ok(Self::InteriorIndexPage)
        }
        
        if value == leaf_index_page {
            return Ok(Self::LeafIndexPage)
        }
        
        if value == leaf_table_page {
            return Ok(Self::LeafTablePage)
        }
        
        if value == interior_table_page {
            return Ok(Self::InteriorTablePage)
        }
        
        return Err(ParsingError::InvalidPageType)
    }
}



pub struct PageHeader {
    pub page_type: BtreePageType,
    pub first_freeblock: u16,
    pub cell_count: u16,
    pub cell_content_start: u16,
    pub fragmented_free_bytes_count: u8,
    pub rightmost_pointer: Option<u32>,
}




pub fn read_page_header(offset: &mut usize, bytes: &[u8]) -> Result<PageHeader, ParsingError> {
    let page_type: u8 = get_num_from_be(offset, bytes)?;
    let page_type = BtreePageType::try_from(page_type)?;
    
    let first_freeblock: u16 = get_num_from_be(offset, bytes)?;
    let cell_count: u16 = get_num_from_be(offset, bytes)?;
    let cell_content_start: u16 = get_num_from_be(offset, bytes)?;
    let fragmented_free_bytes_count: u8 = get_num_from_be(offset, bytes)?;
    
    
    Ok(match page_type {
        BtreePageType::InteriorIndexPage | BtreePageType::InteriorTablePage => PageHeader {
            page_type,
            first_freeblock,
            cell_count,
            cell_content_start,
            fragmented_free_bytes_count,
            rightmost_pointer: Some(get_num_from_be(offset, bytes)?),
        },
        BtreePageType::LeafIndexPage | BtreePageType::LeafTablePage => {
            PageHeader { page_type, first_freeblock, cell_count, cell_content_start, fragmented_free_bytes_count, rightmost_pointer: None }
        },
    })
}
