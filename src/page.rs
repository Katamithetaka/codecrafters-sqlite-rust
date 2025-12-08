use crate::page_header::PageHeader;

pub struct Page {
    pub page_header: PageHeader,
    pub page: Vec<u8>,
    pub page_start: usize,
}

impl Page {
    
    pub fn parse_cell_pointer_array(&self) -> Vec<u16> {
        
        let buffer = &self.page[0..(self.page_header.cell_count as usize  * 2)];
        
        let (values, remainder) = buffer.as_chunks::<2>();
        assert!(remainder.len() == 0);
        
        values.iter().map(|c| u16::from_be_bytes(*c)).collect()
    }
}
