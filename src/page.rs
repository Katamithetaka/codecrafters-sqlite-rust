use std::rc::Rc;

use crate::page_header::PageHeader;

pub struct Page {
    pub page_header: PageHeader,
    pub page: Rc<[u8]>,
    pub page_start: usize,
    pub page_offset: usize, // 100 for page 1 (SQLite header), 0 for other pages
}

impl Page {
    pub fn parse_cell_pointer_array(&self) -> Vec<u16> {
        let begin = self.page_start;
        let end = begin + self.page_header.cell_count as usize * 2;
        let buffer = &self.page[begin..end];

        let (values, remainder) = buffer.as_chunks::<2>();
        assert!(remainder.len() == 0);

        values.iter().map(|c| u16::from_be_bytes(*c)).collect()
    }
}
