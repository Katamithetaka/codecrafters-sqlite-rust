use crate::page_header::PageHeader;

pub struct Page {
    pub page_header: PageHeader,
    pub page: Vec<u8>
}
