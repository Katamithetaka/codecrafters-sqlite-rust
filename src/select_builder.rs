use crate::{cell::get_cells_lazy, parsing_error::ParsingError, reader::SqliteReader};

pub enum Op {
    Eq,
    Lt,
    Gt,
    GtEq,
    LtEq
}

impl Op {
    pub fn apply(&self, lhs: &String, rhs: &String) -> bool {
        match self {
            Op::Eq => lhs == rhs,
            Op::Lt => lhs < rhs,
            Op::Gt => lhs >  rhs,
            Op::GtEq => lhs >= rhs,
            Op::LtEq => lhs <= rhs,
        }
    }
}



pub struct SelectBuilder {
    pub(crate) index: Option<(u64, usize)>,
    pub(crate) table: u64,
    // None means COUNT(*)
    pub(crate) columns: Vec<Option<usize>>,
    pub(crate) where_comps: Option<(usize, Op, String)>
}

impl SelectBuilder {
    pub fn new(table: u64, columns: Vec<Option<usize>>) -> Self {
        return SelectBuilder {
            index: None,
            table,
            columns,
            where_comps: None
        }
    }
    
    pub fn where_cmp(self, column: usize, op: Op, value: String) -> Self {
        return Self {
            where_comps: Some((column, op, value)),
            ..self
        }
    }
    
    pub fn with_index(self, index_page: u64, index_column: usize) -> Self {
        return Self {
            index: Some((index_page, index_column)),
            ..self
        }
    }
    
    pub fn execute(self, sqlite_reader: &mut SqliteReader) -> Result<Vec<Vec<String>>, ParsingError> {
        if self.index.is_some() && self.where_comps.is_none() {
            panic!("Can't read index without a comp");
        }
        
        if self.index.is_some() {
            unimplemented!()
        }
        
        let page = sqlite_reader.read_page(self.table)?;
        
        let cells = get_cells_lazy(&page, sqlite_reader)?;
        
        let cells = if let Some(comp) = self.where_comps {
            cells.iter().filter(|cell| {
                cell.get_column_cmp(&page.page, comp.0).is_ok_and(|value| comp.1.apply(&value, &comp.2))
            }).cloned().collect::<Vec<_>>()
        } else {
            cells
        };
        
        let count = cells.len();
        
        return cells.iter().map(|cell| self.columns.iter().map(|column| {
            match column {
                Some(column) => cell.get_column(&page.page, *column),
                None => Ok(count.to_string())
            }
        }).collect()).collect()
    }
}
