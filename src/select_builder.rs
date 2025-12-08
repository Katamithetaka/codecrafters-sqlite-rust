use crate::{cell::get_cells_lazy, leaf_cell::LazyLeafCell, parsing_error::ParsingError, reader::SqliteReader, select_parser::{ParsedCombinator, ParsedSelect, ParsedWhere, is_quoted, quoted}, table_parser::{Table, TableColumn}};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum Column {
    RowId, 
    Count,
    Column(usize)
}

pub enum WhereColumn {
    RowId,
    Column(usize)
}

#[derive(Clone, Copy)]
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
    
    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Lt => "<",
            Op::Gt => ">",
            Op::GtEq => ">=",
            Op::LtEq => "<=",
        }
    }
}

pub struct Expression {
    column: WhereColumn,
    op: Op,
    value: String
}

pub enum Combinator {
    And(Box<Where>),
    Or(Box<Where>)
}

pub struct Where {
    expression: Expression, 
    combinator: Option<Combinator>
}

impl Where {
    pub fn or(self, other: Where) -> Self {
        return Self {
            combinator: Some(Combinator::Or(Box::new(other))),
            ..self
        }
    }
    
    pub fn and(self, other: Where) -> Self {
        return Self {
            combinator: Some(Combinator::And(Box::new(other))),
            ..self
        }
    }
    
    pub fn execute(&self, page_bytes: &[u8], cell: &LazyLeafCell) -> Result<bool, ParsingError> {
        
        
        let column = match self.expression.column {
            WhereColumn::Column(column) => cell.get_column_cmp(page_bytes, column)?,
            WhereColumn::RowId => cell.rowid.to_string(),
        };
        
        let column = if is_quoted(&self.expression.value) {
            quoted(column)
        } else {
            column
        };
        
        let value = if is_quoted(&column) {
            &quoted(&self.expression.value)
        } else {
            &self.expression.value
        };
        
        let own_comp = self.expression.op.apply(value, &column);
        match &self.combinator {
            Some(Combinator::And(value)) => Ok(own_comp && value.execute(page_bytes, cell)?),
            Some(Combinator::Or(value)) => Ok(own_comp || value.execute(page_bytes, cell)?),
            None => Ok(own_comp),
        }
    }

    fn from_table(comp: &ParsedWhere, table: &Table) -> Result<Where, ParsingError> {
        let column = table.get_column_by_name(&comp.expression.column).ok_or(ParsingError::InvalidStatement)?;
        let column = match column {
            TableColumn::RowId(_) => WhereColumn::RowId,
            TableColumn::Column(index, _) => WhereColumn::Column(*index),
        };
        
        let combinator = match &comp.combinator {
            Some(ParsedCombinator::And(comb)) => Some(Combinator::And(Box::new(Self::from_table(&comb, table)?))),
            Some(ParsedCombinator::Or(comb)) => Some(Combinator::Or(Box::new(Self::from_table(&comb, table)?))),
            None => None,
        };
        
        return Ok(Where {
            expression: Expression { column, op: comp.expression.op, value: comp.expression.value.clone() },
            combinator,
        });
    }
}

pub fn where_builder(column: WhereColumn, op: Op, value: String) -> Where {
    return Where {
        expression: Expression { column, op, value },
        combinator: None,
    }
}



pub struct SelectBuilder {
    pub(crate) index: Option<(u64, usize)>,
    pub(crate) table: u64,
    pub(crate) columns: Vec<Column>,
    pub(crate) where_comps: Option<Where>
}

impl SelectBuilder {
    pub fn new(table: u64, columns: Vec<Column>) -> Self {
        return SelectBuilder {
            index: None,
            table,
            columns,
            where_comps: None
        }
    }
    
    pub fn where_cmp(self, comp: Where) -> Self {
        return Self {
            where_comps: Some(comp),
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
                comp.execute(&page.page, cell).is_ok_and(|result| result)
            }).cloned().collect::<Vec<_>>()
        } else {
            cells
        };
        
        let count = cells.len();
        if &self.columns == &[Column::Count] {
            return Ok(vec![vec![count.to_string()]])
        }
        
        return cells.iter().map(|cell| self.columns.iter().map(|column| {
            match column {
                Column::RowId => Ok(cell.rowid.to_string()),
                Column::Count => Ok(count.to_string()),
                Column::Column(column) => cell.get_column(&page.page, *column),
            }
        }).collect()).collect()
    }
    
    pub fn from_select_and_table(root_page: u64, select: ParsedSelect, table: Table) -> Result<SelectBuilder, ParsingError> {
        let columns = select.columns.iter().map(|column| match column {
            Some(column) => {
                table.get_column_by_name(column).ok_or(ParsingError::InvalidStatement).map(|value| {
                    match value {
                        TableColumn::RowId(_) => Column::RowId,
                        TableColumn::Column(index, _) => Column::Column(*index),
                    }
                })
            },
            None => Ok(Column::Count),
        }).collect::<Result<Vec<_>, _>>()?;
        
        let where_comps = select.where_comp.map(|comp| {
            Where::from_table(&comp, &table)
        }).transpose()?;
        
        return Ok(SelectBuilder { index: None, table: root_page, columns, where_comps: where_comps })
    }
}
