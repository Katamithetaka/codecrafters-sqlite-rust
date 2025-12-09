use crate::{parsing_error::ParsingError, parsing_utils::find_keyword};

#[derive(Debug)]
pub enum TableColumn {
    RowId(String),
    Column(usize, String),
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<TableColumn>,
}

impl Table {
    pub fn get_column_by_name(&self, column_name: &str) -> Option<&TableColumn> {
        self.columns.iter().find(|column| {
            match column {
                TableColumn::RowId(name) => return name.as_str() == column_name,
                TableColumn::Column(_, name) =>  return name.as_str() == column_name,
            }
        })
    }
}

pub fn parse_table(sql: &str) -> Result<Table, ParsingError> {
    let sql = sql.trim();
    let create_keyword = find_keyword(sql, "CREATE").ok_or(ParsingError::InvalidStatement)?;
    let table_keyword = find_keyword(sql, "TABLE").ok_or(ParsingError::InvalidStatement)?;

    if create_keyword != 0 || table_keyword <= create_keyword {
        return Err(ParsingError::InvalidStatement);
    };

    let table_name_end = sql.find("(").ok_or(ParsingError::InvalidStatement)?;
    let table_name = sql[(table_keyword + "table".len())..table_name_end].trim();
    if table_name.is_empty() {
        return Err(ParsingError::InvalidStatement);
    };

    let columns_end = sql.rfind(")").ok_or(ParsingError::InvalidStatement)?;
    let columns = &sql[(table_name_end + 1)..columns_end];
    let mut columns = columns
        .split(",")
        .map(|column| {
            column
                .trim()
                .split(" ")
                .map(|token| token.trim())
                .filter(|token| !token.is_empty())
                .collect::<Vec<_>>()
        })
        .map(|column_tokens| {
            // oversimplification: it could come from a sequence too.
            if column_tokens
                .iter()
                .position(|v| v.to_lowercase() == "autoincrement")
                .is_some_and(|position| position != 0)
            {
                return TableColumn::RowId(column_tokens[0].to_string());
            } else {
                return TableColumn::Column(0, column_tokens[0].to_string());
            }
        })
        .collect::<Vec<_>>();
    let mut column_index = 0;
    for column in columns.iter_mut() {
        match column {
            TableColumn::RowId(_) => {
                column_index += 1;
            }
            TableColumn::Column(index, _) => {
                *index = column_index;
                column_index += 1;
            }
        }
    }

    return Ok(Table {
        name: table_name.to_string(),
        columns,
    });
}
