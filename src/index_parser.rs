use crate::{parsing_error::ParsingError, parsing_utils::find_keyword};

#[derive(Debug)]
pub struct IndexData {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub root_page: u64,
}

const INDEX_KEYWORD: &str = "INDEX";
const CREATE_KEYWORD: &str = "CREATE";
const ON_KEYWORD: &str = "ON";

pub fn parse_index(root_page: u64, sql: &str) -> Result<IndexData, ParsingError> {
    let sql = sql.trim();
    let create = find_keyword(sql, CREATE_KEYWORD).ok_or(ParsingError::InvalidStatement)?;
    let index = find_keyword(sql, INDEX_KEYWORD).ok_or(ParsingError::InvalidStatement)?;
    let on = find_keyword(sql, ON_KEYWORD).ok_or(ParsingError::InvalidStatement)?;

    let table_name_end = sql.find("(").ok_or(ParsingError::InvalidStatement)?;
    let columns_end = sql.rfind(")").ok_or(ParsingError::InvalidStatement)?;

    if create >= index || index >= on || on >= table_name_end || table_name_end >= columns_end {
        return Err(ParsingError::InvalidStatement);
    }

    let index_name_begin = index + INDEX_KEYWORD.len();
    let table_name_begin = on + ON_KEYWORD.len();
    let index_name = sql[index_name_begin..on].trim();
    let table_name = sql[table_name_begin..table_name_end].trim();
    let columns = sql[(table_name_end + 1)..columns_end]
        .split(",")
        .map(|v| v.trim().to_string())
        .collect();

    return Ok(IndexData {
        index_name: index_name.to_string(),
        table_name: table_name.to_string(),
        columns,
        root_page: root_page,
    });
}

pub fn get_table_index_for_column_in(
    index: Vec<IndexData>,
    columns: Vec<String>,
) -> Option<IndexData> {
    index
        .into_iter()
        .find(|index| columns.contains(&index.columns[0].to_lowercase()))
}
