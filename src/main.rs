
use anyhow::{Result, bail};

use codecrafters_sqlite::{
    index_parser::{get_table_index_for_column_in, parse_index},
    prelude::*,
    select_builder::{Column, Op, SelectBuilder, WhereColumn, where_builder},
    select_parser::{parse_select, quoted},
    table_parser::parse_table,
};

const SCHEMA_TYPE_COLUMN: usize = 0;
#[allow(unused)]
const SCHEMA_OBJECT_NAME_COLUMN: usize = 1;
const SCHEMA_TABLE_NAME_COLUMN: usize = 2;
const SCHEMA_ROOT_PAGE_COLUMN: usize = 3;
const SCHEMA_SQL_COLUMN: usize = 4;
const SCHEMA_PAGE_NUMBER: u64 = 1;
const TABLE_TYPE_STR: &str = "table";
#[allow(unused)]
const INDEX_TYPE_STR: &str = "index";

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    //let args = vec!["program".to_string(), "sample.db".to_string(), ".tables".to_string()];
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }
    eprintln!("Logs from your program will appear here!");

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut reader = SqliteReader::new(&args[1])?;
            let first_page = reader.read_page(1)?;

            println!("database page size: {}", reader.header.page_size);
            println!("number of tables: {}", first_page.page_header.cell_count)
        }
        ".tables" => {
            let mut reader = SqliteReader::new(&args[1])?;
            let select_where = where_builder(
                WhereColumn::Column(SCHEMA_TYPE_COLUMN),
                Op::Eq,
                quoted(TABLE_TYPE_STR),
            );
            let select = SelectBuilder::new(
                SCHEMA_PAGE_NUMBER,
                vec![Column::Column(SCHEMA_TABLE_NAME_COLUMN)],
            )
            .where_cmp(select_where);

            let table_names = select.execute(&mut reader)?;
            let result = table_names
                .iter()
                .map(|columns| columns.join("|"))
                .collect::<Vec<_>>()
                .join(" ");
            println!("{result}");
        }
        request => {
            // request parsing
            let request = parse_select(request)?;

            eprintln!("{}", request);
            // table parsing
            let mut reader = SqliteReader::new(&args[1])?;
            let table_name = &request.table_name;
            let select_where = where_builder(
                WhereColumn::Column(SCHEMA_TYPE_COLUMN),
                Op::Eq,
                quoted(TABLE_TYPE_STR),
            )
            .and(where_builder(
                WhereColumn::Column(SCHEMA_TABLE_NAME_COLUMN),
                Op::Eq,
                quoted(table_name),
            ));
            let select = SelectBuilder::new(
                SCHEMA_PAGE_NUMBER,
                vec![
                    Column::Column(SCHEMA_ROOT_PAGE_COLUMN),
                    Column::Column(SCHEMA_SQL_COLUMN),
                ],
            )
            .where_cmp(select_where);

            let table_data = select.execute(&mut reader)?;

            // index parsing
            let where_columns = request
                .where_comp
                .as_ref()
                .map(|where_v| where_v.get_columns())
                .unwrap_or_default();

            let where_columns_count = where_columns.len();
            let select_where = where_builder(
                WhereColumn::Column(SCHEMA_TYPE_COLUMN),
                Op::Eq,
                quoted(INDEX_TYPE_STR),
            )
            .and(where_builder(
                WhereColumn::Column(SCHEMA_TABLE_NAME_COLUMN),
                Op::Eq,
                quoted(table_name),
            ));

            let select = SelectBuilder::new(
                SCHEMA_PAGE_NUMBER,
                vec![
                    Column::Column(SCHEMA_ROOT_PAGE_COLUMN),
                    Column::Column(SCHEMA_SQL_COLUMN),
                ],
            )
            .where_cmp(select_where);

            let index_data = select.execute(&mut reader)?;
            
            let table_indices = index_data
                .iter()
                .map(|v| parse_index(u64::from_str_radix(&v[0], 10).unwrap_or(0), v[1].as_str()))
                .collect::<Result<_, _>>()?;
            
            let mut table_index = get_table_index_for_column_in(table_indices, where_columns); // only accept at most one table index for now.
            if where_columns_count != 1 {
                table_index = None; // Only support one column for index search (because I don't want to implement complex logic for indices)
            }
            
            assert!(table_data.len() == 1);

            let (root_page, sql) = (
                u64::from_str_radix(&table_data[0][0], 10)?,
                table_data[0][1].clone(),
            );

            let table = parse_table(&sql)?;
            let select =
                SelectBuilder::from_select_and_table(root_page, request, table, table_index)?;

            let result = select.execute(&mut reader)?;
            let result = result
                .iter()
                .map(|columns| columns.join("|"))
                .collect::<Vec<_>>()
                .join("\n");
            println!("{result}");
        }
    }

    Ok(())
}
