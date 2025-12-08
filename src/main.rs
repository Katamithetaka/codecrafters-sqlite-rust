use anyhow::{Result, bail};

use codecrafters_sqlite::{prelude::*, select_builder::{Op, SelectBuilder}};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    let args = vec!["program".to_string(), "sample.db".to_string(), ".tables".to_string()];
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
        },
        ".tables" => {
            let mut reader = SqliteReader::new(&args[1])?;
            let select = SelectBuilder::new(1, vec![Some(2)]).where_cmp(0, Op::Eq, "\"table\"".to_string());
            
            let table_names = select.execute(&mut reader)?;
            let result = table_names.iter().map(|columns| columns.join("|")).collect::<Vec<_>>().join(" ");
            println!("{result}");
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }
    

    Ok(())
}
