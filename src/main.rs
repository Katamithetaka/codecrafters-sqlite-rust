use anyhow::{Result, bail};

use codecrafters_sqlite::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
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
        _ => bail!("Missing or invalid command passed: {}", command),
    }
    

    Ok(())
}
