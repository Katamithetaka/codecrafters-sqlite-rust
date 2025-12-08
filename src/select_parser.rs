use std::fmt::Display;

use crate::{parsing_error::ParsingError, select_builder::Op};

pub fn is_quoted(value: &str) -> bool {
    return value.starts_with("\"") && value.ends_with("\"")
}

pub fn quoted<T: AsRef<str>>(value: T) -> String {
    let v = value.as_ref();
    if v.starts_with("\"") && v.ends_with("\"") {
        return v.to_string();
    }
    if v.starts_with("'") && v.ends_with("'") {
        return format!("\"{}\"", &v[1..v.len() - 1]);
    }
    return format!("\"{v}\"");
}

pub fn parse_value(value: &str) -> String {
    if value.starts_with("\"") || value.starts_with("\'") { return quoted(value) };
    return value.to_string();
}


pub enum ParsedCombinator {
    And(Box<ParsedWhere>),
    Or(Box<ParsedWhere>),
}

pub struct ParsedExpression {
    pub column: String,
    pub op: Op,
    pub value: String,
}

pub struct ParsedWhere {
    pub expression: ParsedExpression,
    pub combinator: Option<ParsedCombinator>,
}

pub struct ParsedSelect {
    pub table_name: String,
    pub columns: Vec<Option<String>>,
    pub where_comp: Option<ParsedWhere>,
}

impl Display for ParsedWhere {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{} {} {}",
            self.expression.column,
            self.expression.op.as_str(),
            self.expression.value
        ))?;

        match &self.combinator {
            Some(ParsedCombinator::And(value)) => f.write_fmt(format_args!(" AND {}", value)),
            Some(ParsedCombinator::Or(value)) => f.write_fmt(format_args!(" OR {}", value)),
            None => Ok(()),
        }
    }
}

impl Display for ParsedSelect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let columns = self
            .columns
            .iter()
            .map(|c| match c {
                None => "COUNT(*)".to_string(),
                Some(v) => v.to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        f.write_fmt(format_args!("SELECT {} FROM {}", columns, self.table_name))?;
        match &self.where_comp {
            Some(where_v) => f.write_fmt(format_args!(" WHERE {}", where_v)),
            None => return Ok(()),
        }
    }
}

pub fn parse_comma_separated_after(
    select: &str,
    keyword: &str,
    index: usize,
    limit: Option<usize>,
) -> Vec<String> {
    let begin_index = index + keyword.len();
    let value = match limit {
        Some(end) => &select[begin_index..end],
        None => &select[begin_index..],
    };
    return value
        .split(",")
        .map(|value| value.trim().to_string())
        .collect();
}

/*
 * (.1 == true => And)
 * (.1 == false => Or)
 */
pub fn find_next_where_comp(select: &str, index: usize) -> Option<(usize, bool)> {
    let and_index = select[index..].to_uppercase().find("AND");
    let or_index = select[index..].to_uppercase().find("OR");

    match (and_index, or_index) {
        (Some(and_index), Some(or_index)) => Some((and_index.min(or_index), and_index < or_index)),
        (None, Some(or_index)) => Some((or_index, false)),
        (Some(and_index), None) => Some((and_index, true)),
        (None, None) => None,
    }
}

pub fn parse_where_cmp(select: &str) -> Result<ParsedWhere, ParsingError> {
    let select = select.trim();
    let tokens = select
        .split(" ")
        .filter(|entry| !entry.is_empty())
        .collect::<Vec<_>>();
    
    assert!(tokens.len() >= 3);

    let column = tokens[0].trim();
    let op = match tokens[1] {
        "=" => Op::Eq,
        ">" => Op::Gt,
        ">=" => Op::GtEq,
        "<" => Op::Lt,
        "<=" => Op::LtEq,
        _ => return Err(ParsingError::InvalidStatement),
    };
    let op_index = match select.find(op.as_str()) {
        Some(value) => value + op.as_str().len(),
        None => return Err(ParsingError::InvalidStatement),
    };

    let value = parse_value(&select[op_index..].trim());

    return Ok(ParsedWhere {
        expression: ParsedExpression {
            column: column.to_string(),
            op,
            value: value.to_string(),
        },
        combinator: None,
    });
}

pub fn parse_where(select: &str, index: usize) -> Result<ParsedWhere, ParsingError> {
    let next_cmp = find_next_where_comp(select, index);
    match next_cmp {
        Some((end, /* is_and = */ true)) => Ok(ParsedWhere {
            combinator: Some(ParsedCombinator::And(Box::new(parse_where(
                select,
                index + end + "AND".len(),
            )?))),
            ..parse_where_cmp(&select[index..(index + end)])?
        }),
        Some((end, /* is_and = */ false)) => Ok(ParsedWhere {
            combinator: Some(ParsedCombinator::Or(Box::new(parse_where(
                select,
                index + end + "OR".len(),
            )?))),
            ..parse_where_cmp(&select[index..(index + end)])?
        }),
        None => parse_where_cmp(&select[index..]),
    }
}

pub fn parse_select(select: &str) -> Result<ParsedSelect, ParsingError> {
    let select = select.trim_start();
    let select_keyword = select.to_uppercase().find("SELECT");
    let from_keyword = select.to_uppercase().find("FROM");
    let where_keyword = select.to_uppercase().find("WHERE");

    let (Some(select_keyword), Some(from_keyword)) = (select_keyword, from_keyword) else {
        eprintln!("Couldn't find SELECT or FROM");
        return Err(ParsingError::InvalidStatement);
    };

    if !(select_keyword < from_keyword) {
        eprintln!("Select keyword should always be before from");
        return Err(ParsingError::InvalidStatement);
    }
    if let Some(where_keyword) = where_keyword && !(from_keyword < where_keyword) {
        eprintln!("Where keyword should always be after from");
        return Err(ParsingError::InvalidStatement);
    }

    let column_names: Vec<Option<String>> =
        parse_comma_separated_after(select, "SELECT", select_keyword, Some(from_keyword))
            .iter()
            .map(|column| match column.as_str() {
                "COUNT(*)" => None,
                value => Some(value.to_string()),
            })
            .collect();
    let table_name = parse_comma_separated_after(select, "FROM", from_keyword, where_keyword);
    
    let where_cmp = where_keyword
        .map(|index| parse_where(select, index + "WHERE".len()))
        .transpose()?;

    if table_name.len() > 1 {
        eprintln!("Can't currently handle more than one table");
        return Err(ParsingError::InvalidStatement);
    }
    
    if column_names.is_empty() {
        eprintln!("Couldn't find any column names");
        return Err(ParsingError::InvalidStatement);
    }

    let table_name = table_name[0].clone();

    return Ok(ParsedSelect {
        table_name,
        columns: column_names,
        where_comp: where_cmp,
    });
}
