use regex::Regex;


pub fn find_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let re = Regex::new(&format!(r"(?i)\b{}\b", regex::escape(keyword))).ok()?;
    re.find(sql).map(|mat| mat.start())
}
