use lazy_static::lazy_static;
use regex::Regex;

static KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "DELETE", "DROP", "UPDATE", "JOIN",
    "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "SET", "ON", "AND", "OR", "CREATE", "TABLE", "TYPE",
    "AS", "VARIANT", "true", "false",
];

lazy_static! {
    static ref STR_REGEX: Regex = Regex::new(r#""((\\.)|[^\\"])*""#).unwrap();
}

pub(crate) struct Tokenizer<'a> {
    s: &'a str,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenType {
    Keyword,
    Word,
    Number,
    String,
    Symbol,
    Whitespace,
}

impl TokenType {
    pub fn of(c: char) -> TokenType {
        if c.is_whitespace() {
            TokenType::Whitespace
        } else if c.is_numeric() {
            TokenType::Number
        } else if c == '"' {
            TokenType::String
        } else if c.is_ascii_punctuation() && c != '_' {
            TokenType::Symbol
        } else {
            TokenType::Word
        }
    }
}

impl<'a> Tokenizer<'a> {
    pub fn from(s: &'a str) -> Self {
        Tokenizer { s }
    }
}
impl<'a> Iterator for Tokenizer<'a> {
    type Item = (&'a str, TokenType);

    fn next(&mut self) -> Option<Self::Item> {
        if self.s.is_empty() {
            None
        } else {
            let mut chars = self.s.chars();

            let first = chars.next().unwrap();
            let mut end = first.len_utf8();

            let mut tt = TokenType::of(first);

            match tt {
                TokenType::String => {
                    let result;
                    match STR_REGEX.find(self.s) {
                        Some(m) if m.start() == 0 => {
                            result = &self.s[..m.end()];
                            self.s = &self.s[m.end()..];
                        }
                        _ => {
                            result = self.s;
                            self.s = "";
                        }
                    }
                    Some((result, tt))
                }
                _ => {
                    for c in chars {
                        if TokenType::of(c) == tt {
                            end += c.len_utf8();
                        } else {
                            break;
                        }
                    }

                    let result = &self.s[..end];

                    self.s = &self.s[end..];

                    if tt == TokenType::Word && KEYWORDS.contains(&result) {
                        tt = TokenType::Keyword;
                    }

                    Some((result, tt))
                }
            }
        }
    }
}
