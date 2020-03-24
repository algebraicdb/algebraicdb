use crate::ast::{Expr, Select, SelectFrom, WhereClause, WhereItem};
use crate::{pattern::Pattern, types::Value};
use serde_json::Value::{Array, Bool, Null, Number, Object, String as jString};

pub fn translate_select_result(item: &serde_json::Value) -> String {
    match item {
        Object(map) => map
            .iter()
            .map(|(k, v)| format!("{}({})", k, translate_select_result(v)))
            .collect::<Vec<String>>()
            .join("WTF"),
        Array(arr) => arr
            .iter()
            .map(|v| translate_select_result(v))
            .collect::<Vec<String>>()
            .join(","),
        Bool(v) => format!("{:?}", v),
        jString(v) => format!("{:?}", v),
        Number(v) => format!("{:?}", v),
        Null { .. } => "".to_string(),
    }
}

pub fn translate_select(select: &Select) -> String {
    format!(
        "SELECT {} {} {};",
        translate_item(&select.items),
        translate_from(&select.from),
        match &select.where_clause {
            Some(clause) => translate_where(&clause),
            None => String::new(),
        }
    )
}

fn translate_from(sfrom: &Option<SelectFrom>) -> String {
    match sfrom {
        Some(SelectFrom::Table(tab)) => format!("FROM {}", *tab),
        None => String::new(),
        _ => unimplemented!(),
    }
}

fn translate_item(items: &Vec<Expr>) -> String {
    format!(
        "{}",
        items
            .iter()
            .map(|a| translate_expr(&a))
            .collect::<Vec<String>>()
            .join(",")
    )
}

fn translate_expr(expr: &Expr) -> String {
    match expr {
        Expr::Ident(s) => s.clone(),
        Expr::Value(Value::Sum(_, _, _)) => unimplemented!(),
        Expr::Value(val) => format!("{}", val),
        Expr::Equals(e1, e2) => format!("({} = {})", translate_expr(e1), translate_expr(e2)),
        Expr::NotEquals(e1, e2) => format!("({} != {})", translate_expr(e1), translate_expr(e2)),
        Expr::LessEquals(e1, e2) => format!("({} <= {})", translate_expr(e1), translate_expr(e2)),
        Expr::LessThan(e1, e2) => format!("({} < {})", translate_expr(e1), translate_expr(e2)),
        Expr::GreaterThan(e1, e2) => format!("({} > {})", translate_expr(e1), translate_expr(e2)),
        Expr::GreaterEquals(e1, e2) => {
            format!("({} >= {})", translate_expr(e1), translate_expr(e2))
        }
        Expr::And(e1, e2) => format!("({} AND {})", translate_expr(e1), translate_expr(e2)),
        Expr::Or(e1, e2) => format!("({} OR {})", translate_expr(e1), translate_expr(e2)),
        _ => unimplemented!(),
    }
}

fn new_path(path: &String, n: usize, name: &String, pattern: &Pattern) -> String {
    match pattern {
        Pattern::Variant { .. } => format!("{} -> '{}' -> {}", path, name, n),
        _ => format!("{} -> '{}' ->> {}", path, name, n),
    }
}

fn translate_pattern(path: &String, pattern: &Pattern) -> String {
    match pattern {
        Pattern::Ignore => format!("{} IS NOT NULL", path),
        Pattern::Int(val) => format!("{} = '{}'", path, val),
        Pattern::Bool(val) => format!("{} = '{}'", path, val),
        Pattern::Double(val) => format!("{} = '{}'", path, val),
        Pattern::Variant {
            name,
            namespace: _,
            sub_patterns,
        } => sub_patterns
            .iter()
            .enumerate()
            .map(|(n, pat)| translate_pattern(&new_path(path, n, name, pat), pat))
            .collect::<Vec<String>>()
            .join(" AND "),
        _ => unimplemented!(),
    }
}

fn translate_where(clause: &WhereClause) -> String {
    let items_string: Vec<String> = clause
        .items
        .iter()
        .map(|x| translate_where_item(x))
        .collect();
    format!("WHERE {}", items_string.join(" AND "))
}

fn translate_where_item(witem: &WhereItem) -> String {
    match witem {
        WhereItem::Expr(expr) => translate_expr(expr),
        WhereItem::Pattern(name, pattern) => translate_pattern(name, pattern),
    }
}