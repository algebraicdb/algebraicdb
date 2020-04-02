use crate::ast::{Expr, Select, SelectFrom, WhereClause, WhereItem};
use crate::{pattern::Pattern, types::Value};
use serde_json::Value::{Array, Bool, Null, Number, Object, String as jString};
use std::collections::HashMap;

pub fn translate_select_result(item: &serde_json::Value) -> String {
    match item {
        Object(map) => map
            .iter()
            .map(|(k, v)| format!("{}({})", k, translate_select_result(v)))
            .collect::<Vec<String>>()
            .join("__________"),
        Array(arr) => arr
            .iter()
            .map(|v| translate_select_result(v))
            .collect::<Vec<String>>()
            .join(", "),
        Bool(v) => format!("{}", v),
        jString(v) => format!("{}", v),
        Number(v) => format!("{}", v),
        Null { .. } => "".to_string(),
    }
}

pub fn translate_select(select: &Select) -> String {
    let mut bindings = HashMap::new();
    let where_ = match &select.where_clause {
        Some(clause) => translate_where(&clause, &mut bindings),
        None => String::new(),
    };
    format!(
        "SELECT {} {} {};",
        translate_item(&select.items, &bindings),
        translate_from(&select.from),
        where_,
    )
}

fn translate_from(sfrom: &Option<SelectFrom>) -> String {
    match sfrom {
        Some(SelectFrom::Table(tab)) => format!("FROM {}", *tab),
        None => String::new(),
        _ => unimplemented!(),
    }
}

fn translate_item(items: &Vec<Expr>, bindings: &HashMap<String, String>) -> String {
    format!(
        "{}",
        items
            .iter()
            .map(|a| translate_expr(&a, bindings))
            .collect::<Vec<String>>()
            .join(",")
    )
}

fn translate_expr(expr: &Expr, bindings: &HashMap<String, String>) -> String {
    match expr {
        Expr::Ident(s) => bindings.get(s).unwrap_or_else(|| s).clone(),
        Expr::Value(Value::Sum(_, _, _)) => unimplemented!(),
        Expr::Value(val) => format!("{}", val),
        Expr::Equals(e1, e2) => format!(
            "({} = {})",
            translate_expr(e1, bindings),
            translate_expr(e2, bindings)
        ),
        Expr::NotEquals(e1, e2) => format!(
            "({} != {})",
            translate_expr(e1, bindings),
            translate_expr(e2, bindings)
        ),
        Expr::LessEquals(e1, e2) => format!(
            "({} <= {})",
            translate_expr(e1, bindings),
            translate_expr(e2, bindings)
        ),
        Expr::LessThan(e1, e2) => format!(
            "({} < {})",
            translate_expr(e1, bindings),
            translate_expr(e2, bindings)
        ),
        Expr::GreaterThan(e1, e2) => format!(
            "({} > {})",
            translate_expr(e1, bindings),
            translate_expr(e2, bindings)
        ),
        Expr::GreaterEquals(e1, e2) => format!(
            "({} >= {})",
            translate_expr(e1, bindings),
            translate_expr(e2, bindings)
        ),
        Expr::And(e1, e2) => format!(
            "({} AND {})",
            translate_expr(e1, bindings),
            translate_expr(e2, bindings)
        ),
        Expr::Or(e1, e2) => format!(
            "({} OR {})",
            translate_expr(e1, bindings),
            translate_expr(e2, bindings)
        ),
        _ => unimplemented!(),
    }
}

fn new_path(path: &String, n: usize, name: &String, pattern: &Pattern) -> String {
    match pattern {
        Pattern::Variant { .. } | Pattern::Binding { .. } => {
            format!("{} -> '{}' -> {}", path, name, n)
        }
        _ => format!("{} -> '{}' ->> {}", path, name, n),
    }
}

fn translate_pattern(
    path: &String,
    pattern: &Pattern,
    bindings: &mut HashMap<String, String>,
) -> String {
    match pattern {
        Pattern::Ignore => format!("{} IS NOT NULL", path),
        Pattern::Int(val) => format!("{} = '{}'", path, val),
        Pattern::Bool(val) => format!("{} = '{}'", path, val),
        Pattern::Double(val) => format!("{} = '{}'", path, val),
        Pattern::Variant {
            name,
            namespace: _,
            sub_patterns,
        } => {
            if sub_patterns.len() != 0 {
                sub_patterns
                    .iter()
                    .enumerate()
                    .map(|(n, pat)| translate_pattern(&new_path(path, n, name, pat), pat, bindings))
                    .collect::<Vec<String>>()
                    .join(" AND ")
            } else {
                format!("{} -> '{}' IS NOT NULL", path, name)
            }
        }
        Pattern::Binding(bin) => {
            bindings.insert(bin.clone(), path.clone());
            format!("{} IS NOT NULL", path)
        }
    }
}

fn translate_where(clause: &WhereClause, bindings: &mut HashMap<String, String>) -> String {
    let items_string: Vec<String> = clause
        .items
        .iter()
        .map(|x| translate_where_item(x, bindings))
        .collect();
    format!("WHERE {}", items_string.join(" AND "))
}

fn translate_where_item(witem: &WhereItem, bindings: &mut HashMap<String, String>) -> String {
    match witem {
        WhereItem::Expr(expr) => translate_expr(expr, bindings),
        WhereItem::Pattern(name, pattern) => translate_pattern(name, pattern, bindings),
    }
}
