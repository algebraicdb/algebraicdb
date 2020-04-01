use crate::ast::*;
//use crate::pattern::*;
use crate::types::*;
pub fn translate_insert(ins: &Insert) -> String {
    let rows: String = ins
        .rows
        .iter()
        .map(|x| format!("({})", translate_row(x)))
        .collect::<Vec<String>>()
        .join(",");

    format!(
        "INSERT INTO {} ({}) VALUES {};",
        ins.table,
        ins.columns.join(","),
        rows
    )
}

fn translate_row(row: &Vec<Expr>) -> String {
    row.iter()
        .map(|x| translate_exp(x))
        .collect::<Vec<String>>()
        .join(",")
}

fn translate_exp(exp: &Expr) -> String {
    match exp {
        Expr::Value(val @ Value::Sum(_, _, _)) => format!(r#"'{}'"#, translate_value(val)),
        Expr::Value(val) => translate_value(val),
        _ => panic!("Can't insert non-values"),
    }
}

fn translate_value(val: &Value) -> String {
    match val {
        Value::Integer(i) => format!("{}", i),
        Value::Bool(b) => format!("{}", b),
        Value::Double(d) => format!("{}", d),
        Value::Sum(ns, var, vals) => format!(
            r#"{{"{}":[{}]}}"#,
            var,
            vals.iter()
                .map(|x| translate_value(x))
                .collect::<Vec<String>>()
                .join(",")
        ),
    }
}
