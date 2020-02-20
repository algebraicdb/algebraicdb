use crate::ast::*;
//use crate::pattern::*;
use crate::types::*;
pub fn translate(ins: &Insert) -> String {
    format!("INSERT INTO {} ({}) VALUES ({});",
    ins.table, 
    ins.columns.join(","), 
    ins.values.iter().map(|x| translate_exp(x)).collect::<Vec<String>>().join(","))
}

fn translate_exp(exp: &Expr) -> String {
    match exp {
        Expr::Value(val@Value::Sum(_, _, _)) => format!(r#"'{{{}}}'"#, translate_value(val)) ,
        Expr::Value(val) => translate_value(val),
        _ => panic!("Can't insert non-values") 
    }
}

fn translate_value(val: &Value) -> String {
    match val {
        Value::Integer(i) => format!("{}", i),
        Value::Bool(b) => format!("{}", b),
        Value::Double(d) =>format!("{}", d),
        Value::Sum(ns, var, vals) => {
            format!(r#""{}{}":[{}]"#, match ns {
                Some(s) => format!("{}::", s),
                None => String::new(),
            },
            var,
            vals.iter().map(|x| translate_value(x)).collect::<Vec<String>>().join(",")
        )
        },
    }
}