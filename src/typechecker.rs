// Wheres ska evalueras till bools
//
//
use crate::ast::*;
//use crate::table::*;
use crate::global::ResourcesGuard;
use crate::types::*;

pub struct Context<'a> {
    pub globals: &'a ResourcesGuard<'a>,
    pub locals: Vec<(String, TypeId)>,
}

#[derive(Debug)]
pub enum TypeError {
    NotSupported(&'static str),
    Undefined(String),
    MismatchingTypes { type_1: TypeId, type_2: TypeId },
    InvalidType { expected: TypeId, actual: TypeId },
}

pub fn check_stmt(stmt: &Stmt, globals: &ResourcesGuard<'_>) -> Result<(), TypeError> {
    let mut ctx = Context {
        globals,
        locals: vec![],
    };

    match stmt {
        Stmt::Select(select) => check_select(select, &mut ctx),
        Stmt::Update(update) => check_update(update, &mut ctx),
        Stmt::Delete(_delete) => unimplemented!("Stmt::Delete"),
        Stmt::Insert(_insert) => unimplemented!("Stmt::Insert"),
        Stmt::CreateType(_create) => unimplemented!("Stmt::CreateType"),
    }
}

fn check_select(select: &Select, ctx: &mut Context) -> Result<(), TypeError> {
    match &select.from {
        Some(SelectFrom::Select(nsel)) => check_select(&nsel, ctx),
        Some(SelectFrom::Table(_table)) => Err(TypeError::NotSupported("Select from table")),
        Some(SelectFrom::Join(_join)) => Err(TypeError::NotSupported("Select from join")),
        None => Err(TypeError::NotSupported("Selecting from nothing")),
    }
}

fn check_update(update: &Update, ctx: &mut Context) -> Result<(), TypeError> {
    let table = ctx.globals.read_table(&update.table);
    let schema = table.get_schema();

    for (name, type_id) in &schema.columns {
        ctx.locals.push((name.clone(), *type_id));
    }

    for assignment in &update.ass {
        match schema.column(&assignment.col) {
            None => return Err(TypeError::Undefined(assignment.col.clone())),
            Some(expected_type_id) => {
                ctx.locals.push((assignment.col.clone(), expected_type_id));
                let expr_type_id = check_expr(&assignment.expr, ctx)?;

                if expected_type_id != expr_type_id {
                    return Err(TypeError::InvalidType {
                        expected: expected_type_id,
                        actual: expr_type_id,
                    });
                }
            }
        }
    }

    Ok(())
}
fn check_expr(expr: &Expr, ctx: &Context) -> Result<TypeId, TypeError> {
    match expr {
        Expr::Ident(ident) => ctx
            .locals
            .iter()
            .find(|(name, _)| name == ident)
            .map(|(_, type_id)| *type_id)
            .ok_or_else(|| TypeError::Undefined(ident.clone())),

        Expr::Value(value) => value
            .type_of(&ctx.globals.types)
            .ok_or_else(|| TypeError::Undefined("TODO".into())),

        // All types are currently Eq and Ord
        Expr::Equals(e1, e2)
        | Expr::NotEquals(e1, e2)
        | Expr::LessEquals(e1, e2)
        | Expr::LessThan(e1, e2)
        | Expr::GreaterEquals(e1, e2)
        | Expr::GreaterThan(e1, e2) => {
            let type_1 = check_expr(e1, ctx)?;
            let type_2 = check_expr(e2, ctx)?;
            assert_type_eq(type_1, type_2)?;

            // FIXME: Stringly types!
            let bool_id = ctx
                .globals
                .types
                .get_id("Bool")
                .ok_or_else(|| TypeError::Undefined("Bool".into()))?;
            Ok(bool_id)
        }

        Expr::And(e1, e2) | Expr::Or(e1, e2) => {
            let type_1 = check_expr(e1, ctx)?;
            let type_2 = check_expr(e2, ctx)?;

            // FIXME: Stringly types!
            let bool_id = ctx
                .globals
                .types
                .get_id("Bool")
                .ok_or_else(|| TypeError::Undefined("Bool".into()))?;

            assert_type_as(bool_id, type_1)?;
            assert_type_as(bool_id, type_2)
        }
    }
}

fn assert_type_eq(type_1: TypeId, type_2: TypeId) -> Result<TypeId, TypeError> {
    if type_1 != type_2 {
        Err(TypeError::MismatchingTypes { type_1, type_2 })
    } else {
        Ok(type_1)
    }
}

fn assert_type_as(actual: TypeId, expected: TypeId) -> Result<TypeId, TypeError> {
    if actual != expected {
        Err(TypeError::InvalidType { actual, expected })
    } else {
        Ok(actual)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;
    use crate::global::{Resource, ResourcesGuard};
    use crate::table::tests::create_type_map;
    use std::sync::{Arc, RwLock};

    #[test]
    fn type_check_exprs() {
        let (_ids, types) = create_type_map();
        let types = Arc::new(RwLock::new(types));

        let dummy_ctx = Context {
            globals: &ResourcesGuard {
                types: Resource::Read(types.read().unwrap()),
                tables: vec![],
            },
            locals: vec![],
        };

        let valid_examples = vec![
            Expr::Equals(
                box Expr::Value(Value::Integer(3)),
                box Expr::Value(Value::Integer(2)),
            ),
            Expr::Equals(
                box Expr::Value(Value::Bool(true)),
                box Expr::Value(Value::Bool(true)),
            ),
            Expr::Equals(
                box Expr::Value(Value::Double(0.0)),
                box Expr::Value(Value::Double(0.1)),
            ),
            Expr::And(
                box Expr::Value(Value::Bool(false)),
                box Expr::GreaterThan(
                    box Expr::Value(Value::Integer(42)),
                    box Expr::Value(Value::Integer(0)),
                ),
            ),
        ];

        let invalid_examples = vec![
            Expr::Equals(
                box Expr::Value(Value::Bool(false)),
                box Expr::Value(Value::Integer(2)),
            ),
            Expr::And(
                box Expr::Value(Value::Integer(0)),
                box Expr::Value(Value::Integer(0)),
            ),
        ];

        for example in valid_examples {
            check_expr(&example, &dummy_ctx).unwrap();
        }

        for example in invalid_examples {
            check_expr(&example, &dummy_ctx).unwrap_err();
        }
    }
}
