use crate::ast::*;
use crate::global::ResourcesGuard;
use crate::types::*;
use std::collections::HashMap;

pub struct Context<'a> {
    pub globals: &'a ResourcesGuard<'a>,
    locals: Vec<Scope>,
}

type Scope = HashMap<String, Vec<TypeId>>;

impl<'a> Context<'a> {
    pub fn new(globals: &'a ResourcesGuard<'a>) -> Self {
        Context {
            globals,
            locals: vec![HashMap::new()],
        }
    }

    pub fn search_locals(&self, ident: &str) -> Result<TypeId, TypeError> {
        self.locals
            .iter()
            .filter_map(|scope| scope.get(ident))
            .next()
            .map(|res| if res.len() == 1 {
                Ok(res[0])
            } else {
                Err(TypeError::AmbiguousReference(ident.to_string()))
            })
            .unwrap_or_else(|| Err(TypeError::Undefined(ident.to_string())))
    }

    pub fn push_locals_scope(&mut self) {
        self.locals.push(HashMap::new());
    }

    pub fn pop_locals_scope(&mut self) -> Scope {
        self.locals.pop().unwrap_or_else(|| panic!("No scope :(("))
    }

    pub fn merge_scope(&mut self, other: Scope) {
        let scope = self.locals.last_mut().unwrap_or_else(|| panic!("Noo scoope :|"));
        for (name, mut types) in other {
            let existing = scope.entry(name).or_default();
            existing.append(&mut types);
        }
    }

    pub fn push_local(&mut self, name: String, type_id: TypeId) {
        self.locals.last_mut()
            .unwrap_or_else(|| panic!("No scope :c"))
            .entry(name)
            .or_default()
            .push(type_id)
    }
}

#[derive(Debug)]
pub enum TypeError {
    NotSupported(&'static str),
    Undefined(String),
    AmbiguousReference(String),
    AlreadyDefined,
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
        Stmt::Delete(delete) => check_delete(delete, &mut ctx),
        Stmt::Insert(_insert) => unimplemented!("Stmt::Insert"),
        Stmt::CreateType(create) => check_create_type(create, &mut ctx),
    }
}


fn find_bool (ctx: &Context) -> Result<TypeId, TypeError> {
    // FIXME: Stringly types!
    return ctx.globals
        .types
        .get_id("Bool")
        .ok_or_else(|| TypeError::Undefined("Bool".into()))
}

fn check_delete(delete: &Delete, ctx: &mut Context) -> Result<(), TypeError>{
    let bool_id = find_bool(ctx)?;
    match &delete.where_clause {
        Some(WhereClause(cond)) => {
            import_table_columns(&delete.table, ctx);

            let cond_type = check_expr(cond, &ctx)?;
            assert_type_as(cond_type, bool_id)?;

            Ok(())
        }

        None => Ok(()),
    }
}

fn check_select(select: &Select, ctx: &mut Context) -> Result<(), TypeError> {
    if let Some(from) = &select.from {
        check_select_from(from, ctx)?;
    }

    for item in &select.items {
        check_select_item(item, ctx)?;
    }

    Ok(())
}

fn check_select_from(from: &SelectFrom, ctx: &mut Context) -> Result<(), TypeError> {
    match from {
        SelectFrom::Select(nsel) => {
            check_select(&nsel, ctx)?;
        }
        SelectFrom::Table(name) => {
            import_table_columns(name, ctx);
        }
        SelectFrom::Join(join) => {
            ctx.push_locals_scope();
            check_select_from(&join.table_a, ctx)?;
            let scope = ctx.pop_locals_scope();

            ctx.push_locals_scope();
            check_select_from(&join.table_a, ctx)?;

            ctx.merge_scope(scope);

            if let Some(on_clause) = &join.on_clause {
                let clause_type = check_expr(on_clause, ctx)?;
                assert_type_as(clause_type, find_bool(ctx)?)?;
            }
        },
    }
    Ok(())
}

fn check_select_item(item: &SelectItem, ctx: &Context) -> Result<(), TypeError> {
    match item {
        SelectItem::Expr(expr) => {
            check_expr(expr, ctx)?;
        }
        SelectItem::Pattern(_ident, _pattern) => unimplemented!("Type check patterns"),
    }
    Ok(())
}

fn import_table_columns<'a>(name: &str, ctx: &'a mut Context) {
    let table = ctx.globals.read_table(name);
    let schema = table.get_schema();

    for (name, type_id) in &schema.columns {
        ctx.push_local(name.clone(), *type_id);
    }
}

fn check_update(update: &Update, ctx: &mut Context) -> Result<(), TypeError> {
    import_table_columns(&update.table, ctx);
    let table = ctx.globals.read_table(&update.table);
    let schema = table.get_schema();

    for assignment in &update.ass {
        match schema.column(&assignment.col) {
            None => return Err(TypeError::Undefined(assignment.col.clone())),
            Some(expected_type_id) => {
                ctx.push_local(assignment.col.clone(), expected_type_id);
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

fn check_create_type(create: &CreateType, ctx: &mut Context) -> Result<(), TypeError> {
    // For a type:
    // MyVariant = Var1 TypeA | Var2 TypeB TypeC
    // We have to check that the type name MyVariant is not taken
    // as well ass that Type{A,B,C} exists
    // TODO: recursive types
    match create {
        CreateType::Variant(name, variants) => {
            if ctx.globals.types.get_id(name).is_some() {
                return Err(TypeError::AlreadyDefined);
            }

            for (_variant, types) in variants {
                for t_name in types {
                    if ctx.globals.types.get_id(t_name).is_none() {
                        return Err(TypeError::Undefined(t_name.clone()));
                    }
                }
            }
        }
    }
    Ok(())
}

fn check_expr(expr: &Expr, ctx: &Context) -> Result<TypeId, TypeError> {
    match expr {
        Expr::Ident(ident) => ctx.search_locals(ident),

        Expr::Value(value) => type_of_value(&value, &ctx.globals.types),

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

            Ok(find_bool(ctx)?)
        }

        Expr::And(e1, e2) | Expr::Or(e1, e2) => {
            let type_1 = check_expr(e1, ctx)?;
            let type_2 = check_expr(e2, ctx)?;

            let bool_id = find_bool(ctx)?;

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

pub fn type_of_value(value: &Value, types: &TypeMap) -> Result<TypeId, TypeError> {
    match value {
        // TODO: maybe we should have a list of "keywords" somewhere we can use
        Value::Integer(_) => types
            .get_id("Integer")
            .ok_or_else(|| panic!("Integer is undefined")),
        Value::Double(_) => types
            .get_id("Double")
            .ok_or_else(|| panic!("Double is undefined")),
        Value::Bool(_) => types
            .get_id("Bool")
            .ok_or_else(|| panic!("Bool is undefined")),
        Value::Sum(namespace, variant, _) => {
            if let Some(namespace) = namespace {
                types
                    .get_id(namespace)
                    .ok_or_else(|| TypeError::Undefined(namespace.clone()))
            } else {
                let possible_constructors = types.constructors_of(variant);

                if let Some(possible_constructors) = possible_constructors {
                    if possible_constructors.len() == 0 {
                        Err(TypeError::Undefined(variant.clone()))
                    } else if possible_constructors.len() == 1 {
                        Ok(possible_constructors[0])
                    } else {
                        Err(TypeError::AmbiguousReference(variant.clone()))
                    }
                } else {
                    Err(TypeError::Undefined(variant.clone()))
                }
            }
        }
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
