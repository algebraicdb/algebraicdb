use crate::ast::*;
use crate::local::{ResourcesGuard, TTable};
use crate::pattern::Pattern;
use crate::types::*;
use std::collections::HashMap;
use std::collections::HashSet;

pub struct Context<'a, T> {
    pub globals: &'a ResourcesGuard<'a, T>,
    locals: Vec<Scope>,
}

type Scope = HashMap<String, Vec<TypeId>>;

impl<'a, T: TTable> Context<'a, T> {
    pub fn new(globals: &'a ResourcesGuard<'a, T>) -> Self {
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
            .map(|res| {
                if res.len() == 1 {
                    Ok(res[0])
                } else {
                    Err(TypeError::AmbiguousReference(ident.to_string()))
                }
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
        let scope = self
            .locals
            .last_mut()
            .unwrap_or_else(|| panic!("Noo scoope :|"));
        for (name, mut types) in other {
            let existing = scope.entry(name).or_default();
            existing.append(&mut types);
        }
    }

    pub fn push_local(&mut self, name: String, type_id: TypeId) {
        self.locals
            .last_mut()
            .unwrap_or_else(|| panic!("No scope :c"))
            .entry(name)
            .or_default()
            .push(type_id)
    }

    pub fn locals(&self) -> &[Scope] {
        &self.locals[..]
    }
}

#[derive(Debug)]
pub enum TypeError {
    NotSupported(&'static str),
    Undefined(String),
    AmbiguousReference(String),
    NotASumType,
    AlreadyDefined,
    MissingColumn(String),
    MismatchingTypes { type_1: TypeId, type_2: TypeId },
    InvalidType { expected: TypeId, actual: TypeId },
    InvalidCount { expected: usize, actual: usize },
}

pub fn check_stmt<T: TTable>(
    stmt: &Stmt,
    globals: &ResourcesGuard<'_, T>,
) -> Result<(), TypeError> {
    let mut ctx = Context::new(globals);

    match stmt {
        Stmt::Select(select) => check_select(select, &mut ctx),
        Stmt::Update(update) => check_update(update, &mut ctx),
        Stmt::Delete(delete) => check_delete(delete, &mut ctx),
        Stmt::Drop(drop) => check_drop(drop, &mut ctx),
        Stmt::Insert(insert) => check_insert(insert, &mut ctx),
        Stmt::CreateTable(create_table) => check_create_table(create_table, &mut ctx),
        Stmt::CreateType(create_type) => check_create_type(create_type, &mut ctx),
    }
}

fn import_table_columns<'a, T: TTable>(name: &str, ctx: &'a mut Context<'_, T>) {
    let table = ctx.globals.read_table(name);
    let schema = table.get_schema();

    for (name, type_id) in &schema.columns {
        ctx.push_local(name.clone(), *type_id);
    }
}

fn check_select<T: TTable>(select: &Select, ctx: &mut Context<T>) -> Result<(), TypeError> {
    if let Some(from) = &select.from {
        check_select_from(from, ctx)?;
    }

    for item in &select.items {
        check_select_item(item, ctx)?;
    }

    Ok(())
}

fn check_select_from<T: TTable>(from: &SelectFrom, ctx: &mut Context<T>) -> Result<(), TypeError> {
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
                assert_type_as(
                    clause_type,
                    ctx.globals.type_map.get_base_id(BaseType::Bool),
                )?;
            }
        }
    }
    Ok(())
}

fn check_select_item<T: TTable>(item: &SelectItem, ctx: &Context<T>) -> Result<(), TypeError> {
    match item {
        SelectItem::Expr(expr) => {
            check_expr(expr, ctx)?;
        }
        SelectItem::Pattern(ident, pattern) => {
            // TODO: get type of ident

            let type_id = ctx.search_locals(ident)?;
            check_pattern(pattern, type_id, ctx)?;
        }
    }
    Ok(())
}

fn check_pattern<T: TTable>(
    pattern: &Pattern,
    type_id: TypeId,
    ctx: &Context<T>,
) -> Result<(), TypeError> {
    match pattern {
        Pattern::Int(_) => {
            assert_type_as(ctx.globals.type_map.get_base_id(BaseType::Integer), type_id)?;
        }
        Pattern::Bool(_) => {
            assert_type_as(ctx.globals.type_map.get_base_id(BaseType::Bool), type_id)?;
        }
        Pattern::Double(_) => {
            assert_type_as(ctx.globals.type_map.get_base_id(BaseType::Double), type_id)?;
        }
        Pattern::Ignore => {}
        Pattern::Binding(_) => {
            // TODO: add to ctx
        }
        Pattern::Variant {
            namespace,
            name,
            sub_patterns,
        } => {
            let types = &ctx.globals.type_map;
            if let Some(namespace) = namespace {
                if types.get_name(type_id).unwrap() == namespace {
                    return Err(TypeError::InvalidType{
                        expected: type_id,
                        actual: 0,
                    })
                }
            }

            if let Type::Sum(variants) = &types[&type_id] {
                let (i, (_, sub_types)) = variants
                    .iter()
                    .enumerate()
                    .find(|(_, (variant, _))| variant == name)
                    .ok_or_else(|| TypeError::Undefined(name.clone()))?;

                if sub_types.len() != sub_patterns.len() {
                    return Err(TypeError::InvalidCount {
                        expected: sub_types.len(),
                        actual: sub_patterns.len(),
                    });
                }

                for (t, p) in sub_types.iter().zip(sub_patterns.iter()) {
                    check_pattern(p, t.clone(), ctx)?;
                }
            } else if let Some(namespace) = namespace {
                    let actual_type_id = types.get_id(namespace)
                        .ok_or_else(|| TypeError::Undefined(name.clone()))?;
                    return Err(TypeError::InvalidType {
                        expected: type_id,
                        actual: actual_type_id,
                    });
            } else {
                return Err(TypeError::NotASumType);
            }
        }
    }

    Ok(())
}

fn check_update<T: TTable>(update: &Update, ctx: &mut Context<T>) -> Result<(), TypeError> {
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

fn check_delete<T: TTable>(delete: &Delete, ctx: &mut Context<T>) -> Result<(), TypeError> {
    let bool_id = ctx.globals.type_map.get_base_id(BaseType::Bool);
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

fn check_drop<T: TTable>(_drop: &Drop, _ctx: &mut Context<T>) -> Result<(), TypeError> {
    // no need to add stuff hihi
    Ok(())
}

fn check_insert<T: TTable>(insert: &Insert, ctx: &mut Context<T>) -> Result<(), TypeError> {
    let table = ctx.globals.read_table(&insert.table);
    let schema = table.get_schema();

    let mut populated_columns = HashSet::new();

    for row in insert.rows.iter() {
        // Make sure there is a value for every column
        if insert.columns.len() != row.len() {
            return Err(TypeError::InvalidCount {
                expected: insert.columns.len(),
                actual: row.len(),
            });
        }

        for (column, expr) in insert.columns.iter().zip(row.iter()) {
            // Make sure the types of the values match the types of the columns
            let expected_type = schema
                .column(column)
                .ok_or_else(|| TypeError::Undefined(column.clone()))?;

            let actual_type = check_expr(expr, ctx)?;

            if expected_type != actual_type {
                return Err(TypeError::InvalidType {
                    actual: actual_type,
                    expected: expected_type,
                });
            }

            // Make sure the user doesn't assign to the same column twice
            if !populated_columns.insert(column) {
                return Err(TypeError::AlreadyDefined);
            }
        }

        // Make sure all columns have a value
        for (column, _) in &table.get_schema().columns {
            if populated_columns.get(column).is_none() {
                // TODO: default values
                return Err(TypeError::MissingColumn(column.clone()));
            }
        }
        populated_columns.clear();
    }

    Ok(())
}

fn check_create_table<T: TTable>(
    create_table: &CreateTable,
    ctx: &mut Context<T>,
) -> Result<(), TypeError> {
    if create_table.columns.len() == 0 {
        return Err(TypeError::NotSupported("Creating empty tables"));
    }

    let columns = &create_table.columns;
    for (_, column_type) in columns {
        if ctx.globals.type_map.get(column_type).is_none() {
            return Err(TypeError::Undefined(column_type.clone()));
        }
    }

    for i in 0..columns.len() {
        for j in 0..i {
            if columns[i] == columns[j] {
                return Err(TypeError::AlreadyDefined);
            }
        }
    }

    Ok(())
}

fn check_create_type<T: TTable>(
    create: &CreateType,
    ctx: &mut Context<T>,
) -> Result<(), TypeError> {
    // For a type:
    // MyVariant = Var1 TypeA | Var2 TypeB TypeC
    // We have to check that the type name MyVariant is not taken
    // as well ass that Type{A,B,C} exists
    // TODO: recursive types
    match create {
        CreateType::Variant(name, variants) => {
            if ctx.globals.type_map.get_id(name).is_some() {
                return Err(TypeError::AlreadyDefined);
            }

            for (_variant, types) in variants {
                for t_name in types {
                    if ctx.globals.type_map.get_id(t_name).is_none() {
                        return Err(TypeError::Undefined(t_name.clone()));
                    }
                }
            }
        }
    }
    Ok(())
}

fn check_expr<T: TTable>(expr: &Expr, ctx: &Context<T>) -> Result<TypeId, TypeError> {
    match expr {
        Expr::Ident(ident) => ctx.search_locals(ident),

        Expr::Value(value) => type_of_value(&value, &ctx.globals.type_map),

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

            Ok(ctx.globals.type_map.get_base_id(BaseType::Bool))
        }

        Expr::And(e1, e2) | Expr::Or(e1, e2) => {
            let type_1 = check_expr(e1, ctx)?;
            let type_2 = check_expr(e2, ctx)?;

            let bool_id = ctx.globals.type_map.get_base_id(BaseType::Bool);

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
        Value::Integer(_) => Ok(types.get_base_id(BaseType::Integer)),
        Value::Double(_) => Ok(types.get_base_id(BaseType::Double)),
        Value::Bool(_) => Ok(types.get_base_id(BaseType::Bool)),
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
    use crate::local::{Resource, ResourcesGuard};
    use crate::table::{tests::create_type_map, Table};
    use futures::executor::block_on;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[test]
    fn type_check_exprs() {
        let (_ids, type_map) = create_type_map();
        let type_map = Arc::new(RwLock::new(type_map));

        let dummy_ctx: Context<Table> = Context {
            globals: &ResourcesGuard {
                type_map: Resource::Read(block_on(type_map.read())),
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
