use crate::ast::*;
use crate::local::{ResourcesGuard, TTable};
use crate::pattern::Pattern;
use crate::types::*;
use std::collections::HashMap;
use std::collections::HashSet;

pub struct Context<'ast, T> {
    pub globals: &'ast ResourcesGuard<'ast, T>,
    locals: Vec<Scope>,
}

type Scope = HashMap<String, Vec<TypeId>>;

#[derive(Debug)]
pub enum TypeError<'ast> {
    NotSupported(&'static str),
    Undefined {
        kind: &'static str,
        item: &'ast str,
    },
    AmbiguousReference(&'ast str),
    AlreadyDefined(&'ast str),
    MissingColumn(&'ast str),
    MismatchingTypes {
        type_1: String,
        type_2: String,
    },
    InvalidType {
        expected: String,
        actual: String,
    },
    InvalidUnknownType {
        expected: String,
        actual: &'ast str,
    },
    InvalidCount {
        item: &'ast str,
        expected: usize,
        actual: usize,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum DuckType<'ast> {
    Concrete(TypeId),
    Variant(&'ast str, &'ast [Value<'ast>]),
}

impl From<TypeId> for DuckType<'static> {
    fn from(id: TypeId) -> DuckType<'static> {
        DuckType::Concrete(id)
    }
}

impl<'ast, T: TTable> Context<'ast, T> {
    pub fn new(globals: &'ast ResourcesGuard<'ast, T>) -> Self {
        Context {
            globals,
            locals: vec![HashMap::new()],
        }
    }

    pub fn search_locals<'err>(&self, ident: &'err str) -> Result<TypeId, TypeError<'err>> {
        self.locals
            .iter()
            .filter_map(|scope| scope.get(ident))
            .next()
            .map(|res| {
                if res.len() == 1 {
                    Ok(res[0])
                } else {
                    Err(TypeError::AmbiguousReference(ident))
                }
            })
            .unwrap_or_else(|| {
                Err(TypeError::Undefined {
                    kind: "identifier",
                    item: ident,
                })
            })
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

pub fn check_stmt<'ast, T: TTable>(
    stmt: &'ast Stmt<'ast>,
    globals: &'ast ResourcesGuard<'ast, T>,
) -> Result<(), TypeError<'ast>> {
    let mut ctx = Context::new(globals);

    match stmt {
        Stmt::Select(select) => check_select(select, &mut ctx).map(|_| ()),
        Stmt::Update(update) => check_update(update, &mut ctx),
        Stmt::Delete(delete) => check_delete(delete, &mut ctx),
        Stmt::Drop(_) => Ok(()), // Nothing to do here...
        Stmt::Insert(insert) => check_insert(insert, &mut ctx),
        Stmt::CreateTable(create_table) => check_create_table(create_table, &mut ctx),
        Stmt::CreateType(create_type) => check_create_type(create_type, &mut ctx),
    }
}

fn import_table_columns<'ast, T: TTable>(name: &str, ctx: &mut Context<'_, T>) {
    let table = ctx.globals.read_table(name);
    let schema = table.get_schema();

    for (name, type_id) in &schema.columns {
        ctx.push_local(name.clone(), *type_id);
    }
}

fn check_select<'ast, 'err, 'ctx, T: TTable>(
    select: &'ast Select<'ast>,
    ctx: &'ctx mut Context<'err, T>,
) -> Result<Vec<DuckType<'ast>>, TypeError<'err>>
where
    'ast: 'err,
{
    if let Some(from) = &select.from {
        check_select_from(from, ctx)?;
    }

    if let Some(where_clause) = &select.where_clause {
        check_where_clause(where_clause, ctx)?;
    }

    // Collect a Result<Vec<_>> from an Iter<Result<_>>
    select
        .items
        .iter()
        .map(|expr| check_expr(expr, ctx))
        .collect()
}

fn check_select_from<'ast, T: TTable>(
    from: &'ast SelectFrom<'ast>,
    ctx: &mut Context<'ast, T>,
) -> Result<(), TypeError<'ast>> {
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
            check_select_from(&join.table_b, ctx)?;

            ctx.merge_scope(scope);

            if let Some(on_clause) = &join.on_clause {
                let clause_type = check_expr(on_clause, ctx)?;
                let type_map = &ctx.globals.type_map;
                assert_type_as(clause_type, type_map.get_base_id(BaseType::Bool), type_map)?;
            }
        }
    }
    Ok(())
}

fn check_where_clause<'ast, T: TTable>(
    clause: &'ast WhereClause<'ast>,
    ctx: &mut Context<'ast, T>,
) -> Result<(), TypeError<'ast>> {
    let type_map = &ctx.globals.type_map;
    for item in &clause.items {
        match item {
            WhereItem::Expr(expr) => {
                let expr_type = check_expr(expr, ctx)?;
                let bool_id = type_map.get_base_id(BaseType::Bool);
                assert_type_as(expr_type, bool_id, type_map)?;
            }
            WhereItem::Pattern(ident, pattern) => {
                let type_id = ctx.search_locals(ident)?;
                check_pattern(pattern, type_id, ctx)?;
            }
        }
    }
    Ok(())
}

fn check_pattern<'ast, T: TTable>(
    pattern: &Pattern<'ast>,
    type_id: TypeId,
    ctx: &mut Context<T>,
) -> Result<(), TypeError<'ast>> {
    let type_map = &ctx.globals.type_map;
    match pattern {
        Pattern::Char(_) => {
            assert_type_as(type_map.get_base_id(BaseType::Char), type_id, type_map)?;
        }
        Pattern::Int(_) => {
            assert_type_as(type_map.get_base_id(BaseType::Integer), type_id, type_map)?;
        }
        Pattern::Bool(_) => {
            assert_type_as(type_map.get_base_id(BaseType::Bool), type_id, type_map)?;
        }
        Pattern::Double(_) => {
            assert_type_as(type_map.get_base_id(BaseType::Double), type_id, type_map)?;
        }
        Pattern::Ignore => {}
        Pattern::Binding(name) => ctx.push_local(name.to_string(), type_id),
        Pattern::Variant {
            namespace,
            name,
            sub_patterns,
        } => {
            let type_map = &ctx.globals.type_map;
            if let Some(namespace) = namespace {
                let actual_type_id =
                    type_map
                        .get_id(namespace)
                        .ok_or_else(|| TypeError::Undefined {
                            kind: "type",
                            item: namespace,
                        })?;
                if actual_type_id != type_id {
                    return Err(TypeError::InvalidType {
                        expected: type_map.get_name(type_id).unwrap().to_string(),
                        actual: type_map.get_name(actual_type_id).unwrap().to_string(),
                    });
                }
            }

            if let Type::Sum(variants) = &type_map[&type_id] {
                // The type we are pattern matching is a sum type
                let (_, sub_types) = variants
                    .iter()
                    .find(|(variant, _)| variant == name)
                    .ok_or_else(|| TypeError::Undefined {
                        kind: "constructor",
                        item: name,
                    })?;

                if sub_types.len() != sub_patterns.len() {
                    return Err(TypeError::InvalidCount {
                        item: name,
                        expected: sub_types.len(),
                        actual: sub_patterns.len(),
                    });
                }

                for (t, p) in sub_types.iter().zip(sub_patterns.iter()) {
                    check_pattern(p, t.clone(), ctx)?;
                }
            } else if let Some(namespace) = namespace {
                // The type we are pattern matching is NOT a sum type
                // ...but we know what type the pattern tries to match
                let actual_type_id =
                    type_map
                        .get_id(namespace)
                        .ok_or_else(|| TypeError::Undefined {
                            kind: "type",
                            item: name,
                        })?;
                return Err(TypeError::InvalidType {
                    expected: type_map.get_name(type_id).unwrap().to_string(),
                    actual: type_map.get_name(actual_type_id).unwrap().to_string(),
                });
            } else {
                // The type we are pattern matching is NOT a sum type
                // ...and we don't know what type this is supposed to be
                return Err(TypeError::InvalidUnknownType {
                    expected: type_map.get_name(type_id).unwrap().to_string(),
                    actual: name,
                });
            }
        }
    }

    Ok(())
}

fn check_update<'ast, T: TTable>(
    update: &'ast Update<'ast>,
    ctx: &mut Context<'ast, T>,
) -> Result<(), TypeError<'ast>> {
    import_table_columns(&update.table, ctx);
    let table = ctx.globals.read_table(&update.table);
    let schema = table.get_schema();

    for assignment in &update.ass {
        match schema.column(&assignment.col) {
            None => {
                return Err(TypeError::Undefined {
                    kind: "column",
                    item: assignment.col,
                })
            }
            Some(expected_type_id) => {
                ctx.push_local(assignment.col.to_string(), expected_type_id);
                let expr_type = check_expr(&assignment.expr, ctx)?;

                assert_type_as(expr_type, expected_type_id, &ctx.globals.type_map)?;
            }
        }
    }

    Ok(())
}

fn check_delete<'ast, T: TTable>(
    delete: &'ast Delete<'ast>,
    ctx: &mut Context<'ast, T>,
) -> Result<(), TypeError<'ast>> {
    match &delete.where_clause {
        Some(clause) => {
            import_table_columns(&delete.table, ctx);
            check_where_clause(clause, ctx)
        }

        None => Ok(()),
    }
}

fn check_insert<'ast, 'err, T: TTable>(
    insert: &'ast Insert<'ast>,
    ctx: &mut Context<'err, T>,
) -> Result<(), TypeError<'err>>
where
    'ast: 'err,
{
    let table = ctx.globals.read_table(&insert.table);
    let schema = table.get_schema();

    let mut populated_columns: HashSet<&str> = HashSet::new();

    match &insert.from {
        InsertFrom::Values(rows) => {
            for row in rows.iter() {
                // Make sure there is a value for every specified column
                // INSERT INTO t(a,b) VALUES (1,2);
                // ┍╌╌╌╌╌╌╌╌╌╌╌╌╌┷╌┷╌╌┑    ┍╌╌┷╌┷╌╌┑
                if insert.columns.len() != row.len() {
                    return Err(TypeError::InvalidCount {
                        item: "VALUES",
                        expected: insert.columns.len(),
                        actual: row.len(),
                    });
                }

                for (column, expr) in insert.columns.iter().zip(row.iter()) {
                    // Make sure the types of the values match the types of the columns
                    let expected_type =
                        schema.column(column).ok_or_else(|| TypeError::Undefined {
                            kind: "column",
                            item: column,
                        })?;
                    let actual_type = check_expr(expr, ctx)?;
                    assert_type_as(actual_type, expected_type, &ctx.globals.type_map)?;

                    // Make sure the user doesn't assign to the same column twice
                    if !populated_columns.insert(column) {
                        return Err(TypeError::AlreadyDefined(column));
                    }
                }

                // Make sure all columns have a value
                for (column, _) in &table.get_schema().columns {
                    if populated_columns.get(column.as_str()).is_none() {
                        // TODO: Support for default values
                        return Err(TypeError::MissingColumn(column));
                    }
                }
                populated_columns.clear();
            }
        }

        InsertFrom::Select(select) => {
            let types = check_select(select, ctx)?;

            // Make sure there is a value for every specified column
            if insert.columns.len() != types.len() {
                return Err(TypeError::InvalidCount {
                    item: "SELECT items",
                    expected: insert.columns.len(),
                    actual: types.len(),
                });
            }

            for (column, actual_type) in insert.columns.iter().zip(types.into_iter()) {
                // Make sure the types of the values match the types of the columns
                let expected_type = schema.column(column).ok_or_else(|| TypeError::Undefined {
                    kind: "column",
                    item: column,
                })?;

                assert_type_as(actual_type, expected_type, &ctx.globals.type_map)?;

                // Make sure the user doesn't assign to the same column twice
                if !populated_columns.insert(column) {
                    return Err(TypeError::AlreadyDefined(column));
                }
            }

            // Make sure all columns have a value
            for (column, _) in &table.get_schema().columns {
                if populated_columns.get(column.as_str()).is_none() {
                    // TODO: Support for default values
                    return Err(TypeError::MissingColumn(column));
                }
            }
            populated_columns.clear();
        }
    }

    Ok(())
}

fn check_create_table<'ast, T: TTable>(
    create_table: &CreateTable<'ast>,
    ctx: &mut Context<T>,
) -> Result<(), TypeError<'ast>> {
    if create_table.columns.len() == 0 {
        return Err(TypeError::NotSupported("Creating empty tables"));
    }

    let columns = &create_table.columns;
    for (_, column_type) in columns {
        if ctx.globals.type_map.get(column_type).is_none() {
            return Err(TypeError::Undefined {
                kind: "type",
                item: column_type,
            });
        }
    }

    // Make sure no two columns has the same name
    for i in 0..columns.len() {
        for j in 0..i {
            if columns[i].0 == columns[j].0 {
                return Err(TypeError::AlreadyDefined(columns[i].0));
            }
        }
    }

    Ok(())
}

fn check_create_type<'ast, T: TTable>(
    create: &CreateType<'ast>,
    ctx: &mut Context<T>,
) -> Result<(), TypeError<'ast>> {
    // For a type:
    // MyVariant = Var1 TypeA | Var2 TypeB TypeC
    // We have to check that the type name MyVariant is not taken
    // as well ass that Type{A,B,C} exists
    // TODO: recursive types
    match create {
        CreateType::Variant(name, variants) => {
            if ctx.globals.type_map.get_id(name).is_some() {
                return Err(TypeError::AlreadyDefined(name));
            }

            for (_variant, types) in variants {
                for t_name in types {
                    if ctx.globals.type_map.get_id(t_name).is_none() {
                        return Err(TypeError::Undefined {
                            kind: "type",
                            item: t_name,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

fn check_expr<'ast, 'err, T: TTable>(
    expr: &'ast Expr<'ast>,
    ctx: &Context<'err, T>,
) -> Result<DuckType<'ast>, TypeError<'err>>
where
    'ast: 'err,
{
    let type_map = &ctx.globals.type_map;
    match expr {
        Expr::Ident(ident) => Ok(ctx.search_locals(ident)?.into()),

        Expr::Value(value) => type_of_value(&value, type_map),

        // All types are currently Eq and Ord
        Expr::Equals(e1, e2)
        | Expr::NotEquals(e1, e2)
        | Expr::LessEquals(e1, e2)
        | Expr::LessThan(e1, e2)
        | Expr::GreaterEquals(e1, e2)
        | Expr::GreaterThan(e1, e2) => {
            let type_1 = check_expr(e1, ctx)?;
            let type_2 = check_expr(e2, ctx)?;
            assert_type_eq(type_1, type_2, type_map)?;

            Ok(type_map.get_base_id(BaseType::Bool).into())
        }

        Expr::And(e1, e2) | Expr::Or(e1, e2) => {
            let type_1 = check_expr(e1, ctx)?;
            let type_2 = check_expr(e2, ctx)?;

            let bool_id = type_map.get_base_id(BaseType::Bool);

            assert_type_as(type_1, bool_id, type_map)?;
            assert_type_as(type_2, bool_id, type_map)?;

            Ok(bool_id.into())
        }
    }
}

fn assert_type_eq<'ast, 'err, T1, T2>(
    type_1: T1,
    type_2: T2,
    type_map: &'err TypeMap,
) -> Result<DuckType<'ast>, TypeError<'ast>>
where
    DuckType<'ast>: From<T1>,
    DuckType<'ast>: From<T2>,
{
    let (type_1, type_2) = (DuckType::from(type_1), DuckType::from(type_2));
    use DuckType::*;
    match (type_1, type_2) {
        (Concrete(type_1), Concrete(type_2)) => {
            if type_1 != type_2 {
                return Err(TypeError::MismatchingTypes {
                    type_1: type_map.get_name(type_1).unwrap().to_string(),
                    type_2: type_map.get_name(type_2).unwrap().to_string(),
                });
            }
        }
        (Concrete(concrete_type), variant @ Variant(_, _))
        | (variant @ Variant(_, _), Concrete(concrete_type)) => {
            return assert_type_as(variant, concrete_type, type_map).map(Into::into);
        }
        (_, _) => unimplemented!("Comparing, duck-types"),
    }

    Ok(type_1)
}

fn assert_type_as<'ast, 'err, T>(
    actual: T,
    expected: TypeId,
    type_map: &'err TypeMap,
) -> Result<TypeId, TypeError<'ast>>
where
    T: Into<DuckType<'ast>>,
{
    match actual.into() {
        DuckType::Concrete(actual) => {
            if actual != expected {
                return Err(TypeError::InvalidType {
                    actual: type_map.get_name(actual).unwrap().to_string(),
                    expected: type_map.get_name(expected).unwrap().to_string(),
                });
            }
        }
        DuckType::Variant(variant_name, sub_values) => {
            let t = type_map.get_by_id(expected);

            if let Type::Sum(variants) = t {
                let (_, sub_types) = variants
                    .iter()
                    .find(|(name, _)| name == variant_name)
                    .ok_or_else(|| TypeError::Undefined {
                        kind: "constructor",
                        item: variant_name,
                    })?;

                if sub_types.len() != sub_values.len() {
                    return Err(TypeError::InvalidCount {
                        item: variant_name,
                        expected: sub_types.len(),
                        actual: sub_values.len(),
                    });
                }

                for (sub_type, sub_value) in sub_types.iter().zip(sub_values.iter()) {
                    let vt = type_of_value(sub_value, type_map)?;
                    assert_type_as(vt, *sub_type, type_map)?;
                }
            } else {
                return Err(TypeError::InvalidUnknownType {
                    expected: type_map.get_name(expected).unwrap().to_string(),
                    actual: variant_name,
                });
            }
        }
    }

    Ok(expected)
}

// DuckType : Value
// TypeError : Value
pub fn type_of_value<'ast, 'b>(
    value: &'ast Value<'ast>,
    types: &'b TypeMap,
) -> Result<DuckType<'ast>, TypeError<'ast>> {
    match value {
        Value::Char(_) => Ok(types.get_base_id(BaseType::Char).into()),
        Value::Integer(_) => Ok(types.get_base_id(BaseType::Integer).into()),
        Value::Double(_) => Ok(types.get_base_id(BaseType::Double).into()),
        Value::Bool(_) => Ok(types.get_base_id(BaseType::Bool).into()),
        Value::Sum(namespace, variant_name, sub_values) => {
            if let Some(namespace) = namespace {
                let type_id = types
                    .get_id(namespace)
                    .ok_or_else(|| TypeError::Undefined {
                        kind: "type",
                        item: namespace,
                    })?;

                assert_type_as(DuckType::Variant(variant_name, sub_values), type_id, types)?;

                Ok(DuckType::Concrete(type_id))
            } else {
                Ok(DuckType::Variant(variant_name, sub_values))
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
