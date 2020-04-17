use crate::ast::*;
use crate::state::Resource;
use crate::table::Schema;
use crate::types::*;
use std::collections::HashMap;
use std::collections::HashSet;

pub struct Context<'ast> {
    //pub globals: &'ast ResourcesGuard<'ast, T>,
    pub type_map: &'ast Resource<'ast, TypeMap>,
    pub schemas: &'ast HashMap<&'ast str, Resource<'ast, Schema>>,
    locals: Vec<Scope>,
}

type Scope = HashMap<String, Vec<TypeId>>;

#[derive(Debug)]
pub enum TypeError {
    NotSupported(&'static str),
    Undefined {
        span: Option<Span>,
        kind: &'static str,
        item: String,
    },
    AmbiguousReference {
        span: Option<Span>,
        ident: String,
    },
    AlreadyDefined {
        span: Option<Span>,
        ident: String,
    },
    MissingColumn {
        span: Option<Span>,
        name: String,
    },
    MismatchingTypes {
        span: Option<Span>,
        type_1: String,
        type_2: String,
    },
    InvalidType {
        span: Option<Span>,
        expected: String,
        actual: String,
    },
    InvalidUnknownType {
        span: Option<Span>,
        expected: String,
    },
    InvalidCount {
        span: Option<Span>,
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

impl<'ast> Context<'ast> {
    pub fn new(type_map: &'ast Resource<'ast, TypeMap>, schemas: &'ast HashMap<&'ast str, Resource<'ast, Schema>>) -> Self {
        Context {
            //globals,
            type_map,
            schemas,
            locals: vec![HashMap::new()],
        }
    }

    pub fn search_locals(&self, ident: &Spanned<String>) -> Result<TypeId, TypeError> {
        self.locals
            .iter()
            .filter_map(|scope| scope.get(&ident.value))
            .next()
            .map(|res| {
                if res.len() == 1 {
                    Ok(res[0])
                } else {
                    Err(TypeError::AmbiguousReference {
                        span: ident.span,
                        ident: ident.to_string(),
                    })
                }
            })
            .unwrap_or_else(|| {
                Err(TypeError::Undefined {
                    span: ident.span,
                    kind: "identifier",
                    item: ident.to_string(),
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

pub fn check_stmt(
    stmt: &Stmt,
    type_map: &Resource<TypeMap>,
    schemas: &HashMap<&str, Resource<Schema>>,
) -> Result<(), TypeError> {
    let mut ctx = Context::new(type_map, schemas);

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

fn import_table_columns(name: &str, ctx: &mut Context) {
    let schema = &ctx.schemas[name];

    for (name, type_id) in &schema.columns {
        ctx.push_local(name.clone(), *type_id);
    }
}

fn check_select<'ast>(
    select: &'ast Select,
    ctx: &mut Context,
) -> Result<Vec<DuckType<'ast>>, TypeError> {
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
            check_select_from(&join.table_b, ctx)?;

            ctx.merge_scope(scope);

            if let Some(on_clause) = &join.on_clause {
                let clause_type = check_expr(on_clause, ctx)?;
                let type_map = &ctx.type_map;
                assert_type_as(
                    clause_type,
                    type_map.get_base_id(BaseType::Bool),
                    on_clause.span,
                    type_map,
                )?;
            }
        }
    }
    Ok(())
}

fn check_where_clause(
    clause: &WhereClause,
    ctx: &mut Context,
) -> Result<(), TypeError> {
    for item in &clause.items {
        match item {
            WhereItem::Expr(expr) => {
                let type_map = &ctx.type_map;
                let expr_type = check_expr(expr, ctx)?;
                let bool_id = type_map.get_base_id(BaseType::Bool);
                assert_type_as(expr_type, bool_id, expr.span, type_map)?;
            }
            WhereItem::Pattern(ident, pattern) => {
                let type_id = ctx.search_locals(ident)?;
                check_pattern(pattern, type_id, ctx)?;
            }
        }
    }
    Ok(())
}

fn check_pattern(
    pattern: &Spanned<Pattern>,
    type_id: TypeId,
    ctx: &mut Context,
) -> Result<(), TypeError> {
    let type_map = &ctx.type_map;
    match &pattern.value {
        Pattern::Char(_) => {
            assert_type_as(
                type_map.get_base_id(BaseType::Char),
                type_id,
                pattern.span,
                type_map,
            )?;
        }
        Pattern::Int(_) => {
            assert_type_as(
                type_map.get_base_id(BaseType::Integer),
                type_id,
                pattern.span,
                type_map,
            )?;
        }
        Pattern::Bool(_) => {
            assert_type_as(
                type_map.get_base_id(BaseType::Bool),
                type_id,
                pattern.span,
                type_map,
            )?;
        }
        Pattern::Double(_) => {
            assert_type_as(
                type_map.get_base_id(BaseType::Double),
                type_id,
                pattern.span,
                type_map,
            )?;
        }
        Pattern::Ignore => {}
        Pattern::Binding(name) => ctx.push_local(name.to_string(), type_id),
        Pattern::Variant {
            namespace,
            name,
            sub_patterns,
        } => {
            let type_map = &ctx.type_map;
            if let Some(namespace) = namespace {
                let actual_type_id =
                    type_map
                        .get_id(namespace)
                        .ok_or_else(|| TypeError::Undefined {
                            span: namespace.span,
                            kind: "type",
                            item: namespace.to_string(),
                        })?;
                if actual_type_id != type_id {
                    return Err(TypeError::InvalidType {
                        span: pattern.span,
                        expected: type_map.get_name(type_id).unwrap().to_string(),
                        actual: type_map.get_name(actual_type_id).unwrap().to_string(),
                    });
                }
            }

            if let Type::Sum(variants) = &type_map[&type_id] {
                // The type we are pattern matching is a sum type
                let (_, sub_types) = variants
                    .iter()
                    .find(|(variant, _)| variant == name.as_ref())
                    .ok_or_else(|| TypeError::Undefined {
                        span: name.span,
                        kind: "constructor",
                        item: name.to_string(),
                    })?;

                if sub_types.len() != sub_patterns.len() {
                    return Err(TypeError::InvalidCount {
                        span: pattern.span,
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
                            span: namespace.span,
                            kind: "type",
                            item: name.to_string(),
                        })?;
                return Err(TypeError::InvalidType {
                    span: pattern.span,
                    expected: type_map.get_name(type_id).unwrap().to_string(),
                    actual: type_map.get_name(actual_type_id).unwrap().to_string(),
                });
            } else {
                // The type we are pattern matching is NOT a sum type
                // ...and we don't know what type this is supposed to be
                return Err(TypeError::InvalidUnknownType {
                    span: pattern.span,
                    expected: type_map.get_name(type_id).unwrap().to_string(),
                });
            }
        }
    }

    Ok(())
}

fn check_update(update: &Update, ctx: &mut Context) -> Result<(), TypeError> {
    import_table_columns(&update.table, ctx);
    let schema = &ctx.schemas[update.table.as_str()];

    for assignment in &update.ass {
        match schema.column(&assignment.col) {
            None => {
                return Err(TypeError::Undefined {
                    span: assignment.col.span,
                    kind: "column",
                    item: assignment.col.to_string(),
                })
            }
            Some(expected_type_id) => {
                ctx.push_local(assignment.col.to_string(), expected_type_id);
                let expr_type = check_expr(&assignment.expr, ctx)?;

                assert_type_as(
                    expr_type,
                    expected_type_id,
                    assignment.expr.span,
                    &ctx.type_map,
                )?;
            }
        }
    }

    Ok(())
}

fn check_delete(delete: &Delete, ctx: &mut Context) -> Result<(), TypeError> {
    match &delete.where_clause {
        Some(clause) => {
            import_table_columns(&delete.table, ctx);
            check_where_clause(clause, ctx)
        }

        None => Ok(()),
    }
}

fn check_insert(insert: &Insert, ctx: &mut Context) -> Result<(), TypeError> {
    let schema = &ctx.schemas[insert.table.as_str()];

    let mut populated_columns: HashSet<&str> = HashSet::new();

    match &insert.from {
        InsertFrom::Values(rows) => {
            for row in rows.iter() {
                // Make sure there is a value for every specified column
                // INSERT INTO t(a,b) VALUES (1,2);
                // ┍╌╌╌╌╌╌╌╌╌╌╌╌╌┷╌┷╌╌┑    ┍╌╌┷╌┷╌╌┑
                if insert.columns.len() != row.len() {
                    return Err(TypeError::InvalidCount {
                        span: row.span,
                        expected: insert.columns.len(),
                        actual: row.len(),
                    });
                }

                for (column, expr) in insert.columns.iter().zip(row.iter()) {
                    // Make sure the types of the values match the types of the columns
                    let expected_type =
                        schema.column(column).ok_or_else(|| TypeError::Undefined {
                            span: column.span,
                            kind: "column",
                            item: column.to_string(),
                        })?;
                    let actual_type = check_expr(expr, ctx)?;
                    assert_type_as(actual_type, expected_type, expr.span, &ctx.type_map)?;

                    // Make sure the user doesn't assign to the same column twice
                    if !populated_columns.insert(column) {
                        return Err(TypeError::AlreadyDefined {
                            span: column.span,
                            ident: column.to_string(),
                        });
                    }
                }

                // Make sure all columns have a value
                for (column, _) in &schema.columns {
                    if populated_columns.get(column.as_str()).is_none() {
                        // TODO: Support for default values
                        return Err(TypeError::MissingColumn {
                            span: insert.columns.span,
                            name: column.to_string(),
                        });
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
                    span: select.span,
                    expected: insert.columns.len(),
                    actual: types.len(),
                });
            }

            for (column, actual_type) in insert.columns.iter().zip(types.into_iter()) {
                // Make sure the types of the values match the types of the columns
                let expected_type = schema.column(column).ok_or_else(|| TypeError::Undefined {
                    span: column.span,
                    kind: "column",
                    item: column.to_string(),
                })?;

                assert_type_as(
                    actual_type,
                    expected_type,
                    select.span,
                    &ctx.type_map,
                )?;

                // Make sure the user doesn't assign to the same column twice
                if !populated_columns.insert(column) {
                    return Err(TypeError::AlreadyDefined {
                        span: column.span,
                        ident: column.to_string(),
                    });
                }
            }

            // Make sure all columns have a value
            for (column, _) in &schema.columns {
                if populated_columns.get(column.as_str()).is_none() {
                    // TODO: Support for default values
                    return Err(TypeError::MissingColumn {
                        span: insert.columns.span,
                        name: column.to_string(),
                    });
                }
            }
            populated_columns.clear();
        }
    }

    Ok(())
}

fn check_create_table(
    create_table: &CreateTable,
    ctx: &mut Context,
) -> Result<(), TypeError> {
    if create_table.columns.len() == 0 {
        return Err(TypeError::NotSupported("Creating empty tables"));
    }

    let columns = &create_table.columns;
    for (_, column_type) in columns {
        if ctx.type_map.get(column_type).is_none() {
            return Err(TypeError::Undefined {
                span: column_type.span,
                kind: "type",
                item: column_type.to_string(),
            });
        }
    }

    // Make sure no two columns has the same name
    for (i, (col_a, _)) in columns.iter().enumerate() {
        for (col_b, _) in columns[0..i].iter() {
            if col_a.as_ref() == col_b.as_ref() {
                return Err(TypeError::AlreadyDefined {
                    span: col_a.span,
                    ident: col_a.to_string(),
                });
            }
        }
    }

    Ok(())
}

fn check_create_type(
    create: &CreateType,
    ctx: &mut Context,
) -> Result<(), TypeError> {
    // For a type:
    // MyVariant = Var1 TypeA | Var2 TypeB TypeC
    // We have to check that the type name MyVariant is not taken
    // as well ass that Type{A,B,C} exists
    // TODO: recursive types
    match create {
        CreateType::Variant { name, variants } => {
            if ctx.type_map.get_id(name).is_some() {
                return Err(TypeError::AlreadyDefined {
                    span: name.span,
                    ident: name.to_string(),
                });
            }

            for (_variant, types) in variants {
                for t_name in types {
                    if ctx.type_map.get_id(t_name).is_none() {
                        return Err(TypeError::Undefined {
                            span: t_name.span,
                            kind: "type",
                            item: t_name.to_string(),
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

fn check_expr<'ast>(
    expr: &'ast Spanned<Expr>,
    ctx: &Context,
) -> Result<DuckType<'ast>, TypeError> {
    let type_map = &ctx.type_map;
    match &expr.value {
        Expr::Ident(ident) => Ok(ctx.search_locals(ident)?.into()),

        Expr::Value(value) => type_of_value(&value, expr.span, type_map),

        // All types are currently Eq and Ord
        Expr::Eql(box (e1, e2))
        | Expr::NEq(box (e1, e2))
        | Expr::LEq(box (e1, e2))
        | Expr::LTh(box (e1, e2))
        | Expr::GEq(box (e1, e2))
        | Expr::GTh(box (e1, e2)) => {
            let type_1 = check_expr(e1, ctx)?;
            let type_2 = check_expr(e2, ctx)?;
            assert_type_eq(type_1, type_2, expr.span, type_map)?;

            Ok(type_map.get_base_id(BaseType::Bool).into())
        }

        Expr::And(box (e1, e2)) | Expr::Or(box (e1, e2)) => {
            let type_1 = check_expr(e1, ctx)?;
            let type_2 = check_expr(e2, ctx)?;

            let bool_id = type_map.get_base_id(BaseType::Bool);

            assert_type_as(type_1, bool_id, e1.span, type_map)?;
            assert_type_as(type_2, bool_id, e2.span, type_map)?;

            Ok(bool_id.into())
        }
    }
}

fn assert_type_eq<'ast, T1, T2>(
    type_1: T1,
    type_2: T2,
    span: Option<Span>,
    type_map: &TypeMap,
) -> Result<DuckType<'ast>, TypeError>
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
                    span,
                    type_1: type_map.get_name(type_1).unwrap().to_string(),
                    type_2: type_map.get_name(type_2).unwrap().to_string(),
                });
            }
        }
        (Concrete(concrete_type), variant @ Variant(_, _))
        | (variant @ Variant(_, _), Concrete(concrete_type)) => {
            return assert_type_as(variant, concrete_type, span, type_map).map(Into::into);
        }
        (_, _) => unimplemented!("Comparing, duck-types"),
    }

    Ok(type_1)
}

fn assert_type_as<'ast, T>(
    actual: T,
    expected: TypeId,
    span: Option<Span>,
    type_map: &TypeMap,
) -> Result<TypeId, TypeError>
where
    T: Into<DuckType<'ast>>,
{
    match actual.into() {
        DuckType::Concrete(actual) => {
            if actual != expected {
                return Err(TypeError::InvalidType {
                    span,
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
                        span,
                        kind: "constructor",
                        item: variant_name.to_string(),
                    })?;

                if sub_types.len() != sub_values.len() {
                    return Err(TypeError::InvalidCount {
                        span,
                        expected: sub_types.len(),
                        actual: sub_values.len(),
                    });
                }

                for (sub_type, sub_value) in sub_types.iter().zip(sub_values.iter()) {
                    let vt = type_of_value(sub_value, span, type_map)?;
                    assert_type_as(vt, *sub_type, span, type_map)?;
                }
            } else {
                return Err(TypeError::InvalidUnknownType {
                    span,
                    expected: type_map.get_name(expected).unwrap().to_string(),
                });
            }
        }
    }

    Ok(expected)
}

pub fn type_of_value<'ast>(
    value: &'ast Value,
    span: Option<Span>,
    types: &TypeMap,
) -> Result<DuckType<'ast>, TypeError> {
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
                        span,
                        kind: "type",
                        item: namespace.to_string(),
                    })?;

                assert_type_as(
                    DuckType::Variant(variant_name, sub_values),
                    type_id,
                    span,
                    types,
                )?;

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
    use crate::state::{Resource, ResourcesGuard};
    use crate::table::{tests::create_type_map, Table};
    use futures::executor::block_on;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[test]
    fn type_check_exprs() {
        let (_ids, type_map) = create_type_map();
        let type_map = Arc::new(RwLock::new(type_map));

        let dummy_ctx: Context<Table> = Context::new(
            Resource::Read(block_on(type_map.read())),
            &[],
        );

        let valid_examples = vec![
            Expr::Eql(box (
                Expr::Value(Value::Integer(3).into()).into(),
                Expr::Value(Value::Integer(2).into()).into(),
            )),
            Expr::Eql(box (
                Expr::Value(Value::Bool(true).into()).into(),
                Expr::Value(Value::Bool(true).into()).into(),
            )),
            Expr::Eql(box (
                Expr::Value(Value::Double(0.0).into()).into(),
                Expr::Value(Value::Double(0.1).into()).into(),
            )),
            Expr::And(box (
                Expr::Value(Value::Bool(false).into()).into(),
                Expr::GTh(box (
                    Expr::Value(Value::Integer(42).into()).into(),
                    Expr::Value(Value::Integer(0).into()).into(),
                ))
                .into(),
            )),
        ];

        let invalid_examples = vec![
            Expr::Eql(box (
                Expr::Value(Value::Bool(false).into()).into(),
                Expr::Value(Value::Integer(2).into()).into(),
            )),
            Expr::And(box (
                Expr::Value(Value::Integer(0).into()).into(),
                Expr::Value(Value::Integer(0).into()).into(),
            )),
        ];

        for example in valid_examples {
            check_expr(&example.into(), &dummy_ctx).unwrap();
        }

        for example in invalid_examples {
            check_expr(&example.into(), &dummy_ctx).unwrap_err();
        }
    }
}
