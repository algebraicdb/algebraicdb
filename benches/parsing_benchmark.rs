use criterion::{black_box, criterion_group, criterion_main, Criterion};
use algebraicdb::grammar::StmtParser;


fn lalrpop_parse_benchmark(c: &mut Criterion) {
    let parser = StmtParser::new();
    c.bench_function(".parse testbench", |b| b.iter(|| parser.parse(black_box(r#"SELECT col FROM t1 LEFT JOIN t2;"#))));
}

criterion_group!(benches, lalrpop_parse_benchmark);
criterion_main!(benches);