extern crate lalrpop;

fn main() {
    lalrpop::Configuration::new()
        .use_cargo_dir_conventions()
        .process_file("src/grammar.lalrpop")
        .expect("LALRPOP processing failed");
    println!("cargo:rerun-if-changed=src/grammar.lalrpop");
}
