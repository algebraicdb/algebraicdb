pub fn translate_drop(table: &String) -> String {
    format!("DROP TABLE {};", table)
}
