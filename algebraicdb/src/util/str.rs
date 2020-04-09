/// Returns the line number corresponding to the char at the byte index
///
/// Also returns the line, and the byte index of the character within that line
pub fn byte_pos_to_line(s: &str, i: usize) -> (&str, usize, usize) {
    let mut iter = s.chars();
    let mut byte: usize = 0;
    let mut line_num = 0;
    let mut line_start = 0;
    while let Some(c) = iter.next() {
        byte += c.len_utf8();
        if c == '\n' {
            line_start = byte;
            line_num += 1;
        }
        if byte >= i {
            let s = &s[line_start..];
            let s = s.split('\n').next().unwrap();
            return (s, line_num, byte - line_start);
        }
    }
    return (s, line_num, byte - line_start);
}
