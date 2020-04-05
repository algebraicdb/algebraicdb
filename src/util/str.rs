/// Given two strings, "whole" and "slice", where "slice" is a slice of "whole", find the start and
/// end index of "slice" in "whole".
pub fn get_internal_slice_pos(whole: &str, slice: &str) -> Option<(usize, usize)> {
    let other_s = whole.as_ptr() as usize;
    let other_e = other_s + whole.len();
    let our_s = slice.as_ptr() as usize;
    let our_e = our_s + slice.len();

    if other_s == our_s && other_e == our_e {
        None
    } else if other_s <= our_s && other_e >= our_e {
        let start = our_s - other_s;
        Some((start, start + slice.len()))
    } else {
        None
    }
}

/// Returns the linee
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
