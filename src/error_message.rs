use crate::typechecker::TypeError;
use crate::util::str::*;
use std::fmt::{self, Write};

pub trait ErrorMessage {
    fn display(&self, input: &str) -> String;
}

impl ErrorMessage for TypeError<'_> {
    fn display(&self, input: &str) -> String {
        match self {
            TypeError::Undefined { kind, item } => {
                fmt_error_message(input, item, &format!("{} \"{}\" is undefined", kind, item))
            }
            TypeError::AmbiguousReference(ident) => {
                fmt_error_message(input, ident, &format!("\"{}\" is ambiguous", ident))
            }
            TypeError::AlreadyDefined(ident) => {
                fmt_error_message(input, ident, &format!("\"{}\" is defined elsewhere", ident))
            }
            TypeError::MissingColumn(ident) => {
                fmt_error_message(input, input, &format!("\"{}\" needs to be defined", ident))
            }
            TypeError::InvalidUnknownType { expected, actual } => fmt_error_message(
                input,
                actual,
                &format!("expected \"{}\", found unknown type", expected),
            ),
            TypeError::InvalidCount {
                item,
                expected,
                actual,
            } => fmt_error_message(
                input,
                item,
                &format!(
                    "invalid number of items: found {}, expected {}",
                    actual, expected
                ),
            ),
            TypeError::NotSupported(feature) => fmt_error_message(input, input, &format!("not supported: {}", feature)),
            TypeError::MismatchingTypes { type_1, type_2 } => {
                fmt_error_message(input, input, &format!("mismatching types: \"{}\" and \"{}\"", type_1, type_2))
            }
            TypeError::InvalidType { expected, actual } =>
                fmt_error_message(input, input, &format!(
                "invalid type: found \"{}\", expected \"{}\"",
                actual, expected
            )),
        }
    }
}

/// Display a pretty error message
///
/// This function will print a pretty error message, highlighting the offending part of the input.
pub fn fmt_error_message(input: &str, offending_slice: &str, message: &str) -> String {
    let inner = || -> Result<String, fmt::Error> {
        let mut output = String::new();

        if let Some((start, end)) = get_internal_slice_pos(input, offending_slice) {

            let (line, start_line, start_byte_offset) = byte_pos_to_line(input, start);
            let (_, end_line, _end_byte_offset) = byte_pos_to_line(input, end);

            if start_line == end_line {
                writeln!(&mut output, "    --> ERROR")?;

                // Write the line containing the offending part of the input
                writeln!(&mut output, "     |")?;
                writeln!(&mut output, "{:4} | {}", start_line + 1, line)?;
                output.push_str("     | ");

                // Highlight the offending part of the input
                let offset = line[0..start_byte_offset].chars().count();
                let length = offending_slice.chars().count();

                (0..offset).for_each(|_| output.push(' '));
                (0..length).for_each(|_| output.push('^'));
                writeln!(&mut output)?;

                // Display the accompanying message beneath the highlighting
                let msg_length = message.chars().count();
                let msg_offset = (offset + length / 2)
                    .checked_sub(msg_length / 2)
                    .unwrap_or(0);
                output.push_str("     * ");
                (0..msg_offset).for_each(|_| output.push(' '));
                writeln!(&mut output, "{}", message)?;
            } else {
                writeln!(&mut output, "    --> ERROR")?;
                writeln!(&mut output, "     |")?;

                for (i, line) in input
                    .lines()
                    .enumerate()
                    .skip(start_line)
                    .take(end_line - start_line + 1)
                {
                    writeln!(&mut output, "{:4} | {}", i, line)?;
                }

                writeln!(&mut output, "     |")?;
                writeln!(&mut output, "     * {}", message)?;
            }
        } else {
            writeln!(&mut output, "    --> ERROR")?;
            writeln!(&mut output, "     |")?;

            for (i, line) in input
                .lines()
                .enumerate()
            {
                writeln!(&mut output, "{:4} | {}", i, line)?;
            }

            writeln!(&mut output, "     |")?;
            writeln!(&mut output, "     * {}", message)?;
        }

        Ok(output)
    };

    inner().expect("Call to write! can't fail with String buffer")
}
