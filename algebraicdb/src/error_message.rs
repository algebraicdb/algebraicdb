use crate::ast::Span;
use crate::typechecker::TypeError;
use crate::util::str::*;
use lalrpop_util::ParseError;
use std::fmt::{self, Write};

pub trait ErrorMessage {
    fn display(&self, input: &str) -> String;
}

impl<T> ErrorMessage for ParseError<usize, T, &str> {
    fn display(&self, input: &str) -> String {
        match self {
            &ParseError::InvalidToken { location } => {
                fmt_error_message(input, Some(Span(location, location)), "invalid token")
            }

            ParseError::UnrecognizedToken {
                token: (start, _token, end),
                expected: _,
            } => fmt_error_message(input, Some(Span(*start, *end)), "unrecognized token"),

            //&ParseError::UnrecognizedToken { token: None, expected } => fmt_error_message(input, None, "unexpected EOF"),
            ParseError::UnrecognizedEOF { .. } => fmt_error_message(input, None, "unexpected EOF"),

            ParseError::ExtraToken {
                token: (start, _token, end),
            } => fmt_error_message(input, Some(Span(*start, *end)), "unexpected token"),

            ParseError::User { error } => fmt_error_message(input, None, error),
        }
    }
}

impl ErrorMessage for TypeError {
    fn display(&self, input: &str) -> String {
        match self {
            TypeError::Undefined { span, kind, item } => {
                fmt_error_message(input, *span, &format!("{} \"{}\" is undefined", kind, item))
            }
            TypeError::AmbiguousReference { span, ident } => {
                fmt_error_message(input, *span, &format!("\"{}\" is ambiguous", ident))
            }
            TypeError::AlreadyDefined { span, ident } => {
                fmt_error_message(input, *span, &format!("\"{}\" is defined elsewhere", ident))
            }
            TypeError::MissingColumn { span, name } => {
                fmt_error_message(input, *span, &format!("\"{}\" needs to be defined", name))
            }
            TypeError::InvalidUnknownType { span, expected } => fmt_error_message(
                input,
                *span,
                &format!("expected \"{}\", found unknown type", expected),
            ),
            TypeError::InvalidCount {
                span,
                expected,
                actual,
            } => fmt_error_message(
                input,
                *span,
                &format!(
                    "invalid number of items: found {}, expected {}",
                    actual, expected
                ),
            ),
            TypeError::NotSupported(feature) => {
                fmt_error_message(input, None, &format!("not supported: {}", feature))
            }
            TypeError::MismatchingTypes {
                span,
                type_1,
                type_2,
            } => fmt_error_message(
                input,
                *span,
                &format!("mismatching types: \"{}\" and \"{}\"", type_1, type_2),
            ),
            TypeError::InvalidType {
                span,
                expected,
                actual,
            } => fmt_error_message(
                input,
                *span,
                &format!(
                    "invalid type: found \"{}\", expected \"{}\"",
                    actual, expected
                ),
            ),
        }
    }
}

/// Formats a pretty error message
///
/// This function will format a pretty error message, highlighting the offending part of the input.
pub fn fmt_error_message(input: &str, span: Option<Span>, message: &str) -> String {
    let inner = || -> Result<String, fmt::Error> {
        let mut output = String::new();

        if let Some(Span(start, end)) = span {
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
                let length = end - start;

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

            for (i, line) in input.lines().enumerate() {
                writeln!(&mut output, "{:4} | {}", i, line)?;
            }

            writeln!(&mut output, "     |")?;
            writeln!(&mut output, "     * {}", message)?;
        }

        Ok(output)
    };

    inner().expect("Call to write! can't fail with String buffer")
}
