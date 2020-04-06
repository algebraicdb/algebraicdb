use std::fmt::{self, Display, Formatter};
use std::num::ParseIntError;
use std::str::FromStr;

#[derive(Debug, Copy, Clone)]
pub struct NumBytes(pub usize);

#[derive(Debug)]
pub enum ParseNumBytesError {
    IntError(ParseIntError),
}

const GIBIBYTE: usize = 1024usize.pow(3);
const MEBIBYTE: usize = 1024usize.pow(2);
const KIBIBYTE: usize = 1024usize;

impl FromStr for NumBytes {
    type Err = ParseNumBytesError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            _ if s.ends_with("G") => {
                let num: usize = s[..s.len() - 1].parse()?;
                Ok(NumBytes(num * GIBIBYTE))
            }
            _ if s.ends_with("M") => {
                let num: usize = s[..s.len() - 1].parse()?;
                Ok(NumBytes(num * MEBIBYTE))
            }
            _ if s.ends_with("K") => {
                let num: usize = s[..s.len() - 1].parse()?;
                Ok(NumBytes(num * KIBIBYTE))
            }
            _ => {
                let num: usize = s.parse()?;
                Ok(NumBytes(num))
            }
        }
    }
}

impl From<ParseIntError> for ParseNumBytesError {
    fn from(e: ParseIntError) -> Self {
        Self::IntError(e)
    }
}

impl Display for ParseNumBytesError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ParseNumBytesError::IntError(e) => write!(f, "{}", e),
        }
    }
}
