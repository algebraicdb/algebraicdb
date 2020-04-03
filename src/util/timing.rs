use std::fmt::{self, Display, Formatter};
use std::num::ParseIntError;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Copy, Clone)]
pub enum Timing {
    Never(),
    Every(Duration),
}

#[derive(Debug)]
pub enum ParseTimingError {
    IntError(ParseIntError),
    Invalid(),
}

impl FromStr for Timing {
    type Err = ParseTimingError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "never" => Ok(Timing::Never()),
            _ if s.ends_with("h") => {
                let secs: u64 = s[..s.len() - 1].parse()?;
                Ok(Timing::Every(Duration::from_secs(secs * 60 * 60)))
            }
            _ if s.ends_with("m") => {
                let secs: u64 = s[..s.len() - 1].parse()?;
                Ok(Timing::Every(Duration::from_secs(secs * 60)))
            }
            _ if s.ends_with("s") => {
                let secs: u64 = s[..s.len() - 1].parse()?;
                Ok(Timing::Every(Duration::from_secs(secs)))
            }
            _ => Err(ParseTimingError::Invalid()),
        }
    }
}

impl From<ParseIntError> for ParseTimingError {
    fn from(e: ParseIntError) -> Self {
        Self::IntError(e)
    }
}

impl Display for ParseTimingError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ParseTimingError::IntError(e) => write!(f, "{}", e),
            ParseTimingError::Invalid() => write!(
                f,
                "Invalid syntax. Valid examples are: 'never', '12s', '30m', '1h'."
            ),
        }
    }
}
