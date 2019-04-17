//! Module for errors.
use sounding_analysis::AnalysisError;
use std::error::Error;
use std::fmt::Display;

pub type Result<T> = std::result::Result<T, BufkitDataErr>;

/// FIXME: Rename this error.
/// Error from the archive interface.
#[derive(Debug)]
pub enum BufkitDataErr {
    //
    // Inherited errors from sounding stack
    //
    /// Error forwarded from sounding-analysis
    SoundingAnalysis(AnalysisError),

    //
    // Inherited errors from std
    //
    /// Error forwarded from std
    Io(::std::io::Error),
    /// Error converting bytes to utf8 string.
    Utf8(::std::str::Utf8Error),

    //
    // Other forwarded errors
    //
    /// Database error
    Database(::rusqlite::Error),
    /// Error forwarded from the strum crate
    StrumError(strum::ParseError),
    /// General error with any cause information erased and replaced by a string
    GeneralError(String),

    //
    // My own errors from this crate
    //
    /// Not enough data to complete the task.
    NotEnoughData,
}

impl Display for BufkitDataErr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        use crate::BufkitDataErr::*;

        match self {
            SoundingAnalysis(err) => write!(f, "error from sounding-analysis: {}", err),

            Io(err) => write!(f, "std lib io error: {}", err),
            Utf8(err) => write!(f, "error converting bytes to utf8: {}", err),

            Database(err) => write!(f, "database error: {}", err),
            StrumError(err) => write!(f, "error forwarded from strum crate: {}", err),
            GeneralError(msg) => write!(f, "general error forwarded: {}", msg),

            NotEnoughData => write!(f, "not enough data to complete task"),
        }
    }
}

impl Error for BufkitDataErr {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        use crate::BufkitDataErr::*;

        match self {
            SoundingAnalysis(err) => Some(err),
            Io(err) => Some(err),
            Utf8(err) => Some(err),
            Database(err) => Some(err),
            StrumError(err) => Some(err),
            GeneralError(msg) => None,
            NotEnoughData => None,
        }
    }
}

impl From<AnalysisError> for BufkitDataErr {
    fn from(err: AnalysisError) -> BufkitDataErr {
        BufkitDataErr::SoundingAnalysis(err)
    }
}

impl From<::std::io::Error> for BufkitDataErr {
    fn from(err: ::std::io::Error) -> BufkitDataErr {
        BufkitDataErr::Io(err)
    }
}

impl From<::std::str::Utf8Error> for BufkitDataErr {
    fn from(err: ::std::str::Utf8Error) -> BufkitDataErr {
        BufkitDataErr::Utf8(err)
    }
}

impl From<::rusqlite::Error> for BufkitDataErr {
    fn from(err: ::rusqlite::Error) -> BufkitDataErr {
        BufkitDataErr::Database(err)
    }
}

impl From<strum::ParseError> for BufkitDataErr {
    fn from(err: strum::ParseError) -> BufkitDataErr {
        BufkitDataErr::StrumError(err)
    }
}

impl From<Box<dyn Error>> for BufkitDataErr {
    fn from(err: Box<dyn Error>) -> BufkitDataErr {
        BufkitDataErr::GeneralError(err.to_string())
    }
}
