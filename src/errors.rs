//! Module for errors.
use crate::{location::Location, site::Site, sounding_type::SoundingType};
use sounding_analysis::AnalysisError;
use std::{error::Error, fmt::Display};

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
    /// No such site in the database.
    InvalidSite(Site),
    /// No such sounding type in the index.
    InvalidSoundingType(SoundingType),
    /// No such location in the index.
    InvalidLocation(Location),
    /// Unknown file type
    UnknownFileType,
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
            InvalidSite(site) => write!(f, "no such site in the index: {}", site.short_name()),
            InvalidSoundingType(st) => {
                write!(f, "no such sounding type in the index: {}", st.source())
            }
            InvalidLocation(loc) => write!(
                f,
                "no such location in the index: lat: {}, lon: {}, elev: {}",
                loc.latitude(),
                loc.longitude(),
                loc.elevation()
            ),
            UnknownFileType => write!(f, "unkown file type for"),
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
            GeneralError(_) => None,
            NotEnoughData => None,
            InvalidSite(_) => None,
            InvalidSoundingType(_) => None,
            InvalidLocation(_) => None,
            UnknownFileType => None,
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
