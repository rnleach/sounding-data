//! Crate to manage and interface with an archive of
//! [bufkit](https://training.weather.gov/wdtd/tools/BUFKIT/index.php) and other files for storing model
//! and upper air files.
//!
//! This is developed originally as a component crate for
//! [sonde](https://github.com/rnleach/sonde.git), but it also supports a set of command line tools
//! for utilizing the archive.
//!
//! The current implementation uses an [sqlite](https://www.sqlite.org/index.html) database to keep 
//! track of files stored in a common directory. The files are compressed, and so should only be 
//! accessed via the api provided by this crate.
#![deny(missing_docs)]

//
// Public API
//
pub use archive::Archive;
pub use errors::BufkitDataErr;
pub use inventory::Inventory;
pub use models::Model;
pub use site::{Site, StateProv};

//
// Implementation only
//
extern crate chrono;
extern crate flate2;
extern crate rusqlite;
extern crate sounding_analysis;
extern crate sounding_bufkit;
extern crate strum;
#[macro_use]
extern crate strum_macros;

mod archive;
mod errors;
mod inventory;
mod models;
mod site;

#[cfg(test)]
extern crate tempdir;
