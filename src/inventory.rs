use crate::{errors::BufkitDataErr, location::Location, site::Site, sounding_type::SoundingType};
use chrono::{Duration, NaiveDateTime};
use std::collections::{HashMap, HashSet};

/// Inventory lists first & last initialization times of the models in the archive for a site &
/// model. It also contains a list of model initialization times that are missing between the first
/// and last.
#[derive(Debug, PartialEq)]
pub struct Inventory {
    /// The site this is an inventory for.
    pub site: Site,
    /// The type of sounding file
    pub sounding_types: HashSet<SoundingType>,
    /// The earliest and latest init_time in the archive.
    pub range: HashMap<SoundingType, (NaiveDateTime, NaiveDateTime)>,
    /// A list of start and end times for missing model runs.
    pub missing: HashMap<SoundingType, Vec<(NaiveDateTime, NaiveDateTime)>>,
    /// Locations
    pub locations: HashMap<SoundingType, Vec<Location>>,
}
