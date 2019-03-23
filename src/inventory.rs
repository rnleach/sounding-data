use crate::{location::Location, site::Site, sounding_type::SoundingType};
use chrono::NaiveDateTime;

use fnv::{FnvHashMap, FnvHashSet};

/// Inventory lists first & last initialization times of the models in the archive for a site &
/// model. It also contains a list of model initialization times that are missing between the first
/// and last.
#[derive(Debug, PartialEq)]
pub struct Inventory {
    /// The site this is an inventory for.
    site: Site,
    /// The type of sounding file
    sounding_types: FnvHashSet<SoundingType>,
    /// The earliest and latest init_time in the archive.
    range: FnvHashMap<SoundingType, (NaiveDateTime, NaiveDateTime)>,
    /// A list of start and end times for missing model runs.
    missing: FnvHashMap<SoundingType, Vec<(NaiveDateTime, NaiveDateTime)>>,
    /// Locations
    locations: FnvHashMap<SoundingType, Vec<Location>>,
}

impl Inventory {
    /// Create a new inventory
    pub fn new(site: Site) -> Self {
        Inventory {
            site,
            sounding_types: FnvHashSet::default(),
            range: FnvHashMap::default(),
            missing: FnvHashMap::default(),
            locations: FnvHashMap::default(),
        }
    }

    /// Add/Update a range of dates for which we have data.
    pub fn add_update_range(
        mut self,
        sounding_type: SoundingType,
        range: (NaiveDateTime, NaiveDateTime),
    ) -> Self {
        self.sounding_types.insert(sounding_type.clone());
        self.range.insert(sounding_type, range);
        self
    }

    /// Add/Update a range of missing values
    pub fn add_missing_range(
        mut self,
        sounding_type: SoundingType,
        range: (NaiveDateTime, NaiveDateTime),
    ) -> Self {
        self.sounding_types.insert(sounding_type.clone());
        let missing = self.missing.entry(sounding_type).or_insert(vec![]);
        missing.push(range);
        self
    }

    /// Add/Update a location
    pub fn add_location(mut self, sounding_type: SoundingType, location: Location) -> Self {
        self.sounding_types.insert(sounding_type.clone());
        let locations = self.locations.entry(sounding_type).or_insert(vec![]);
        locations.push(location);
        self
    }

    /// Get the range of dates we have data for.
    pub fn range(&self, sounding_type: &SoundingType) -> Option<(NaiveDateTime, NaiveDateTime)> {
        self.range
            .get(sounding_type)
            .map(|&(start, finish)| (start, finish))
    }

    /// Get the ranges of dates for which we are missing data.
    pub fn missing(&self, sounding_type: &SoundingType) -> &[(NaiveDateTime, NaiveDateTime)] {
        self.missing
            .get(sounding_type)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get the locations for which we have data at a given site.
    pub fn locations(&self, sounding_type: &SoundingType) -> &[Location] {
        self.locations
            .get(sounding_type)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}
