use crate::{errors::BufkitDataErr, location::Location, site::Site, sounding_type::SoundingType};
use chrono::{Duration, NaiveDateTime};
use fnv::{FnvHashMap, FnvHashSet};
use rusqlite::Connection;

/// Inventory lists first & last initialization times of the models in the archive for a site &
/// model. It also contains a list of model initialization times that are missing between the first
/// and last.
#[derive(Debug)]
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

/// Get an inventory of models and dates for a sounding
pub fn inventory(db: &Connection, site: Site) -> Result<Inventory, BufkitDataErr> {
    debug_assert!(site.id() > 0);

    // Get all the sounding types for this site
    let sounding_types: FnvHashSet<_> =
        crate::sounding_type::all_sounding_types_for_site(db, &site)?
            .into_iter()
            .collect();

    let mut range = FnvHashMap::default();
    let mut missing = FnvHashMap::default();
    let mut locations = FnvHashMap::default();
    for sounding_type in sounding_types.iter() {
        // Add locations
        let locs_for_type =
            crate::location::retrieve_locations_for_site_and_type(db, &site, &sounding_type)?;
        locations.insert(sounding_type.clone(), locs_for_type);

        // Add the range
        let mut stmt = db.prepare(
            "
                SELECT MIN(init_time), MAX(init_time)
                FROM files
                WHERE site_id = ?1 AND type_id = ?2;
            ",
        )?;

        let rng: (NaiveDateTime, NaiveDateTime) = stmt
            .query_row(&[site.id(), sounding_type.id()], |row| {
                (row.get(0), row.get(1))
            })?;
        range.insert(sounding_type.clone(), rng);

        // Add the missing values
        if let Some(delta_hours) = sounding_type.hours_between_initializations() {
            let mut missing_trs = vec![];
            let delta_t = Duration::hours(delta_hours as i64);

            let mut stmt = db.prepare(
                "
                    SELECT init_time
                    FROM files
                    WHERE site_id = ?1 AND type_id = ?2
                    ORDER BY init_time ASC;
            ",
            )?;

            let mut next_time = rng.0;
            stmt.query_map(&[site.id(), sounding_type.id()], |row| {
                row.get::<_, NaiveDateTime>(0)
            })?
            .filter_map(|res| res.ok())
            .for_each(|init_time| {
                if next_time < init_time {
                    let start = next_time;
                    let mut end = next_time;
                    while next_time < init_time {
                        end = next_time;
                        next_time += delta_t;
                    }

                    missing_trs.push((start, end));
                }

                next_time += delta_t;
            });

            missing.insert(sounding_type.clone(), missing_trs);
        }
    }

    Ok(Inventory {
        site,
        sounding_types,
        range,
        missing,
        locations,
    })
}
