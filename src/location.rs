use crate::{
    errors::{BufkitDataErr, Result},
    site::Site,
    sounding_type::SoundingType,
};
use rusqlite::{types::ToSql, Connection, Row, NO_PARAMS};

/// A geographic location.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Location {
    /// Decimal degrees latitude
    latitude: f64,
    /// Decimal degrees longitude
    longitude: f64,
    /// Elevation in meters.
    elevation_m: i32,
    /// Time zone offset from UTC in seconds
    tz_offset: Option<i32>,
    /// row id in the database
    id: i64,
}

impl Location {
    /// Create a new location.
    ///
    /// Panics if latitude is outside the canonical [-90, 90] range or longitude is outside the
    /// canonical [-180, 180] range.
    pub fn new<T>(lat: f64, lon: f64, elev: i32, tz_offset: T) -> Self
    where
        Option<i32>: From<T>,
    {
        assert!(lat <= 90.0 && lat >= -90.0, "Latitude Range Error");
        assert!(lon <= 180.0 && lon >= -180.0, "Longitude Range Error");

        Location {
            latitude: lat,
            longitude: lon,
            elevation_m: elev,
            tz_offset: Option::from(tz_offset),
            id: -1,
        }
    }

    /// Create a new location.
    ///
    /// Returns `None` the if latitude is outside the canonical [-90, 90] range or longitude is
    /// outside the canonical [-180, 180] range.
    pub fn checked_new<T, U>(lat: f64, lon: f64, elev: i32, tz_offset: T) -> Option<Self>
    where
        Option<i32>: From<T>,
    {
        if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 {
            None
        } else {
            Some(Location {
                latitude: lat,
                longitude: lon,
                elevation_m: elev,
                tz_offset: Option::from(tz_offset),
                id: -1,
            })
        }
    }

    /// Add elevation in meters data to a location.
    pub fn with_elevation(self, elev: i32) -> Self {
        Location {
            elevation_m: elev,
            ..self
        }
    }

    /// Add timezone data to a location, offset from UTC in seconds.
    pub fn with_tz_offset<T>(self, tz_offset: T) -> Self
    where
        Option<i32>: From<T>,
    {
        Location {
            tz_offset: Option::from(tz_offset),
            ..self
        }
    }

    /// Get the latitude in degrees.
    pub fn latitude(&self) -> f64 {
        self.latitude
    }

    /// Get the longitude in degrees.
    pub fn longitude(&self) -> f64 {
        self.longitude
    }

    /// Get the elevation in meters.
    pub fn elevation(&self) -> i32 {
        self.elevation_m
    }

    /// Get the time zone offset from UTC in seconds.
    pub fn tz_offset(&self) -> Option<i32> {
        self.tz_offset
    }

    /// Determine if this location has been verified as being in the archive index.
    pub fn is_valid(&self) -> bool {
        self.id > 0
    }

    pub(crate) fn id(&self) -> i64 {
        self.id
    }
}

/// Get a list of locations from the index
#[inline]
pub(crate) fn all_locations(db: &Connection) -> Result<Vec<Location>> {
    let mut stmt = db.prepare(
        "
            SELECT id, latitude, longitude, elevation_meters, tz_offset_seconds
            FROM locations;
        ",
    )?;

    let vals: Result<Vec<Location>> = stmt
        .query_and_then(NO_PARAMS, parse_row_to_location)?
        .collect();

    vals
}

/// Retrieve the location associated with these coordinates.
#[inline]
pub(crate) fn retrieve_location(
    db: &Connection,
    latitude: f64,
    longitude: f64,
    elevation_m: i32,
) -> Result<Option<Location>> {
    match db.query_row(
        "
            SELECT id, latitude, longitude, elevation_meters, tz_offset_seconds
            FROM locations
            WHERE latitude = ?1 AND longitude = ?2 AND elevation_meters = ?3
        ",
        &[
            &((latitude * 1_000_000.0) as i64),
            &((longitude * 1_000_000.0) as i64),
            &elevation_m as &ToSql,
        ],
        parse_row_to_location,
    ) {
        Ok(Ok(location)) => Ok(Some(location)),
        Ok(Err(err)) => Err(err),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(BufkitDataErr::from(err)),
    }
}

/// Retrieve the location associated with these coordinates, it it doesn't exist yet add it to the
/// index.
#[inline]
pub(crate) fn retrieve_or_add_location(
    db: &Connection,
    latitude: f64,
    longitude: f64,
    elevation_m: i32,
) -> Result<Location> {
    match db.query_row(
        "
            SELECT id, latitude, longitude, elevation_meters, tz_offset_seconds
            FROM locations
            WHERE latitude = ?1 AND longitude = ?2 AND elevation_meters = ?3
        ",
        &[
            &((latitude * 1_000_000.0) as i64),
            &((longitude * 1_000_000.0) as i64),
            &elevation_m as &ToSql,
        ],
        parse_row_to_location,
    ) {
        Ok(Ok(location)) => Ok(location),
        Ok(Err(err)) => Err(err),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            // Query worked, but found nothing
            insert_location_(db, latitude, longitude, elevation_m, None)
        }
        Err(err) => Err(BufkitDataErr::from(err)),
    }
}

/// Update the location information in the index.
#[inline]
pub(crate) fn update_location(db: &Connection, location: Location) -> Result<Location> {
    db.execute(
        "
                UPDATE locations
                SET (tz_offset_seconds)
                = (?2)
                WHERE id = ?1
            ",
        &[&location.id, &location.tz_offset as &ToSql],
    )?;

    retrieve_location(
        db,
        location.latitude,
        location.longitude,
        location.elevation_m,
    )
    .map(|opt| opt.unwrap())
}

/// Insert the location information in the index.
#[inline]
pub(crate) fn insert_location(db: &Connection, location: Location) -> Result<Location> {
    insert_location_(
        db,
        location.latitude,
        location.longitude,
        location.elevation_m,
        location.tz_offset,
    )
}

fn insert_location_(
    db: &Connection,
    latitude: f64,
    longitude: f64,
    elevation_m: i32,
    tz_offset: Option<i32>,
) -> Result<Location> {
    db.execute(
        "
            INSERT INTO locations(latitude, longitude, elevation_meters, tz_offset_seconds) 
            VALUES(?1, ?2, ?3, ?4)
        ",
        &[
            &((latitude * 1_000_000.0) as i64),
            &((longitude * 1_000_000.0) as i64),
            &elevation_m as &ToSql,
            &tz_offset,
        ],
    )?;

    let row_id = db.last_insert_rowid();
    Ok(Location {
        id: row_id,
        latitude,
        longitude,
        elevation_m,
        tz_offset,
    })
}

/// Retrieve all the different location associated with a given `Site` and `SoundingType`.
#[inline]
pub(crate) fn all_locations_for_site_and_type(
    db: &Connection,
    site: &Site,
    sounding_type: &SoundingType,
) -> Result<Vec<Location>> {
    let mut stmt = db.prepare(
        "
            SELECT id, latitude, longitude, elevation_meters, tz_offset_seconds 
            FROM locations
            WHERE locations.id IN
                (SELECT DISTINCT files.location_id 
                 FROM files 
                 WHERE files.site_ID = ?1 AND files.type_id = ?2
                );
        ",
    )?;

    let vals: Result<Vec<Location>> = stmt
        .query_and_then(&[site.id(), sounding_type.id()], parse_row_to_location)?
        .collect();

    vals
}

fn parse_row_to_location(row: &Row) -> Result<Location> {
    let id: i64 = row.get_checked(0)?;
    let latitude: f64 = row.get_checked::<_, i64>(1)? as f64 / 1_000_000.0;
    let longitude: f64 = row.get_checked::<_, i64>(2)? as f64 / 1_000_000.0;
    let elevation_m: i32 = row.get_checked(3)?;
    let tz_offset: Option<i32> = row.get_checked(4)?;

    Ok(Location {
        id,
        latitude,
        longitude,
        elevation_m,
        tz_offset,
    })
}

#[cfg(test)]
mod tests {
    // TODO: make some tests
}
