use crate::errors::BufkitDataErr;
use rusqlite::{Connection, OptionalExtension, types::ToSql};

/// A geographic location.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Location {
    /// Decimal degrees latitude
    latitude: f64,
    /// Decimal degrees longitude
    longitude: f64,
    /// Elevation in meters.
    elevation_m: Option<i32>,
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
    pub fn new<T, U>(lat: f64, lon: f64, elev: T, tz_offset: U) -> Self
    where
        Option<i32>: From<T>,
        Option<i32>: From<U>,
    {
        assert!(lat <= 90.0 && lat >= -90.0, "Latitude Range Error");
        assert!(lon <= 180.0 && lon >= -180.0, "Longitude Range Error");

        Location {
            latitude: lat,
            longitude: lon,
            elevation_m: Option::from(elev),
            tz_offset: Option::from(tz_offset),
            id: -1,
        }
    }

    /// Create a new location.
    ///
    /// Returns `None` the if latitude is outside the canonical [-90, 90] range or longitude is
    /// outside the canonical [-180, 180] range.
    pub fn checked_new<T, U>(lat: f64, lon: f64, elev: T, tz_offset: U) -> Option<Self>
    where
        Option<i32>: From<T>,
        Option<i32>: From<U>,
    {
        if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 {
            None
        } else {
            Some(Location {
                latitude: lat,
                longitude: lon,
                elevation_m: Option::from(elev),
                tz_offset: Option::from(tz_offset),
                id: -1,
            })
        }
    }

    /// Add elevation in meters data to a location.
    pub fn with_elevation<T>(self, elev: T) -> Self
    where
        Option<i32>: From<T>,
    {
        Location {
            elevation_m: Option::from(elev),
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
    pub fn elevation(&self) -> Option<i32> {
        self.elevation_m
    }

    /// Get the time zone offset from UTC in seconds
    pub fn tz_offset(&self) -> Option<i32> {
        self.tz_offset
    }

    /// Determine if this location has a valid id number
    pub fn is_known(&self) -> bool {
        self.id > 0
    }

    pub(crate) fn id(&self) ->i64 {
        self.id
    }
}

/// Insert or update the location information in the database.
#[inline]
pub(crate) fn insert_or_update_location(db: &Connection, location: Location) -> Result<Location, BufkitDataErr> {
    
    if let Some(row_id) = db.query_row(
            "
                SELECT rowid 
                FROM locations 
                WHERE latitude = ?1 AND longitude = ?2 and elevation_meters = ?3
            ",
        &[
            &((location.latitude * 1_000_000.0) as i64), 
            &((location.longitude * 1_000_000.0) as i64),  
            &location.elevation_m as &ToSql,
        ],
        |row| row.get::<_,i64>(0),
    ).optional()?
    {
        // row already exists - so update
        db.execute(
            "
                UPDATE locations
                SET (tz_offset_seconds)
                = (?2)
                WHERE id = ?1
            ",
            &[
                &location.id, 
                &location.tz_offset as &ToSql,
            ],
        )?;
        
        Ok(Location {id: row_id, ..location})
    } else {
        // insert
        db.execute(
            "
                INSERT INTO locations(latitude, longitude, elevation_meters, tz_offset_seconds) 
                VALUES(?1, ?2, ?3, ?4)
            ", 
            &[
                &((location.latitude * 1_000_000.0) as i64), 
                &((location.latitude * 1_000_000.0) as i64), 
                &location.elevation_m as &ToSql, 
                &location.tz_offset,
            ])?;

        let row_id = db.last_insert_rowid();
        Ok(Location {id: row_id, ..location})
    }
}

#[cfg(test)]
mod tests {
    // TODO: make some tests
}
