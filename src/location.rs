/// A geographic location.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Location {
    /// Decimal degrees latitude
    latitude: f64,
    /// Decimal degrees longitude
    longitude: f64,
    /// Elevation in meters.
    elevation_m: Option<f64>,
    /// Time zone offset from UTC in seconds
    tz_offset: Option<i32>,
}

impl Location {
    /// Create a new location.
    ///
    /// Panics if latitude is outside the canonical [-90, 90] range or longitude is outside the
    /// canonical [-180, 180] range.
    pub fn new<T, U>(lat: f64, lon: f64, elev: T, tz_offset: U) -> Self
    where
        Option<f64>: From<T>,
        Option<i32>: From<U>,
    {
        assert!(lat <= 90.0 && lat >= -90.0, "Latitude Range Error");
        assert!(lon <= 180.0 && lon >= -180.0, "Longitude Range Error");

        Location {
            latitude: lat,
            longitude: lon,
            elevation_m: Option::from(elev),
            tz_offset: Option::from(tz_offset),
        }
    }

    /// Create a new location.
    ///
    /// Returns `None` the if latitude is outside the canonical [-90, 90] range or longitude is
    /// outside the canonical [-180, 180] range.
    pub fn checked_new<T, U>(lat: f64, lon: f64, elev: T, tz_offset: U) -> Option<Self>
    where
        Option<f64>: From<T>,
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
            })
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
    pub fn elevation(&self) -> Option<f64> {
        self.elevation_m
    }
}

#[cfg(test)]
mod tests {
    // TODO: make some tests
}
