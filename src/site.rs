use crate::errors::{Result, BufkitDataErr};
use rusqlite::{types::ToSql, Connection, OptionalExtension, Row, NO_PARAMS};
use std::str::FromStr;
use strum::AsStaticRef;
use strum_macros::{AsStaticStr, EnumIter, EnumString};

/// Description of a site with a sounding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Site {
    /// Site id, usually a 3 or 4 letter identifier (e.g. kord katl ksea).
    short_name: String,
    /// A longer, more human readable name.
    long_name: Option<String>,
    /// Any relevant notes about the site.
    notes: Option<String>,
    /// The state or providence where this location is located. This allows querying sites by what
    /// state or providence they are in.
    state: Option<StateProv>,
    /// Does this site represent a mobile unit.
    is_mobile: bool,
    /// Row id from the database
    id: i64,
}

impl Site {
    /// Create a new site with the short name.
    #[inline]
    pub fn new(short_name: &str) -> Self {
        Self {
            short_name: short_name.to_owned(),
            long_name: None,
            notes: None,
            state: None,
            is_mobile: false,
            id: -1,
        }
    }

    /// Add a long name description.
    #[inline]
    pub fn with_long_name<T>(self, long_name: T) -> Self
    where
        Option<String>: From<T>,
    {
        Self {
            long_name: Option::from(long_name),
            ..self
        }
    }

    /// Add notes to a site.
    #[inline]
    pub fn with_notes<T>(self, notes: T) -> Self
    where
        Option<String>: From<T>,
    {
        Self {
            notes: Option::from(notes),
            ..self
        }
    }

    /// Add a state/providence association to a site.
    #[inline]
    pub fn with_state_prov<T>(self, state: T) -> Self
    where
        Option<StateProv>: From<T>,
    {
        Self {
            state: Option::from(state),
            ..self
        }
    }

    /// Set whether or not this is a mobile site.
    #[inline]
    pub fn set_mobile(self, is_mobile: bool) -> Self {
        Self { is_mobile, ..self }
    }

    /// Get the short name, or id for this site
    #[inline]
    pub fn short_name(&self) -> &str {
        &self.short_name
    }

    /// Get the long name for this site.
    #[inline]
    pub fn long_name(&self) -> Option<&str> {
        self.long_name.as_ref().map(|val| val.as_ref())
    }

    /// Get the notes for this site.
    #[inline]
    pub fn notes(&self) -> Option<&str> {
        self.notes.as_ref().map(|val| val.as_ref())
    }

    /// Get the state/providence for this site.
    #[inline]
    pub fn state_prov(&self) -> Option<StateProv> {
        self.state
    }

    /// Get whether or not this is a mobile site.
    #[inline]
    pub fn is_mobile(&self) -> bool {
        self.is_mobile
    }

    /// Get whether or not the site has been verified as being in the database.
    #[inline]
    pub fn is_known(&self) -> bool {
        self.id > 0 // sqlite starts at row id = 1
    }

    pub(crate) fn id(&self) -> i64 {
        self.id
    }

    /// Return true if there is any missing data. It ignores the notes field since this is only
    /// rarely used.
    #[inline]
    pub fn incomplete(&self) -> bool {
        self.long_name.is_none() || self.state.is_none()
    }
}

/// Retrieve the sounding type information from the database for the given source name.
#[inline]
pub(crate) fn retrieve_site(db: &Connection, short_name: &str) -> Result<Option<Site>> {
    match db.query_row(
        "
            SELECT id, short_name, long_name, state, notes, mobile_sounding_site 
            FROM sites 
            WHERE short_name = ?1
        ",
        &[&short_name],
        parse_row_to_site,
    ) {
        Ok(Ok(site)) => Ok(Some(site)),
        Ok(Err(err)) => Err(err),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(BufkitDataErr::from(err)),
    }
}

/// Insert or update the site information in the database.
#[inline]
pub(crate) fn insert_or_update_site(db: &Connection, site: Site) -> Result<Site> {
    if let Some(row_id) = db
        .query_row(
            "SELECT rowid FROM sites WHERE short_name = ?1",
            &[site.short_name()],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
    {
        // row already exists - so update
        db.execute(
            "
                UPDATE sites 
                SET (long_name, state, notes, mobile_sounding_site)
                = (?2, ?3, ?4, ?5)
                WHERE short_name = ?1
            ",
            &[
                &site.short_name,
                &site.long_name as &ToSql,
                &site.state_prov().map(|st| st.as_static()) as &ToSql,
                &site.notes(),
                &site.is_mobile(),
            ],
        )?;

        Ok(Site { id: row_id, ..site })
    } else {
        // insert
        db.execute(
            "
                INSERT INTO sites(short_name, long_name, state, notes, mobile_sounding_site) 
                VALUES(?1, ?2, ?3, ?4, ?5)
            ",
            &[
                &site.short_name,
                &site.long_name as &ToSql,
                &site.state_prov().map(|st| st.as_static()) as &ToSql,
                &site.notes(),
                &site.is_mobile(),
            ],
        )?;

        let row_id = db.last_insert_rowid();
        Ok(Site { id: row_id, ..site })
    }
}

/// Get a list of sites from the index
#[inline]
pub(crate) fn all_sites(db: &Connection) -> Result<Vec<Site>> {
    let mut stmt = db.prepare(
        "
            SELECT id, short_name, long_name, state, notes, mobile_sounding_site
            FROM sites;
        ",
    )?;

    let vals: Result<Vec<Site>> =
        stmt.query_and_then(NO_PARAMS, parse_row_to_site)?.collect();

    vals
}

fn parse_row_to_site(row: &Row) -> Result<Site> {
    let short_name: String = row.get_checked(1)?;
    let long_name: Option<String> = row.get_checked(2)?;
    let notes: Option<String> = row.get_checked(4)?;
    let is_mobile = row.get_checked(5)?;
    let state: Option<StateProv> = row
        .get_checked::<_, String>(3)
        .ok()
        .and_then(|a_string| StateProv::from_str(&a_string).ok());
    let id: i64 = row.get_checked(0)?;

    Ok(Site {
        short_name,
        long_name,
        notes,
        is_mobile,
        state,
        id,
    })
}

/// State/Providence abreviations for declaring a state in the site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, AsStaticStr, EnumIter)]
#[allow(missing_docs)]
pub enum StateProv {
    AL, // Alabama
    AK, // Alaska
    AZ, // Arizona
    AR, // Arkansas
    CA, // California
    CO, // Colorado
    CT, // Connecticut
    DE, // Delaware
    FL, // Florida
    GA, // Georgia
    HI, // Hawaii
    ID, // Idaho
    IL, // Illinois
    IN, // Indiana
    IA, // Iowa
    KS, // Kansas
    KY, // Kentucky
    LA, // Louisiana
    ME, // Maine
    MD, // Maryland
    MA, // Massachussetts
    MI, // Michigan
    MN, // Minnesota
    MS, // Mississippi
    MO, // Missouri
    MT, // Montana
    NE, // Nebraska
    NV, // Nevada
    NH, // New Hampshire
    NJ, // New Jersey
    NM, // New Mexico
    NY, // New York
    NC, // North Carolina
    ND, // North Dakota
    OH, // Ohio
    OK, // Oklahoma
    OR, // Oregon
    PA, // Pensylvania
    RI, // Rhode Island
    SC, // South Carolina
    SD, // South Dakota
    TN, // Tennessee
    TX, // Texas
    UT, // Utah
    VT, // Vermont
    VA, // Virginia
    WA, // Washington
    WV, // West Virginia
    WI, // Wisconsin
    WY, // Wyoming
    // US Commonwealth and Territories
    AS, // American Samoa
    DC, // District of Columbia
    FM, // Federated States of Micronesia
    MH, // Marshall Islands
    MP, // Northern Mariana Islands
    PW, // Palau
    PR, // Puerto Rico
    VI, // Virgin Islands
}

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;
    use rusqlite::{Connection, OpenFlags};
    use std::{str::FromStr};
    use strum::{AsStaticRef, IntoEnumIterator};
    use tempdir::TempDir;

    #[test]
    fn test_site_incomplete() {
        let complete_site = Site {
            short_name: "kxly".to_owned(),
            long_name: Some("tv station".to_owned()),
            state: Some(StateProv::VI),
            notes: Some("".to_owned()),
            is_mobile: false,
            id: -1,
        };

        let incomplete_site = Site {
            short_name: "kxly".to_owned(),
            long_name: Some("tv station".to_owned()),
            state: None,
            notes: None,
            is_mobile: false,
            id: -1,
        };

        assert!(!complete_site.incomplete());
        assert!(incomplete_site.incomplete());
    }

    #[test]
    fn test_to_string_for_state_prov() {
        assert_eq!(StateProv::AL.as_static(), "AL");
    }

    #[test]
    fn test_from_string_for_state_prov() {
        assert_eq!(StateProv::from_str("AL").unwrap(), StateProv::AL);
    }

    #[test]
    fn round_trip_strings_for_state_prov() {
        for state_prov in StateProv::iter() {
            assert_eq!(
                StateProv::from_str(state_prov.as_static()).unwrap(),
                state_prov
            );
        }
    }

    #[test]
    fn test_insert_retrieve_site() -> Result<()> {
        let tmp = TempDir::new("bufkit-data-test-archive")?;
        let db_file = tmp.as_ref().join("test_index.sqlite");
        let db_conn = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        db_conn.execute_batch(include_str!("create_index.sql"))?;

        insert_or_update_site(&db_conn, Site::new("kmso"))?;
        let site = dbg!(retrieve_site(&db_conn, "kmso"))?.unwrap();

        assert_eq!(site.short_name(), "kmso");

        Ok(())
    }
}
