use crate::{
    errors::{BufkitDataErr, Result},
    site::Site,
};
use rusqlite::{types::ToSql, Connection, Row, NO_PARAMS};
use std::str::FromStr;
use strum::AsStaticRef;
use strum_macros::{AsStaticStr, EnumString};

/// Stores information about the type of this sounding, including a unique source name.
///
/// This is used to keep track of the data source, such as "GFS", "NAM", "NamNest", "NOAA Archived".
/// It also includes information about whether this is a model or observed sounding type, and the
/// expected hours between initializations (models) or launches (observed).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SoundingType {
    observed: bool, // False if it is a model generated sounding
    file_type: FileType,
    source: String,             // Description such as model name or RAWIN_SONDE
    hours_between: Option<u16>, // Hours between observations or model initializations
    id: i64,                    // id code from the database
}

impl SoundingType {
    /// Create a new sounding type.
    pub fn new<T>(src: &str, observed: bool, file_type: FileType, hours_between: T) -> Self
    where
        Option<u16>: From<T>,
    {
        SoundingType {
            observed,
            file_type,
            source: src.to_uppercase(),
            hours_between: Option::from(hours_between),
            id: -1, // Uninitialized in the database.
        }
    }

    /// Create a new sounding type that assumes a model.
    #[inline]
    pub fn new_model<T>(src: &str, file_type: FileType, hours_between: T) -> Self
    where
        Option<u16>: From<T>,
    {
        Self::new(src, false, file_type, hours_between)
    }

    /// Create a new sounding type that assumes it is observed.
    #[inline]
    pub fn new_observed<T>(src: &str, file_type: FileType, hours_between: T) -> Self
    where
        Option<u16>: From<T>,
    {
        Self::new(src, true, file_type, hours_between)
    }

    /// `true` if this type represents a model sounding.
    #[inline]
    pub fn is_modeled(&self) -> bool {
        !self.observed
    }

    /// `true` if this type represents an observed sounding.
    #[inline]
    pub fn is_observed(&self) -> bool {
        self.observed
    }

    /// `true` if this type has been verified to be in the archive index.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.id > -0
    }

    /// Get the unique string that represents the sounding source such as "GFS", "NAM", etc.
    #[inline]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Hours between model initialization for models and between launches for observed soundings.
    #[inline]
    pub fn hours_between_initializations(&self) -> Option<u16> {
        self.hours_between
    }

    /// This is the file type that the decompressed data is stored in.
    // FIXME: factor this out to its own module and store it in the database on its own.
    #[inline]
    pub fn file_type(&self) -> FileType {
        self.file_type
    }

    pub(crate) fn id(&self) -> i64 {
        self.id
    }
}

/// Retrieve the sounding type information from the database for the given source name.
#[inline]
pub(crate) fn retrieve_sounding_type(
    db: &Connection,
    sounding_type_as_str: &str,
) -> Result<Option<SoundingType>> {
    match db.query_row(
        "
            SELECT id, type, file_type, interval, observed
            FROM types
            WHERE type = ?1
        ",
        &[sounding_type_as_str],
        parse_row_to_sounding_type,
    ) {
        Ok(Ok(sounding_type)) => Ok(Some(sounding_type)),
        Ok(Err(err)) => Err(err),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(BufkitDataErr::from(err)),
    }
}

/// Update the sounding type information in the index.
#[inline]
pub(crate) fn update_sounding_type(
    db: &Connection,
    sounding_type: SoundingType,
) -> Result<SoundingType> {
    db.execute(
        "
                UPDATE types
                SET (interval, observed)
                = (?2, ?3)
                WHERE type = ?1
            ",
        &[
            &sounding_type.source,
            &sounding_type.hours_between as &ToSql,
            &sounding_type.observed,
        ],
    )?;

    retrieve_sounding_type(db, &sounding_type.source).map(|opt| opt.unwrap())
}

/// Insert the `SoundingType` information in the index.
#[inline]
pub(crate) fn insert_sounding_type(
    db: &Connection,
    sounding_type: SoundingType,
) -> Result<SoundingType> {
    db.execute(
        "
            INSERT INTO types(type, file_type, interval, observed) 
            VALUES(?1, ?2, ?3, ?4)
        ",
        &[
            &sounding_type.source,
            &sounding_type.file_type.as_static() as &ToSql,
            &sounding_type.hours_between as &ToSql,
            &sounding_type.observed,
        ],
    )?;

    let row_id = db.last_insert_rowid();
    Ok(SoundingType {
        id: row_id,
        ..sounding_type
    })
}

/// Get a list of sites from the index
#[inline]
pub(crate) fn all_sounding_types(db: &Connection) -> Result<Vec<SoundingType>> {
    let mut stmt = db.prepare(
        "
             SELECT id, type, file_type, interval, observed
             FROM types;
        ",
    )?;

    let vals: Result<Vec<SoundingType>> = stmt
        .query_and_then(NO_PARAMS, parse_row_to_sounding_type)?
        .collect();

    vals
}

/// Get a list of all the sounding types stored in the database for a particular site
#[inline]
pub(crate) fn all_sounding_types_for_site(
    db: &Connection,
    site: &Site,
) -> Result<Vec<SoundingType>> {
    let mut stmt = db.prepare(
        "
            SELECT id, type, file_type, interval, observed 
            FROM types
            WHERE types.id IN 
                (SELECT DISTINCT files.type_id FROM files WHERE files.site_id = ?1);
        ",
    )?;

    let vals: Result<Vec<SoundingType>> = stmt
        .query_and_then(&[&site.id()], parse_row_to_sounding_type)?
        .collect();

    vals
}

fn parse_row_to_sounding_type(row: &Row) -> Result<SoundingType> {
    let id: i64 = row.get_checked(0)?;
    let source = row.get_checked(1)?;
    let file_type: FileType = FileType::from_str(&row.get_checked::<_, String>(2)?)?;
    let hours_between = row.get_checked(3)?;
    let observed = row.get_checked(4)?;

    Ok(SoundingType {
        id,
        source,
        file_type,
        hours_between,
        observed,
    })
}

/// Flag for how the sounding data is encoded in the file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, AsStaticStr)]
pub enum FileType {
    /// A bufkit encoded file.
    BUFKIT,
    /// A bufr encoded file.
    BUFR,
}

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;
    use rusqlite::{Connection, OpenFlags};
    use tempdir::TempDir;

    #[test]
    fn test_insert_retrieve_sounding_type() -> Result<()> {
        let tmp = TempDir::new("bufkit-data-test-archive")?;
        let db_file = tmp.as_ref().join("test_index.sqlite");
        let db_conn = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        db_conn.execute_batch(include_str!("create_index.sql"))?;

        insert_sounding_type(
            &db_conn,
            SoundingType::new_model("GFS3", FileType::BUFKIT, 6),
        )?;
        let snd_tp = retrieve_sounding_type(&db_conn, "GFS3")?.expect("No such sounding type.");

        assert_eq!(snd_tp.source(), "GFS3");

        Ok(())
    }
}
