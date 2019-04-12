use crate::errors::BufkitDataErr;
use rusqlite::{types::ToSql, Connection, OptionalExtension};
use strum::AsStaticRef;
use strum_macros::{AsStaticStr, EnumString};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SoundingType {
    observed: bool, // False if it is a model generated sounding
    file_type: FileType,
    source: String,             // Description such as model name or RAWIN_SONDE
    hours_between: Option<u16>, // Hours between observations or model initializations
    id: i64,                    // id code from the database
}

impl SoundingType {
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

    #[inline]
    pub fn new_model<T>(src: &str, file_type: FileType, hours_between: T) -> Self
    where
        Option<u16>: From<T>,
    {
        Self::new(src, false, file_type, hours_between)
    }

    #[inline]
    pub fn new_observed<T>(src: &str, file_type: FileType, hours_between: T) -> Self
    where
        Option<u16>: From<T>,
    {
        Self::new(src, true, file_type, hours_between)
    }

    #[inline]
    pub fn is_modeled(&self) -> bool {
        !self.observed
    }

    #[inline]
    pub fn is_observed(&self) -> bool {
        self.observed
    }

    #[inline]
    pub fn is_known(&self) -> bool {
        self.id > -0
    }

    #[inline]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[inline]
    pub fn hours_between_initializations(&self) -> Option<u16> {
        self.hours_between
    }

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
) -> Result<SoundingType, BufkitDataErr> {
    unimplemented!()
}

/// Insert or update the sounding type information in the database.
#[inline]
pub(crate) fn insert_or_update_sounding_type(
    db: &Connection,
    sounding_type: SoundingType,
) -> Result<SoundingType, BufkitDataErr> {
    if let Some(row_id) = db
        .query_row(
            "SELECT rowid FROM types where type = ?1",
            &[sounding_type.source()],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
    {
        // row already exists - so update
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

        Ok(SoundingType {
            id: row_id,
            ..sounding_type
        })
    } else {
        // insert
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
}

/// Flag for how the sounding data is encoded in the file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, AsStaticStr)]
pub enum FileType {
    /// A bufkit encoded file.
    BUFKIT,
    /// A bufr encoded file.
    BUFR,
}
