//! An archive of soundings in various formats.

use chrono::{FixedOffset, NaiveDate, NaiveDateTime};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use rusqlite::{types::ToSql, Connection, OpenFlags, Row, NO_PARAMS};
use sounding_analysis::Analysis;
use std::{
    collections::HashSet,
    ffi::{OsStr, OsString},
    fs::{create_dir, create_dir_all, read_dir, remove_file, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
};
use strum::AsStaticRef;

use crate::errors::BufkitDataErr;
use crate::inventory::Inventory;
use crate::site::{Site, StateProv};
use crate::sounding_type::SoundingType;

/// The archive.
#[derive(Debug)]
pub struct Archive {
    root: PathBuf,       // The root directory.
    file_dir: PathBuf,   // the directory containing the downloaded files.
    db_conn: Connection, // An sqlite connection.
}

impl Archive {
    // ---------------------------------------------------------------------------------------------
    // Connecting, creating, and maintaining the archive.
    // ---------------------------------------------------------------------------------------------

    /// Initialize a new archive.
    pub fn create<T>(root: T) -> Result<Self, BufkitDataErr>
    where
        T: AsRef<Path>,
    {
        let file_dir = root.as_ref().join(Archive::FILE_DIR);
        let db_file = root.as_ref().join(Archive::INDEX);
        let root = root.as_ref().to_path_buf();

        create_dir_all(&root)?;
        create_dir(&file_dir)?;

        // Create and set up the archive
        let db_conn = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        db_conn.execute_batch(
            "BEGIN;

            CREATE TABLE types (
                id          INTEGER PRIMARY KEY,  -- Used as foreign key in other tables
                type        TEXT UNIQUE NOT NULL, -- GFS, NAM, NAM4KM, MOBIL, RAWINSONDE, 
                description TEXT,                 -- Human readable description
                observed    INT NOT NULL          -- 0 if false (e.g. model data), 1 if observed
            );

            CREATE TABLE sites (
                id                   INTEGER PRIMARY KEY,
                short_name           TEXT UNIQUE NOT NULL, -- External identifier, WMO#, ICAO id...
                long_name            TEXT DEFUALT NULL,    -- common name
                state                TEXT DEDAULT NULL,    -- State/Providence code
                notes                TEXT DEFAULT NULL,    -- Human readable notes
                mobile_sounding_site INTEGER DEFAULT 0     -- true if this is a a mobile platform
            );

            CREATE TABLE locations (
                id                INTEGER PRIMARY KEY,
                latitude          NUMERIC DEFAULT NULL, -- Decimal degrees
                longitude         NUMERIC DEFAULT NULL, -- Decimal degrees
                elevation_meters  INT     DEFAULT NULL, 
                tz_offset_seconds INT     DEFAULT NULL  -- Offset from UTC in seconds
            );

            CREATE TABLE files (
                type_id     INTEGER     NOT NULL,
                site_id     INTEGER     NOT NULL,
                location_id INTEGER     NOT NULL,
                init_time   TEXT        NOT NULL,
                file_name   TEXT UNIQUE NOT NULL,
                FOREIGN KEY (type_id)     REFERENCES types(id),
                FOREIGN KEY (site_id)     REFERENCES sites(id),
                FOREIGN KEY (location_id) REFERENCES locations(id)
            );

            COMMIT;",
        )?;

        Ok(Archive {
            root,
            file_dir,
            db_conn,
        })
    }

    /// Open an existing archive.
    pub fn connect<T>(root: T) -> Result<Self, BufkitDataErr>
    where
        T: AsRef<Path>,
    {
        let file_dir = root.as_ref().join(Archive::FILE_DIR);
        let db_file = root.as_ref().join(Archive::INDEX);
        let root = root.as_ref().to_path_buf();

        // Create and set up the archive
        let db_conn = Connection::open_with_flags(db_file, OpenFlags::SQLITE_OPEN_READ_WRITE)?;

        Ok(Archive {
            root,
            file_dir,
            db_conn,
        })
    }

    /// Check for errors in the index.
    ///
    /// Return a list of files in the index that are missing on the system and a list of files on
    /// the system that are not in the index.
    ///
    /// The first set returned in the tuple is the files in the index but not the file system. The
    /// second set returned in the tuple is the files on the system but not in the index.
    pub fn check(&self) -> Result<(Vec<OsString>, Vec<OsString>), BufkitDataErr> {
        self.db_conn.execute("PRAGMA cache_size=10000", NO_PARAMS)?;

        self.db_conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS fname ON files (file_name)",
            NO_PARAMS,
        )?;

        let mut all_files_stmt = self.db_conn.prepare("SELECT file_name FROM files")?;

        let index_vals: Result<HashSet<OsString>, BufkitDataErr> = all_files_stmt
            .query_map(NO_PARAMS, |row| -> String { row.get(0) })?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .map(|res| res.map(OsString::from))
            .collect();
        let index_vals = index_vals?;

        let file_system_vals: HashSet<OsString> = read_dir(&self.file_dir)?
            .filter_map(|de| de.ok())
            .map(|de| de.path())
            .filter(|p| p.is_file())
            .filter_map(|p| p.file_name().map(|p| p.to_owned()))
            .collect();

        let files_in_index_but_not_on_file_system: Vec<OsString> = index_vals
            .difference(&file_system_vals)
            .map(|s| s.to_owned())
            .collect();
        let files_not_in_index: Vec<OsString> = file_system_vals
            .difference(&index_vals)
            .map(|s| s.to_owned())
            .collect();

        Ok((files_in_index_but_not_on_file_system, files_not_in_index))
    }

    /// Given a list of files, remove them from the index, but NOT the file system.
    pub fn remove_from_index(&self, file_names: &[OsString]) -> Result<(), BufkitDataErr> {
        // TODO: implement
        unimplemented!()
    }

    /// Given a list of files, remove them from the file system. This assumes they have already been
    /// removed from the index (or never existed there.)
    pub fn remove_from_data_store(&self, file_names: &[String]) -> Result<(), BufkitDataErr> {
        // TODO: implement
        unimplemented!()
    }

    /// Remove files from both the index and the file store.
    // FIXME: Use OsString instead of String.
    pub fn remove_files(&self, file_names: &[String]) -> Result<(), BufkitDataErr> {
        // TODO: implement
        unimplemented!()
    }

    // ---------------------------------------------------------------------------------------------
    // The file system aspects of the archive, e.g. the root directory of the archive
    // ---------------------------------------------------------------------------------------------
    const FILE_DIR: &'static str = "files";
    const INDEX: &'static str = "index.sqlite";

    // ---------------------------------------------------------------------------------------------
    // Query or modify site metadata
    // ---------------------------------------------------------------------------------------------

    fn parse_row_to_site(row: &Row) -> Result<Site, rusqlite::Error> {
        let short_name: String = row.get_checked(0)?;
        let long_name: Option<String> = row.get_checked(1)?;
        let notes: Option<String> = row.get_checked(3)?;
        let is_mobile = row.get_checked(4)?;
        let state: Option<StateProv> = row
            .get_checked::<_, String>(2)
            .ok()
            .and_then(|a_string| StateProv::from_str(&a_string).ok());

        Ok(Site::new(&short_name)
            .with_long_name(long_name)
            .with_notes(notes)
            .with_state_prov(state)
            .set_mobile(is_mobile))
    }

    fn short_name_to_site(&self, short_name: &str) -> Result<Site, BufkitDataErr> {
        unimplemented!()
    }

    fn sounding_type_from_str(&self, sounding_type: &str) -> Result<SoundingType, BufkitDataErr> {
        unimplemented!()
    }

    /// Retrieve a list of sites in the archive.
    pub fn sites(&self) -> Result<Vec<Site>, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare(
            "
                    SELECT 
                        short_name,
                        long_name,
                        state,
                        notes,
                        mobile_sounding_site 
                    FROM sites;",
        )?;

        let vals: Result<Vec<Site>, BufkitDataErr> = stmt
            .query_and_then(NO_PARAMS, Self::parse_row_to_site)?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        vals
    }

    /// Retrieve the information about a single site.
    pub fn site_info(&self, short_name: &str) -> Result<Site, BufkitDataErr> {
        self.db_conn
            .query_row_and_then(
                "
                SELECT 
                    short_name,
                    long_name,
                    state,
                    notes,
                    mobile_sounding_site
                FROM sites
                WHERE short_name = ?1
            ",
                &[&short_name.to_uppercase()],
                Self::parse_row_to_site,
            )
            .map_err(BufkitDataErr::Database)
    }

    /// Modify a sites values.
    pub fn set_site_info(&self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "
                UPDATE sites 
                SET (long_name, state, notes, mobile_sounding_site)
                = (?2, ?3, ?4, ?5)
                WHERE site = ?1
            ",
            &[
                &site.short_name() as &ToSql,
                &site.long_name() as &ToSql,
                &site.state_prov().map(|state_prov| state_prov.as_static()) as &ToSql,
                &site.notes() as &ToSql,
                &site.is_mobile(),
            ],
        )?;

        Ok(())
    }

    /// Add a site to the list of sites.
    pub fn add_site(&self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "INSERT INTO sites (short_name, long_name, state, notes, mobile_sounding_site)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
            &[
                &site.short_name() as &ToSql,
                &site.long_name() as &ToSql,
                &site.state_prov().map(|state_prov| state_prov.as_static()) as &ToSql,
                &site.notes() as &ToSql,
                &site.is_mobile(),
            ],
        )?;

        Ok(())
    }

    /// Check if a site already exists
    pub fn site_exists(&self, short_name: &str) -> Result<bool, BufkitDataErr> {
        let number: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM sites WHERE short_name = ?1",
            &[&short_name.to_uppercase()],
            |row| row.get(0),
        )?;

        Ok(number >= 1)
    }

    // ---------------------------------------------------------------------------------------------
    // Query archive inventory
    // ---------------------------------------------------------------------------------------------

    /// Get a list of all the available model initialization times for a given site and type.
    pub fn init_times(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
    ) -> Result<Vec<NaiveDateTime>, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare(
            "
                SELECT init_time 
                FROM files 
                    JOIN sites ON sites.id = files.site_id 
                    JOIN types ON types.id = files.type_id
                WHERE sites.short_name = ?1 AND types.type = ?2
                ORDER BY init_time ASC
            ",
        )?;

        let init_times: Result<Vec<Result<NaiveDateTime, _>>, BufkitDataErr> = stmt
            .query_map(&[site.short_name(), sounding_type.source()], |row| {
                row.get_checked(0)
            })?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        let init_times: Vec<NaiveDateTime> =
            init_times?.into_iter().filter_map(|res| res.ok()).collect();

        Ok(init_times)
    }

    /// Get an inventory of soundings for a site & model.
    pub fn inventory(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
    ) -> Result<Inventory, BufkitDataErr> {
        // let init_times = self.init_times(site_id, model)?;

        // let site = &self.site_info(site_id)?;

        // Inventory::new(init_times, model, site)
        unimplemented!()
    }

    /// Get a list of models in the archive for this site.
    pub fn sounding_types(&self, site: &Site) -> Result<Vec<SoundingType>, BufkitDataErr> {
        // let mut stmt = self
        //     .db_conn
        //     .prepare("SELECT DISTINCT model FROM files WHERE site = ?1")?;

        // let vals: Result<Vec<Model>, BufkitDataErr> = stmt
        //     .query_map(&[&site_id.to_uppercase()], |row| {
        //         let model: String = row.get(0);
        //         Model::from_str(&model).map_err(|_err| BufkitDataErr::InvalidModelName(model))
        //     })?
        //     .flat_map(|res| res.map_err(BufkitDataErr::Database).into_iter())
        //     .collect();

        // vals
        unimplemented!()
    }

    /// Retrieve the model initialization time of the most recent model in the archive.
    pub fn most_recent_valid_time(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
    ) -> Result<NaiveDateTime, BufkitDataErr> {
        // let init_time: NaiveDateTime = self.db_conn.query_row(
        //     "
        //         SELECT init_time FROM files
        //         WHERE site = ?1 AND model = ?2
        //         ORDER BY init_time DESC
        //         LIMIT 1
        //     ",
        //     &[&site_id.to_uppercase(), model.as_static()],
        //     |row| row.get_checked(0),
        // )??;

        // Ok(init_time)
        unimplemented!()
    }

    /// Check to see if a file is present in the archive and it is retrieveable.
    pub fn file_exists(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> Result<bool, BufkitDataErr> {
        // let num_records: i32 = self.db_conn.query_row(
        //     "SELECT COUNT(*) FROM files WHERE site = ?1 AND model = ?2 AND init_time = ?3",
        //     &[
        //         &site_id.to_uppercase() as &ToSql,
        //         &model.as_static() as &ToSql,
        //         init_time as &ToSql,
        //     ],
        //     |row| row.get_checked(0),
        // )??;

        // Ok(num_records == 1)
        unimplemented!()
    }

    /// Get the number of files stored in the archive.
    pub fn count(&self) -> Result<i64, BufkitDataErr> {
        let num_records: i64 =
            self.db_conn
                .query_row("SELECT COUNT(*) FROM files", NO_PARAMS, |row| {
                    row.get_checked(0)
                })??;

        Ok(num_records)
    }

    // ---------------------------------------------------------------------------------------------
    // Add, remove, and retrieve files from the archive
    // ---------------------------------------------------------------------------------------------

    /// Add a file to the archive.
    pub fn add(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
        file_name: &OsStr,
    ) -> Result<(), BufkitDataErr> {
        // Fetch or insert the type and get id
        // Fetch or insert the site and get the id
        // Fetch or insert the location and get the id
        // Build a file name
        // Check if this file or (site, type, init_time) exist in the database
        // If the file exists with a different site, type, or init_time return an error
        // If the site, type, init_time are the same, delete the file in the archive and add
        //     this one in its place
        // If there is no conflict, add the information to the database index
        // Open the file in binary mode and compress it into a file with the above found name

        unimplemented!()
    }

    /// Retrieve a file from the archive.
    pub fn retrieve(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> Result<Vec<Analysis>, BufkitDataErr> {
        // let file_name: String = self.db_conn.query_row(
        //     "SELECT file_name FROM files WHERE site = ?1 AND model = ?2 AND init_time = ?3",
        //     &[
        //         &site_id.to_uppercase() as &ToSql,
        //         &model.as_static() as &ToSql,
        //         init_time as &ToSql,
        //     ],
        //     |row| row.get_checked(0),
        // )??;

        // let file = File::open(self.file_dir.join(file_name))?;
        // let mut decoder = GzDecoder::new(file);
        // let mut s = String::new();
        // decoder.read_to_string(&mut s)?;
        // Ok(s)
        unimplemented!()
    }

    /// Retrieve and uncompress a file, then save it in the given `export_dir`.
    pub fn export(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
        export_dir: &OsStr,
    ) -> Result<(), BufkitDataErr> {
        unimplemented!()
    }

    /// Retrieve the  most recent file as a sounding.
    pub fn most_recent_file(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
    ) -> Result<Vec<Analysis>, BufkitDataErr> {
        // let init_time = self.most_recent_valid_time(site_id, model)?;
        // self.retrieve(site_id, model, &init_time)
        unimplemented!()
    }

    fn compressed_file_name(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> String {
        // let file_string = init_time.format("%Y%m%d%HZ").to_string();

        // format!(
        //     "{}_{}_{}.buf.gz",
        //     file_string,
        //     model.as_static(),
        //     site_id.to_uppercase()
        // )
        unimplemented!()
    }

    fn retrieve_sounding_type_for(
        &self,
        sounding_type_as_str: &str,
    ) -> Result<SoundingType, BufkitDataErr> {
        unimplemented!()
    }

    fn parse_compressed_file_name(fname: &OsStr) -> Option<(NaiveDateTime, SoundingType, String)> {
        // let tokens: Vec<&str> = fname.split(|c| c == '_' || c == '.').collect();

        // if tokens.len() != 5 {
        //     return None;
        // }

        // let year = tokens[0][0..4].parse::<i32>().ok()?;
        // let month = tokens[0][4..6].parse::<u32>().ok()?;
        // let day = tokens[0][6..8].parse::<u32>().ok()?;
        // let hour = tokens[0][8..10].parse::<u32>().ok()?;
        // let init_time = NaiveDate::from_ymd(year, month, day).and_hms(hour, 0, 0);

        // let model = Model::from_str(tokens[1]).ok()?;

        // let site = tokens[2].to_owned();

        // if tokens[3] != "buf" || tokens[4] != "gz" {
        //     return None;
        // }

        // Some((init_time, model, site))
        unimplemented!()
    }

    /// Get the file name this would have if uncompressed.
    pub fn file_name(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> String {
        // let file_string = init_time.format("%Y%m%d%HZ").to_string();

        // format!(
        //     "{}_{}_{}.buf",
        //     file_string,
        //     model.as_static(),
        //     site_id.to_uppercase()
        // )
        unimplemented!()
    }

    /// Remove a file from the archive.
    pub fn remove(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> Result<(), BufkitDataErr> {
        //     let file_name: String = self.db_conn.query_row(
        //         "SELECT file_name FROM files WHERE site = ?1 AND model = ?2 AND init_time = ?3",
        //         &[
        //             &site_id.to_uppercase() as &ToSql,
        //             &model.as_static() as &ToSql,
        //             init_time as &ToSql,
        //         ],
        //         |row| row.get_checked(0),
        //     )??;

        //     remove_file(self.file_dir.join(file_name)).map_err(BufkitDataErr::IO)?;

        //     self.db_conn.execute(
        //         "DELETE FROM files WHERE site = ?1 AND model = ?2 AND init_time = ?3",
        //         &[
        //             &site_id.to_uppercase() as &ToSql,
        //             &model.as_static() as &ToSql,
        //             init_time as &ToSql,
        //         ],
        //     )?;

        //     Ok(())
        unimplemented!()
    }
}

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;

    use std::fs::read_dir;

    use chrono::NaiveDate;
    use tempdir::TempDir;

    use sounding_bufkit::BufkitFile;

    // struct to hold temporary data for tests.
    struct TestArchive {
        tmp: TempDir,
        arch: Archive,
    }

    // Function to create a new archive to test.
    fn create_test_archive() -> Result<TestArchive, BufkitDataErr> {
        let tmp = TempDir::new("bufkit-data-test-archive")?;
        let arch = Archive::create(tmp.path())?;

        Ok(TestArchive { tmp, arch })
    }

    // Function to fetch a list of test files.
    fn get_test_data() -> Result<Vec<(Site, SoundingType, NaiveDateTime, OsString)>, BufkitDataErr>
    {
        let path = PathBuf::new().join("example_data");

        let files = read_dir(path)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                entry.file_type().ok().and_then(|ft| {
                    if ft.is_file() {
                        Some(entry.path())
                    } else {
                        None
                    }
                })
            });

        let mut to_return = vec![];

        for path in files {
            //
            // FIXME: handle multiple file types, like BUFR and whatever else types we want to work
            //
            let bufkit_file = BufkitFile::load(&path)?;
            let anal = bufkit_file
                .data()?
                .into_iter()
                .nth(0)
                .ok_or(BufkitDataErr::NotEnoughData)?;
            let snd = anal.sounding();

            let model = if path.to_string_lossy().to_string().contains("gfs") {
                SoundingType::new("GFS", false, 6)
            } else {
                SoundingType::new("NAM", false, 6)
            };
            let site = if path.to_string_lossy().to_string().contains("kmso") {
                Site::new("kmso")
            } else {
                panic!("Unprepared for this test data!");
            };

            let init_time = snd.valid_time().expect("NO VALID TIME?!");

            to_return.push((site.to_owned(), model, init_time, OsString::from(path)))
        }

        Ok(to_return)
    }

    // Function to fill the archive with some example data.
    fn fill_test_archive(arch: &mut Archive) -> Result<(), BufkitDataErr> {
        let test_data = get_test_data().expect("Error loading test data.");

        for (site, sounding_type, init_time, raw_data) in test_data {
            arch.add(&site, &sounding_type, &init_time, &raw_data)?;
        }
        Ok(())
    }

    // ---------------------------------------------------------------------------------------------
    // Connecting, creating, and maintaining the archive.
    // ---------------------------------------------------------------------------------------------
    #[test]
    fn test_archive_create_new() {
        assert!(create_test_archive().is_ok());
    }

    #[test]
    fn test_archive_connect() {
        let TestArchive { tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");
        drop(arch);

        assert!(Archive::connect(tmp.path()).is_ok());
        assert!(Archive::connect("unlikely_directory_in_my_project").is_err());
    }

    // ---------------------------------------------------------------------------------------------
    // Query or modify site metadata
    // ---------------------------------------------------------------------------------------------
    #[test]
    fn test_sites_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            Site::new("kord")
                .with_long_name("Chicago/O'Hare".to_owned())
                .with_notes("Major air travel hub.".to_owned())
                .with_state_prov(StateProv::IL)
                .set_mobile(false),
            Site::new("ksea")
                .with_long_name("Seattle".to_owned())
                .with_notes("A coastal city with coffe and rain".to_owned())
                .with_state_prov(StateProv::WA)
                .set_mobile(false),
            Site::new("kmso")
                .with_long_name("Missoula".to_owned())
                .with_notes("In a valley.".to_owned())
                .with_state_prov(None)
                .set_mobile(false),
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        assert!(arch.site_exists("ksea").expect("Error checking existence"));
        assert!(arch.site_exists("kord").expect("Error checking existence"));
        assert!(!arch.site_exists("xyz").expect("Error checking existence"));

        let retrieved_sites = arch.sites().expect("Error retrieving sites.");

        for site in retrieved_sites {
            println!("{:#?}", site);
            assert!(test_sites.iter().find(|st| **st == site).is_some());
        }
    }

    #[test]
    fn test_get_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            Site::new("kord")
                .with_long_name("Chicago/O'Hare".to_owned())
                .with_notes("Major air travel hub.".to_owned())
                .with_state_prov(StateProv::IL)
                .set_mobile(false),
            Site::new("ksea")
                .with_long_name("Seattle".to_owned())
                .with_notes("A coastal city with coffe and rain".to_owned())
                .with_state_prov(StateProv::WA)
                .set_mobile(false),
            Site::new("kmso")
                .with_long_name("Missoula".to_owned())
                .with_notes("In a valley.".to_owned())
                .with_state_prov(None)
                .set_mobile(false),
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        assert_eq!(arch.site_info("ksea").unwrap(), test_sites[1]);
    }

    #[test]
    fn test_set_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = &[
            Site::new("kord")
                .with_long_name("Chicago/O'Hare".to_owned())
                .with_notes("Major air travel hub.".to_owned())
                .with_state_prov(StateProv::IL)
                .set_mobile(false),
            Site::new("ksea")
                .with_long_name("Seattle".to_owned())
                .with_notes("A coastal city with coffe and rain".to_owned())
                .with_state_prov(StateProv::WA)
                .set_mobile(false),
            Site::new("kmso")
                .with_long_name("Missoula".to_owned())
                .with_notes("In a valley.".to_owned())
                .with_state_prov(None)
                .set_mobile(false),
        ];

        for site in test_sites {
            arch.add_site(site).expect("Error adding site.");
        }

        let zootown = Site::new("kmso")
            .with_long_name("Zootown".to_owned())
            .with_notes("Mountains, not coast.".to_owned())
            .with_state_prov(None)
            .set_mobile(false);

        arch.set_site_info(&zootown).expect("Error updating site.");

        assert_eq!(arch.site_info("kmso").unwrap(), zootown);
        assert_ne!(arch.site_info("kmso").unwrap(), test_sites[2]);
    }

    // ---------------------------------------------------------------------------------------------
    // Query archive inventory
    // ---------------------------------------------------------------------------------------------
    #[test]
    fn test_models_for_site() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let types: Vec<String> = arch
            .sounding_types(&Site::new("kmso"))
            .expect("Error querying archive.")
            .iter()
            .map(|t| t.source().to_owned())
            .collect();

        assert!(types.contains(&"GFS".to_owned()));
        assert!(types.contains(&"NAM".to_owned()));
        assert!(!types.contains(&"NAM4KM".to_owned()));
        assert!(!types.contains(&"LocalWrf".to_owned()));
        assert!(!types.contains(&"Other".to_owned()));
    }

    // #[test]
    // fn test_inventory() {
    //     let TestArchive {
    //         tmp: _tmp,
    //         mut arch,
    //     } = create_test_archive().expect("Failed to create test archive.");

    //     fill_test_archive(&mut arch).expect("Error filling test archive.");

    //     let first = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
    //     let last = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);
    //     let missing = vec![(
    //         NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0),
    //         NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0),
    //     )];

    //     let expected = Inventory {
    //         first,
    //         last,
    //         missing,
    //         auto_download: false, // this is the default value
    //     };
    //     assert_eq!(arch.inventory("kmso", Model::NAM).unwrap(), expected);
    // }

    #[test]
    fn test_count() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        // 7 and not 10 because of duplicate GFS models in the input.
        assert_eq!(arch.count().expect("db error"), 7);
    }

    // ---------------------------------------------------------------------------------------------
    // Add, remove, and retrieve files from the archive
    // ---------------------------------------------------------------------------------------------
    #[test]
    fn test_files_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_data = get_test_data().expect("Error loading test data.");

        for (site, sounding_type, init_time, file_name) in test_data {
            arch.add(&site, &sounding_type, &init_time, &file_name)
                .expect("Failure to add.");
            let recovered_anal = arch
                .retrieve(&site, &sounding_type, &init_time)
                .expect("Failure to load.");

            assert_eq!(
                recovered_anal[0].sounding().valid_time().unwrap(),
                init_time
            );
        }
    }

    #[test]
    fn test_get_most_recent_file() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = Site::new("kmso");
        let snd_type = SoundingType::new_model("GFS", None);

        let init_time = arch
            .most_recent_valid_time(&kmso, &snd_type)
            .expect("Error getting valid time.");

        assert_eq!(init_time, NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0));

        arch.most_recent_file(&kmso, &snd_type)
            .expect("Failed to retrieve sounding.");
    }

    #[test]
    fn test_file_exists() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = Site::new("kmso");
        let snd_type = SoundingType::new_model("GFS", None);

        println!("Checking for files that should exist.");
        assert!(arch
            .file_exists(
                &kmso,
                &snd_type,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                &kmso,
                &snd_type,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(6, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                &kmso,
                &snd_type,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(12, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(arch
            .file_exists(
                &kmso,
                &snd_type,
                &NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0)
            )
            .expect("Error checking for existence"));

        println!("Checking for files that should NOT exist.");
        assert!(!arch
            .file_exists(
                &kmso,
                &snd_type,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(0, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                &kmso,
                &snd_type,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(6, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                &kmso,
                &snd_type,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(12, 0, 0)
            )
            .expect("Error checking for existence"));
        assert!(!arch
            .file_exists(
                &kmso,
                &snd_type,
                &NaiveDate::from_ymd(2018, 4, 1).and_hms(18, 0, 0)
            )
            .expect("Error checking for existence"));
    }

    // #[test]
    // fn test_remove_file() {
    //     let TestArchive {
    //         tmp: _tmp,
    //         mut arch,
    //     } = create_test_archive().expect("Failed to create test archive.");

    //     fill_test_archive(&mut arch).expect("Error filling test archive.");

    //     let init_time = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
    //     let model = Model::GFS;
    //     let site = "kmso";

    //     assert!(arch
    //         .file_exists(site, model, &init_time)
    //         .expect("Error checking db"));
    //     arch.remove(site, model, &init_time)
    //         .expect("Error while removing.");
    //     assert!(!arch
    //         .file_exists(site, model, &init_time)
    //         .expect("Error checking db"));
    // }
}
