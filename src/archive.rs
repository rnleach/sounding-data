//! An archive of soundings in various formats.

use crate::{
    errors::{BufkitDataErr, Result},
    inventory::Inventory,
    location::Location,
    site::Site,
    sounding_type::{FileType, SoundingType},
};
use chrono::NaiveDateTime;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use rusqlite::{types::ToSql, Connection, OpenFlags, NO_PARAMS};
use sounding_analysis::Analysis;
use sounding_bufkit::BufkitData;
use std::{
    collections::HashSet,
    fs::{create_dir, create_dir_all, read_dir, remove_file, File},
    io::Read,
    path::{Path, PathBuf},
    str::from_utf8,
};

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
    pub fn create<T>(root: T) -> Result<Self>
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

        db_conn.execute_batch(include_str!("create_index.sql"))?;

        Ok(Archive {
            root,
            file_dir,
            db_conn,
        })
    }

    /// Open an existing archive.
    pub fn connect<T>(root: T) -> Result<Self>
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
    pub fn check(&self) -> Result<(Vec<String>, Vec<String>)> {
        self.db_conn.execute("PRAGMA cache_size=10000", NO_PARAMS)?;

        let mut all_files_stmt = self.db_conn.prepare("SELECT file_name FROM files")?;

        let index_vals: Result<HashSet<String>> = all_files_stmt
            .query_map(NO_PARAMS, |row| -> String { row.get(0) })?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .map(|res| res.map(String::from))
            .collect();
        let index_vals = index_vals?;

        let file_system_vals: HashSet<String> = read_dir(&self.file_dir)?
            .filter_map(|de| de.ok())
            .map(|de| de.path())
            .filter(|p| p.is_file())
            .filter_map(|p| p.file_name().map(|f| f.to_string_lossy().to_string()))
            .collect();

        let files_in_index_but_not_on_file_system: Vec<String> = index_vals
            .difference(&file_system_vals)
            .map(|s| s.to_owned())
            .collect();
        let files_not_in_index: Vec<String> = file_system_vals
            .difference(&index_vals)
            .map(|s| s.to_owned())
            .collect();

        Ok((files_in_index_but_not_on_file_system, files_not_in_index))
    }

    // ---------------------------------------------------------------------------------------------
    // The file system aspects of the archive, e.g. the root directory of the archive
    // ---------------------------------------------------------------------------------------------
    const FILE_DIR: &'static str = "files";
    const INDEX: &'static str = "index.sqlite";

    // ---------------------------------------------------------------------------------------------
    // Query or modify site metadata
    // ---------------------------------------------------------------------------------------------

    /// Retrieve a list of all the `Site`s in the archive.
    pub fn sites(&self) -> Result<Vec<Site>> {
        crate::site::all_sites(&self.db_conn)
    }

    /// Retrieve the information about a single `Site` with the supplied `short_name`.
    ///
    /// Returns `Ok(None)` if none exists in the archive, and returns `Ok(Some(_))` with the
    /// corresponding `Site` object if one does exist.
    pub fn site_info(&self, short_name: &str) -> Result<Option<Site>> {
        crate::site::retrieve_site(&self.db_conn, short_name)
    }

    /// Modify an existing `Site`'s values.
    ///
    /// The supplied site need not be validated, the returned site will be. It is an error if there
    /// is not a site in the index with the same `short_name` to modify.
    pub fn set_site_info(&self, site: Site) -> Result<Site> {
        crate::site::update_site(&self.db_conn, site)
    }

    /// Validate that this `Site` is in the index.
    ///
    /// Any object returned in an `Ok(_)` from this method will return true from the `.is_valid()`
    /// method.
    pub fn validate_site(&self, site: Site) -> Result<Site> {
        if site.is_valid() {
            Ok(site)
        } else if let Some(retrieved_site) =
            crate::site::retrieve_site(&self.db_conn, site.short_name())?
        {
            Ok(retrieved_site)
        } else {
            Err(BufkitDataErr::InvalidSite(site))
        }
    }

    /// Validate that this `Site` is in the index, if not, insert it into the index.
    ///
    /// Any object returned in an `Ok(_)` from this method will return true from the `.is_valid()`
    /// method.
    pub fn validate_or_add_site(&self, site: Site) -> Result<Site> {
        if site.is_valid() {
            Ok(site)
        } else {
            if let Some(retrieved_site) =
                crate::site::retrieve_site(&self.db_conn, site.short_name())?
            {
                Ok(retrieved_site)
            } else {
                crate::site::insert_site(&self.db_conn, site)
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Query or modify sounding type metadata
    // ---------------------------------------------------------------------------------------------

    /// Retrieve a list of all the `SoundingType`s in the archive.
    pub fn sounding_types(&self) -> Result<Vec<SoundingType>> {
        crate::sounding_type::all_sounding_types(&self.db_conn)
    }

    /// Retrieve the information about a single `SoundingType` with the supplied description, which
    /// is the same as the result from its `source()` method.
    ///
    /// Returns `Ok(None)` if none exists in the archive, and returns `Ok(Some(_))` with the
    /// corresponding `SoundingType` object if one does exist.
    pub fn sounding_type_info(&self, sounding_type: &str) -> Result<Option<SoundingType>> {
        crate::sounding_type::retrieve_sounding_type(&self.db_conn, sounding_type)
    }

    /// Modify an existing `SoundingType`'s values.
    ///
    /// The supplied sounding type need not be validated, the returned one will be. It is an error
    /// if there is not a sounding type in the index with the same `.source()` to modify.
    pub fn set_sounding_type_info(&self, sounding_type: SoundingType) -> Result<SoundingType> {
        crate::sounding_type::update_sounding_type(&self.db_conn, sounding_type)
    }

    /// Get a list of `SoundingType`s in the archive for this `site`.
    pub fn sounding_types_for_site(&self, site: &Site) -> Result<Vec<SoundingType>> {
        debug_assert!(site.id() > 0);
        crate::sounding_type::all_sounding_types_for_site(&self.db_conn, site)
    }

    /// Validate that this `SoundingType` is in the index.
    ///
    /// Any object returned in an `Ok(_)` from this method will return true from the `.is_valid()`
    /// method.
    pub fn validate_sounding_type(&self, sounding_type: SoundingType) -> Result<SoundingType> {
        if sounding_type.is_valid() {
            Ok(sounding_type)
        } else if let Some(retrieved_st) =
            crate::sounding_type::retrieve_sounding_type(&self.db_conn, sounding_type.source())?
        {
            Ok(retrieved_st)
        } else {
            Err(BufkitDataErr::InvalidSoundingType(sounding_type))
        }
    }

    /// Validate that this `SoundingType` is in the index, if not, add it to the index.
    ///
    /// Any object returned in an `Ok(_)` from this method will return true from the `.is_valid()`
    /// method.
    pub fn validate_or_add_sounding_type(
        &self,
        sounding_type: SoundingType,
    ) -> Result<SoundingType> {
        if sounding_type.is_valid() {
            Ok(sounding_type)
        } else {
            if let Some(retrieved_st) =
                crate::sounding_type::retrieve_sounding_type(&self.db_conn, sounding_type.source())?
            {
                Ok(retrieved_st)
            } else {
                crate::sounding_type::insert_sounding_type(&self.db_conn, sounding_type)
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Query or modify location metadata
    // ---------------------------------------------------------------------------------------------

    /// Retrieve a list of all the `Location`s in the archive.
    pub fn all_locations(&self) -> Result<Vec<Location>> {
        crate::location::all_locations(&self.db_conn)
    }

    /// Get the `Location` object for these coordinates.
    ///
    /// If there were no errors while querying the index, this will return an `Ok(None)` meaning
    /// that no errors occurred, but there was matching locaiton in the index.
    pub fn location_info(
        &self,
        latitude: f64,
        longitude: f64,
        elevation_m: i32,
    ) -> Result<Option<Location>> {
        crate::location::retrieve_location(&self.db_conn, latitude, longitude, elevation_m)
    }

    /// Retrieve the `Location` object associated with these coordinates, or insert a new one into
    /// the index.
    pub fn retrieve_or_add_location(
        &self,
        latitude: f64,
        longitude: f64,
        elevation_m: i32,
    ) -> Result<Location> {
        crate::location::retrieve_or_add_location(&self.db_conn, latitude, longitude, elevation_m)
    }

    /// Modify an existing `Location`'s values.
    ///
    /// The supplied location need not be validated, the returned one will be. It is an error if
    /// there is not a matching `Location` in the index with the same coordinates to modify.
    /// Basically you can only modify the time zone offset information.
    pub fn set_location_info(&self, location: Location) -> Result<Location> {
        crate::location::update_location(&self.db_conn, location)
    }

    /// Get a list of `Location`s in the archive for this site.
    pub fn locations_for_site_and_type(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
    ) -> Result<Vec<Location>> {
        debug_assert!(site.id() > 0);
        crate::location::all_locations_for_site_and_type(&self.db_conn, site, sounding_type)
    }

    /// Validate that this `Location` is in the index.
    ///
    /// Any object returned in an `Ok(_)` from this method will return true from the `.is_valid()`
    /// method.
    pub fn validate_location(&self, location: Location) -> Result<Location> {
        if location.is_valid() {
            Ok(location)
        } else if let Some(retrieved_loc) = crate::location::retrieve_location(
            &self.db_conn,
            location.latitude(),
            location.longitude(),
            location.elevation(),
        )? {
            Ok(retrieved_loc)
        } else {
            Err(BufkitDataErr::InvalidLocation(location))
        }
    }

    /// Validate that this `Location` is in the index, if not, add it to the index.
    ///
    /// Any object returned in an `Ok(_)` from this method will return true from the `.is_valid()`
    /// method.
    pub fn validate_or_add_location(&self, location: Location) -> Result<Location> {
        if location.is_valid() {
            Ok(location)
        } else {
            if let Some(retrieved_loc) = crate::location::retrieve_location(
                &self.db_conn,
                location.latitude(),
                location.longitude(),
                location.elevation(),
            )? {
                Ok(retrieved_loc)
            } else {
                crate::location::insert_location(&self.db_conn, location)
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Query archive inventory
    // ---------------------------------------------------------------------------------------------

    /// Get an inventory of soundings for a `Site` and `SoundingType`.
    pub fn inventory(&self, site: &Site) -> Result<Inventory> {
        debug_assert!(site.id() > 0);
        crate::inventory::inventory(&self.db_conn, site.clone())
    }

    /// Retrieve the model initialization time of the most recent model in the archive.
    pub fn most_recent_valid_time(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
    ) -> Result<NaiveDateTime> {
        debug_assert!(site.id() > 0);
        debug_assert!(sounding_type.id() > 0);

        let init_time: NaiveDateTime = self.db_conn.query_row(
            "
                SELECT init_time FROM files
                WHERE site_id = ?1 AND type_id = ?2
                ORDER BY init_time DESC
                LIMIT 1
            ",
            &[&site.id(), &sounding_type.id()],
            |row| row.get_checked(0),
        )??;

        Ok(init_time)
    }

    /// Check to see if a file is present in the archive and it is retrieveable.
    pub fn file_exists(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> Result<bool> {
        debug_assert!(site.id() > 0);
        debug_assert!(sounding_type.id() > 0);

        let num_records: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM files WHERE site_id = ?1 AND type_id = ?2 AND init_time = ?3",
            &[&site.id(), &sounding_type.id(), init_time as &ToSql],
            |row| row.get_checked(0),
        )??;

        Ok(num_records == 1)
    }

    /// Get the number of files stored in the archive.
    pub fn count(&self) -> Result<i64> {
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
    pub fn add_file(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        location: &Location,
        init_time: &NaiveDateTime,
        file_name: &str,
    ) -> Result<()> {
        debug_assert!(site.is_valid());
        debug_assert!(sounding_type.is_valid());
        debug_assert!(location.is_valid());

        let fname: String = self.compressed_file_name(&site, &sounding_type, init_time);

        let mut in_file = File::open(file_name)?;
        let out_file = File::create(self.file_dir.join(&fname))?;
        let mut encoder = GzEncoder::new(out_file, Compression::default());
        std::io::copy(&mut in_file, &mut encoder)?;

        self.db_conn.execute(
            "
                INSERT OR REPLACE INTO files (type_id, site_id, location_id, init_time, file_name)
                VALUES (?1, ?2, ?3, ?4, ?5)
            ",
            &[
                &sounding_type.id(),
                &site.id(),
                &location.id(),
                &init_time as &ToSql,
                &fname,
            ],
        )?;

        Ok(())
    }

    fn get_file_name_for(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> Result<String> {
        debug_assert!(site.id() > 0, "Site not checked or added in index");
        debug_assert!(
            sounding_type.id() > 0,
            "Sounding type not checked or added in index."
        );

        let file_name: String = self.db_conn.query_row(
            "SELECT file_name FROM files WHERE site_id = ?1 AND type_id = ?2 AND init_time = ?3",
            &[&site.id(), &sounding_type.id(), init_time as &ToSql],
            |row| row.get_checked(0),
        )??;

        Ok(file_name)
    }

    fn load_data(&self, file_name: &str) -> Result<Vec<u8>> {
        let file = File::open(self.file_dir.join(file_name))?;
        let mut decoder = GzDecoder::new(file);
        let mut buf: Vec<u8> = vec![];
        let _bytes_read = decoder.read_to_end(&mut buf)?;

        Ok(buf)
    }

    fn decode_data(buf: &[u8], description: &str, ftype: FileType) -> Result<Vec<Analysis>> {
        match ftype {
            FileType::BUFKIT => {
                let bufkit_str = from_utf8(&buf)?;
                let bufkit_data = BufkitData::init(bufkit_str, description)?;
                let bufkit_anals: Vec<Analysis> = bufkit_data.into_iter().collect();
                Ok(bufkit_anals)
            }
            FileType::BUFR => unimplemented!(),
        }
    }

    /// Retrieve a file from the archive.
    pub fn retrieve(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> Result<Vec<Analysis>> {
        let file_name = self.get_file_name_for(site, sounding_type, init_time)?;
        let data = self.load_data(&file_name)?;
        Self::decode_data(&data, &file_name, sounding_type.file_type())
    }

    /// Retrieve and uncompress a file.
    pub fn export(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> Result<impl Read> {
        let file_name = self.get_file_name_for(site, sounding_type, init_time)?;
        let file = File::open(self.file_dir.join(file_name))?;
        Ok(GzDecoder::new(file))
    }

    /// Retrieve the  most recent file as a sounding.
    pub fn most_recent_analysis(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
    ) -> Result<Vec<Analysis>> {
        let init_time = self.most_recent_valid_time(site, sounding_type)?;
        self.retrieve(site, sounding_type, &init_time)
    }

    fn compressed_file_name(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> String {
        let file_string = init_time.format("%Y-%m-%dT%H%MZ").to_string();

        format!(
            "{}_{}_{}.gz",
            file_string,
            sounding_type.source(),
            site.short_name(),
        )
        .into()
    }

    // fn parse_compressed_file_name(fname: &str) -> Option<(NaiveDateTime, SoundingType, String)> {
    //     // let tokens: Vec<&str> = fname.split(|c| c == '_' || c == '.').collect();

    //     // if tokens.len() != 5 {
    //     //     return None;
    //     // }

    //     // let year = tokens[0][0..4].parse::<i32>().ok()?;
    //     // let month = tokens[0][4..6].parse::<u32>().ok()?;
    //     // let day = tokens[0][6..8].parse::<u32>().ok()?;
    //     // let hour = tokens[0][8..10].parse::<u32>().ok()?;
    //     // let init_time = NaiveDate::from_ymd(year, month, day).and_hms(hour, 0, 0);

    //     // let model = Model::from_str(tokens[1]).ok()?;

    //     // let site = tokens[2].to_owned();

    //     // if tokens[3] != "buf" || tokens[4] != "gz" {
    //     //     return None;
    //     // }

    //     // Some((init_time, model, site))
    //     unimplemented!()
    // }

    /// Remove a file from the archive.
    pub fn remove(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> Result<()> {
        let file_name: String = self.db_conn.query_row(
            "SELECT file_name FROM files WHERE site_id = ?1 AND type_id = ?2 AND init_time = ?3",
            &[&site.id(), &sounding_type.id(), init_time as &ToSql],
            |row| row.get_checked(0),
        )??;

        remove_file(self.file_dir.join(file_name)).map_err(BufkitDataErr::Io)?;

        self.db_conn.execute(
            "DELETE FROM files WHERE site_id = ?1 AND type_id = ?2 AND init_time = ?3",
            &[&site.id(), &sounding_type.id(), init_time as &ToSql],
        )?;

        Ok(())
    }
}

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;
    use crate::{FileType, Location, StateProv};
    use chrono::NaiveDate;
    use metfor::Quantity;
    use sounding_bufkit::BufkitFile;
    use std::fs::read_dir;
    use tempdir::TempDir;

    // struct to hold temporary data for tests.
    struct TestArchive {
        tmp: TempDir,
        arch: Archive,
    }

    // Function to create a new archive to test.
    fn create_test_archive() -> Result<TestArchive> {
        let tmp = TempDir::new("bufkit-data-test-archive")?;
        let arch = Archive::create(tmp.path())?;

        Ok(TestArchive { tmp, arch })
    }

    // Function to fetch a list of test files.
    fn get_test_data() -> Result<Vec<(Site, SoundingType, NaiveDateTime, Location, String)>> {
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
                SoundingType::new("GFS", false, FileType::BUFKIT, 6)
            } else {
                SoundingType::new("NAM", false, FileType::BUFKIT, 6)
            };
            let site = if path.to_string_lossy().to_string().contains("kmso") {
                Site::new("kmso")
            } else {
                panic!("Unprepared for this test data!");
            };

            let init_time = snd.valid_time().expect("NO VALID TIME?!");

            let (lat, lon) = snd.station_info().location().unwrap();
            let elev_m = snd.station_info().elevation().unwrap().unpack();
            let loc = Location::new(lat, lon, elev_m as i32, None);

            to_return.push((
                site.to_owned(),
                model,
                init_time,
                loc,
                path.to_string_lossy().to_string(),
            ))
        }

        Ok(to_return)
    }

    // Function to fill the archive with some example data.
    fn fill_test_archive(arch: &mut Archive) -> Result<()> {
        let test_data = get_test_data().expect("Error loading test data.");

        for (site, sounding_type, init_time, loc, file_name) in test_data {
            let site = arch.validate_or_add_site(site)?;
            let sounding_type = arch.validate_or_add_sounding_type(sounding_type)?;
            let loc = arch.validate_or_add_location(loc)?;
            arch.add_file(&site, &sounding_type.clone(), &loc, &init_time, &file_name)?;
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

    #[test]
    fn test_check() -> Result<()> {
        let TestArchive { tmp, mut arch } =
            create_test_archive().expect("Failed to create test archive.");
        fill_test_archive(&mut arch).expect("Error filling test archive.");

        // Rename all files with "NAM" in them
        let files_dir = tmp.path().join("files");
        std::fs::read_dir(files_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().contains("NAM"))
            .for_each(|entry| {
                let mut fname = entry.path().to_string_lossy().to_string();
                let start = fname.find("NAM").unwrap();
                let end = start + 3;
                fname.replace_range(start..end, "NAMM");
                std::fs::rename(entry.path(), fname).unwrap();
            });

        let (missing_files, extra_files) = dbg!(arch.check().unwrap());

        assert_eq!(missing_files.len(), 3);
        assert_eq!(missing_files.len(), extra_files.len());

        for fname in missing_files {
            assert!(fname.contains("_NAM_"));
            assert!(!fname.contains("_NAMM_"));
            assert!(!fname.contains("_GFS_"));
        }

        for fname in extra_files {
            assert!(fname.contains("_NAMM_"));
            assert!(!fname.contains("_NAM_"));
            assert!(!fname.contains("_GFS_"));
        }

        Ok(())
    }

    // ---------------------------------------------------------------------------------------------
    // Query or modify site metadata
    // ---------------------------------------------------------------------------------------------
    #[test]
    fn test_sites() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_sites = [
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

        for site in test_sites.iter_mut() {
            *site = arch
                .validate_or_add_site(site.clone())
                .expect("Error adding site.");
        }

        let sites = dbg!(arch.sites())?;
        let sites: Vec<_> = sites.iter().map(|s| s.short_name()).collect();

        assert_eq!(sites.len(), 3);
        assert!(sites.contains(&"kmso"));
        assert!(sites.contains(&"ksea"));
        assert!(sites.contains(&"kord"));
        assert!(!sites.contains(&"xyz"));

        Ok(())
    }

    #[test]
    fn test_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_sites = [
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

        for site in test_sites.iter_mut() {
            assert!(!site.is_valid());

            *site = arch
                .validate_or_add_site(site.clone())
                .expect("Error adding site.");

            assert!(site.is_valid());
        }

        for site in test_sites.iter() {
            let retr_site = arch.site_info(site.short_name()).unwrap().unwrap();

            assert!(retr_site.is_valid());
            assert_eq!(site.short_name(), retr_site.short_name());
            assert_eq!(site.long_name(), retr_site.long_name());
            assert_eq!(site.state_prov(), retr_site.state_prov());
            assert_eq!(site.notes(), retr_site.notes());
        }
    }

    #[test]
    fn test_set_site_info() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_sites = [
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

        for site in test_sites.iter_mut() {
            *site = arch
                .validate_or_add_site(site.clone())
                .expect("Error adding site.");
        }

        let retr_site = arch.site_info("kmso").unwrap().unwrap();
        assert_eq!(retr_site.short_name(), test_sites[2].short_name());
        assert_eq!(retr_site.long_name(), test_sites[2].long_name());
        assert_eq!(retr_site.notes(), test_sites[2].notes());
        assert_eq!(retr_site.state_prov(), test_sites[2].state_prov());

        let zootown = Site::new("kmso")
            .with_long_name("Zootown".to_owned())
            .with_notes("Mountains, not coast.".to_owned())
            .with_state_prov(None)
            .set_mobile(false);

        arch.set_site_info(zootown.clone())
            .expect("Error updating site.");

        let retr_site = arch.site_info("kmso").unwrap().unwrap();
        assert!(retr_site.is_valid());
        assert_eq!(retr_site.short_name(), test_sites[2].short_name());
        assert_ne!(retr_site.long_name(), test_sites[2].long_name());
        assert_ne!(retr_site.notes(), test_sites[2].notes());
        assert_eq!(retr_site.state_prov(), test_sites[2].state_prov());

        assert_eq!(retr_site.short_name(), zootown.short_name());
        assert_eq!(retr_site.long_name(), zootown.long_name());
        assert_eq!(retr_site.notes(), zootown.notes());
        assert_eq!(retr_site.state_prov(), zootown.state_prov());
    }

    #[test]
    fn test_validate_site() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_sites = [
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

        for site in test_sites.iter() {
            arch.validate_or_add_site(site.clone())?;
        }

        for site in test_sites.iter() {
            let valid_site = arch.validate_site(site.clone())?;

            assert!(valid_site.is_valid());
            assert_eq!(valid_site.short_name(), site.short_name());
        }

        let bad_site = Site::new("kxyz")
            .with_long_name("not real".to_owned())
            .with_notes("I made this up, it may be real anyway.".to_owned())
            .with_state_prov(None)
            .set_mobile(false);

        assert!(arch.validate_site(bad_site).is_err());

        Ok(())
    }

    #[test]
    fn test_validate_or_add_site() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_sites = [
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

        for site in test_sites.iter_mut() {
            *site = arch.validate_or_add_site(site.clone())?;

            assert!(site.is_valid());
        }

        Ok(())
    }

    #[test]
    fn test_sites_round_trip() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_sites = [
            Site::new("kord")
                .with_long_name("Chicago/O'Hare".to_owned())
                .with_notes("Major air travel hub.".to_owned())
                .with_state_prov(StateProv::IL)
                .set_mobile(false),
            Site::new("ksea")
                .with_long_name("Seattle".to_owned())
                .with_notes("A coastal city with coffee and rain".to_owned())
                .with_state_prov(StateProv::WA)
                .set_mobile(false),
            Site::new("kmso")
                .with_long_name("Missoula".to_owned())
                .with_notes("In a valley.".to_owned())
                .with_state_prov(None)
                .set_mobile(false),
        ];

        for site in test_sites.iter_mut() {
            *site = arch.validate_or_add_site(site.clone())?;
        }

        assert_eq!(arch.site_info("ksea")?.unwrap().short_name(), "ksea");
        assert_eq!(arch.site_info("kord")?.unwrap().short_name(), "kord");
        assert_eq!(arch.site_info("xyz")?, None);

        let retrieved_sites = arch.sites().expect("Error retrieving sites.");

        for site in retrieved_sites {
            println!("{:#?}", site);
            assert!(test_sites
                .iter()
                .find(|st| st.short_name() == site.short_name())
                .is_some());
        }
        Ok(())
    }

    // ---------------------------------------------------------------------------------------------
    // Query or modify sounding type metadata
    // ---------------------------------------------------------------------------------------------

    #[test]
    fn test_sounding_types() -> Result<()> {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let types: Vec<String> = arch
            .sounding_types()?
            .iter()
            .map(|t| t.source().to_owned())
            .collect();

        assert!(types.contains(&"GFS".to_owned()));
        assert!(types.contains(&"NAM".to_owned()));
        assert!(!types.contains(&"NAM4KM".to_owned()));
        assert!(!types.contains(&"LocalWrf".to_owned()));
        assert!(!types.contains(&"Other".to_owned()));

        Ok(())
    }

    #[test]
    fn test_sounding_type_info() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_sts = [
            SoundingType::new("GFS", false, FileType::BUFKIT, 6),
            SoundingType::new("NAM", false, FileType::BUFKIT, 6),
            SoundingType::new("NamNest", false, FileType::BUFKIT, 6),
            SoundingType::new("Incident", true, FileType::BUFR, None),
            SoundingType::new("SREF", false, FileType::BUFKIT, 6),
        ];

        for st in test_sts.iter_mut() {
            assert!(!st.is_valid());

            *st = arch
                .validate_or_add_sounding_type(st.clone())
                .expect("Error adding sounding type.");

            assert!(st.is_valid());
        }

        for st in test_sts.iter() {
            let retr_st = arch.sounding_type_info(st.source()).unwrap().unwrap();

            assert!(retr_st.is_valid());
            assert_eq!(st.source(), retr_st.source());
            assert_eq!(st.is_modeled(), retr_st.is_modeled());
            assert_eq!(st.is_observed(), retr_st.is_observed());
            assert_eq!(
                st.hours_between_initializations(),
                retr_st.hours_between_initializations()
            );
            assert_eq!(st.file_type(), retr_st.file_type());
        }

        Ok(())
    }

    #[test]
    fn test_set_sounding_type_info() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_sts = [
            SoundingType::new("GFS", false, FileType::BUFKIT, 6),
            SoundingType::new("NAM", false, FileType::BUFKIT, 6),
            SoundingType::new("NamNest", false, FileType::BUFKIT, 6),
            SoundingType::new("Incident", true, FileType::BUFR, None),
            SoundingType::new("SREF", false, FileType::BUFKIT, 6),
        ];

        for st in test_sts.iter_mut() {
            *st = arch
                .validate_or_add_sounding_type(st.clone())
                .expect("Error adding sounding type.");
        }

        let retr_st = arch.sounding_type_info("SREF").unwrap().unwrap();
        assert_eq!(retr_st.source(), test_sts[4].source());
        assert_eq!(retr_st.is_modeled(), test_sts[4].is_modeled());
        assert_eq!(retr_st.is_observed(), test_sts[4].is_observed());
        assert_eq!(
            retr_st.hours_between_initializations(),
            test_sts[4].hours_between_initializations()
        );
        assert_eq!(retr_st.file_type(), test_sts[4].file_type());

        let sref = SoundingType::new("SREF", false, FileType::BUFKIT, None);

        arch.set_sounding_type_info(sref.clone())
            .expect("Error updating sounding type.");

        let retr_st = arch.sounding_type_info("SREF").unwrap().unwrap();
        assert!(retr_st.is_valid());
        assert_eq!(retr_st.source(), test_sts[4].source());
        assert_eq!(retr_st.is_modeled(), test_sts[4].is_modeled());
        assert_eq!(retr_st.is_observed(), test_sts[4].is_observed());
        assert_ne!(
            retr_st.hours_between_initializations(),
            test_sts[4].hours_between_initializations()
        );
        assert_eq!(retr_st.file_type(), test_sts[4].file_type());

        assert_eq!(retr_st.source(), sref.source());
        assert_eq!(retr_st.is_modeled(), sref.is_modeled());
        assert_eq!(retr_st.is_observed(), sref.is_observed());
        assert_eq!(
            retr_st.hours_between_initializations(),
            sref.hours_between_initializations()
        );
        assert_eq!(retr_st.file_type(), sref.file_type());

        Ok(())
    }

    #[test]
    fn test_sounding_types_for_site() -> Result<()> {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let site = arch.site_info("kmso")?.expect("No such site.");

        let types: Vec<String> = arch
            .sounding_types_for_site(&site)?
            .iter()
            .map(|t| t.source().to_owned())
            .collect();

        assert!(types.contains(&"GFS".to_owned()));
        assert!(types.contains(&"NAM".to_owned()));
        assert!(!types.contains(&"NAM4KM".to_owned()));
        assert!(!types.contains(&"LocalWrf".to_owned()));
        assert!(!types.contains(&"Other".to_owned()));

        Ok(())
    }

    #[test]
    fn test_validate_sounding_type() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_sts = [
            SoundingType::new("GFS", false, FileType::BUFKIT, 6),
            SoundingType::new("NAM", false, FileType::BUFKIT, 6),
            SoundingType::new("NamNest", false, FileType::BUFKIT, 6),
            SoundingType::new("Incident", true, FileType::BUFR, None),
            SoundingType::new("SREF", false, FileType::BUFKIT, 6),
        ];

        for st in test_sts.iter_mut() {
            *st = arch
                .validate_or_add_sounding_type(st.clone())
                .expect("Error adding sounding type.");
        }

        for st in test_sts.iter() {
            arch.validate_or_add_sounding_type(st.clone())?;
        }

        for st in test_sts.iter() {
            let valid_st = arch.validate_sounding_type(st.clone())?;

            assert!(valid_st.is_valid());
            assert_eq!(valid_st.source(), st.source());
        }

        let bad_st = SoundingType::new("drill into ground", false, FileType::BUFR, 1);

        assert!(arch.validate_sounding_type(bad_st).is_err());

        Ok(())
    }

    #[test]
    fn test_validate_or_add_sounding_type() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_sts = [
            SoundingType::new("GFS", false, FileType::BUFKIT, 6),
            SoundingType::new("NAM", false, FileType::BUFKIT, 6),
            SoundingType::new("NamNest", false, FileType::BUFKIT, 6),
            SoundingType::new("Incident", true, FileType::BUFR, None),
            SoundingType::new("SREF", false, FileType::BUFKIT, 6),
        ];

        for st in test_sts.iter_mut() {
            *st = arch
                .validate_or_add_sounding_type(st.clone())
                .expect("Error adding sounding type.");

            assert!(st.is_valid());
        }

        Ok(())
    }

    // ---------------------------------------------------------------------------------------------
    // Query or modify location metadata
    // ---------------------------------------------------------------------------------------------

    fn populate_test_locations(arch: &Archive) -> [Location; 5] {
        let mut test_locs = [
            Location::new(43.0, -110.0, 599, None),
            Location::new(45.0, -112.0, 699, None),
            Location::new(47.0, -114.0, 799, None),
            Location::new(49.0, -116.0, 999, None),
            Location::new(49.0, -116.0, 999, None), // Duplicate!
        ];

        for loc in test_locs.iter_mut() {
            assert!(!loc.is_valid());

            *loc = arch
                .validate_or_add_location(loc.clone())
                .expect("Error adding location.");

            assert!(loc.is_valid());
        }

        test_locs
    }

    #[test]
    fn test_all_locations() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let _ = populate_test_locations(&arch);

        let locs = dbg!(arch.all_locations())?;
        let locs: Vec<_> = locs.iter().map(|s| s.elevation()).collect();

        assert_eq!(locs.len(), 4);
        assert!(locs.contains(&599));
        assert!(locs.contains(&699));
        assert!(locs.contains(&799));
        assert!(locs.contains(&999));
        assert!(!locs.contains(&899));

        Ok(())
    }

    #[test]
    fn test_location_info() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_locs = populate_test_locations(&arch);

        for loc in test_locs.iter() {
            let retr_loc = arch
                .location_info(loc.latitude(), loc.longitude(), loc.elevation())
                .unwrap()
                .unwrap();

            assert!(loc.is_valid());
            assert!(retr_loc.is_valid());
            assert_eq!(loc.latitude(), retr_loc.latitude());
            assert_eq!(loc.longitude(), retr_loc.longitude());
            assert_eq!(loc.elevation(), retr_loc.elevation());
            assert_eq!(loc.tz_offset(), retr_loc.tz_offset());
        }

        Ok(())
    }

    #[test]
    fn test_retrieve_or_add_location() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let _ = populate_test_locations(&arch);

        Ok(())
    }

    #[test]
    fn test_set_location_info() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_locs = populate_test_locations(&arch);

        let loc = test_locs[0];
        assert!(loc.is_valid());
        let loc = loc.with_tz_offset(-3600 * 6);

        arch.set_location_info(loc)?;

        let retr_loc = arch
            .location_info(loc.latitude(), loc.longitude(), loc.elevation())?
            .unwrap();

        assert_eq!(retr_loc.tz_offset(), loc.tz_offset());
        assert_ne!(retr_loc.tz_offset(), test_locs[0].tz_offset());

        Ok(())
    }

    #[test]
    fn test_locations_for_site_and_type() -> Result<()> {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let site = arch.site_info("kmso")?.expect("No such site.");
        let sounding_type = arch
            .sounding_types_for_site(&site)?
            .into_iter()
            .filter(|st| st.source() == "GFS")
            .nth(0)
            .unwrap();

        let locs: Vec<Location> = arch.locations_for_site_and_type(&site, &sounding_type)?;

        assert_eq!(locs.len(), 1);
        let loc = locs.into_iter().nth(0).unwrap();
        assert_eq!(loc.latitude(), 46.92);
        assert_eq!(loc.longitude(), -114.08);
        assert_eq!(loc.elevation(), 972);
        assert!(loc.tz_offset().is_none());

        Ok(())
    }

    #[test]
    fn test_validate_location() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_locations = populate_test_locations(&arch);

        for loc in test_locations.iter_mut() {
            *loc = arch.validate_location(*loc)?;

            assert!(loc.is_valid());
        }

        assert_eq!(test_locations[3].id(), test_locations[4].id());

        Ok(())
    }

    #[test]
    fn test_validate_or_add_location() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let mut test_locations = populate_test_locations(&arch);

        for loc in test_locations.iter_mut() {
            *loc = arch
                .validate_or_add_location(*loc)
                .expect("Error adding location.");

            assert!(loc.is_valid());
        }

        assert_eq!(test_locations[3].id(), test_locations[4].id());

        Ok(())
    }

    // ---------------------------------------------------------------------------------------------
    // Query archive inventory
    // ---------------------------------------------------------------------------------------------

    #[test]
    fn test_inventory() -> Result<()> {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let site = arch.site_info("kmso")?.expect("No such site.");
        let gfs = arch
            .sounding_type_info("GFS")?
            .expect("No such sounding type.");
        let nam = arch
            .sounding_type_info("NAM")?
            .expect("No such sounding type.");

        let first = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let last = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);

        let inv = arch.inventory(&site)?;

        assert_eq!(inv.range(&gfs).unwrap(), (first, last));
        assert_eq!(inv.range(&nam).unwrap(), (first, last));

        let gfs_locations = dbg!(inv.locations(&gfs));
        assert_eq!(gfs_locations.len(), 1);
        assert_eq!(gfs_locations[0].latitude(), 46.92);
        assert_eq!(gfs_locations[0].longitude(), -114.08);
        assert_eq!(gfs_locations[0].elevation(), 972);
        assert!(gfs_locations[0].is_valid());

        let nam_locations = inv.locations(&nam);
        assert_eq!(nam_locations.len(), 1);
        assert_eq!(nam_locations[0].latitude(), 46.87);
        assert_eq!(nam_locations[0].longitude(), -114.16);
        assert_eq!(nam_locations[0].elevation(), 1335);
        assert!(nam_locations[0].is_valid());

        Ok(())
    }

    #[test]
    fn test_most_recent_valid_time() -> Result<()> {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let site = dbg!(arch.site_info("kmso"))?.unwrap();
        let sounding_type = dbg!(arch.sounding_type_info("GFS"))?.unwrap();
        let most_recent = dbg!(arch.most_recent_valid_time(&site, &sounding_type))?;

        let most_recent_should_be = NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0);
        assert_eq!(most_recent, most_recent_should_be);

        let sounding_type = dbg!(arch.sounding_type_info("NAM"))?.unwrap();
        let most_recent = dbg!(arch.most_recent_valid_time(&site, &sounding_type))?;

        assert_eq!(most_recent, most_recent_should_be);

        Ok(())
    }

    #[test]
    fn test_file_exists() -> Result<()> {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = arch.site_info("kmso")?.unwrap();
        let snd_type = arch.sounding_type_info("GFS")?.unwrap();

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

        Ok(())
    }

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
    fn test_files_round_trip() -> Result<()> {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_data = get_test_data().expect("Error loading test data.");

        for (site, sounding_type, init_time, loc, file_name) in test_data {
            let site = arch.validate_or_add_site(site)?;
            let sounding_type = arch.validate_or_add_sounding_type(sounding_type)?;
            let loc = arch.validate_or_add_location(loc)?;

            arch.add_file(&site, &sounding_type.clone(), &loc, &init_time, &file_name)
                .expect("Failure to add.");

            let site = arch
                .site_info(site.short_name())
                .expect("Error retrieving site.")
                .expect("Site not in index.");
            let sounding_type = arch
                .sounding_type_info(sounding_type.source())
                .expect("Error retrieving sounding_type")
                .expect("Sounding type not in index.");

            let recovered_anal = arch
                .retrieve(&site, &sounding_type, &init_time)
                .expect("Failure to load.");

            assert_eq!(
                recovered_anal[0].sounding().valid_time().unwrap(),
                init_time
            );
        }
        Ok(())
    }

    #[test]
    fn test_export() -> Result<()> {
        unimplemented!()
    }

    #[test]
    fn test_get_most_recent_analysis() -> Result<()> {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = arch.site_info("kmso")?.expect("Site not in index.");
        let snd_type = arch
            .sounding_type_info("GFS")?
            .expect("Sounding type not in index");

        let init_time = arch
            .most_recent_valid_time(&kmso, &snd_type)
            .expect("Error getting valid time.");

        assert_eq!(init_time, NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0));

        arch.most_recent_analysis(&kmso, &snd_type)
            .expect("Failed to retrieve sounding.");

        Ok(())
    }

    #[test]
    fn test_remove_file() -> Result<()> {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let init_time = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let kmso = arch.site_info("kmso")?.expect("No such site.");
        let snd_type = arch
            .sounding_type_info("GFS")?
            .expect("No such sounding type.");

        assert!(arch
            .file_exists(&kmso, &snd_type, &init_time)
            .expect("Error checking db"));
        arch.remove(&kmso, &snd_type, &init_time)
            .expect("Error while removing.");
        assert!(!arch
            .file_exists(&kmso, &snd_type, &init_time)
            .expect("Error checking db"));

        Ok(())
    }
}
