//! An archive of soundings in various formats.

use crate::{
    errors::BufkitDataErr,
    inventory::Inventory,
    location::{insert_or_update_location, Location},
    site::{insert_or_update_site, Site},
    sounding_type::{insert_or_update_sounding_type, FileType, SoundingType},
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
use strum::AsStaticRef;

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

        db_conn.execute_batch(include_str!("create_index.sql"))?;

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
    pub fn check(&self) -> Result<(Vec<String>, Vec<String>), BufkitDataErr> {
        self.db_conn.execute("PRAGMA cache_size=10000", NO_PARAMS)?;

        let mut all_files_stmt = self.db_conn.prepare("SELECT file_name FROM files")?;

        let index_vals: Result<HashSet<String>, BufkitDataErr> = all_files_stmt
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

    // /// Given a list of files, remove them from the index, but NOT the file system.
    // fn remove_from_index(&self, file_names: &[String]) -> Result<(), BufkitDataErr> {
    //     // TODO: implement
    //     unimplemented!()
    // }

    // /// Given a list of files, remove them from the file system. This assumes they have already been
    // /// removed from the index (or never existed there.)
    // fn remove_from_data_store(&self, file_names: &[String]) -> Result<(), BufkitDataErr> {
    //     // TODO: implement
    //     unimplemented!()
    // }

    // /// Given a list of files, attempt to parse the file names and add them to the index.
    // fn add_to_index(&self, file_names: &[String]) -> Result<(), BufkitDataErr> {
    //     // TODO: implement
    //     unimplemented!()
    // }

    // ---------------------------------------------------------------------------------------------
    // The file system aspects of the archive, e.g. the root directory of the archive
    // ---------------------------------------------------------------------------------------------
    const FILE_DIR: &'static str = "files";
    const INDEX: &'static str = "index.sqlite";

    // ---------------------------------------------------------------------------------------------
    // Query or modify site metadata
    // ---------------------------------------------------------------------------------------------
    pub fn short_name_to_site(&self, short_name: &str) -> Result<Site, BufkitDataErr> {
        crate::site::retrieve_site(&self.db_conn, short_name)
    }

    pub fn sounding_type_from_str(
        &self,
        sounding_type: &str,
    ) -> Result<SoundingType, BufkitDataErr> {
        crate::sounding_type::retrieve_sounding_type(&self.db_conn, sounding_type)
    }

    /// Retrieve a list of sites in the archive.
    pub fn sites(&self) -> Result<Vec<Site>, BufkitDataErr> {
        crate::site::all_sites(&self.db_conn)
    }

    /// Retrieve the information about a single site.
    pub fn site_info(&self, short_name: &str) -> Result<Site, BufkitDataErr> {
        crate::site::retrieve_site(&self.db_conn, short_name)
    }

    /// Modify a sites values.
    pub fn set_site_info(&self, site: &Site) -> Result<(), BufkitDataErr> {
        self.db_conn.execute(
            "
                UPDATE sites 
                SET (long_name, state, notes, mobile_sounding_site)
                = (?2, ?3, ?4, ?5)
                WHERE short_name = ?1
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
            &[short_name],
            |row| row.get(0),
        )?;

        Ok(number >= 1)
    }

    // ---------------------------------------------------------------------------------------------
    // Query archive inventory
    // ---------------------------------------------------------------------------------------------

    /// Get an inventory of soundings for a site & model.
    pub fn inventory(&self, site: &Site) -> Result<Inventory, BufkitDataErr> {
        debug_assert!(site.id() > 0);
        crate::inventory::inventory(&self.db_conn, site.clone())
    }

    /// Get a list of models in the archive for this site.
    pub fn sounding_types(&self, site: &Site) -> Result<Vec<SoundingType>, BufkitDataErr> {
        debug_assert!(site.id() > 0);
        crate::sounding_type::all_sounding_types_for_site(&self.db_conn, site)
    }

    /// Retrieve the model initialization time of the most recent model in the archive.
    pub fn most_recent_valid_time(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
    ) -> Result<NaiveDateTime, BufkitDataErr> {
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
    ) -> Result<bool, BufkitDataErr> {
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
        site: Site,
        sounding_type: SoundingType,
        location: Location,
        init_time: &NaiveDateTime,
        file_name: &str,
    ) -> Result<(), BufkitDataErr> {
        let site = if site.is_known() {
            site
        } else {
            self.update_or_insert_site(site)?
        };

        let sounding_type = if sounding_type.is_known() {
            sounding_type
        } else {
            self.update_or_insert_sounding_type(sounding_type)?
        };

        let fname: String = self.compressed_file_name(&site, &sounding_type, init_time);

        let sounding_type = if sounding_type.is_known() {
            sounding_type
        } else {
            insert_or_update_sounding_type(&self.db_conn, sounding_type)?
        };

        let site = if site.is_known() {
            site
        } else {
            insert_or_update_site(&self.db_conn, site)?
        };

        let location = if location.is_known() {
            location
        } else {
            insert_or_update_location(&self.db_conn, location)?
        };

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

        // Open the file in binary mode and compress it into a file with the above found name

        Ok(())
    }

    /// Insert or update a sounding type.
    pub fn update_or_insert_sounding_type(
        &self,
        sounding_type: SoundingType,
    ) -> Result<SoundingType, BufkitDataErr> {
        crate::sounding_type::insert_or_update_sounding_type(&self.db_conn, sounding_type)
    }

    /// Insert or update a site.
    pub fn update_or_insert_site(&self, site: Site) -> Result<Site, BufkitDataErr> {
        crate::site::insert_or_update_site(&self.db_conn, site)
    }

    fn get_file_name_for(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> Result<String, BufkitDataErr> {
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

    fn load_data(&self, file_name: &str) -> Result<Vec<u8>, BufkitDataErr> {
        let file = File::open(self.file_dir.join(file_name))?;
        let mut decoder = GzDecoder::new(file);
        let mut buf: Vec<u8> = vec![];
        let _bytes_read = decoder.read_to_end(&mut buf)?;

        Ok(buf)
    }

    fn decode_data(
        buf: &[u8],
        description: &str,
        ftype: FileType,
    ) -> Result<Vec<Analysis>, BufkitDataErr> {
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
    ) -> Result<Vec<Analysis>, BufkitDataErr> {
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
    ) -> Result<impl Read, BufkitDataErr> {
        let file_name = self.get_file_name_for(site, sounding_type, init_time)?;
        let file = File::open(self.file_dir.join(file_name))?;
        Ok(GzDecoder::new(file))
    }

    /// Retrieve the  most recent file as a sounding.
    pub fn most_recent_file(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
    ) -> Result<Vec<Analysis>, BufkitDataErr> {
        let init_time = self.most_recent_valid_time(site, sounding_type)?;
        self.retrieve(site, sounding_type, &init_time)
    }

    fn compressed_file_name(
        &self,
        site: &Site,
        sounding_type: &SoundingType,
        init_time: &NaiveDateTime,
    ) -> String {
        let file_string = init_time.format("%Y%m%d%HZ").to_string();

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
    ) -> Result<(), BufkitDataErr> {
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
    use std::{error::Error, fs::read_dir};
    use tempdir::TempDir;

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
    fn get_test_data(
    ) -> Result<Vec<(Site, SoundingType, NaiveDateTime, Location, String)>, BufkitDataErr> {
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
    fn fill_test_archive(arch: &mut Archive) -> Result<(), BufkitDataErr> {
        let test_data = get_test_data().expect("Error loading test data.");

        for (site, sounding_type, init_time, loc, fname) in test_data {
            arch.add(site, sounding_type, loc, &init_time, &fname)?;
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
                .with_notes("A coastal city with coffee and rain".to_owned())
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
            assert!(test_sites
                .iter()
                .find(|st| st.short_name() == site.short_name())
                .is_some());
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

        for site in test_sites {
            let retr_site = arch.site_info(site.short_name()).unwrap();

            assert_eq!(site.short_name(), retr_site.short_name());
            assert_ne!(site.id(), retr_site.id());
            assert!(site.id() <= 0);
            assert!(retr_site.id() > 0);
            assert_eq!(site.long_name(), retr_site.long_name());
            assert_eq!(site.state_prov(), retr_site.state_prov());
            assert_eq!(site.notes(), retr_site.notes());
        }
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

        let retr_site = arch.site_info("kmso").unwrap();
        assert_eq!(retr_site.short_name(), test_sites[2].short_name());
        assert_eq!(retr_site.long_name(), test_sites[2].long_name());
        assert_eq!(retr_site.notes(), test_sites[2].notes());
        assert_eq!(retr_site.state_prov(), test_sites[2].state_prov());

        let zootown = Site::new("kmso")
            .with_long_name("Zootown".to_owned())
            .with_notes("Mountains, not coast.".to_owned())
            .with_state_prov(None)
            .set_mobile(false);

        arch.set_site_info(&zootown).expect("Error updating site.");

        let retr_site = arch.site_info("kmso").unwrap();
        assert_eq!(retr_site.short_name(), test_sites[2].short_name());
        assert_ne!(retr_site.long_name(), test_sites[2].long_name());
        assert_ne!(retr_site.notes(), test_sites[2].notes());
        assert_eq!(retr_site.state_prov(), test_sites[2].state_prov());

        assert_eq!(retr_site.short_name(), zootown.short_name());
        assert_eq!(retr_site.long_name(), zootown.long_name());
        assert_eq!(retr_site.notes(), zootown.notes());
        assert_eq!(retr_site.state_prov(), zootown.state_prov());
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

        let site = arch.site_info("kmso").expect("Error retrieving site.");

        let types: Vec<String> = arch
            .sounding_types(&site)
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

    #[test]
    fn test_inventory() -> Result<(), Box<Error>> {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let site = arch.site_info("kmso")?;
        let gfs = arch.sounding_type_from_str("GFS")?;
        let nam = arch.sounding_type_from_str("NAM")?;

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
        assert!(gfs_locations[0].is_known());

        let nam_locations = inv.locations(&nam);
        assert_eq!(nam_locations.len(), 1);
        assert_eq!(nam_locations[0].latitude(), 46.87);
        assert_eq!(nam_locations[0].longitude(), -114.16);
        assert_eq!(nam_locations[0].elevation(), 1335);
        assert!(nam_locations[0].is_known());

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
    fn test_files_round_trip() {
        let TestArchive { tmp: _tmp, arch } =
            create_test_archive().expect("Failed to create test archive.");

        let test_data = get_test_data().expect("Error loading test data.");

        for (site, sounding_type, init_time, loc, file_name) in test_data {
            arch.add(
                site.clone(),
                sounding_type.clone(),
                loc,
                &init_time,
                &file_name,
            )
            .expect("Failure to add.");

            let site = arch
                .site_info(site.short_name())
                .expect("Error retrieving site.");
            let sounding_type = arch
                .sounding_type_from_str(sounding_type.source())
                .expect("Error retrieving sounding_type");

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
    fn test_get_most_recent_file() -> Result<(), Box<Error>> {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = arch.site_info("kmso")?;
        let snd_type = arch.sounding_type_from_str("GFS")?;;

        let init_time = arch
            .most_recent_valid_time(&kmso, &snd_type)
            .expect("Error getting valid time.");

        assert_eq!(init_time, NaiveDate::from_ymd(2017, 4, 1).and_hms(18, 0, 0));

        arch.most_recent_file(&kmso, &snd_type)
            .expect("Failed to retrieve sounding.");

        Ok(())
    }

    #[test]
    fn test_file_exists() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let kmso = arch.short_name_to_site("kmso").unwrap();
        let snd_type = arch.sounding_type_from_str("GFS").unwrap();

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

    #[test]
    fn test_remove_file() {
        let TestArchive {
            tmp: _tmp,
            mut arch,
        } = create_test_archive().expect("Failed to create test archive.");

        fill_test_archive(&mut arch).expect("Error filling test archive.");

        let init_time = NaiveDate::from_ymd(2017, 4, 1).and_hms(0, 0, 0);
        let kmso = arch.short_name_to_site("kmso").unwrap();
        let snd_type = arch.sounding_type_from_str("GFS").unwrap();

        assert!(arch
            .file_exists(&kmso, &snd_type, &init_time)
            .expect("Error checking db"));
        arch.remove(&kmso, &snd_type, &init_time)
            .expect("Error while removing.");
        assert!(!arch
            .file_exists(&kmso, &snd_type, &init_time)
            .expect("Error checking db"));
    }
}
