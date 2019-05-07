BEGIN;

CREATE TABLE types (
    id        INTEGER PRIMARY KEY,  -- Used as foreign key in other tables
    type      TEXT UNIQUE NOT NULL, -- GFS, NAM, NAM4KM, MOBIL, RAWINSONDE,
    file_type TEXT        NOT NULL, -- BUFR, BUFKIT, etc.
    interval  INTEGER,              -- Hours between model runs/launches/etc.
    observed  INT NOT NULL          -- 0 if false (e.g. model data), 1 if observed
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
    latitude          INTEGER DEFAULT NULL, -- Decimal degrees * 1,000,000 and truncated
    longitude         INTEGER DEFAULT NULL, -- Decimal degrees * 1,000,000 and truncated
    elevation_meters  INT     DEFAULT NULL, 
    tz_offset_seconds INT     DEFAULT NULL  -- Offset from UTC in seconds
);

CREATE TABLE files (
    type_id     INTEGER     NOT NULL,
    site_id     INTEGER     NOT NULL,
    location_id INTEGER     NOT NULL,
    init_time   TEXT        NOT NULL,
    end_time    TEXT        NOT NULL,
    file_name   TEXT UNIQUE NOT NULL,
    FOREIGN KEY (type_id)     REFERENCES types(id),
    FOREIGN KEY (site_id)     REFERENCES sites(id),
    FOREIGN KEY (location_id) REFERENCES locations(id)
);

-- For fast searches by file name.
CREATE UNIQUE INDEX fname ON files(file_name);  

-- For fast searches by metadata.
CREATE UNIQUE INDEX no_dups_files ON files(type_id, site_id, init_time); 

-- Force unique locations
CREATE UNIQUE INDEX no_dups_locations ON locations(latitude, longitude, elevation_meters);

COMMIT;