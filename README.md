[![Build Status](https://ci.appveyor.com/api/projects/status/github/rnleach/sounding-data?branch=master&svg=true)](https://ci.appveyor.com/project/rnleach/sounding-data/branch/master)
[![Build Status](https://travis-ci.org/rnleach/sounding-data.svg?branch=master)](https://travis-ci.org/rnleach/sounding-data)

# sounding-data

Crate to manage and interface with an archive of
[bufkit](https://training.weather.gov/wdtd/tools/BUFKIT/index.php) and other files for storing model
and upper air files.

This is developed originally as a component crate for
[sonde](https://github.com/rnleach/sonde.git), but it also supports a set of command line tools
for utilizing the archive.

The current implementation uses an [sqlite](https://www.sqlite.org/index.html) database to keep 
track of files stored in a common directory. The files are compressed, and so should only be 
accessed via the api provided by this crate.

License: MIT

