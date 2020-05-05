|DOI| \_\_\_

ERA5
====

The era5 python code is an interface to the CDS api to download ERA5
data from the CDS data server. It uses a modified version of the CDS api
which stops after a request has been submitted and executed. The target
download url is saved and downloads are run in parallel by the code
using the Pool multiprocessing module. As well as managing the downloads
the code gets all the necessary information on available variables from
local json configuration files. Before submitting a request the code
will check that the file is not already available locally by quering a
sqlite database. After downloading new files it is important to update
the database to avoid downloading twice the same file. Files are first
downloaded in a staging area, a quick qc to see if the file is a valid
netcdf file is run and finally the file is converted to netcdf4 format
with internal compression. The code default behaviour is to download
netcdf files, but it's also possible to download grib, however we
haven't tested the full workflow for this option.

Getting started
---------------

To run a download::

::

    python cli.py download -s surface -y 2018 -m 11 -p 228.128

Download ERA5 variables, to be preferred if adding a new variable, if
month argument is not passed then the entire year will be downloaded. By
default it downloads hourly data in netcdf format.

Options: + -q, --queue Create json file to add request to queue + -s,
--stream [surface\|wave\|pressure\|land] ECMWF stream currently
operative analysis surface, pressure levels, wave model and ERA5 land
[required] + -y, --year TEXT year to download [required] + -m, --month
TEXT month/s to download, if not specified all months for year will be
downloaded + -t, --timestep [mon\|hr] timestep hr or mon, if not
specified hr + -b, --back Request backwards all years and months as one
file, works only for monthly data + --format [grib\|netcdf] Format
output: grib or netcdf + -p, --param TEXT Grib code parameter for
selected variable, pass as param.table i.e. 132.128 [required] + --help
Show this message and exit.

To update files when a new month is released::

::

    python cli.py update -s surface -y 2019 -m 05 

Update ERA5 variables, to be used for regular monthly updates. Only the
stream argument without params will update all the variables listed in
the era5\_\_.json file. Options are the same as for download.

The 'download' and 'update' commands will actually download the data
unless you use the 'queue' flag. If you want only to create a request::

::

    python cli.py download -s surface -y 2018 -m 11 -p 228.128 -q

This will create a json file which stores the arguments passed:

::

    era5_request_<timestamp>.json
    {"update": false, "format": "netcdf", "stream": "surface", "params": ["228.128"], 
     "year": "2018", "months": ["11"], "timestep": "hr", "back": false}

For this request to be downloaded the file should be moved to

::

    /g/data/ub4/Work/Scripts/ERA5/Requests

To execute the request the tool is used with the 'scan' command option::

::

    python cli.py scan -f era5_request_<timestamp>.json

click era5 command code and input files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  cli.py -- main code, includes click commands and functions to buil
   request and submit it with pool python cli.py --help to check usage
-  era5\_functions.py -- functions used by cli.py, just separated them
   so it is overall more readable
-  update\_json\_vars.py -- script that uses the clef db to update
   era5\_vars.json needs clef module, currently module use
   /g/data/hh5/tmp/public/modules module load conda/analysis3
-  era5\_update\_db.py -- crawls /g/data/ub4/era5/netcdf and adds new
   netcdf files to the sqlite database

To configure the tool
~~~~~~~~~~~~~~~~~~~~~

-  config.json -- to set configuration: ..+ staging, data, logs
   directories, ..+ database name and location, ..+ bash commands to
   download, resume download, qc and compress files, ..+ number of
   threads, ..+ number of resume download attempts, ..+ slow and fast
   ips

-  era5\_pressure\_hr.json -- pressure levels stream arguments to build
   request and list of params to download at hourly temporal resolution
-  era5\_pressure\_mon.json -- pressure levels stream arguments to build
   request and list of params to download at monthly temporal resolution
-  era5\_wave\_hr.json -- wave model surface level stream arguments to
   build request and list of params to download at hourly temporal
   resolution
-  era5\_wave\_mon.json -- wave model surface level stream arguments to
   build request and list of params to download at monthly temporal
   resolution
-  era5\_surface\_hr.json -- surface level stream arguments to build
   request and list of params to download at hourly temporal resolution
-  era5\_surface\_mon.json -- surface level stream arguments to build
   request and list of params to download at monthly temporal resolution
-  era5\_land\_hr.json -- Land model surface level stream arguments to
   build request and list of params to download at hourly temporal
   resolution
-  era5\_vars.json -- Json file with list of grib codes that can be
   downloaded from CDS and respective variable and cds names

Other files
~~~~~~~~~~~

(not included in git)

-  era5.sqlite -- sqlite database
-  setup.py.template -- template for setup.py

Modified cdsapi code
~~~~~~~~~~~~~~~~~~~~

-  cdsapi: **init**.py **pycache** api.py

.. |DOI| image:: https://zenodo.org/badge/DOI/10.5281/zenodo.3549078.svg
   :target: https://doi.org/10.5281/zenodo.3549078
