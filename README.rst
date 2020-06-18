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

    python cli.py download -s surface -y 2018 -m 11 -p 228.128

Download ERA5 variables, if month argument is not passed then the entire year will be downloaded. By default it downloads hourly data in netcdf format.

Options: 
      * -q, --queue Create json file to add request to queue 
      * -s, --stream [surface|wave|pressure|land|cems_fire|agera5|wdfe5] ECMWF stream currently operative analysis surface, pressure levels, wave model, and derived products ERA5 land, CEMS_fire, AGERA5 and WDFE5
[required] 
      * -y, --year TEXT year to download [required] 
      * -m, --month TEXT month/s to download, if not specified all months for year will be downloaded 
      * -t, --timestep [mon|hr|day] timestep if not specified hr is default
      * -b, --back Request backwards all years and months as one file, works only for monthly data 
      * --format [grib|netcdf|tgz|zip] Format output: netcdf default, some formats work only for certain streams
      * -p, --param TEXT Grib code parameter for selected variable, pass as param.table i.e. 132.128 If none passed then allthe params listed in era5_<stream>_<tstep>.json will be used. 
      * --help
Show this message and exit.

To update files when a new month is releasedi, omit param flag::

    python cli.py download -s surface -y 2019 -m 05 


The 'download' sub command will actually request and download the data
unless you use the 'queue' flag. If you want only to create a request::

    python cli.py download -s surface -y 2018 -m 11 -p 228.128 -q

This will create a json file which stores the arguments passed::

    era5_request_<timestamp>.json
    {"update": false, "format": "netcdf", "stream": "surface", "params": ["228.128"], 
     "year": "2018", "months": ["11"], "timestep": "hr", "back": false}

To execute the request the tool is used with the 'scan' command option::

    era5 scan -f era5_request_<timestamp>.json

To manage the database use 'era5 db' subcommand::

   era5 db -s surface -p u10

for example will add all new surface hourly (tstep is hr by default) u10 files in database. NB that in this case you pass the variable name not the grib code to the -p flag.   
'db' can also list all variables in a stream:: 

   era5 db -a list -s land -t mon

Unless a variable is specified this will show all variables regularly updated for the stream, how many corrsponding files are on the filesystem and in the databse. These numbers are compared to the expected number of files based on the current date.

Finally to delete records::

   era5 db -a delete -s land -t mon -p ro -y 2019

 This will delete all corresponding records but will list all records to be deleted and ask for confirmation first.



click era5 command code and input files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  cli.py -- main code, includes click commands and functions to buil
   request and submit it with pool python cli.py --help to check usage
-  era5_functions.py -- functions used by cli.py, just separated them
   so it is overall more readable
-  era5_db.py -- has all the fuctions relating to db operations 
-  update_json_vars.py -- script that uses the clef db to update
   era5_vars.json needs clef module

To configure the tool
~~~~~~~~~~~~~~~~~~~~~

-  config.json -- to set configuration: ..+ staging, data, logs
   directories, ..+ database name and location, ..+ bash commands to
   download, resume download, qc and compress files, ..+ number of
   threads, ..+ number of resume download attempts, ..+ slow and fast
   ips

-  era5_pressure_hr.json -- pressure levels stream arguments to build
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
