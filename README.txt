# click era5 command code and input files
cli.py   -- code using clef db
cli_nodb.py  -- code using json table (will become cli.py)
config.json  -- to set configuration: staging and data directories, bash commands used by code, number of threads and of times to try to resume downloads
era5_pressure.json -- pressure levels stream arguments to build request and list of params to download
era5_wave.json -- wave stream arguments to build request and list of params to download
era5_surface.json -- surface stream arguments to build request and list of params to download
era5_vars.json  -- Json file with list of grib codes that can be downloaded from CDS and respective variable and cds names
setup.py.template  -- template for setup.py 

#Random files, not to be included in git
incomplete.nc --  example of incomplete netcdf file to test QC
ips_slow.txt -- list of potentially slow ips 

#Matt original version of script:
#I've integrated do_request and Pool download part from era5_donwload.py in cli.py
#I already had a complete list of variables from the ECMWF tables in clef db so we don't need necessarily need the db.
#We can still use the code to create a era5_nc list just changing it to output to json to be consistent
era5_nc.list
era5_download.py
era5_config.py
dump_db.py
era5_update_db.py
era5.sqlite
__pycache__:
cli.cpython-36.pyc
cli.cpython-37.pyc
era5_config.cpython-36.pyc

#Modified cdsapi code
cdsapi:
__init__.py
__pycache__
api.py
