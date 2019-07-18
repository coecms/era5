# click era5 command code and input files
cli.py  -- main code, includes click commands and functions to buil request and submit it with pool 
           python cli.py --help to check usage
           when the module will be installed can be called as
           era5 --help 
era5_functions.py -- functions used by cli.py, just spearated them so it is overall more readable
update_json_vars.py -- script that uses the clef db to update era5_vars.json
                       needs clef module, currently 
                       module use /g/data/hh5/tmp/public/modules
                       module load clef
                       source activate clef-test
                    I will run that from raijin when necessary, i.e. when new variables are defined, the most likely variables to be requested are already in the file
config.json  -- to set configuration: staging and data directories, bash commands used by code, number of threads and of times to try to resume downloads
era5_pressure.json -- pressure levels stream arguments to build request and list of params to download
era5_wave.json -- wave stream arguments to build request and list of params to download
era5_surface.json -- surface stream arguments to build request and list of params to download
era5_vars.json  -- Json file with list of grib codes that can be downloaded from CDS and respective variable and cds names
setup.py.template  -- template for setup.py 

#Other files, not to be included in git
incomplete.nc --  example of incomplete netcdf file to test QC
ips_slow.txt -- list of potentially slow ips 
prova.sqlite -- a copy of era5.sqlite I'm using to test code

#Matt original version of script:
#I've integrated do_request and Pool download part from era5_donwload.py in cli.py
#I already had a complete list of variables from the ECMWF tables in clef db so we don't need necessarily need the db.
# I'm also using era5_update_db.py to create list of existing nc files on the fly while running the code.
# the list is limited to yr and var requested
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
