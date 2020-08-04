Getting Started
===============

ERA5 is an interface to the CDSapi (Copernicus Climate Data Store API). It helps automating and submitting csapi requests. ERA5 is python3 based and uses the click module so it can be run as command line.
The configuration of the tool and specific information on the variables is provided by json files. 

Once is installed the tool is accessed through the command-line `era5` program. There are
presently three subcommands:

 * :code:`clef download` to submit a request and download new files, at least one stream has to be specified. If you omit the variable then all the variables listed in the corresponding stream json file will be downloaded. this is useful for regular updates.

 * :code:`clef scan` to execute a download request starting from a json constraints file previously saved using download with the --queue/-q flag.

Examples
--------


download
+++++
::

    $ era5 download -s pressure -y 2020 -m 02 -p 132.128 

This will put in a request for temperature (whose grib code is 132.128) for February 2020 on pressure levels and at 1hr timesteps.
The area and grid resolution are defined in the 
era5/data/era5_pressure_hr.json file

NB that you can change the timestep by using the `-t` flag, `hr` is the default value, other possibilities are `mon` and `day` . Depending on the stream some of these options might not be valid.

Other flags are:
     * -h/--help
     * -q/--queue  which saves the constraints to a json file that can be passed later to the `scan` subcommand
     * -f/--file works only with `scan`
     * --back works only with monthly data, it allows to download backwards all available data to the year passed with `-y`. 

Scan example::
     $ era5 scan -f era5_request_file.json

Update the database::

     $ era5 db -s land -t mon -p ro

NB
     * -a/--action works only for db subcommand to choose what db action to execute: update (default), list, or delete 


Installation
============
The tool uses python >=3.6 and you need to install the click package.

To setup the tool::
    $ python setup.py install

The github repository includes a modified version of the cdsapi. 
For this to work you need to setup a .csdapirc file in your home directory with the api url and your own api key. It should look like::

     url: https://cds.climate.copernicus.eu/api/v2
     key: #########################::
     

The instructions are available from the ECMWF confluence.
NB most of the tool functionalities ar epreserved even if you use the original api. What the modified api does is to stop the api working before it can download the files. Separating the file download from the requests helps managing the downloads as parallel tasks and avoid potential sisue should the connection being interrupted. 

Step2
-----
Configure the tool by defining the following settings in era5/data/config.json:

  The following folders:
 * log  for log outputs
 * staging  where the files are initially downloaded
 * netcdf for the compressed files
 * era5_derived if you are downloading ERA% derived products
 * And update directories paths and the sqlite db name
 * the command you want to use to download/compress the files

