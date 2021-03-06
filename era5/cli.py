#!/usr/bin/env python
# Copyright 2019 ARC Centre of Excellence for Climate Extremes (CLEx) and NCI
# Author: Paola Petrelli <paola.petrelli@utas.edu.au> for CLEx 
#         Matt Nethery <matt.nethery@nci.org.au> for NCI 
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# To download ERA5 data from the Copernicus data server
# Download all the files for a year or selected months
# Timestep is 1hr or monthly averages
# Resolution is 0.1X0.1 degrees for ERA5 Land, 0.25X0.25 degrees for pressure and surface levels
#  and 0.5X0.5 degrees for wave model surface level
# Area is global for surface, wave model and land variables variable while pressure levels are downloaded on
# an Australian extended region defined as 
# -57 to 20 Lat and 78 to -140 Lon
# Use: change year and adjust mntlist if necessary
#     python era5_download.py -y <year> -m <month> -v <variable> -t  <stream> 
# depends on cdapi.py that can be downloaded from the Copernicus website
#   https://cds.climate.copernicus.eu/api-how-to
# contact: paolap@utas.edu.au
# last updated 09/06/2020
#!/usr/bin/python

import click
import os
import sys
import yaml
from itertools import product as iproduct
from multiprocessing.dummy import Pool as ThreadPool
#from era5.era5_update_db import db_connect, query
from era5.era5_functions import *
from era5.era5_db import query, db_connect, update_db, delete_record, variables_stats
import era5.cdsapi as cdsapi


def era5_catch():
    debug_logger = logging.getLogger('era5_debug')
    debug_logger.setLevel(logging.CRITICAL)
    try:
        era5()
    except Exception as e:
        click.echo('ERROR: %s'%e)
        debug_logger.exception(e)
        sys.exit(1)


def do_request(r):
    """
    Issue the download request. param 'r' is a tuple:
    [0] dataset name
    [1] the query
    [2] file staging path
    [3] file target path
    [4] ip for download url
    [5] userid

    Download to staging area first, compress netcdf (nccopy)
    """
    tempfn = r[2]
    fn = r[3]
  
    # the actual retrieve part
    # set api key explicitly so you can alternate
    with open(f'/mnt/pvol/era5/.cdsapirc{r[5]}', 'r') as f:
        credentials = yaml.safe_load(f)
    # create client instance
    c = cdsapi.Client(url=credentials['url'], key=credentials['key'], verify=1)
    era5log.info(f'Requesting {tempfn} ... ')
    era5log.info(f'Request: {r[1]}')
    # need to capture exceptions
    apirc = False
    try:
        # issue the request (to modified api)
        res = c.retrieve(r[0], r[1], tempfn)
        apirc = True
    except Exception as e:
        era5log.error(f'ERROR: {e}')
        apirc = False
    # if request successful download file
    if apirc:
        # get download url and replace ip
        url = res.location
        # get size from response to check file complete
        size = res.content_length
        # check for slow IPs
        for ip in cfg['slowips']:
            if f'.{ip}/' in res.location:
                url = res.location.replace(f'.{ip}/', f'.{r[4]}/')
        if file_down(url, tempfn, size, era5log):            # successful
            # if netcdf compress file, assuming it'll fail if file is corrupted
            # if tgz  untar, if zip unzip
            # if grib skip
            if tempfn[-3:] == '.nc':
                era5log.info(f'Compressing {tempfn} ...')
                cmd = f"{cfg['nccmd']} {tempfn} {fn}"
            elif tempfn[-4:] == '.tgz':
                era5log.info(f'Untarring and concatenating {tempfn} ...')
                base = os.path.dirname(tempfn) 
                cmd =  f"{cfg['untar']} {tempfn} -C {base}; {cfg['concat']} {base}/*.nc {fn[:-4]}.nc; rm {base}/*.nc"
            elif tempfn[-4:] == '.zip':
                era5log.info(f'Unzipping and concatenating {tempfn} ...')
                base = os.path.dirname(tempfn) 
                cmd =  f"unzip {tempfn} -d {base}; {cfg['concat']} {base}/*.nc {fn[:-4]}.nc; rm {base}/*.nc"
            else:
                cmd = "echo 'nothing to do'"
            era5log.debug(f"{cmd}")
            p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
            out,err = p.communicate()
            era5log.debug(f"Popen out/err: {out}, {err}")
            if not p.returncode:       # check was successful
                era5log.info(f'ERA5 download success: {fn}')
            else:
                era5log.info(f'ERA5 nc command failed! (deleting compressed file {fn})\n{err.decode()}')
    return


def api_request(oformat, stream, params, yr, mntlist, tstep, back):
    """ Build a list of CDSapi requests based on arguments
        Call do_request to submit them and start parallel download
        If download successful, compress file and move to era5/netcdf
    """
    # open connection to era5 files db 
    conn = db_connect(cfg)
    # create empty list to  store cdsapi requests
    rqlist = []
    # list of faster ips to alternate
    ips = cfg['altips']
    users = cfg['users']
    i = 0 
    # list of years when ERA5.1 should be donwloaded instead of ERA5
    era51 = [str(y) for y in range(2000,2007)]
    if mntlist == []:  
        mntlist = ["%.2d" % i for i in range(1,13)]
    # retrieve stream arguments
    dsargs = define_args(stream, tstep)
    era5log.debug(f'Stream attributes: {dsargs}')
    # get variables details from json file
    vardict = read_vars(stream)
    # define params to download
    if params == []:
        params = dsargs['params']
    era5log.debug(f'Params: {params}')
    
    # according to ECMWF, best to loop through years and months and do either multiple
    # variables in one request, or at least loop through variables in the innermost loop.
    
    for y in yr:
        era5log.debug(f'Year: {y}')
        # change dsid if pressure and year between 2000 and 2006 included
        mars = False
        if y in era51 and stream == 'pressure':
            era5log.debug(f'Submitting using mars for ERA5.1')
            mars = True
            dsargs = define_args(stream+"51", tstep)
            dsargs['dsid'] = 'reanalysis-era5.1-complete'
        # build Copernicus requests for each month and submit it using cdsapi modified module
        for mn in mntlist:
            era5log.debug(f'Month: {mn}')
            # for each output file build request and append to list
            # loop through params and months requested
            for varp in params:
                era5log.debug(f'Param: {varp}')
                queue, var, cdsname =  define_var(vardict, varp, era5log)
                # if grib code exists but cds name is not defined skip var and print warning
                if not queue:
                    continue
                # create list of filenames already existing for this var and yr
                nclist = []
                sql = "select filename from file where location=?" 
                tup = (f"{stream}/{var}/{y}",)
                if tstep == 'mon':
                    tup = (f"{stream}/{var}/monthly",)
                nclist += query(conn, sql, tup)
                era5log.debug(nclist)

                stagedir, destdir, fname, daylist = target(stream, var,
                                    y, mn, dsargs, tstep, back, oformat)
                # if file already exists in datadir then skip
                if file_exists(fname, nclist):
                    era5log.info(f'Skipping {fname} already exists')
                    continue
                if mars:
                    rdict = build_mars(dsargs, y, mn, varp, oformat, tstep, back)
                else:
                    rdict = build_dict(dsargs, y, mn, cdsname, daylist, oformat, tstep, back)
                rqlist.append((dsargs['dsid'], rdict, os.path.join(stagedir,fname),
                           os.path.join(destdir, fname), ips[i % len(ips)],
                           users[i % len(users)])) 
                # progress index to alternate between ips and users
                i+=1
                era5log.info(f'Added request for {fname}')
            if back:
                era5log.debug(f'Breaking cycle back is True')
                break
    
    era5log.debug(f'{rqlist}')
    # parallel downloads
    if len(rqlist) > 0:
        # set num of threads = number of params, or use default from config
        if len(params) > 1:
            nthreads = len(params)
        else:
            nthreads = cfg['nthreads']
        pool = ThreadPool(nthreads)
        results = pool.imap(do_request, rqlist)
        pool.close()
        pool.join()
    else:
        era5log.info('No files to download!')
    era5log.info('--- Done ---')


@click.group()
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
def era5(debug):
    """
    Request and download ERA5 data from the climate copernicus
    server using the cdsapi module.
    """
    global era5log 
    era5log = config_log(debug)


def common_args(f):
    constraints = [
        click.option('--month', '-m', multiple=True,
                     help="month/s to download, if not specified all months for year will be downloaded "),
        click.option('--timestep', '-t', type=click.Choice(['mon','hr','day']), default='hr',
                     help="timestep hr or mon, if not specified hr"),
        click.option('--format', 'oformat', type=click.Choice(['grib','netcdf','zip','tgz']), default='netcdf',
                     help="Format output: grib, nc (for netcdf), tgz (compressed tar file) or zip")
    ]
    for c in reversed(constraints):
        f = c(f)
    return f


def download_args(f):
    '''Arguments to use with db sub-command '''
    constraints = [
        click.option('--stream', '-s', required=True,
                     type=click.Choice(['surface','wave','pressure', 'land', 'cems_fire', 'agera5', 'wfde5']),
                     help="ECMWF stream currently operative analysis surface, pressure levels, "+\
                     "wave model, ERA5 land, CESM_Fire, AgERA5, WFDE5"),
        click.option('--param', '-p', multiple=True,
             help="Grib code parameter for selected variable, pass as param.table i.e. 132.128. If not passed all parameters in <stream_tstep>.json will be downloaded"),
        click.option('--year', '-y', multiple=True, required=True,
                     help="year to download"),
        click.option('--queue', '-q', is_flag=True, default=False,
                     help="Create json file to add request to queue"),
        click.option('--back', '-b', is_flag=True, default=False,
                     help="Request backwards all years and months as one file, works only for monthly or daily data"),
        click.option('--urgent', '-u', is_flag=True, default=False,
                     help="high priority request, default False, if specified request is saved in Urgent folder which is pick first by wrapper. Works only for queued requests.")
    ]
    for c in reversed(constraints):
        f = c(f)
    return f


def db_args(f):
    '''Arguments to use with db sub-command '''
    constraints = [
        click.option('--stream', '-s', required=False,
                     type=click.Choice(['surface','wave','pressure', 'land', 'cems_fire', 'agera5', 'wfde5']),
                     help="ECMWF stream currently operative analysis surface, pressure levels, "+\
                     "wave model, ERA5 land, CESM_Fire, AgERA5, WFDE5. It is required in list and delete mode."),
        click.option('--param', '-p', multiple=True,
             help="Variable name. At least one value is required in delete mode. If not passed in list mode all variables <stream_tstep>.json will be listed"),
        click.option('--year', '-y', multiple=True, required=False,
                     help="year to download"),
        click.option('-a','--action', type=click.Choice(['list','delete','update']), default='update',
        help="db subcommand running mode: `update` (default) updates the db, `delete` deletes a record from db, `list` list all variables in db for the stream")
    ]
    for c in reversed(constraints):
        f = c(f)
    return f


@era5.command()
@common_args
@download_args
def download(oformat, param, stream, year, month, timestep, back, queue, urgent):
    """ 
    Download ERA5 variables, to be preferred 
    if adding a new variable,
    if month argument is not passed 
    then the entire year will be downloaded. 
    By default downloads hourly data in netcdf format.
    \f
    Grid and other stream settings are in the era5_<stream>_<timestep>.json files.
    """
    if back and stream != 'wfde5' and timestep not in ['mon', 'day']:
        print('You can the backwards option only with monthly and some daily data')
        sys.exit()
    valid_format = list(iproduct(['tgz','zip'],['cems_fire','agera5', 'wfde5']))
    valid_format.extend( list(iproduct( ['netcdf', 'grib'],
                         ['pressure','surface','land','wave'])))
    if (oformat,stream) not in valid_format:
        print(f'Download format {oformat} not available for {stream} product')
        sys.exit()
    if queue:
        dump_args(oformat, stream, list(param), list(year), list(month), timestep, back, urgent)
    else:    
        api_request(oformat, stream, list(param), list(year), list(month), timestep, back)


@era5.command()
@click.option('--file', '-f', 'infile', default='/g/data/ub4/Work/Requests/requests.json',
             help="Pass json file with list of requests, instead of arguments")
def scan(infile):
    """ 
    Load arguments from file instead of separately
    """
    with open(infile, 'r') as fj:
         args = json.load(fj)
    api_request( args['format'], args['stream'], 
                args['params'], args['year'], args['months'], 
                args['timestep'], args['back'])


@era5.command()
@common_args
@db_args
def db(oformat, param, stream, year, month, timestep, action):
    """ 
    Work on database, options are 
    - update database,
    - delete record,
    - build variables list for a stream, check how many files are on disk,
      on db and how many are expected for each of them
    - check missing files for a variable
    """
    
    if action == 'update':
        update_db(cfg, stream, timestep, list(param))
    elif action == 'delete':    
        if not stream or not param:
            print('A stream and at least one variable should be selected: -s <stream> -p <var-name>')
            sys.exit()
        delete_record(cfg, stream, list(param), list(year), list(month), timestep) #, oformat)
    else:    
        varlist = [] 
        variables_stats(cfg, stream, timestep, list(param))

if __name__ == '__main__':
    era5()
