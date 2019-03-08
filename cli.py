# To download ERA5 data from the Copernicus data server
# Download all the files for a year or selected months
# Timestep is 1hr
# Resolution is 0.25X0.25 degree, area is global for surface variable and 
# and Australian extended region defined as 
# -57 to 20 Lat and 78 to -140 Lon
# to change them change the relative fields
# Use: change year and adjust mntlist if necessary
#      python era5_download.py -y <year> -m <month> -v <variable> -t  <stream> 
# depends on cdapi.py that can be downloaded from the Copernicus website
#   https://cds.climate.copernicus.eu/api-how-to
# Author: Paola Petrelli for ARC Centre of Excellence for Climate Extremes
#         Matt Nethery NCI 
# contact: paolap@utas.edu.au
# last updated 28/02/2019
#!/usr/bin/python

import click
import os
from multiprocessing.dummy import Pool as ThreadPool
from era5_update_db import db_connect, query
from era5_functions import *


def do_request(r):
    """
    Issue the download request. param 'r' is a tuple:
    [0] dataset name
    [1] the query
    [2] file staging path
    [3] file target path
    [4] ip for download url

    Download to staging area first, compress netcdf (nccopy)
    """
    tempfn = r[2]
    fn = r[3]
  
    # the actual retrieve part
    # create client instance
    c = cdsapi.Client()
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
        if '.198/' in res.location:
            url = res.location.replace('.198/', f'.{r[4]}/')
        if file_down(url, tempfn, size, era5log):            # successful
            # do some compression on the file - assuming 1. it's netcdf, 2. that nccopy will fail if file is corrupt
            era5log.info(f'Compressing {tempfn} ...')
            cmd = f"{cfg['nccmd']} {tempfn} {fn}"
            p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
            out,err = p.communicate()
            if not p.returncode:       # check was successful
                era5log.info(f'ERA5 download success: {fn}')
            else:
                era5log.info(f'ERA5 nc command failed! (deleting compressed file {fn})\n{err.decode()}')
    return



def api_request(update, oformat, stream, params, yr, mntlist):
    """ Build a list of CDSapi requests based on arguments
        Call do_request to submit them and start parallel download
        If download successful, compress file and move to era5/netcdf
    """
    # open connection to era5 files db 
    conn = db_connect(cfg)
    # create empty list to  store cdsapi requests
    rqlist = []
    # list of faster ips to alternate
    #ips = ['110', '210']
    ips = cfg['altips']
    i = 0 
    # assign year and list of months
    if mntlist == []:  
        mntlist = ["%.2d" % i for i in range(1,13)]
    # retrieve stream arguments
    dsargs = define_args(stream)
    era5log.debug(f'Stream attributes: {dsargs}')
    # get variables details from json file
    vardict = read_vars()
    # define params to download
    if update and params == []:
        params = dsargs['params']
    # loop through params and months requested
    for varp in params:
        queue, var, cdsname =  define_var(vardict, varp)
        # if grib code exists but cds name is not defined skip var and print warning
        if not queue:
            continue
    # create list of filenames already existing for this var and yr
        sql = "select filename from file where location=?" 
        tup = (f"{stream}/{var}/{yr}",)
        nclist = query(conn, sql, tup)
        era5log.debug(nclist)
    # build Copernicus requests for each month and submit it using cdsapi modified module
        for mn in mntlist:
            # for each output file build request and append to list
            stagedir, destdir, fname, daylist = target(stream, var, yr, mn, dsargs)
    # if file already exists in datadir then skip
            if file_exists(fname, nclist):
                era5log.info(f'Skipping {fname} already exists')
                continue
            rdict = build_dict(dsargs, yr, mn, cdsname, daylist, oformat)
            rqlist.append((dsargs['dsid'], rdict, os.path.join(stagedir,fname),
                           os.path.join(destdir, fname), ips[i % len(ips)])) 
            # progress index to alternate between ips
            i+=1
            era5log.info(f'Added request for {fname}')
        era5log.debug(f'{rqlist}')

    # parallel downloads
    if len(rqlist) > 0:
        pool = ThreadPool(cfg['nthreads'])
        results = pool.map(do_request, rqlist)
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
        click.option('--queue', '-q', is_flag=True, default=False,
                     help="Create json file to add request to queue"),
        click.option('--stream', '-s', required=True, type=click.Choice(['surface','wave','pressure']),
                     help="ECMWF stream currently operative analysis surface, wave or pressure levels"),
        click.option('--year', '-y', required=True,
                     help="year to download"),
        click.option('--month', '-m', multiple=True,
                     help="month/s to download, if not specified all months for year will be downloaded "),
        click.option('--format', 'oformat', type=click.Choice(['grib','netcdf']), default='netcdf',
                     help="Format output: grib or netcdf")
    ]
    for c in reversed(constraints):
        f = c(f)
    return f


@era5.command()
@common_args
@click.option('--param', '-p', multiple=True,
             help="Grib code parameter for selected variable, pass as param.table i.e. 132.128. If not passed all parametes for the stream will be updated")
def update(oformat, param, stream, year, month, queue):
    """ 
    Update ERA5 variables, if regular monthly update 
    then passing only the stream argument will update
    all the variables listed in the era5_<stream>.json file.
    \f
    Grid and other stream settings are in the era5_<stream>.json file.
    """
    ####I'm separating this in update and download, so eventually update can check if no yr/mn passed or only yr passed which was the last month downloaded
    
    update = True
    if queue:
        dump_args(update, oformat, stream, list(param), year, list(month))
    else:    
        api_request(update, oformat, stream, list(param), year, list(month))


@era5.command()
@common_args
@click.option('--param', '-p', multiple=True, required=True,
             help="Grib code parameter for selected variable, pass as param.table i.e. 132.128")
def download(oformat, param, stream, year, month, queue):
    """ 
    Download ERA5 variables, to be preferred 
    if adding a new variable,
    if month argument is not passed 
    then the entire year will be downloaded. 
    \f
    Grid and other stream settings are in the stream.json file.
    """
    update = False
    if queue:
        dump_args(update, oformat, stream, list(param), year, list(month))
    else:    
        api_request(update, oformat, stream, list(param), year, list(month))


@era5.command()
@click.option('--file', '-f', 'infile', default='/g/data/ub4/Work/Requests/requests.json',
             help="Pass json file with list of requests, instead of arguments")
def scan(infile):
    """ 
    Load arguments from file instead of separately
    """
    with open(infile, 'r') as fj:
         args = json.load(fj)
    api_request(args['update'], args['format'], args['stream'], 
                args['params'], args['year'], args['months'])


if __name__ == '__main__':
    era5()
