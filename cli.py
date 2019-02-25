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
# last updated 22/02/2019
#!/usr/bin/python

import click
import logging
import json
from clef.collections import connect, ECMWF, Variable
#from clef.update_collections import update_item
import cdsapi
from calendar import monthrange
from datetime import datetime
import sys
import os
from multiprocessing.dummy import Pool as ThreadPool
#from multiprocessing import Pool as ThreadPool
import subprocess as sp


def era5_catch():
    debug_logger = logging.getLogger('era5_debug')
    debug_logger.setLevel(logging.CRITICAL)
    try:
        era5()
    except Exception as e:
        click.echo('ERROR: %s'%e)
        debug_logger.exception(e)
        sys.exit(1)

def config_log(debug):
    ''' configure log file to keep track of users queries '''
    # start a logger
    logger = logging.getLogger('era5log')
    # set a formatter to manage the output format of our handler
    formatter = logging.Formatter('%(asctime)s; %(message)s',"%Y-%m-%d %H:%M:%S")
    #formatter = logging.Formatter('%(levelname) %(asctime)s; %(message)s',"%Y-%m-%d %H:%M:%S")
    # set era5log level explicitly otherwise it inherits the root logger level:WARNING
    # if debug: level DEBUG otherwise INFO
    # because this is the logger level it will determine the lowest possible level for thehandlers
    if debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logger.setLevel(level)
    
    # add a handler to send WARNING level messages to console
    clog = logging.StreamHandler()
    if debug:
        level = logging.DEBUG
    else:
        level = logging.WARNING
    clog.setLevel(level)
    logger.addHandler(clog)    

    # add a handler to send INFO level messages to file 
    # the messagges will be appended to the same file
    # create a new log file every month
    month = datetime.now().strftime("%Y%m") 
    logname = '/g/data/ub4/Work/Logs/ERA5/era5_log_' + month + '.txt' 
    flog = logging.FileHandler(logname) 
    try:
        os.chmod(logname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO);
    except:
        pass 
    flog.setLevel(logging.INFO)
    flog.setFormatter(formatter)
    logger.addHandler(flog)

    # return the logger object
    return logger


def read_config():
    """
    Read config from config.json file
    """
    global era5log
    try:
        with open('config.json','r') as fj:
            cfg = json.load(fj)
    except FileNotFoundError:
        era5log.info(f"Can't find file config.json in {os.getcwd()}")
        sys.exit()
    return cfg


def define_dates(yr,mn):
    """ return a date range for each file depending on selected type """ 
    startday=1
    endday=monthrange(int(yr),int(mn))[1]
    daylist=["%.2d" % i for i in range(startday,endday+1)]    
    return daylist 


def define_var(clefdb, varparam):
    """ Find grib code in db table and return relevant info
    """
    global era5log
    queue = True
    vals = clefdb.query(ECMWF).filter(ECMWF.code == varparam).one_or_none()
    if vals is None:
       era5log.info(f'Selected parameter code {varparam} is not available')
       queue = False
       return queue, None, None
    if vals.cds_name is None or vals.cds_name == '':
       era5log.info(f'CDS name for {varparam} is not available')
       queue = False
    print(vals.name,  ":  ", vals.cds_name)
    return queue, vals.name, vals.cds_name


def define_args(stream):
    ''' Return parameters and levels lists and step, time depending on stream type'''
    # this import the stream_dict dictionary <stream> : ['time','step','params','levels']
    # I'm getting the information which is common to all pressure/surface/wave variables form here, plus a list of the variables we download for each stream
    with open(f'era5_{stream}.json', 'r') as fj:
        dsargs = json.load(fj)
    return  dsargs


def db_connect():
    """Connect to clef collections db to get param info
    """
    db = connect()
    return db.session


def build_dict(dsargs, yr, mn, var, daylist, oformat):
    """Builds request dictionary to pass to retrieve command 
    """
    timelist = ["%.2d:00" % i for i in range(24)]
    rdict={ 'product_type':'reanalysis',
            'variable'    : var,
            'year'        : str(yr),
            'month'       : str(mn),
            'day'         : daylist,
            'time'       : timelist,
            'format'      : oformat,
            'area'        : dsargs['area']} 
    if dsargs['levels'] != []:
        rdict['pressure_level']= dsargs['levels']
    return rdict 


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
    global cfg, era5log

    tempfn = r[2]
    fn = r[3] 
    era5log.debug(f"{tempfn}")
    era5log.debug(f"{fn}")
   
    # the actual retrieve part
    # create client instance
    c = cdsapi.Client()
    era5log.info(f'Downloading {tempfn} ... ')
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
        if '.198/' in res.location:
            url = res.location.replace('.198/', f'.{r[4]}/')
        cmd = f"{cfg['getcmd']} {tempfn} {url}"
        era5log.info(f'ERA5 Downloading: {url} to {tempfn}')
        #return
        p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out,err = p.communicate()
        if not p.returncode:            # successful
            # do some compression on the file - assuming 1. it's netcdf, 2. that nccopy will fail if file is corrupt
            cmd = f"{cfg['nccmd']} {tempfn} {fn}"
            p1 = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
            out,err = p1.communicate()
            if not p1.returncode:       # check was successful
                era5log.info(f'ERA5 download success: {fn}')
                # move to correct destination
                #if not os.rename(tempfn, r[3]):
                #    era5log.info(f'ERA5 moved to {r[3]}')
                #
                # update db
                # ...
            else:
                era5log.info(f'ERA5 nc command failed! (deleting temp file {tempfn})\n{err.decode()}')
                #if not ctx.obj['debug']:
                #    os.remove(tempfn)

        else:
            era5log.info(f'ERA5 download error: \n{err.decode()}')
            # should try to resume before deleting
            #os.remove(tempfn)
    return


def target(stream, var, yr, mn, dsargs):
    """Build output paths and filename, 
       build list of days to process based on year and month
    """
    global cfg
    # set output path
    stagedir = os.path.join(cfg['staging'],stream, var,yr)
    destdir = os.path.join(cfg['datadir'],stream,var,yr)
    # create path if required
    if not os.path.exists(stagedir):
            os.makedirs(stagedir)
    if not os.path.exists(destdir):
            os.makedirs(destdir)
    # define filename based on var, yr, mn and stream attributes
    startmn=mn
    daylist = define_dates(yr,mn) 
    fname = f"{var}_era5_{dsargs['grid']}_{yr}{startmn}{daylist[0]}_{yr}{mn}{daylist[-1]}.nc"
    return stagedir, destdir, fname, daylist


def api_request(update, oformat, stream, params, yr, mntlist):
    """ Build a list of CDSapi requests based on arguments
        Call do_request to submit them and start parallel download
        If download successful, compress file and move to era5/netcdf
    """
    global cfg, era5log 
    # create empty list to  store cdsapi requests
    rqlist = []
    # list of faster ips to alternate
    ips = ['110', '210']
    i = 0 
    # assign year and list of months
    if mntlist == []:  
        mntlist = ["%.2d" % i for i in range(1,13)]
    # retrieve stream arguments
    dsargs = define_args(stream)
    era5log.debug(f'Stream attributes: {dsargs}')
    # open db session
    clefdb = db_connect() 
    # define params to download
    if update and params == []:
        params = dsargs['params']
    # loop through params and months requested
    for varp in params:
        queue, var, cdsname =  define_var(clefdb, varp)
        # if grib code exists but cds name is not defined skip var and print warning
        if not queue:
            continue
    # build Copernicus requests for each month and submit it using cdsapi modified module
        for mn in mntlist:
            # for each output file build request and append to list
            stagedir, destdir, fname, daylist = target(stream, var, yr, mn, dsargs)
            rdict = build_dict(dsargs, yr, mn, cdsname, daylist, oformat)
            rqlist.append((dsargs['dsid'], rdict, os.path.join(stagedir,fname),
                           os.path.join(destdir, fname), ips[i % len(ips)])) 
            # progress index to alternate between ips
            i+=1
            era5log.info(f'Added request for {fname}')
        era5log.debug(f'{rqlist}')

    # parallel downloads
    pool = ThreadPool(cfg['nthreads'])
    results = pool.map(do_request, rqlist)
    ##results = pool.imap_unordered(do_request, rqlist)
    ##close the pool and wait for the work to finish
    pool.close()
    pool.join()
    era5log.info('--- Done ---')


@click.group()
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
def era5(debug):
    """
    Request and download ERA5 data from the climate copernicus
    server using the cdsapi module.
    """
    global cfg, era5log 
    era5log = config_log(debug)
    # read config file
    cfg = read_config()


def common_args(f):
    constraints = [
        #click.argument('query', nargs=-1),
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
def update(oformat, param, stream, year, month):
    """ 
    Update ERA5 variables, if regular monthly update 
    then passing only the stream argument will update
    all the variables listed in the stream.json file.
    \f
    Grid and other stream settings are in the stream.json file.
    """
    ####I'm separating this in update and download, so eventually update can check if no yr/mn passed or only yr passed which was the last month downloaded
    
    update = True
    #ctx.obj['format'] = oformat
    #ctx.obj['stream'] = stream
    #ctx.obj['params'] = list(param)
    #ctx.obj['year'] = year
    #ctx.obj['month'] = list(month)
    api_request(update, oformat, stream, list(param), year, list(month))


@era5.command()
@common_args
@click.option('--param', '-p', multiple=True, required=True,
             help="Grib code parameter for selected variable, pass as param.table i.e. 132.128")
def download(oformat, param, stream, year, month):
    """ 
    Download ERA5 variables, to be preferred 
    if adding a new variable,
    if month argument is not passed 
    then the entire year will be downloaded. 
    \f
    Grid and other stream settings are in the stream.json file.
    """
    update = False
    api_request(update, oformat, stream, list(param), year, list(month))


if __name__ == '__main__':
    era5()

