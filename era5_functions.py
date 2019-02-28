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

import logging
import json
import cdsapi
from calendar import monthrange
from datetime import datetime
import os
import subprocess as sp


def config_log(debug):
    ''' configure log file to keep track of users queries '''
    # start a logger
    logger = logging.getLogger('era5log')
    # set a formatter to manage the output format of our handler
    formatter = logging.Formatter('%(levelname)s %(asctime)s; %(message)s',"%Y-%m-%d %H:%M:%S")
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
    # create a new log file every day 
    date = datetime.now().strftime("%Y%m%d") 
    logname = '/g/data/ub4/Work/Logs/ERA5/era5_log_' + date + '.txt' 
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
    try:
        with open('config.json','r') as fj:
            cfg = json.load(fj)
    except FileNotFoundError:
        print(f"Can't find file config.json in {os.getcwd()}")
        raise SystemExit() 
    return cfg


def define_dates(yr,mn):
    """ return a date range for each file depending on selected type """ 
    startday=1
    endday=monthrange(int(yr),int(mn))[1]
    daylist=["%.2d" % i for i in range(startday,endday+1)]    
    return daylist 


def define_var(vardict, varparam):
    """ Find grib code in vardict dictionary and return relevant info
    """
    queue = True
    try:
        name, cds_name = vardict[varparam]
    except:
       era5log.info(f'Selected parameter code {varparam} is not available')
       queue = False
       return queue, None, None
    return queue, name, cds_name


def define_args(stream):
    ''' Return parameters and levels lists and step, time depending on stream type'''
    # this import the stream_dict dictionary <stream> : ['time','step','params','levels']
    # I'm getting the information which is common to all pressure/surface/wave variables form here, plus a list of the variables we download for each stream
    with open(f'era5_{stream}.json', 'r') as fj:
        dsargs = json.load(fj)
    return  dsargs


def read_vars():
    """Read parameters info from era5_vars.json file
    """
    with open('era5_vars.json','r') as fj:
         vardict = json.load(fj)
    return vardict 


def file_exists(fn, nclist):
    """ check if file already exists
    """
    return fn in nclist 


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


def file_down(url, tempfn, size, era5log):
    """ Open process to download file
        If fails try tor esume at least once
        :return: success: true or false
    """
    cmd = f"{cfg['getcmd']} {tempfn} {url}"
    era5log.info(f'ERA5 Downloading: {url} to {tempfn}')
    p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
    out,err = p.communicate()
    # alternative check using actual file size
    # limited to number of retry set in config.json
    n = 0
    while os.path.getsize(tempfn) < size and n < cfg['retry']:
        cmd = f"{cfg['resumecmd']} {tempfn} {url}"
        era5log.info(f'ERA5 Resuming download {n+1}: {url} to {tempfn}')
        p1 = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out,err = p1.communicate()
        # need to add something to break cycle if file unavailable or at least limits reruns
        if not p1.returncode:            # successful
            return True
        #elif "unavailable":
            #return False
        else:
            n+=1
     # if there's always a returncode for all retry return false
    return False
    

def target(stream, var, yr, mn, dsargs):
    """Build output paths and filename, 
       build list of days to process based on year and month
    """
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

cfg = read_config()

