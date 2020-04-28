#!/usr/bin/python
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
# This file contains functions called by the main cli.py program
# contact: paolap@utas.edu.au
# last updated 22/07/2019

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
    # the messages will be appended to the same file
    # create a new log file every day 
    date = datetime.now().strftime("%Y%m%d") 
    #logname = '/g/data/ub4/Work/Logs/ERA5/era5_log_' + date + '.txt' 
    logname = cfg['logdir'] + '/era5_log_' + date + '.txt' 
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


def define_var(vardict, varparam, era5log):
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


def define_args(stream, tstep):
    ''' Return parameters and levels lists and step, time depending on stream type'''
    # this import the stream_dict dictionary <stream> : ['time','step','params','levels']
    # I'm getting the information which is common to all pressure/surface/wave variables form here, plus a list of the variables we download for each stream
    with open(f'era5_{stream}_{tstep}.json', 'r') as fj:
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


def build_dict(dsargs, yr, mn, var, daylist, oformat, tstep, back):
    """Builds request dictionary to pass to retrieve command 
    """
    timelist = ["%.2d:00" % i for i in range(24)]
    rdict={ 'variable'    : var,
            'year'        : str(yr),
            'month'       : str(mn),
            'format'      : oformat,
            'area'        : dsargs['area']} 
    if 'product_type' in dsargs.keys():
        rdict['product_type'] = dsargs['product_type']
    if dsargs['levels'] != []:
        rdict['pressure_level']= dsargs['levels']
    if tstep == 'mon':
        rdict['time'] = '00:00'
        if back:
            rdict['month'] = ["%.2d" % i for i in range(1,13)]
            if dsargs['dsid'] == 'reanalysis-era5-land-monthly-means':
                rdict['year'] = ["%.2d" % i for i in range(1981,2019)]
            elif dsargs['dsid'] == 'reanalysis-era5-single-levels-monthly-means':
                rdict['year'] = ["%.2d" % i for i in range(1979,2020)]
    else:
        rdict['day'] = daylist
        rdict['time'] = timelist
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
    if os.path.getsize(tempfn) == size:
        return True
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
    

def target(stream, var, yr, mn, dsargs, tstep, back):
    """Build output paths and filename, 
       build list of days to process based on year and month
    """
    # did is era5land for land stream and era5 for anything else
    did = 'era5'
    if stream == 'land':
        did+='land'
    # set output path
    if tstep == 'mon':
        ydir = 'monthly'
        fname = f"{var}_{did}_mon_{dsargs['grid']}_{yr}{mn}.nc"
        daylist = []
        if back:
            if stream == 'land':
                fname = f"{var}_{did}_mon_{dsargs['grid']}_198101_201812.nc"
            if stream == 'pressure':
                fname = f"{var}_{did}_mon_{dsargs['grid']}_{yr}01_{yr}12.nc"
            else:
                fname = f"{var}_{did}_mon_{dsargs['grid']}_197901_201912.nc"
    else:
        ydir = yr
    # define filename based on var, yr, mn and stream attributes
        startmn=mn
        daylist = define_dates(yr,mn) 
        fname = f"{var}_{did}_{dsargs['grid']}_{yr}{startmn}{daylist[0]}_{yr}{mn}{daylist[-1]}.nc"
    stagedir = os.path.join(cfg['staging'],stream, var,ydir)
    destdir = os.path.join(cfg['datadir'],stream,var,ydir)
    # create path if required
    if not os.path.exists(stagedir):
            os.makedirs(stagedir)
    if not os.path.exists(destdir):
            os.makedirs(destdir)
    return stagedir, destdir, fname, daylist


def dump_args(up, of, st, ps, yr, mns, tstep, back):
    """ Create arguments dictionary and dump to json file
    """
    tstamp = datetime.now().strftime("%Y%m%d%H%M%S") 
    fname = f'era5_request_{tstamp}.json'
    args = {}
    args['update'] = up
    args['format'] = of
    args['stream'] = st
    args['params'] = ps
    args['year'] = yr
    args['months'] = mns
    args['timestep'] = tstep
    args['back'] = back
    with open(fname, 'w+') as fj:
         json.dump(args, fj)
    return


cfg = read_config()
