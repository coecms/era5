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
import os
import pkg_resources
import subprocess as sp
from calendar import monthrange
from datetime import datetime


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
        cfg_file = pkg_resources.resource_filename(__name__, 'data/config.json')
        with open(cfg_file,'r') as fj:
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
    stream_file = pkg_resources.resource_filename(__name__, f'data/era5_{stream}_{tstep}.json')
    with open(stream_file, 'r') as fj:
        dsargs = json.load(fj)
    return  dsargs


def read_vars(stream):
    """Read parameters info from era5_vars.json file
    """
    if stream == 'fire':
        var_file = pkg_resources.resource_filename(__name__, 'data/era5_indices.json')
    else:
        var_file = pkg_resources.resource_filename(__name__, 'data/era5_vars.json')
    with open(var_file,'r') as fj:
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
    if 'version' in dsargs.keys():
        rdict['version'] = dsargs['version']
    if 'dataset' in dsargs.keys():
        rdict['dataset'] = dsargs['dataset']
    if dsargs['levels'] != []:
        rdict['pressure_level']= dsargs['levels']
    if tstep == 'mon':
        rdict['time'] = '00:00'
    elif tstep == 'day':
        rdict['day'] = daylist
    elif tstep == 'hr':
        rdict['day'] = daylist
        rdict['time'] = timelist
    # for pressure mon, fire and agro daily download a yera at the time
    # for surface monthly donwload all years fully available
    if back:
        rdict['month'] = ["%.2d" % i for i in range(1,13)]
        if dsargs['dsid'] == 'reanalysis-era5-land-monthly-means':
            rdict['year'] = ["%.2d" % i for i in range(1981,2019)]
        elif dsargs['dsid'] == 'reanalysis-era5-single-levels-monthly-means':
            rdict['year'] = ["%.2d" % i for i in range(1979,2020)]
    return rdict 

def build_mars(dsargs, yr, mn, param, oformat, tstep, back):
    """ Create request for MARS """
    datestr = f'{yr}-{mn}-01/to/{yr}-{mn}-{monthrange(int(yr),int(mn))[1]}'
    rdict={ 'param'    : param,
            'date': datestr,
            'levtype': 'pl',
            'stream': 'oper',
            'type': 'an',
            'grid' : '0.25/0.25',
            'format'      : oformat,
            'area'        : dsargs['area']} 
    if dsargs['levels'] != []:
        rdict['levelist']= dsargs['levels']
    #if tstep == 'mon':
    #    rdict['time'] = '00:00'
    #    if back:
    #        rdict['month'] = ["%.2d" % i for i in range(1,13)]
    #        if dsargs['dsid'] == 'reanalysis-era5-land-monthly-means':
    #            rdict['year'] = ["%.2d" % i for i in range(1981,2019)]
    #        elif dsargs['dsid'] == 'reanalysis-era5-single-levels-monthly-means':
    #            rdict['year'] = ["%.2d" % i for i in range(1979,2020)]
    #else:
    rdict['time'] = '00:00:00/01:00:00/02:00:00/03:00:00/04:00:00/05:00:00/06:00:00/07:00:00/08:00:00/09:00:00/10:00:00/11:00:00/12:00:00/13:00:00/14:00:00/15:00:00/16:00:00/17:00:00/18:00:00/19:00:00/20:00:00/21:00:00/22:00:00/23:00:00' 
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
    

def target(stream, var, yr, mn, dsargs, tstep, back, oformat):
    """Build output paths and filename, 
       build list of days to process based on year and month
    """
    # temporary fix to go from netcdf to nc
    if oformat == 'netcdf': oformat='nc'
    # did is era5land for land stream and era5 for anything else
    did = 'era5'
    if stream in ['land','fire','agro']:
        did+=stream
    # set output path
    ydir = yr
    if tstep in ['mon','day']:
        fname = f"{var}_{did}_{tstep}_{dsargs['grid']}_{yr}{mn}.{oformat}"
        if back:
            if stream == 'land':
                fname = f"{var}_{did}_{tstep}_{dsargs['grid']}_198101_201812.{oformat}"
            elif stream == 'pressure':
                fname = f"{var}_{did}_{tstep}_{dsargs['grid']}_{yr}01_{yr}12.{oformat}"
            elif stream in ['fire', 'agro']:
                fname = f"{var}_{did}_{tstep}_{dsargs['grid']}_{yr}0101_{yr}1231.{oformat}"
            else:
                fname = f"{var}_{did}_{tstep}_{dsargs['grid']}_197901_201912.{oformat}"
    else:
    # define filename based on var, yr, mn and stream attributes
        startmn=mn
        fname = f"{var}_{did}_{dsargs['grid']}_{yr}{startmn}{daylist[0]}_{yr}{mn}{daylist[-1]}.{oformat}"
    # if monthly data change ydir and create empty daylist
    if tstep == 'mon':
        daylist = []
        ydir = 'monthly'
    else:
        daylist = define_dates(yr,mn) 
    stagedir = os.path.join(cfg['staging'],stream, var,ydir)
    if tstep == 'day':
        destdir = os.path.join(cfg['datadir'],stream,var)
    else:
        destdir = os.path.join(cfg['datadir'],stream,var,ydir)
    # create path if required
    if not os.path.exists(stagedir):
            os.makedirs(stagedir)
    if not os.path.exists(destdir):
            os.makedirs(destdir)
    return stagedir, destdir, fname, daylist


def dump_args(of, st, ps, yr, mns, tstep, back):
    """ Create arguments dictionary and dump to json file
    """
    tstamp = datetime.now().strftime("%Y%m%d%H%M%S") 
    fname = f'era5_request_{tstamp}.json'
    args = {}
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
