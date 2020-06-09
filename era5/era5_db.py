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
# Crawl era5 netcdf directories and update ERA db with new files found
# contact: paolap@utas.edu.au
# last updated 28/04/2020

from glob import glob
import os
from datetime import datetime
import sqlite3
#from era5.era5_functions import read_config


def db_connect(cfg):
    """ connect to ERA5 files sqlite db
    """
    return sqlite3.connect(cfg['db'], timeout=10, isolation_level=None)


def create_table(conn):
    """ create file table if database doesn't exists or empty
        :conn  connection object
    """
    file_sql = 'CREATE TABLE IF NOT EXISTS file( filename TEXT PRIMARY KEY, location TEXT, ncidate TEXT, size INT);' 
    try:
        c = conn.cursor()
        c.execute(file_sql)
    except Error as e:
        print(e)


def query(conn, sql, tup):
    """ generic query
    """
    with conn:
        c = conn.cursor()
        c.execute(sql, tup)
        return [ x[0] for x in c.fetchall() ]


def crawl(g, xl, basedir):
    """ Crawl base directory for all netcdf files
    """
    # get stats for all files not in db yet
    tsfmt = '%FT%T'
    file_list = []
    for f in g:
        dn, fn = os.path.split(f)
        if not fn in xl:
            s = os.stat(f)
            l = dn.replace(basedir + '/', '')
            ts = datetime.fromtimestamp(s.st_mtime).strftime(tsfmt)
            file_list.append( (fn, l, ts, s.st_size) )
    return file_list


def build_match(stream, var, tstep):
    """ Build matching expression based on stream and timestep
    """
    if tstep == 'hr':
        match=f"{stream}/{var}/____"
    elif tstep == 'mon':
        match=f"{stream}/{var}/monthly"
    return match


def get_basedir(stream):
    """ Return base directory base don stream
    """
    if stream in ['cems_fire', 'agera5', 'wfde5']:
        return cfg['derivdir'] 
    else:
        return cfg['datadir'] 


def list_files(basedir, match):
    """ List all matching files for given base dir, stream and tstep
    """
    d = os.path.join(basedir, match)
    print(f'Searching: {d} ...')
    g = glob(d)
    print(f'Found {len(g)} files.')
    file_list = crawl(g,xl,basedir)
    return file_list


def exp_files(stream,tstep):
    """ Return expected number of files to date for stream/tstep
    """
    #wfde5 and agera5 have fixed lengths
    if stream in ['wfde5', 'agera5']:
        return 40
    # assume start year as 1979 and no delay
    delay = 0
    start_yr = 1979
    # work out current month and delay from date, after 15th of month we should have downloaded last month unless delay
    today = datetime.today().strftime('%Y-%m-%d')
    yr, mn, day = today.split('-')
    end_yr = int(yr)
    if int(day) <= 15:
        end_mn = int(mn) - 2
    else:
        end_mn = int(mn) - 1
    # land starts currently from 1981
    if stream in ['land']:
        start_yr = 1981
    # cems_fire, land and all the monthly are delayed an extra month
    if stream in ['cems_fire', 'land'] or tstep == 'mon':
        delay = -1
    if tstep == 'hr':
        # should have 12 files x year + monthly for current year
        nfiles = (end_yr - start_yr)*12 + end_mn + delay
    elif tstep == 'mon':
        # should have in total 1 file for past years + 1 for each month
        nfiles = 1 + end_mn + delay
    else:
        # should have 1 file x year + monthly for current year
        nfiles = end_yr - start_yr + end_mn + delay
    return nfiles


def compare(basedir, match, var):
    """
    """
    # get a list of matching files on filesystem
    match2 = match.replace("____","????")
    # replace with list_files
    d = os.path.join(basedir,match2)
    print(f'Searching: {d} ...')
    g = glob(d)
    total = len(g)
    print(f'Found {total} files for {var}.')
    # up to here
    if nfiles == total:
        print(f'All expected files are present for {var}')
    elif nfiles > total:
        print(f'{nfiles-total} files are missing for {var}')
    else:
        print(f'{total-nfiles} extra files than expected for {var}')
    # get a list of matching files in db
    sql = f"SELECT filename FROM file AS t WHERE t.location LIKE '{match}' ORDER BY filename ASC"
    xl = query(conn, sql, ())
    print(f'Records already in db: {len(xl)}')


def update_db(cfg, stream, tstep):
    # read configuration and open ERA5 files database
    conn = db_connect(cfg)
    create_table(conn)

    # get a list of files already in db

    # List all netcdf files in datadir and derivdir
    if not stream:
        sql = 'SELECT filename FROM FILE ORDER BY filename ASC'
        xl = query(conn, sql, ())
        print(f'Records already in db: {len(xl)}')
        filelist = list_files(cfg['datadir'],'*/*/*/*.nc')
        filelist.extend( list_files(cfg['derivdir'],'*/*/*.nc') )
        print(f'New files found: {len(file_list)}')
    else:
        match = build_match(stream, '*', tstep)
        basedir = get_basedir(stream)
        filelist = list_files(basedir, match)
        print(f'New files found: {len(file_list)}')

    # insert into db
    if len(file_list) > 0:
        print('Updating db ...')
        with conn:
            c = conn.cursor()
            sql = 'INSERT OR IGNORE INTO file (filename, location, ncidate, size) values (?,?,?,?)'
            #debug(sql)
            c.executemany(sql, file_list)
            c.execute('select total_changes()')
            print('Rows modified:', c.fetchall()[0][0])
    print('--- Done ---')


def variables_stats(cfg,stream,tstep,varlist):
    """
    """
    # read configuration and open ERA5 files database
    conn = db_connect(cfg)

    # If no variable is selected list all variables in stream json file
    dsargs = define_args(stream, tstep)
    if not any(varlist):
        #DELETE?
        varlist=[]
        vardict = read_vars(stream)
        print('Variables currently updated for this stream are:\n')
        for code in dsargs['params']:
            print(f'{vardict[code][0]} - {vardict[code][1]} - {code}')
            varlist.append(vardict[code][0])
    # calculate expected number of files
    nfiles = exp_files(stream, tstep)
    basedir = get_basedir(stream)

    for var in varlist:
        match = build_match(stream, var, tstep)
        compare(basedir, match, var)
