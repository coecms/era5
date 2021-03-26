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

import os
import sqlite3
from datetime import datetime
from glob import glob
from itertools import repeat
from era5.era5_functions import define_args, read_vars
from calendar import monthrange 
import sys


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


def set_query(st, var, tstep, yr='____', mn='%'):
    """ Set up the sql query based on constraints
    """
    if st in ['wfde5','cems_fire','agera5']:
        fname = f'{var}_%_{yr}{mn}01_%.nc'
        location = f'{st}/{var}'
    elif tstep == 'hr':
        fname = f'{var}_%_{yr}{mn}01_%.nc'
        location = f'{st}/{var}/{yr}'
    else:
        fname = f'{var}_%_mon_%_{yr}{mn}.nc'
        location = f'{st}/{var}/monthly'
    return fname, location


def get_basedir(cfg, stream):
    """ Return base directory base don stream
    """
    if stream in ['cems_fire', 'agera5', 'wfde5']:
        return cfg['derivdir'] 
    else:
        return cfg['datadir'] 


def list_files(basedir, match):
    """ List all matching files for given base dir, stream and tstep
    """
    fsmatch = match.replace("____", "????")
    fsmatch = fsmatch.replace("%", "*")
    d = os.path.join(basedir, fsmatch, "*.nc")
    print(f'Searching on filesystem: {d} ...')
    g = glob(d)
    print(f'Found {len(g)} files.')
    return g 


def exp_files(stream, tstep):
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
    # return the actual end_mn and end_yr corrected by delay
    end_mn = end_mn + delay
    if end_mn <=0:
        end_mn = 12 + end_mn
        end_yr = end_yr - 1
    return nfiles, start_yr, end_yr, end_mn


def exp_names(stream, var, tstep, yr, end_mn):
    """Return list of files expected based on configuration files and current date
    """
    #load ds args for stream, time step
    dsargs = define_args(stream, tstep)
    if stream == 'land':
        did = 'era5land'
    else:
        did = 'era5'
    flist = []
    # define filename based on var, yr, mn and stream attributes
    for mn in [str(i).zfill(2) for i in range(1,end_mn+1)]:
        #if tstep == 'mon':
        #    flist.append(os.path.join(stream,var,'monthly',f"{var}_{did}_{dsargs['grid']}_{yr}{mn}01.nc"))
        #else:
            last = monthrange(int(yr),int(mn))[1]
            flist.append(os.path.join(stream,var,yr,f"{var}_{did}_{dsargs['grid']}_{yr}{mn}01_{yr}{mn}{last}.nc"))
    return flist


def find_names(fs_list, basedir, stream, var, tstep, start_yr, end_yr, end_mn, difference):
    """Find out exactly which files are missing or extra and print the list.
    """
    if tstep == 'mon' or stream in ['wdfe5', 'agera5', 'cesmfire']:
        print('Not yet working for monthly data and derived products')
        return
    exp_list = []
    # loop over each year and add to list all expected filenames for that year
    for yr in [str(i) for i in range(start_yr, end_yr+1)]: 
        last_mn = 12
        if yr == str(end_yr):
            last_mn = end_mn
        exp_list.extend(exp_names(stream, var, tstep, yr, last_mn))
    # loop over files returned by filesystem crawl, if they are not in expected filenames list print filename 
    if difference == 'extra':
        print("\nThese files are extra:\n")
        for f in fs_list:
            if f not in exp_list:
                print(basedir + '/' + f)
        print()
    # loop over expected files list, if they are not in filesystem crawl list print filename 
    elif difference == 'missing':
        print("\nThese files are missing:\n")
        for f in exp_list:
            if f not in fs_list:
                print(basedir + '/' + f)
        print()
    else:
        print(f"Unrecognised option for difference: {difference}, should be `extra` or `missing`")
    return


def compare(conn, basedir, match, var, nfiles):
    """Get list of files on database for selected stream/timestep and compare to the number of expected files.
    If there are missing or extra files in database and verbose is selected, print a list of missing and/or extra files
    """
    # get a list of matching files on filesystem
    flist = list_files(basedir, match)
    fs_list = [f.replace(basedir+'/','') for f in flist]
    total = len(fs_list)
    # Print out comparisons with expected number of files
    if nfiles == total:
        print(f'All expected files are present for {var}\n')
    elif nfiles > total:
        print(f'{nfiles-total} files are missing for {var}\n')
        difference = 'missing' 
    else:
        print(f'{total-nfiles} extra files than expected for {var}\n')
        difference = 'extra' 
    # get a list of matching files in db
    sql = f"SELECT filename FROM file AS t WHERE t.location LIKE '{match}' ORDER BY filename ASC"
    xl = query(conn, sql, ())
    print(f'Records already in db: {len(xl)}')
    return fs_list, xl, difference


def update_db(cfg, stream, tstep, var):
    # read configuration and open ERA5 files database
    conn = db_connect(cfg)
    create_table(conn)

    # List all netcdf files in datadir and derivdir
    if not stream:
        sql = 'SELECT filename FROM FILE ORDER BY filename ASC'
        fs_list = list_files(cfg['datadir'],'*/*/*')
        fs_list2 = list_files(cfg['derivdir'],'*/*')
    else:
        fs_list = []
        if not var:
            var=['%']
        for v in var:
            fname, location = set_query(stream, v, tstep)
            basedir = get_basedir(cfg, stream)
            sql = f"SELECT filename FROM file AS t WHERE t.location LIKE '{location}' ORDER BY filename ASC"
            fs_list.extend(list_files(basedir, location))

    xl = query(conn, sql, ())
    if not stream:
        stats_list = crawl(fs_list, xl, cfg['datadir'])
        stats_list.extend( crawl(fs_list2, xl, cfg['derivdir']))
        fs_list.extend(fs_list2)
    else:
        stats_list = crawl(fs_list, xl, basedir)
    print(f'Records already in db: {len(xl)}')
    print(f'New files found: {len(fs_list)-len(xl)}')
    # insert into db
    if len(stats_list) > 0:
        print('Updating db ...')
        with conn:
            c = conn.cursor()
            sql = 'INSERT OR IGNORE INTO file (filename, location, ncidate, size) values (?,?,?,?)'
            #debug(sql)
            c.executemany(sql, stats_list)
            c.execute('select total_changes()')
            print('Rows modified:', c.fetchall()[0][0])
    print('--- Done ---')


def variables_stats(cfg, stream, tstep, varlist=[], verbose=False):
    """
    """
    # read configuration and open ERA5 files database
    conn = db_connect(cfg)

    # If no variable is selected list all variables in stream json file
    dsargs = define_args(stream, tstep)
    if not any(varlist):
        varlist=[]
        vardict = read_vars(stream)
        print('Variables currently updated for this stream are:\n')
        for code in dsargs['params']:
            print(f'{vardict[code][0]}   -   {vardict[code][1]}   -   {code}')
            varlist.append(vardict[code][0])
    # calculate expected number of files
    nfiles, start_yr, end_yr, end_mn = exp_files(stream, tstep)
    basedir = get_basedir(cfg, stream)

    for var in varlist:
        fname, location = set_query(stream, var, tstep)
        fs_list, xl, difference = compare(conn, basedir, location, var, nfiles)
        if verbose and difference:
            find_names(fs_list, basedir, stream, var, tstep, start_yr, end_yr, end_mn, difference)


def delete_record(cfg, st, var, yr, mn, tstep):
    # connect to db
    conn = db_connect(cfg)
    mn, yr, var = tuple(['%'] if not x else x for x in [mn, yr, var] ) 

    # Set up query
    for v in var:
        for y in yr:
            for m in mn:
                fname, location = set_query(st, v, tstep, y, m)
                sql = f'SELECT filename FROM file WHERE file.location="{location}" AND file.filename LIKE "{fname}"'
                print(sql)
                xl = query(conn, sql, ())
                print(f'Selected records in db: {xl}')
    # Delete from db
                if len(xl) > 0:
                    confirm = input('Confirm deletion from database: Y/N   ')
                    if confirm == 'Y':
                        print('Updating db ...')
                        for fname in xl:
                            with conn:
                                c = conn.cursor()
                                sql = f'DELETE from file where filename="{fname}" AND location="{location}"'
                                c.execute(sql)
                                c.execute('select total_changes()')
                                print('Rows modified:', c.fetchall()[0][0])
    return
