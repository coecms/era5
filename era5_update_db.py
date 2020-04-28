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
from era5_functions import read_config

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

def main():
    # read configuration and open ERA5 files database
    cfg = read_config()
    conn = db_connect(cfg)
    create_table(conn)

    # List all netcdf files in datadir
    d = os.path.join(cfg['datadir'], '*/*/*/*.nc')
    print(f'Searching: {d} ...')
    g = glob(d)
    print(f'Found {len(g)} files.')

    # get a list of files already in db
    sql = 'select filename from file order by filename asc'
    xl = query(conn, sql, ())
    print(f'Records already in db: {len(xl)}')

    # get stats for all files not in db yet
    tsfmt = '%FT%T'
    fl = []
    for f in g:
        dn, fn = os.path.split(f)
        if not fn in xl:
            s = os.stat(f)
            l = dn.replace(cfg['datadir'] + '/', '')
            ts = datetime.fromtimestamp(s.st_mtime).strftime(tsfmt)
            fl.append( (fn, l, ts, s.st_size) )

    print(f'New files found: {len(fl)}')

    # insert into db
    if len(fl) > 0:
        print('Updating db ...')
        with conn:
            c = conn.cursor()
        #sql = 'insert or replace into file (filename, location, ncidate, size) values (?,?,?,?)'
            sql = 'insert or ignore into file (filename, location, ncidate, size) values (?,?,?,?)'
        #print(sql)
            c.executemany(sql, fl)
            c.execute('select total_changes()')
            print('Rows modified:', c.fetchall()[0][0])

    print('--- Done ---')

if __name__ == '__main__':
    main()
