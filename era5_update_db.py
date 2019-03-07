#!/usr/bin/env python
#
# update ERA5 db with files already found
#
from __future__ import print_function   # Py < 3
from glob import glob
import os
from datetime import datetime
import sqlite3
from era5_functions import read_config

def db_connect(cfg):
    """ connect to ERA5 files sqlite db
    """
    return sqlite3.connect(cfg['db'], timeout=10, isolation_level=None)

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
