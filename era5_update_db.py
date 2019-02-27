#!/usr/bin/env python
#
# update ERA5 db with files already found
#
from __future__ import print_function   # Py < 3
from glob import glob
import os
from datetime import datetime
import sqlite3
from era5_config import cfg

datadir = '/g/data1a/ub4/era5'
# /g/data/ub4/era5/netcdf/surface/TP/2018/
#d = os.path.join(cfg['datadir'], 'netcdf/*/*/uncompressed/*/*.nc')
d = os.path.join(datadir, 'netcdf/*/*/????/*.nc')
print('Searching: {} ...'.format(d))
#exit()

g = glob(d)
print('Found {} files.'.format(len(g)))

# get a list of already in db
conn = sqlite3.connect(cfg['db'], timeout=10, isolation_level=None)
with conn:
    c = conn.cursor()
    sql = 'select filename from file order by filename asc'
    c.execute(sql)
    xl = [ x[0] for x in c.fetchall() ]

print('Records already in db: {}'.format(len(xl)))

tsfmt = '%FT%T'
fl = []
#for f in g[:10]:
for f in g:
    if not 'uncompressed' in f:
        dn, fn = os.path.split(f)
        if not fn in xl:
            s = os.stat(f)
            l = dn.replace(datadir + '/', '')
            ts = datetime.fromtimestamp(s.st_mtime).strftime(tsfmt)
            fl.append( (fn, l, ts, s.st_size) )

print('New files found: {}'.format(len(fl)))

# insert into db
if len(fl) > 0:
    print('Updating db ...')
    #conn = sqlite3.connect(cfg['db'], timeout=10, isolation_level=None)
    with conn:
        c = conn.cursor()
        #sql = 'insert or replace into file (filename, location, ncidate, size) values (?,?,?,?)'
        sql = 'insert or ignore into file (filename, location, ncidate, size) values (?,?,?,?)'
        #print(sql)
        c.executemany(sql, fl)
        c.execute('select total_changes()')
        print('Rows modified:', c.fetchall()[0][0])

print('--- Done ---')

