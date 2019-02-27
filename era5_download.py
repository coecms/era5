#!/usr/bin/env python

from __future__ import print_function   # Py < 3
import os, sys
import calendar
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
#from multiprocessing import Pool as ThreadPool
import subprocess as sp
import sqlite3
#from io import StringIO
import cdsapi
from era5_config import cfg, cds

# make a list of existing files
# eg. glob
# get a list of already in db
conn = sqlite3.connect(cfg['db'], timeout=10, isolation_level=None)
with conn:
    c = conn.cursor()
    sql = 'select filename from file order by filename asc'
    c.execute(sql)
    nclist = [ x[0] for x in c.fetchall() ]


# but in meantime use a pre-generated one
#with open('era5_nc.list') as fin:
#    nclist = sorted( [ x.rstrip() for x in list(fin) ] )

# list of alternate ip address for downloading from
# the default server with ip = .198 is very slow
ipl = ['110', '210']

print('Generating CDSAPI requests ...')

# create a list of tuples containing the requests to be issued to cdsapi
rqlist = []
# loop thru each dataset
for ds in cds[1:2]:
#for ds in cds:
    # set suffix
    if ds['qry']['format'] == 'grib':
        sfx = fmtdir = 'grib'
    elif ds['qry']['format'] == 'netcdf':
        sfx = 'nc'; fmtdir = 'netcdf'

    # set geographic domain
    if 'area' in ds['qry']: domain = 'aus'
    else: domain = 'global'

    # find start and end year
    if 'time_range' in ds:
        startyr = int(ds['time_range'][:4])
        d2 = ds['time_range'].split('-')
        if not d2[1] == '':
            endyr = int(d2[1][:4])
        else:
            endyr = datetime.now().year
        yrs = [ str(x) for x in range(startyr, endyr + 1) ]
    elif 'year' in ds['qry']:
        yrs = ds['qry']['year']
    else:
        yrs = [ str(datetime.now().year) ]

    # find months
    if 'month' in ds['qry']:
        mths = ds['qry']['month']
    else:
        mths = [ '{:0>2}'.format(x) for x in range(1,13) ]

    # loop thru each variable
    for v,w in {'total_precipitation': 'TP'}.items():
    #for v,w in ds['var_list'].items():

        # loop thru each year
        #for y in yrs:
        for y in ['2003']:

            # set output path
            destdir = os.path.join(cfg['datadir'], fmtdir, ds['subdir'], w, y)
            stagedir = os.path.join(destdir, cfg['staging'])
            # create path if required
            if not os.path.exists(stagedir):
                os.makedirs(stagedir)

            # loop thru each month
            for m in mths:
            #for m in ['11']:

                # set start and end dates for the month
                startd = '{}{}01'.format(y, m)
                endd = '{}{}{}'.format(y, m, str(calendar.monthrange(int(y), int(m))[1]) )

                # set output file name
                fname = '{}_era5_{}_{}_{}.{}'.format(w, domain, startd, endd, sfx)
                destfname = os.path.join(destdir, fname)
                # check if file already exists
                if not destfname in nclist:
                    # update the qry(copy) and add to list
                    tqry = dict(ds['qry'])
                    tqry.update({'variable': v})
                    tqry.update({'year': y})
                    tqry.update({'month': m})
                    # randomise ip to use in download url
                    ip = ipl[len(rqlist) % len(ipl)]
                    rqlist.append( (ds['dataset'], tqry, destfname, ip) )
# end of dataset loop

print('Total requests: ', len(rqlist))

def get_timestamp():
    ts = datetime.now()
    return '{:%F %H:%M:%S},{:03}'.format(ts, ts.microsecond//1000)
#

def do_request(r):
    """
    Issue the download request. param 'r' is a tuple:
    [0] dataset name
    [1] the qry 
    [2] target filename
    [3] ip for download url

    Download to staging area first, compress netcdf (nccopy)
    """
    head, tail = os.path.split(r[2])
    tempfn = os.path.join(head, cfg['staging'], tail)
    
    # the actual retrieve part
    # create client instance
    c = cdsapi.Client()
    #print('Downloading {} ... '.format(tempfn))
    #print('Request: {}'.format(r[1]))
    # need to capture exceptions
    apirc = False
    try:
        # issue the request (to modified api)
        res = c.retrieve(r[0], r[1], tempfn)
        apirc = True
    except Exception as e:
        print ('ERROR: %s' % e) 
        apirc = False

    if apirc:
        # get download url and replace ip
        url = res.location
        if '.198/' in res.location:
            url = res.location.replace('.198/', '.{}/'.format(r[3])) 
        cmd = '{} {} {}'.format(cfg['getcmd'], tempfn, url)
        print('{} ERA5 Downloading: {} to {}'.format(get_timestamp(), url, tempfn))
        #return
        p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out,err = p.communicate()
        if not p.returncode:		# successful
            # do some compression on the file - assuming 1. it's netcdf, 2. that nccopy will fail if file is corrupt
            cmd = '{} {} {}'.format(cfg['nccmd'], tempfn, r[2])
            p1 = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
            out,err = p1.communicate()
            if not p1.returncode:       # check was successful
                print('{} ERA5 download success: {}'.format(get_timestamp(), tempfn))
                # move to correct destination
                #if not os.rename(tempfn, r[2]):
                #    print('{} ERA5 moved to {}'.format(get_timestamp(), r[2]))
                #
                # update db
                # ...
            else:
                print('{} ERA5 nc command failed! (deleting file {})\n{}'.format(get_timestamp(), tempfn, err.decode()))
                os.remove(tempfn)

        else:
            print('{} ERA5 download error: (file {})\n{}'.format(get_timestamp(), tempfn, err.decode()))
            os.remove(tempfn)
            # retry ???

#

# parallel downloads
pool = ThreadPool(cfg['nthreads'])
results = pool.map(do_request, rqlist)
#results = pool.imap_unordered(do_request, rqlist)
#close the pool and wait for the work to finish
pool.close()
pool.join()

print('--- Done ---')

### END ###
