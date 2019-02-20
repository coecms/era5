#!/usr/bin/env python

from __future__ import print_function   # Py < 3
import os, sys
import calendar
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
#from multiprocessing import Pool as ThreadPool
import subprocess as sp
#from io import StringIO
import cdsapi
from era5_config import cfg, cds

# make a list of existing files
# eg. glob
# but in meantime use a pre-generated one
with open('era5_nc.list') as fin:
    nclist = sorted( [ x.rstrip() for x in list(fin) ] )

# create a list of tuples containing the requests to be issued to cdsapi
rqlist = []

# list of alternate ip address for downloading from
# the default server with ip = .198 is very slow
ipl = ['110', '210']

print('Generating CDSAPI requests ...')

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
        for y in ['2005', '2004']:

            # set output path
            destdir = os.path.join(cfg['datadir'], fmtdir, ds['subdir'], w, y)
            # create path if required
            if not os.path.exists(destdir):
                os.makedirs(destdir)

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

    Download to staging area first, do some checks (gdal ?) before moving to destination
    """
    tempfn = os.path.join(cfg['staging'], os.path.basename(r[2]))
    
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
            # do some QC on the file eg. gdalinfo tempfn
            cmd = '{} {}'.format(cfg['qccmd'], tempfn)
            p1 = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
            out,err = p1.communicate()
            if not p1.returncode:       # check was successful
                print('{} ERA5 Download and QC success: {}'.format(get_timestamp(), tempfn))
                # rewrite nc file with compression
                # ???
                # move to correct destination
                if not os.rename(tempfn, r[2]):
                    print('{} ERA5 moved to {}'.format(get_timestamp(), r[2]))
            else:
                print('{} ERA5 QC Failed! (deleting temp file {})'.format(get_timestamp(), tempfn))
                os.remove(tempfn)

        else:
            print('{} ERA5 Error: \n{}'.format(get_timestamp(), err.decode()))
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
