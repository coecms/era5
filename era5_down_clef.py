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

from clef.collections import connect, ECMWF, Variable
from clef.update_collections import update_item
import cdsapi
import argparse
from calendar import monthrange
import pickle     # to load dictionaries saved in external files
import sys


def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description=r''' Download ERA5 netcdf files from the climate copernicus server using the cdsapi module.
    Output file format changes depending on selected stream, can download more than one month
    at one time but only one year. If month is not specified will default to entire year. User can 
    choose a start-end file number for model and pressure level streams, where a month is split in 
    6 files. ''', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-t','--type', help='ERAI type in the form stream_type_level', required=True)
    parser.add_argument('-y','--year', help='Year', type=str, required=True)
    parser.add_argument('-m','--month', type=str, nargs="*", help='Month, default all months', required=False)
    parser.add_argument('-p','--param', type=str, nargs=1, help='Param to download', required=True)
    parser.add_argument('-s','--start', type=int, 
                        help='start from file, default first file, works only for pressure and model levels that have 6 files',
                        default=1, choices=range(1,7), required=False)
    parser.add_argument('-e','--end', type=int,
                        help='end with file, default last file, works only for pressure and model levels that have 6 files',
                        choices=range(1,7), required=False)
    return vars(parser.parse_args())


def define_dates(yr,mn,start,end):
    ''' return a date range for each file depending on selected type '''
    startday=1
    endday=monthrange(int(yr),int(mn))[1]
    daylist=["%.2d" % i for i in range(startday,endday+1)]    
    return daylist 


def define_args(varparam):
    ''' Return parameters and levels lists and step, time depending on stream type'''
    global stype
    # this import the stream_dict dictionary <stream> : ['time','step','params','levels']
    # I'm getting the information which is common to all pressure/surface/wave variables form here, plus a list of the variables we download for each stream
    unpicklefile = open('/g/data1/ua8/Download/ERA5/era5_stream_pickle', 'rb')
    dset = pickle.load(unpicklefile)
    unpicklefile.close()
    dsargs=dset[stype]
    # this connect to clef collections db to get param info
    # I'm getting mostly the name to pass to the cdsapi from here, potentially more info if we were to get grib files and had to convert
    db = connect()
    clefdb = db.session
    vals = clefdb.query(ECMWF).filter(ECMWF.code == varparam).one_or_none()
    if vals is None:
       print('Selected parameter code is not available for ' + stype)
       sys.exit()
    print(vals.name,  ":  ", vals.cds_name)
    return dsargs,vals.name,vals.cds_name

def build_request(dsargs, yr, mn, variable, daylist):
    '''Builds request dictionary to pass to retrieve command''' 
    global stype
    timelist= ['00:00','01:00','02:00','03:00','04:00','05:00',
               '06:00','07:00','08:00','09:00','10:00','11:00',
               '12:00','13:00','14:00','15:00','16:00','17:00',
               '18:00','19:00','20:00','21:00','22:00','23:00']
    rdict={ 'product_type':'reanalysis',
            'variable'    : variable,
            'year'        : str(yr),
            'month'       : str(mn),
            'day'         : daylist,
            'time'       : timelist,
            'format'      : 'netcdf',
            'area'        : dsargs['area']} 
    if stype == 'pressure':
        rdict['pressure_level']= dsargs['levels']
    return rdict 

def do_request(r):
    """
    Issue the download request. param 'r' is a tuple:
    [0] dataset name
    [1] the qry
    [2] target filename
    [3] ip for download url

    Download to staging area first, compress netcdf (nccopy)
    """

    tempfn = os.path.join(stagedir,r[2]) 
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
        cmd = f'{cfg['getcmd']} {tempfn} {url}'
        #print('{} ERA5 Downloading: {} to {}'.format(get_timestamp(), url, tempfn))
        print(f'{get_timestamp()} ERA5 Downloading: {url} to {tempfn}')
        #return
        p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out,err = p.communicate()
        if not p.returncode:            # successful
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
                print('{} ERA5 nc command failed! (deleting temp file {})\n{}'.format(get_timestamp(), tempfn), err.decode())
                os.remove(tempfn)

        else:
            print('{} ERA5 download error: \n{}'.format(get_timestamp(), err.decode()))
            os.remove(tempfn)

    return

def fix_ip(ip,alt):
    ''' if ip adress is .198 then change to faster server ''' 
    # list of alternate ip address for downloading from
    # the default server with ip = .198 is very slow
    ipl = ['110', '210']
    return ip.replace('.198',ipl[alt])


def target():
     # set output path
    datadir = '/g/data/ub4/era5'
    stagedir = os.path.join(datadir,'staging',stream, var,yr)
    destdir = os.path.join(datadir,stream,var,yr)
    #head, tail = os.path.split(r[2])
    #tempfn = os.path.join(head, cfg['staging'], tail)
    # destdir = os.path.join(cfg['datadir'], fmtdir, ds['subdir'], w, y)
    # stagedir = os.path.join(destdir, cfg['staging'])
    # create path if required
    if not os.path.exists(stagedir):
            os.makedirs(stagedir)
    return stagedir, destdir

def main():
    global stype
    # parse input arguments
    args=parse_input()
    stype = args["type"]
    varparam = args["param"][0]
    yr = args['year']
    # assign time, step, param and levelist values for server request based on stream
    dsargs,varname,cdsname =  define_args(varparam)
    # assign year and list of months
    mntlist = args["month"]
    if mntlist is None:  mntlist = ["01","02","03","04","05","06","07","08","09","10","11","12"]
    # assign arguments start and end to subset pressure and model stream file list if available
    start = args["start"]
    end = args["end"]

    # build Copernicus requests for each month and submit it using cdsapi modified module

    for mn in mntlist:
        print( yr, mn)
        startmn=mn
        daylist = define_dates(yr,mn,start,end) 
        # for each output file build filename and submit request
        targetstr = varname+ "_era5_"+ dsargs['grid'] + "_" +  yr + startmn + daylist[0] + "_" + yr + mn + daylist[-1] + ".nc"
        print(targetstr)
        rdict = build_request(dsargs, yr, mn, cdsname, daylist)
        try:
            #print(rdict)
            c = cdsapi.Client()
            c.retrieve(dsargs['dsid'], rdict, targetstr)
        except RuntimeError:
            print("Runtime error")

    # parallel downloads
    pool = ThreadPool(cfg['nthreads'])
    results = pool.map(do_request, rqlist)
    #results = pool.imap_unordered(do_request, rqlist)
    #close the pool and wait for the work to finish
    pool.close()
    pool.join()

    print('--- Done ---')

if __name__ == "__main__":
    main()
