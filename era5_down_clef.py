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
# Author: Paola Petrelli for ARC Centre of Excellence for Climate System Science
# contact: paolap@utas.edu.au
# last updated 20/09/2018
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

    # build Copernicus requests for each month and submit it using cdsap module

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


if __name__ == "__main__":
    main()
