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
# last updated 22/07/2019

import sqlite3
import argparse
import os

from glob import glob
from datetime import datetime

from era5.era5_functions import read_config


def parse():
    '''Parse input arguments
    '''
    parser = argparse.ArgumentParser(description=r'''Select and delete records from era5.sqlite database
    ''', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-s','--stream', choices=['surface','pressure','land','wave'],
                        help='Stream: wave, surface, pressure, land', required=True)
    parser.add_argument('-y','--year', type=str, help='Year', required=True)
    parser.add_argument('-m','--month', type=str, help='Month', required=True)
    parser.add_argument('-v','--variable', type=str, help='Variable to select', required=True)
    parser.add_argument('-t','--timestep', type=str, choices=['mon','hr'], default='hr',
                        help='Timestep hr or mon, default is hr', required=False)
    return vars(parser.parse_args())



def db_connect(cfg):
    """Connect to ERA5 files sqlite db
    """
    return sqlite3.connect(cfg['db'], timeout=10, isolation_level=None)
    #return sqlite3.connect('copy.sqlite', timeout=10, isolation_level=None)

def query(conn, sql, tup):
    """Generic query
    """
    with conn:
        c = conn.cursor()
        c.execute(sql, tup)
        return [ x[0] for x in c.fetchall() ]

def main():
    # read configuration and open ERA5 files database
    cfg = read_config()
    conn = db_connect(cfg)

    # Input all files you want to delete can be glob style

    # Set up query 
    args = parse()
    st = args['stream']
    var = args['variable'] 
    yr = args['year'] 
    mn = args['month'] 
    tstep = args['timestep'] 
    if tstep == 'hr':
        fname = f'{var}_era5_%_{yr}{mn}01_%.nc'
        location = f'{st}/{var}/{yr}'
    else:
        fname = f'{var}_era5_mon_%_{yr}{mn}.nc'
        location = f'{st}/{var}/monthly'
    sql = f'select filename from file where file.location == "{location}" and file.filename like "{fname}"'
    print(sql)
    xl = query(conn, sql, ())
    print(f'Selected records in db: {xl}')

    # Delete from db
    if len(xl) > 0:
        print('Updating db ...')
        for fname in xl:
           with conn:
               c = conn.cursor()
               sql = f'DELETE from file where filename="{fname}" AND location="{location}"'
               print(sql)
               c.execute(sql)
               c.execute('select total_changes()')
               print('Rows modified:', c.fetchall()[0][0])

    print('--- Done ---')

if __name__ == '__main__':
    main()
