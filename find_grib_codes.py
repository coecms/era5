# code that helps finding all the grib codes of a list of variables export from CDS website

import sys
import json
from era5_functions import read_vars

# read era5_vars.json into dict
vardict = read_vars()
# read list of variables to donwload from file
fname = sys.argv[1]
with open(fname,'r') as fj:
    data = json.load(fj)
variables = data['variables']

# loop through variables in era5_vars and print grib code for the ones which are included in your list
# NB this is not checking if all the variables in the file are in era5_vars.json
allvars=[]
for k,v in vardict.items():
    allvars.append(v[1])
    if v[1] in variables:
        print(k)

# This checks if variable not in era5_vars.json and print name of missing ones
for v in variables:
    if v not in allvars:
        print(v)


