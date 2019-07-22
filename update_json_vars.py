# From clef db dump a list of all the ERA5 variables that can be downloaded from the Copernicus data server
# if a cds_name doesn't exist then variable is ignored 
# Author: Paola Petrelli for ARC Centre of Excellence for Climate Extremes
# contact: paolap@utas.edu.au
# last updated 28/02/2019
#!/usr/bin/python

import json
from clef.collections import connect, ECMWF, Variable

db = connect()
clefdb = db.session
vals = clefdb.query(ECMWF).filter(ECMWF.cds_name != "").all()
vars={}
for v in vals:
    vars[v.code] = (v.name, v.cds_name)

#write in json file
with open(f'era5_vars.json', 'w') as fj:
    json.dump(vars, fj)

