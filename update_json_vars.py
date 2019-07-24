#!/usr/bin/python
# Copyright 2019 ARC Centre of Excellence for Climate Extremes (CLEx)
# Author: Paola Petrelli <paola.petrelli@utas.edu.au> for CLEx 
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
# From clef db dump a list of all the ERA5 variables that can be downloaded from the Copernicus data server
# if a cds_name doesn't exist then variable is ignored 
# contact: paolap@utas.edu.au
# last updated 28/02/2019

import json
from clef.collections import connect, ECMWF, Variable

db = connect()
clefdb = db.session
vals = clefdb.query(ECMWF).filter(ECMWF.cds_name != "").all()
vars={}
#print(dir(vals[0]))
for v in vals:
    vars[v.code] = (v.name, v.cds_name)

#write in json file
with open(f'era5_vars.json', 'w') as fj:
    json.dump(vars, fj)

