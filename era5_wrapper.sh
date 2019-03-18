#!/bin/bash
#
# Wrapper script (called by cron)
#
# Usage: era5_wrapper.sh

# set some vars
LOCKFILE="/tmp/era5_wrapper.lock"
TSTAMP=$(date -u +%Y%m%dT%H%M%S)
cd $HOME/ERA5
REQUESTDIR="./requests-matt"

echo "--- Starting $0 ($TSTAMP) ---"

# set exclusive lock
echo "Setting lock ..."
exec 8>$LOCKFILE
flock -nx 8 || echo "  already locked - exiting!"; exit 1

# set the environment, load required modules
echo "Loading modules ..."
module load python3/3.6.2 netcdf
export LANG=en_AU.utf8
export LC_ALL=$LANG

# refresh repo 
# couple of options: git pull; or rsync ; or nothing
echo "Refreshing repo ..."

# loop through list of request files and run the download command
echo "Starting download ..."
REQUESTS="$(ls $REQUESTDIR/era5_request*.json)"
for J in $REQUESTS ; do
  echo "  $J"
  python3 cli.py scan -f $J 1>logs/era5_error.log 2>&1
  #python3 cli.py scan -f $J
done

# update sqlite db
echo "Updating database ..."
python3 era5_update_db.py

echo "--- Done ---"
