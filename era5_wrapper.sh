#!/bin/bash
#
# Wrapper script (called by cron)
#
# Usage: era5_wrapper.sh

# set some vars
LOCKFILE="/tmp/era5_wrapper.lock"
TSTAMP=$(date -u +%Y%m%dT%H%M%S)
#ERRORLOG="/g/data/ub4/Work/Logs/ERA5/era5_wrapper_error.log"
SCRIPTDIR=$(dirname $0)
cd $SCRIPTDIR
ERRORLOG="../log/ERA5/era5_wrapper_error.log"
REQUESTDIR="../requests"

echo "--- Starting $0 ($TSTAMP) ---"

# set exclusive lock
echo "Setting lock ..."
LOCKED="NO"
exec 8>$LOCKFILE
#flock -nx 8 || exit 1
flock -nx 8 || LOCKED="YES"
if [ "$LOCKED" == "YES" ] ; then
  echo "  already locked - exiting!"
  exit 1
fi

# set the environment, load required modules
echo "Loading modules ..."
module load python3/3.6.2 netcdf
export LANG=en_AU.utf8
export LC_ALL=$LANG

# refresh requests
# couple of options: git pull; or rsync ; or nothing
echo "Checking for new requests ..."
REQUESTS="$(ls $REQUESTDIR/era5_request*.json)"

# loop through list of request files and run the download command
echo "Starting download ..."
for J in $REQUESTS ; do
  echo "  $J"
  python3 cli.py scan -f $J 1>/dev/null 2>>$ERRORLOG
  #python3 cli.py scan -f $J
done

# update sqlite db
echo "Updating database ..."
python3 era5_update_db.py

echo "--- Done ---"
