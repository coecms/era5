#!/bin/bash
#
# Wrapper script (called by cron)
#
# Usage: era5_wrapper.sh <priority>

# set some vars
LOCKFILE="/tmp/era5_wrapper.lock"
TSTAMP=$(date -u +%Y%m%dT%H%M%S)
SCRIPTDIR=$(dirname $0)
cd $SCRIPTDIR
ERRORLOG="/mnt/pvol/era5/log/era5_wrapper_error.log"
REQUESTDIR="/mnt/pvol/era5/Requests/${1}"
COMPLETEDIR="/mnt/pvol/era5/Requests/Completed"

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


# refresh requests
# couple of options: git pull; or rsync ; or nothing
echo "Checking for new requests, priority: $1"
REQUESTS="$(ls $REQUESTDIR/era5_request*.json)"

# loop through list of request files and run the download command
echo "Starting download ..."
for J in $REQUESTS ; do
  echo "  $J"
  ~/.local/bin/era5 scan -f $J 1>/dev/null 2>>$ERRORLOG
  echo " Finished , moving request $J"
  mv $J ${COMPLETEDIR}/$(basename "$J")
  #python3 cli.py scan -f $J
done

# update sqlite db
echo "Updating database ..."
~/.local/bin/era5 db 

echo "--- Done ---"
