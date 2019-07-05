#!/bin/bash
#
# sing.sh: utility script for interacting with singularity
#

Usage () {
  cat <<EoT
---  
Usage:
  sing.sh list
  sing.sh start image_file instance_name
  sing.sh stop instance_name
  sing.sh exec [instance://instance_name | image file] command
  sing.sh shell [instance://instance_name | image_file]

---
EoT
  exit 1
}

if [ $# -lt 1 -o "$1" = "help" ] ; then 
  Usage
fi

HOME_DIR=$(pwd)
SING_BIN=$(which singularity)
BINDS="-H $HOME_DIR -B /g/data/id28 -B /g/data/ub4"

### MAIN ###

if [ $1 == "list" ] ; then
  eval "$SING_BIN instance.list"
elif [ $1 == "start" ] ; then
  if [ "x$2" == "x" -o "x$3" == "x" ] ; then Usage ; fi
  eval "$SING_BIN instance.start $BINDS $2 $3"
elif [ $1 == "stop" ] ; then
  if [ "x$2" == "x" ] ; then Usage ; fi
  eval "$SING_BIN instance.stop $2"
elif [ $1 == "exec" ] ; then
  if [ "x$2" == "x" -o "x$3" == "x" ] ; then Usage ; fi
  INST=$2
  shift 2
  eval "$SING_BIN $(echo "exec") $BINDS $INST $*"
elif [ $1 == "shell" ] ; then
  if [ "x$2" == "x" ] ; then Usage ; fi
  eval "$SING_BIN shell $BINDS $2"
else
  echo "Error: $*"
  Usage
fi

exit 0

### END ###
