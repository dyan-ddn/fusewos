#!/bin/bash

fname=$(basename $1)
tgtname=$(date +%s).$fname
echo cp $1 /mnt/fuse/wosfs/test.001/$tgtname
#echo $tgtname
