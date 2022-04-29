#!/bin/bash
in=$1
cat $in | grep 'Hit Ratio' | sed 's/Hit Ratio[: ]*\([0-9.%]*\)/Hit Ratio,\1/'

