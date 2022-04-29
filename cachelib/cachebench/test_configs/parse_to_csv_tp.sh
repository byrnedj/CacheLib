#/bin/bash

in=$1

cat $in | grep -E '^get[ :]*' | sed 's/get[ :]*\([0-9,]*\)\/s[ ,0-9%.a-z:]*/\1/' | sed 's/,//g' | awk '{print $1}'
cat $in | grep -E '^set[ :]*' | sed 's/set[ :]*\([0-9,]*\)\/s[ ,0-9%.a-z:]*/\1/' | sed 's/,//g' | awk '{print $1}'

