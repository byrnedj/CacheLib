# !/bin/bash
in=$1
cat "$in" | grep 'Cache Find API latency p50 ' | rev | cut -d " " -f 2 | rev | awk '{print "find,p50,"$1}'
cat "$in" | grep 'Cache Find API latency p90 ' | rev | cut -d " " -f 2 | rev | awk '{print "find,p90,"$1}'
cat "$in" | grep 'Cache Find API latency p99 ' | rev | cut -d " " -f 2 | rev | awk '{print "find,p99,"$1}'
cat "$in" | grep 'Cache Find API latency p999 ' | rev | cut -d " " -f 2 | rev | awk '{print "find,p999,"$1}'

cat "$in" | grep 'Cache Allocate API latency p50 ' | rev | cut -d " " -f 2 | rev | awk '{print "allocate,p50,"$1}'
cat "$in" | grep 'Cache Allocate API latency p90 ' | rev | cut -d " " -f 2 | rev | awk '{print "allocate,p90,"$1}'
cat "$in" | grep 'Cache Allocate API latency p99 ' | rev | cut -d " " -f 2 | rev | awk '{print "allocate,p99,"$1}'
cat "$in" | grep 'Cache Allocate API latency p999 ' | rev | cut -d " " -f 2 | rev | awk '{print "allocate,p999,"$1}'
