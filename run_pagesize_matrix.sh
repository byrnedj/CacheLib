#!/bin/bash
# Page-size study for the DSA CRC offload (PR #470 experiment repeated with
# the DRAM cache on 4k / 2MB / 1GB pages).
#
# Per page size: offload=off (software CRC baseline), offload=on (DSA CRC),
# offload=on + intercept=on (DSA CRC + transparent DTO memcpy of the cache
# insertions - the path that reads/writes the hugetlb-backed cache memory).
#
# Usage: ./run_pagesize_matrix.sh [reps] [extra run_dsa_cachebench args...]
set -uo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPS="${1:-2}"; shift 2>/dev/null || true
OUT="${OUT:-$HOME/dsa_page_out}"
mkdir -p "$OUT"

# hugetlb pools for the 8GB cdn cache + hash table
echo 4700 | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages > /dev/null
echo 12   | sudo tee /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages > /dev/null
sudo sysctl -q -w vm.hugetlb_shm_group="$(id -g)"
echo "2MB pages free: $(cat /sys/kernel/mm/hugepages/hugepages-2048kB/free_hugepages)"
echo "1GB pages free: $(cat /sys/kernel/mm/hugepages/hugepages-1048576kB/free_hugepages)"

SKIP_DSA=""
for rep in $(seq 1 "$REPS"); do
  for page in 4k 2mb 1gb; do
    for spec in "off off" "on off" "on on"; do
      read -r offload intercept <<< "$spec"
      tag="page_${page}_offload_${offload}_intercept_${intercept}_rep${rep}"
      echo "== $tag =="
      rm -f "$OUT"/navy_cache_file
      if "$REPO/run_dsa_cachebench.sh" --workload cdn \
           --offload "$offload" --intercept "$intercept" --page-size "$page" \
           --skip-build $SKIP_DSA --devices "0 2" --out "$OUT" "$@" \
           > "$OUT/run_$tag.log" 2>&1; then
        mv "$OUT"/cachebench_cdn_offload_${offload}_intercept_${intercept}_page_${page}.log \
           "$OUT/cachebench_$tag.log" 2>/dev/null || true
        echo "   ok: $(grep -E '^Total Ops|^get  |^set  ' "$OUT/cachebench_$tag.log" 2>/dev/null | tr '\n' ' ')"
      else
        echo "   FAILED, tail:"; tail -5 "$OUT/run_$tag.log"
      fi
      SKIP_DSA="--skip-dsa"
    done
  done
done
echo "== summary =="
for f in "$OUT"/cachebench_page_*.log; do
  [ -e "$f" ] || continue
  echo "--- $(basename "$f")"
  grep -E '^(Total Ops|get  |set  |Hit Ratio|NVM Gets|NVM Puts|RAM Hit Ratio)' "$f" | head -8
done
