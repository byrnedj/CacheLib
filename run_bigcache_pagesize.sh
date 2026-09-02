#!/bin/bash
# Page-size study on the BigCache trace replay (PR #470 experiment):
# variants A (no checksum), B (software CRC), C (DSA CRC), C+intercept,
# each with the DRAM cache on 4k / 2MB / 1GB pages. Fiber scheduler, as in
# the original study.
set -uo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-$HOME/bigcache_page_out}"
NUM_OPS="${NUM_OPS:-200000}"
mkdir -p "$OUT"
echo 8800 | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages > /dev/null
echo 18   | sudo tee /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages > /dev/null
sudo sysctl -q -w vm.hugetlb_shm_group="$(id -g)"
echo "2MB free: $(cat /sys/kernel/mm/hugepages/hugepages-2048kB/free_hugepages)  1GB free: $(cat /sys/kernel/mm/hugepages/hugepages-1048576kB/free_hugepages)"
for page in 4k 2mb 1gb; do
  for spec in "none off" "off off" "on off" "on on"; do
    read -r offload intercept <<< "$spec"
    tag="page_${page}_offload_${offload}_intercept_${intercept}"
    echo "== $tag =="
    if "$REPO/run_dsa_cachebench.sh" --workload bigcache --fibers on \
         --offload "$offload" --intercept "$intercept" --page-size "$page" \
         --num-ops "$NUM_OPS" --devices "2" --skip-dsa --skip-build \
         --out "$OUT" > "$OUT/run_$tag.log" 2>&1; then
      mv "$OUT"/cachebench_bigcache_offload_${offload}_intercept_${intercept}_page_${page}.log \
         "$OUT/cachebench_$tag.log" 2>/dev/null || true
      echo "   ok: $(grep -E "^get " "$OUT/cachebench_$tag.log" | head -1 | tr -s " ")"
    else
      echo "   FAILED:"; tail -3 "$OUT/run_$tag.log"
    fi
  done
done
echo "== done =="
