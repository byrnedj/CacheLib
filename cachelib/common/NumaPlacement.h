/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstddef>
#include <vector>

namespace facebook {
namespace cachelib {

// Per-pool NUMA binding: the set of NUMA nodes a pool's slabs should be placed
// on, plus per-node weights describing the proportion of pages to land on each
// node. nodes[i] receives a share of pages proportional to weights[i]. Both
// vectors must be the same length and weights must be >= 1. An empty `nodes`
// means "no per-pool binding" (the pool falls back to the global behavior).
struct PoolNumaBinding {
  std::vector<unsigned int> nodes;
  std::vector<unsigned int> weights;

  bool empty() const noexcept { return nodes.empty() || weights.empty(); }
};

// Cursor capturing the position within a weighted round-robin so that the
// cycle can be resumed across multiple (non-contiguous) regions. This is needed
// because a single region may be smaller than one full weighted cycle (e.g. a
// 4MB cachelib slab is only 2 huge pages, far smaller than a 21:1 weight
// cycle); without a persistent cursor each region would always start at the
// first (highest-weight) node and the low-weight nodes would never receive
// pages. Default-constructed = start of cycle.
struct InterleaveCursor {
  size_t idx{0};       // index into nodes[]/weights[] of the current node
  size_t remaining{0}; // pages still to place on the current node; 0 => (re)init
};

// Distribute the already-faulted pages of [addr, addr+len) across the given
// NUMA `nodes` in proportion to `weights`, using a weighted round-robin over
// page-sized chunks. Placement is done with move_pages() rather than per-page
// mbind() for two reasons:
//   1. move_pages() does not split the VMA, so a large region (e.g. 150GB of
//      2MB pages => 75k pages) does not blow past vm.max_map_count.
//   2. per-page mbind() on a single huge-page range is rejected with EINVAL on
//      some kernels, whereas move_pages() migrates huge pages correctly.
// Because cache items are hashed uniformly across the whole region, the access
// stream hits every node in proportion to its page count regardless of the
// interleave granularity, so this yields the intended bandwidth split.
//
// @param addr      start of the region (must be page aligned)
// @param len       length of the region in bytes
// @param pageSize  size of each page (e.g. 2MB for huge pages)
// @param nodes     NUMA node ids to place pages on
// @param weights   per-node weights; weights.size() must == nodes.size()
// @param cursor    optional in/out cursor to resume the round-robin across
//                  calls; pass the same cursor for successive regions that
//                  should share one weighted cycle. nullptr starts fresh.
//
// @throw std::system_error if nodes/weights mismatch, a weight is 0, or
//        move_pages() fails.
void weightedInterleavePages(void* addr,
                             size_t len,
                             size_t pageSize,
                             const std::vector<unsigned int>& nodes,
                             const std::vector<unsigned int>& weights,
                             InterleaveCursor* cursor = nullptr);

} // namespace cachelib
} // namespace facebook
