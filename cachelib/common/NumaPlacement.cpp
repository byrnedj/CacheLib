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

#include "cachelib/common/NumaPlacement.h"

#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <numaif.h>

#include <cstring>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {

void weightedInterleavePages(void* addr,
                             size_t len,
                             size_t pageSize,
                             const std::vector<unsigned int>& nodes,
                             const std::vector<unsigned int>& weights,
                             InterleaveCursor* cursor) {
  if (nodes.size() != weights.size()) {
    util::throwSystemError(
        EINVAL,
        folly::sformat("numaBindWeights size ({}) must match the number of "
                       "bound NUMA nodes ({})",
                       weights.size(), nodes.size()));
  }
  if (nodes.empty()) {
    return;
  }
  for (auto w : weights) {
    if (w == 0) {
      util::throwSystemError(EINVAL, "numaBindWeights entries must be >= 1");
    }
  }

  char* const startAddr = reinterpret_cast<char*>(addr);
  char* const endAddr = startAddr + len;

  // Migrate in batches to bound the temporary arrays passed to move_pages().
  constexpr size_t kBatch = 4096;
  std::vector<void*> pages;
  std::vector<int> targetNodes;
  std::vector<int> status;
  pages.reserve(kBatch);
  targetNodes.reserve(kBatch);
  status.resize(kBatch);

  size_t migrated = 0;
  size_t failed = 0;
  int firstErr = 0;
  auto flush = [&]() {
    if (pages.empty()) {
      return;
    }
    long ret = move_pages(/*pid=*/0, pages.size(), pages.data(),
                          targetNodes.data(), status.data(), MPOL_MF_MOVE);
    if (ret < 0) {
      util::throwSystemError(
          errno,
          folly::sformat("move_pages() failed: {}", std::strerror(errno)));
    }
    for (size_t i = 0; i < pages.size(); ++i) {
      if (status[i] == targetNodes[i]) {
        ++migrated;
      } else {
        ++failed;
        if (firstErr == 0) {
          firstErr = status[i];
        }
      }
    }
    pages.clear();
    targetNodes.clear();
  };

  // Weighted round-robin: emit weights[i] consecutive pages on nodes[i],
  // then advance to the next bound node, cycling across the region. When a
  // cursor is supplied the position is resumed from / written back to it so
  // that a sequence of small regions (e.g. successive 2-page slabs of the same
  // pool) shares one continuous weighted cycle. Without this, regions smaller
  // than a full cycle would always restart on the highest-weight node and the
  // low-weight nodes would never receive any pages.
  size_t idx = (cursor != nullptr) ? cursor->idx : 0;
  size_t remaining = (cursor != nullptr) ? cursor->remaining : 0;
  if (idx >= nodes.size()) {
    idx = 0;
    remaining = 0;
  }
  if (remaining == 0) {
    remaining = weights[idx];
  }
  for (char* curAddr = startAddr; curAddr < endAddr; curAddr += pageSize) {
    if (remaining == 0) {
      idx = (idx + 1) % nodes.size();
      remaining = weights[idx];
    }
    pages.push_back(curAddr);
    targetNodes.push_back(static_cast<int>(nodes[idx]));
    --remaining;
    if (pages.size() == kBatch) {
      flush();
    }
  }
  flush();
  if (cursor != nullptr) {
    cursor->idx = idx;
    cursor->remaining = remaining;
  }
  XLOGF(DBG,
        "Weighted NUMA interleave: placed {} pages across nodes=[{}] "
        "weights=[{}] ({} migrated, {} not migrated)",
        migrated + failed, folly::join(",", nodes), folly::join(",", weights),
        migrated, failed);
  if (failed > 0) {
    XLOGF(WARN,
          "Weighted NUMA interleave: {} pages could not be migrated to their "
          "target node (first status={}); page distribution may not match the "
          "requested weights",
          failed, firstErr);
  }
}

} // namespace cachelib
} // namespace facebook
