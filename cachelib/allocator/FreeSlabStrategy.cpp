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

#include "cachelib/allocator/FreeSlabStrategy.h"

#include <folly/logging/xlog.h>

namespace facebook::cachelib {

FreeSlabStrategy::FreeSlabStrategy(double lowEvictionAcWatermark,
                                   double highEvictionAcWatermark )
    : lowEvictionAcWatermark(lowEvictionAcWatermark),
      highEvictionAcWatermark(highEvictionAcWatermark) {}

std::vector<size_t> FreeSlabStrategy::calculateBatchSizes(
    const CacheBase& cache,
    std::vector<MemoryDescriptorType> acVec) {
  std::vector<size_t> batches{};
  for (auto [tid, pid, cid] : acVec) {
    const auto& pool = cache.getPoolByTid(pid, tid);
    if (pool.allSlabsAllocated()) {
      if (pool.getApproxFreeSlabs() < 1) {
        batches.push_back(1);
      } else if ((1 - pool.getApproxUsage(cid))*100 < highEvictionAcWatermark) {
        batches.push_back(1);
      } else {
        batches.push_back(0);
      }
    } else {
      batches.push_back(0);
    }
  }
  return batches;
}

} // namespace facebook::cachelib
