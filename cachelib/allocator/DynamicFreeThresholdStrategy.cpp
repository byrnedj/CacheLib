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

#include "cachelib/allocator/DynamicFreeThresholdStrategy.h"

#include <folly/logging/xlog.h>

namespace facebook::cachelib {

DynamicFreeThresholdStrategy::DynamicFreeThresholdStrategy(double lowEvictionAcWatermark,
                                             double highEvictionAcWatermark,
                                             uint64_t maxEvictionBatch,
                                             uint64_t minEvictionBatch)
    : lowEvictionAcWatermark(lowEvictionAcWatermark),
      highEvictionAcWatermark(highEvictionAcWatermark),
      maxEvictionBatch(maxEvictionBatch),
      minEvictionBatch(minEvictionBatch) {
        //for (int tid = 0; tid < cache.getNumTiers(); tid++) {
        //    auto pools = cache.getPoolIds();
        //    for (auto pid : pools) {
        //        auto pool = cache.getPool(pid);
        //        auto cids = pool.getNumClassId();
        //        for (cids : 
        //    }
        //}
      }

std::vector<size_t> DynamicFreeThresholdStrategy::calculateBatchSizes(
    const CacheBase& cache,
    std::vector<MemoryDescriptorType> acVec) {
  std::vector<size_t> batches{};
  for (auto [tid, pid, cid] : acVec) {
    MemoryDescriptorType md(tid,pid,cid);
    if (direction.find(md) == direction.end()) {
      direction[md] = 1;
    }
    if (pAllocLatency.find(md) == pAllocLatency.end()) {
      pAllocLatency[md] = 0;
    }
    if (acMaxEvictionBatch.find(md) == acMaxEvictionBatch.end()) {
      acMaxEvictionBatch[md] = maxEvictionBatch;
    }
    const auto& pool = cache.getPoolByTid(pid, tid);
    if (pool.getApproxFreeSlabs()) {
      batches.push_back(0);
    }
    double usage = pool.getApproxUsage(cid);
    if ((1-usage)*100 < highEvictionAcWatermark && pool.allSlabsAllocated()) {
      auto toFreeMemPercent = highEvictionAcWatermark - (1-usage)*100;
      auto toFreeItems = static_cast<size_t>(
          toFreeMemPercent * (pool.getApproxSlabs(cid) * pool.getPerSlab(cid)) );
      //calculate the max eviction batch by minimizing ac latency
      auto acAllocLatency = cache.getACStats(tid, pid, cid)
               .allocLatencyNs.estimate(); // moving avg latency estimation for
                                           // ac class
      if (pAllocLatency[md] > 0) {
        if (acAllocLatency < pAllocLatency[md]) {
          //keep going in this direction
          acMaxEvictionBatch[md] += direction[md];
        } else {
          //stop and turn around
          direction[md] = direction[md] * -1;
          acMaxEvictionBatch[md] -= direction[md];
        }
        if (acMaxEvictionBatch[md] > maxEvictionBatch) {
          acMaxEvictionBatch[md] = maxEvictionBatch;
        }
        if (acMaxEvictionBatch[md] < minEvictionBatch) {
          acMaxEvictionBatch[md] = minEvictionBatch;
        }
        if (toFreeItems > acMaxEvictionBatch[md]) {
          toFreeItems = acMaxEvictionBatch[md];
        }
      }
      pAllocLatency[md] = acAllocLatency;
      batches.push_back(toFreeItems);
    } else {
      batches.push_back(0);
    }
  }

  if (batches.size() == 0) {
    return batches;
  }

  auto maxBatch = *std::max_element(batches.begin(), batches.end());
  if (maxBatch == 0)
    return batches;
  
  std::transform(
      batches.begin(), batches.end(), batches.begin(), [&](auto numItems) {
        if (numItems == 0) {
          return 0UL;
        }

        auto cappedBatchSize = maxEvictionBatch * numItems / maxBatch;
        if (cappedBatchSize < minEvictionBatch)
          return minEvictionBatch;
        else
          return cappedBatchSize;
      });


  return batches;
}

} // namespace facebook::cachelib
