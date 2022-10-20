/*
 * Copyright (c) Intel and its affiliates.
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

#include "cachelib/allocator/FreeThresholdStrategy.h"

#include "cachelib/allocator/memory/MemoryPoolManager.h"
#include "cachelib/allocator/memory/MemoryAllocator.h"
#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"

#include <folly/logging/xlog.h>

namespace facebook {
namespace cachelib {

FreeThresholdStrategy::FreeThresholdStrategy(double lowEvictionAcWatermark,
                                             double highEvictionAcWatermark,
                                             uint64_t maxEvictionBatch,
                                             uint64_t minEvictionBatch)
    : lowEvictionAcWatermark(lowEvictionAcWatermark),
      highEvictionAcWatermark(highEvictionAcWatermark),
      maxEvictionBatch(maxEvictionBatch),
      minEvictionBatch(minEvictionBatch),
          highEvictionAcWatermarks(CacheBase::kMaxTiers, 
                                std::vector<std::vector<std::vector<double>>>(MemoryPoolManager::kMaxPools,
                                std::vector<std::vector<double>>(MemoryAllocator::kMaxClasses,
                                std::vector<double>(3, highEvictionAcWatermark)))),
      acLatencies(CacheBase::kMaxTiers, 
                  std::vector<std::vector<std::vector<double>>>(MemoryPoolManager::kMaxPools,
                  std::vector<std::vector<double>>(MemoryAllocator::kMaxClasses,
                  std::vector<double>(2, 0.0)))) {}

std::vector<size_t> FreeThresholdStrategy::calculateBatchSizes(
    const CacheBase& cache,
    std::vector<std::tuple<TierId, PoolId, ClassId>> acVec) {
  std::vector<size_t> batches{};
  for (auto [tid, pid, cid] : acVec) {
    auto stats = cache.getAllocationClassStats(tid, pid, cid);
    if (stats.approxFreePercent >= highEvictionAcWatermark) {
      batches.push_back(0);
    } else {
      auto toFreeMemPercent = highEvictionAcWatermark - stats.approxFreePercent;
      auto toFreeItems = static_cast<size_t>(
          toFreeMemPercent * stats.memorySize / stats.allocSize);
      batches.push_back(toFreeItems);
      auto acAllocLatencyNs = cache.getAllocationClassStats(tid, pid, cid).allocLatencyNs.estimate(); //moving avg latency estimation for ac class
      calculateLatency(acAllocLatencyNs, tid, pid, cid);

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

void FreeThresholdStrategy::calculateLatency(uint64_t acLatency, unsigned int tid, PoolId pid, ClassId cid){   
 
    auto best_latency= acLatencies[tid][pid][cid][0];
    acLatencies[tid][pid][cid][1]=best_latency;
    acLatencies[tid][pid][cid][0]=acLatency;



}

BackgroundStrategyStats FreeThresholdStrategy::getStats() { 
    BackgroundStrategyStats s;

   

    auto numClasses = MemoryAllocator::kMaxClasses;
    for (int i = 0; i < 1; i++) {
      for (int j = 0; j < 1; j++) {
        for (int k = 0; k < numClasses; k++) {
          s.highEvictionAcWatermarks[k] =
            std::make_tuple (
                      highEvictionAcWatermarks[i][j][k][0],
                      highEvictionAcWatermarks[i][j][k][1],
                      highEvictionAcWatermarks[i][j][k][2]);
          s.acLatencies[k]=
          std::make_pair (
                      acLatencies[i][j][k][0],
                      acLatencies[i][j][k][1] );

        }
      }
    }
    return s;
}


} // namespace cachelib
} // namespace facebook
