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

#pragma once

#include <gtest/gtest_prod.h>
#include <folly/concurrency/UnboundedQueue.h>

#include "cachelib/allocator/CacheStats.h"
#include "cachelib/common/PeriodicWorker.h"
#include "cachelib/common/AtomicCounter.h"

#include "cachelib/allocator/BackgroundEvictor.h"
#include "cachelib/allocator/BackgroundPromoter.h"

namespace facebook {
namespace cachelib {

template <typename CacheT>
class BackgroundManager : public PeriodicWorker {
 public:
  using Cache = CacheT;
  BackgroundManager(Cache& cache, 
          std::vector<std::unique_ptr<BackgroundEvictor<Cache>>> &backgroundEvictors ,
          std::vector<std::unique_ptr<BackgroundPromoter<Cache>>> &backgroundPromoters );
 
  ~BackgroundManager() override;

  uint32_t getBackgroundId(TierId tid, PoolId pid, ClassId cid);
 private:
  //for stats, tids, pids, and cids
  Cache& cache_;
  // implements the actual logic of running the background evictor
  void work() override final;
  std::map<uint32_t,std::vector<std::tuple<TierId,PoolId,ClassId>>> doLinearPartition(
        std::map<std::tuple<TierId, PoolId, ClassId>,uint32_t> batchesMap, 
        size_t kParts);
  std::vector<std::unique_ptr<BackgroundEvictor<CacheT>>> &evictors_;
  std::vector<std::unique_ptr<BackgroundPromoter<CacheT>>> &promoters_; 
  std::map<std::tuple<TierId,PoolId,ClassId>,uint32_t> evictors_ids_;

};
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/BackgroundManager-inl.h"
