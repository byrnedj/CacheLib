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

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {

// Periodic worker that evicts items from tiers in batches
// The primary aim is to reduce insertion times for new items in the
// cache
class BackgroundEvictor : public PeriodicWorker {
 public:
  // @param cache               the cache interface
  // @param target_free         the target amount of memory to keep free in 
  //                            this tier
  // @param tier id             memory tier to perform eviction on 
  BackgroundEvictor(CacheBase& cache,
                    double targetFree
                    unsigned int tid);

  ~BackgroundEvictor() override;


 private:
  // cache allocator's interface for evicting
  CacheBase& cache_;
  double targetFree_;
  unsigned int tid_;

  // implements the actual logic of running the background evictor
  void work() final;
};
} // namespace cachelib
} // namespace facebook
