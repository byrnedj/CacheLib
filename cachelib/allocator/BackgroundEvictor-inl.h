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



namespace facebook {
namespace cachelib {


template <typename CacheT>
BackgroundEvictor<CacheT>::BackgroundEvictor(Cache& cache,
                               std::shared_ptr<BackgroundEvictorStrategy> strategy,
                               unsigned int tid)
    : cache_(cache),
      strategy_(strategy),
      tid_(tid) {

}

template <typename CacheT>
BackgroundEvictor<CacheT>::~BackgroundEvictor() { stop(std::chrono::seconds(0)); }

template <typename CacheT>
void BackgroundEvictor<CacheT>::work() {
  try {
    for (const auto pid : cache_.getRegularPoolIds()) {
      //check if we exceed threshold (per class basis static right now)
      checkAndRun(pid);
    }
  } catch (const std::exception& ex) {
    XLOGF(ERR, "BackgroundEvictor interrupted due to exception: {}", ex.what());
  }
}


// Look for classes that exceed the target memory capacity
// and return those for eviction
template <typename CacheT>
void BackgroundEvictor<CacheT>::checkAndRun(PoolId pid) {
    
  const auto& mpStats = cache_.getPoolByTid(pid,tid_).getStats();
  for (auto& cid : mpStats.classIds) {
    if (strategy_->shouldEvict(cache_,tid_,pid,cid)) {
        unsigned int batch = strategy_->calculateBatchSize(cache_,tid_,pid,cid);
        //try evicting BATCH items from the class in order to reach free target
        unsigned int evicted = 
            BackgroundEvictorAPIWrapper<CacheT>::traverseAndEvictItems(cache_,
                tid_,pid,cid,batch);
        numEvictedItems_ += evicted;
    }
  }
  runCount_ = runCount_ + 1;
}

template <typename CacheT>
BackgroundEvictorStats BackgroundEvictor<CacheT>::getStats() const noexcept {
  BackgroundEvictorStats stats;
  stats.numEvictedItems = numEvictedItems_.load(std::memory_order_relaxed);
  stats.numTraversals = runCount_.load(std::memory_order_relaxed);
  return stats;
}

} // namespace cachelib
} // namespace facebook
