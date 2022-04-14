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
    if (!tasks_.empty()) {
      while (auto entry = tasks_.try_dequeue()) {
        auto [pid, cid] = entry.value();
        auto batch = strategy_->calculateBatchSize(cache_, tid_, pid, cid);
        auto evicted = BackgroundEvictorAPIWrapper<CacheT>::traverseAndEvictItems(cache_,
                tid_,pid,cid,batch);
        numEvictedItemsFromSchedule_.fetch_add(1, std::memory_order_relaxed);
        runCount_.fetch_add(1, std::memory_order_relaxed);
      }
    } else {
      for (const auto pid : cache_.getRegularPoolIds()) {
        //check if the pool is full - probably should be if tier is full
        if (cache_.getPoolByTid(pid,tid_).allSlabsAllocated()) {
          checkAndRun(pid);
        }
      }
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
  unsigned int evictions = 0;
  unsigned int classes = 0;
  for (auto& cid : mpStats.classIds) {
      classes++;
      auto batch = strategy_->calculateBatchSize(cache_,tid_,pid,cid);
      if (!batch) {
        continue;
      }

      //try evicting BATCH items from the class in order to reach free target
      auto evicted =
          BackgroundEvictorAPIWrapper<CacheT>::traverseAndEvictItems(cache_,
              tid_,pid,cid,batch);
      evictions += evicted;
      const size_t cid_id = (size_t)mpStats.acStats.at(cid).allocSize;
      auto it = evictions_per_class_.find(cid_id);
      if (it != evictions_per_class_.end()) {
          it->second += evicted;
      } else {
          evictions_per_class_[cid_id] = 0;
      }
  }
  runCount_.fetch_add(1, std::memory_order_relaxed);
  numEvictedItems_.fetch_add(evictions, std::memory_order_relaxed);
  totalClasses_.fetch_add(classes, std::memory_order_relaxed);
}

template <typename CacheT>
BackgroundEvictorStats BackgroundEvictor<CacheT>::getStats() const noexcept {
  BackgroundEvictorStats stats;
  stats.numEvictedItems = numEvictedItems_.load(std::memory_order_relaxed);
  stats.runCount = runCount_.load(std::memory_order_relaxed);
  stats.numEvictedItemsFromSchedule = numEvictedItemsFromSchedule_.load(std::memory_order_relaxed);
  stats.totalClasses = totalClasses_.load(std::memory_order_relaxed);
  return stats;
}

template <typename CacheT>
std::map<uint32_t,uint64_t> BackgroundEvictor<CacheT>::getClassStats() const noexcept {
  return evictions_per_class_;
}

} // namespace cachelib
} // namespace facebook
