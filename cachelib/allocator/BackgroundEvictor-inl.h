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
                               std::shared_ptr<BackgroundEvictorStrategy> strategy)
    : cache_(cache),
      strategy_(strategy)
{
}

template <typename CacheT>
BackgroundEvictor<CacheT>::~BackgroundEvictor() { stop(std::chrono::seconds(0)); }

template <typename CacheT>
void BackgroundEvictor<CacheT>::work() {
  try {
    checkAndRun();
  } catch (const std::exception& ex) {
    XLOGF(ERR, "BackgroundEvictor interrupted due to exception: {}", ex.what());
  }
}

template <typename CacheT>
void BackgroundEvictor<CacheT>::setAssignedMemory(std::vector<std::tuple<TierId, PoolId, ClassId>> &&assignedMemory)
{
  XLOG(INFO, "Class assigned to background worker:");
  for (auto [tid, pid, cid] : assignedMemory) {
    XLOGF(INFO, "Tid: {}, Pid: {}, Cid: {}", tid, pid, cid);
  }

  mutex.lock_combine([this, &assignedMemory]{
    this->assignedMemory_ = std::move(assignedMemory);
  });
}

// Look for classes that exceed the target memory capacity
// and return those for eviction
template <typename CacheT>
void BackgroundEvictor<CacheT>::checkAndRun() {
  auto assignedMemory = mutex.lock_combine([this]{
    return assignedMemory_;
  });

  unsigned int evictions = 0;
  std::set<ClassId> classes{};
  auto batches = strategy_->calculateBatchSizes(cache_,assignedMemory);
  
  setLastBatch(batches);
  if (batches.size() == 0) {
      return;
  }
  std::vector<bool> done;
  for (size_t i = 0; i < batches.size(); i++) {
      done.push_back(false);
  }
  bool empty = false;
  while (!empty) {
    for (size_t i = 0; i < batches.size(); i++) {
      const auto [tid, pid, cid] = assignedMemory[i];
      auto batch = batches[i];
    
      classes.insert(cid);
      const auto& mpStats = cache_.getPoolByTid(pid,tid).getStats();

      //if this class is done, check if others are done too
      if (!batch && !done[i]) {
        done[i] = true;
        bool alldone = true;
        for (size_t ii = 0; ii < done.size(); ii++) {
            if (!done[ii]) {
                alldone = false;
            }
        }
        if (alldone) {
            empty = true;
            break;
        }
        continue;
      }
      if (batch > 1000) { 
          batch = 1000;  
      }
      //try evicting BATCH items from the class in order to reach free target
      auto evicted =
          BackgroundEvictorAPIWrapper<CacheT>::traverseAndEvictItems(cache_,
              tid,pid,cid,batch);
      evictions += evicted;

      stats.evictionSize.add(evicted * mpStats.acStats.at(cid).allocSize);
      //const size_t cid_id = (size_t)mpStats.acStats.at(cid).allocSize;
      auto it = evictions_per_class_.find(cid);
      if (it != evictions_per_class_.end()) {
          it->second += evicted;
      } else if (evicted > 0) {
          evictions_per_class_[cid] = evicted;
      }
      batches[i] -= evicted;

    }

  }
  //for (size_t i = 0; i < batches.size(); i++) {
  //  const auto [tid, pid, cid] = assignedMemory[i];
  //  auto batch = batches[i];
  //
  //  classes.insert(cid);
  //  const auto& mpStats = cache_.getPoolByTid(pid,tid).getStats();

  //  if (!batch) {
  //    continue;
  //  }
  //  while (batches[i] > 0) {
  //      if (batch > 40) { 
  //          batch = 40;  
  //      }
  //      //try evicting BATCH items from the class in order to reach free target
  //      auto evicted =
  //          BackgroundEvictorAPIWrapper<CacheT>::traverseAndEvictItems(cache_,
  //              tid,pid,cid,batch);
  //      evictions += evicted;

  //      stats.evictionSize.add(evicted * mpStats.acStats.at(cid).allocSize);
  //      //const size_t cid_id = (size_t)mpStats.acStats.at(cid).allocSize;
  //      auto it = evictions_per_class_.find(cid);
  //      if (it != evictions_per_class_.end()) {
  //          it->second += evicted;
  //      } else if (evicted > 0) {
  //          evictions_per_class_[cid] = evicted;
  //      }
  //      batches[i] -= evicted;
  //  }
  //}

  stats.numTraversals.inc();
  stats.numEvictedItems.add(evictions);
  stats.totalClasses.add(classes.size());
}

template <typename CacheT>
BackgroundEvictionStats BackgroundEvictor<CacheT>::getStats() const noexcept {
  BackgroundEvictionStats evicStats;
  evicStats.numEvictedItems = stats.numEvictedItems.get();
  evicStats.runCount = stats.numTraversals.get();
  evicStats.evictionSize = stats.evictionSize.get();
  evicStats.totalClasses = stats.totalClasses.get();

  return evicStats;
}

template <typename CacheT>
void BackgroundEvictor<CacheT>::setLastBatch(std::vector<size_t> batch) {
    batches_ = batch; //just copy
}

template <typename CacheT>
std::map<std::tuple<TierId,PoolId,ClassId>,uint32_t> BackgroundEvictor<CacheT>::getLastBatch() const noexcept {
    
    std::map<std::tuple<TierId,PoolId,ClassId>,uint32_t> lastBatch;
    if (batches_.size() > 0) {
        int index = 0;
        for (auto &tp : assignedMemory_) {
            lastBatch[tp] = batches_[index];
            index++;
        }
    } else {
        int index = 0;
        for (auto &tp : assignedMemory_) {
            lastBatch[tp] = 0;
            index++;
        }
    }
    return lastBatch;

}

template <typename CacheT>
std::map<uint32_t,uint64_t> BackgroundEvictor<CacheT>::getClassStats() const noexcept {
  return evictions_per_class_;
}

} // namespace cachelib
} // namespace facebook
