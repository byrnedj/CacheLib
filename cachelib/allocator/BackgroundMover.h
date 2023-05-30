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

#include "cachelib/allocator/BackgroundMoverStrategy.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {

// wrapper that exposes the private APIs of CacheType that are specifically
// needed for the cache api
template <typename C>
struct BackgroundMoverAPIWrapper {
  static size_t traverseAndEvictItems(C& cache,
                                      unsigned int tid,
                                      unsigned int pid,
                                      unsigned int cid,
                                      size_t batch) {
    return cache.traverseAndEvictItems(tid, pid, cid, batch);
  }

  static size_t traverseAndEvictItemsBatch(C& cache,
                                      unsigned int tid,
                                      unsigned int pid,
                                      unsigned int cid,
                                      size_t batch) {
    return cache.traverseAndEvictItemsBatch(tid, pid, cid, batch);
  }
  static size_t traverseAndPromoteItems(C& cache,
                                        unsigned int tid,
                                        unsigned int pid,
                                        unsigned int cid,
                                        size_t batch) {
    return cache.traverseAndPromoteItems(tid, pid, cid, batch);
  }
  static size_t promoteFromQueue(C& cache,
                                        unsigned int tid,
                                        unsigned int pid,
                                        unsigned int cid,
                                        size_t batch) {
    return cache.promoteFromQueue(tid, pid, cid, batch);
  }
  
  static size_t promoteFromQueueBatch(C& cache,
                                        unsigned int tid,
                                        unsigned int pid,
                                        unsigned int cid,
                                        size_t batch) {
    return cache.promoteFromQueueBatch(tid, pid, cid, batch);
  }
  static size_t getQueueSize(C& cache,
                                        unsigned int tid,
                                        unsigned int pid,
                                        unsigned int cid) {
    return cache.getQueueSize(tid, pid, cid);
  }
  static size_t promoteFromMMContainer(C& cache,
                                        unsigned int tid,
                                        unsigned int pid,
                                        unsigned int cid,
                                        size_t batch) {
    return cache.promoteFromMMContainer(tid, pid, cid, batch);
  }
};

enum class MoverDir { Evict = 0, Promote, PromoteFromQueue };

// Periodic worker that evicts items from tiers in batches
// The primary aim is to reduce insertion times for new items in the
// cache
template <typename CacheT>
class BackgroundMover : public PeriodicWorker {
 public:
  using Cache = CacheT;
  // @param cache               the cache interface
  // @param strategy            the stragey class that defines how objects are
  // moved,
  //                            (promoted vs. evicted and how much)
  BackgroundMover(Cache& cache,
                  std::shared_ptr<BackgroundMoverStrategy> strategy,
                  MoverDir direction_);

  ~BackgroundMover() override;

  BackgroundMoverStats getStats() const noexcept;
  std::vector<std::map<MemoryDescriptorType,uint64_t>> getClassStats() const noexcept;

  void setAssignedMemory(
      std::vector<MemoryDescriptorType>&& assignedMemory);

 private:
  std::map<MemoryDescriptorType,uint64_t> moves_per_class_;
  std::map<MemoryDescriptorType,uint64_t> runs_per_class_;
  std::map<MemoryDescriptorType,uint64_t> queue_per_class_;
  
  struct TraversalStats {
    // record a traversal and its time taken
    void recordTraversalTime(uint64_t msTaken);

    uint64_t getAvgTraversalTimeMs(uint64_t numTraversals) const;
    uint64_t getMinTraversalTimeMs() const { return minTraversalTimeMs_; }
    uint64_t getMaxTraversalTimeMs() const { return maxTraversalTimeMs_; }
    uint64_t getLastTraversalTimeMs() const { return lastTraversalTimeMs_; }
    uint64_t getNumTraversals() const { return numTraversals_; }

   private:
    // time it took us the last time to traverse the cache.
    std::atomic<uint64_t> lastTraversalTimeMs_{0};
    std::atomic<uint64_t> minTraversalTimeMs_{
        std::numeric_limits<uint64_t>::max()};
    std::atomic<uint64_t> maxTraversalTimeMs_{0};
    std::atomic<uint64_t> totalTraversalTimeMs_{0};
    std::atomic<uint64_t> numTraversals_{0};
  };
  
  TraversalStats traversalStats_;
  // cache allocator's interface for evicting
  using Item = typename Cache::Item;

  Cache& cache_;
  std::shared_ptr<BackgroundMoverStrategy> strategy_;
  MoverDir direction_;

  std::function<size_t(
      Cache&, unsigned int, unsigned int, unsigned int, size_t)>
      moverFunc;

  // implements the actual logic of running the background evictor
  void work() override final;
  void checkAndRun();

  AtomicCounter numMovedItems{0};
  AtomicCounter numTraversals{0};
  AtomicCounter totalClasses{0};
  AtomicCounter totalBytesMoved{0};

  std::vector<MemoryDescriptorType> assignedMemory_;
  folly::DistributedMutex mutex;
};
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/BackgroundMover-inl.h"
