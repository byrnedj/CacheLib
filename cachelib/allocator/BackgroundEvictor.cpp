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

#include "cachelib/allocator/BackgroundEvictor.h"

#include <folly/logging/xlog.h>

#include <stdexcept>
#include <thread>

namespace facebook {
namespace cachelib {

BackgroundEvictor::BackgroundEvictor(CacheBase& cache,
                               double targetFree,
                               unsigned int tid)
    : cache_(cache),
      targetFree_(targetFree),
      tid_(tid) {

}

BackgroundEvictor::~BackgroundEvictor() { stop(std::chrono::seconds(0)); }

void BackgroundEvictor::work() {
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
std::vector<ClassId> BackgrounEvictor::checkAndRun(PoolId pid) const {
  std::vector<ClassId> classesToEvict;
  const auto& mpStats = cache_.getPool(pid).getStats();
  for (auto& id : mpStats.classIds) {
    size_t totalMem = mpStats.acStats.at(id,tid_).getTotalMemory();
    size_t freeMem = mpStats.acStats.at(id,tid_).getTotalFreeMemory();
    double currFree = (double)freeMem/(double)totalMem;

    if (currFree < targetFree_) {
        size_t targetMem = (targetFree_ * totalMem) - freeMem;
        size_t batch = (targetMem / mpStats.acStats.at(id,tid_).allocSize);
        //try evicting BATCH items from the class in order to reach free target
        tryEvicting(pid,cid,batch);
    }
  }
}

//
//try evicting a batch of items from a class that is over memory target
//
void BackgroundEvictor::tryEvicting(PoolId pid, ClassId cid, size_t batch) {
  auto& mmContainter = getMMContainer(tid_,pid,cid);
  // Keep searching for a candidate until we were able to evict it
  // or until the search limit has been exhausted
  unsigned int evictions = 0;
  auto itr = mmContainer.getEvictionIterator();
  while (evictions < batch && itr) {

    Item* candidate = itr.get();
    // for chained items, the ownership of the parent can change. We try to
    // evict what we think as parent and see if the eviction of parent
    // recycles the child we intend to.
    
    ItemHandle toReleaseHandle = tryEvictToNextMemoryTier(tid_, pid, itr);
    bool movedToNextTier = false;
    if(toReleaseHandle) {
      movedToNextTier = true;
    } else {
      toReleaseHandle =
          itr->isChainedItem()
              ? advanceIteratorAndTryEvictChainedItem(tid_, pid, itr)
              : advanceIteratorAndTryEvictRegularItem(tid_, pid, mmContainer, itr);
    }

    if (toReleaseHandle) {
      if (toReleaseHandle->hasChainedItem()) {
        (*stats_.chainedItemEvictions)[pid][cid].inc();
      } else {
        (*stats_.regularItemEvictions)[pid][cid].inc();
      }
      ++evictions;


      // we must be the last handle and for chained items, this will be
      // the parent.
      XDCHECK(toReleaseHandle.get() == candidate || candidate->isChainedItem());
      XDCHECK_EQ(1u, toReleaseHandle->getRefCount());

      // We manually release the item here because we don't want to
      // invoke the Item Handle's destructor which will be decrementing
      // an already zero refcount, which will throw exception
      auto& itemToRelease = *toReleaseHandle.release();

      // Decrementing the refcount because we want to recycle the item
      const auto ref = decRef(itemToRelease);
      XDCHECK_EQ(0u, ref);
      
      // check if by releasing the item we intend to, we actually
      // recycle the candidate.
      releaseBackToAllocator(itemToRelease, RemoveContext::kEviction,
                            /* isNascent */ movedToNextTier, candidate);

    }

    // If we destroyed the itr to possibly evict and failed, we restart
    // from the beginning again
    if (!itr) {
      itr.resetToBegin();
    }
  }
  // Invalidate iterator since later on we may use this mmContainer
  // again, which cannot be done unless we drop this iterator
  itr.destroy();

}

} // namespace cachelib
} // namespace facebook
