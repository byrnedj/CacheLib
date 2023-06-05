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

#pragma once

#include <folly/Random.h>

namespace facebook {
namespace cachelib {

template <typename CacheTrait>
CacheAllocator<CacheTrait>::CacheAllocator(Config config)
    : CacheAllocator(InitMemType::kNone, config) {
  initCommon(false);
}

template <typename CacheTrait>
CacheAllocator<CacheTrait>::CacheAllocator(SharedMemNewT, Config config)
    : CacheAllocator(InitMemType::kMemNew, config) {
  initCommon(false);
  shmManager_->removeShm(detail::kShmInfoName);
}

template <typename CacheTrait>
CacheAllocator<CacheTrait>::CacheAllocator(SharedMemAttachT, Config config)
    : CacheAllocator(InitMemType::kMemAttach, config) {
  /* TODO - per tier? */
  for (auto pid : *metadata_.compactCachePools()) {
    isCompactCachePool_[pid] = true;
  }

  initCommon(true);

  // We will create a new info shm segment on shutDown(). If we don't remove
  // this info shm segment here and the new info shm segment's size is larger
  // than this one, creating new one will fail.
  shmManager_->removeShm(detail::kShmInfoName);
}

template <typename CacheTrait>
CacheAllocator<CacheTrait>::CacheAllocator(
    typename CacheAllocator<CacheTrait>::InitMemType type, Config config)
    : isOnShm_{type != InitMemType::kNone ? true
                                          : config.memMonitoringEnabled()},
      config_(config.validate()),
      memoryTierConfigs(config.getMemoryTierConfigs()),
      tempShm_(type == InitMemType::kNone && isOnShm_
                   ? std::make_unique<TempShmMapping>(config_.getCacheSize())
                   : nullptr),
      privMemManager_(type == InitMemType::kNone && !isOnShm_
                          ? std::make_unique<PrivateMemoryManager>()
                          : nullptr),
      shmManager_(type != InitMemType::kNone
                      ? std::make_unique<ShmManager>(config_.cacheDir,
                                                     config_.isUsingPosixShm())
                      : nullptr),
      deserializer_(type == InitMemType::kMemAttach ? createDeserializer()
                                                    : nullptr),
      metadata_{type == InitMemType::kMemAttach
                    ? deserializeCacheAllocatorMetadata(*deserializer_)
                    : serialization::CacheAllocatorMetadata{}},
      allocator_(initAllocator(type)),
      compactCacheManager_(type != InitMemType::kMemAttach
                               ? std::make_unique<CCacheManager>(*allocator_[0] /* TODO: per tier */)
                               : restoreCCacheManager(0/* TODO: per tier */)),
      compressor_(createPtrCompressor()),
      mmContainers_(type == InitMemType::kMemAttach
                        ? deserializeMMContainers(*deserializer_, compressor_)
                        : MMContainers{getNumTiers()}),
      promoQueues_(PromoQueues{getNumTiers()}),
      accessContainer_(initAccessContainer(
          type, detail::kShmHashTableName, config.accessConfig)),
      chainedItemAccessContainer_(
          initAccessContainer(type,
                              detail::kShmChainedItemHashTableName,
                              config.chainedItemAccessConfig)),
      chainedItemLocks_(config_.chainedItemsLockPower,
                        std::make_shared<MurmurHash2>()),
      movesMap_(kShards),
      moveLock_(kShards), 
      promoMap_(kShards),
      cacheCreationTime_{
          type != InitMemType::kMemAttach
              ? util::getCurrentTimeSec()
              : static_cast<uint32_t>(*metadata_.cacheCreationTime())},
      cacheInstanceCreationTime_{type != InitMemType::kMemAttach
                                     ? cacheCreationTime_
                                     : util::getCurrentTimeSec()},
      // Pass in cacheInstnaceCreationTime_ as the current time to keep
      // nvmCacheState's current time in sync
      nvmCacheState_{cacheInstanceCreationTime_, config_.cacheDir,
                     config_.isNvmCacheEncryptionEnabled(),
                     config_.isNvmCacheTruncateAllocSizeEnabled()},
      threadPool_(config_.threadPoolThreads) {}

template <typename CacheTrait>
CacheAllocator<CacheTrait>::~CacheAllocator() {
  XLOG(DBG, "destructing CacheAllocator");
  // Stop all workers. In case user didn't call shutDown, we want to
  // terminate all background workers and nvmCache before member variables
  // go out of scope.
  stopWorkers();
  nvmCache_.reset();
}

template <typename CacheTrait>
ShmSegmentOpts CacheAllocator<CacheTrait>::createShmCacheOpts(TierId tid) {
  ShmSegmentOpts opts;
  opts.alignment = sizeof(Slab);
  auto memoryTierConfigs = config_.getMemoryTierConfigs();
  // TODO: we support single tier so far
  if (memoryTierConfigs.size() > 2) {
    throw std::invalid_argument("CacheLib only supports two memory tiers");
  }
  opts.memBindNumaNodes = memoryTierConfigs[tid].getMemBind();
  return opts;
}

template <typename CacheTrait>
size_t CacheAllocator<CacheTrait>::memoryTierSize(TierId tid) const {
  auto partitions = std::accumulate(memoryTierConfigs.begin(), memoryTierConfigs.end(), 0UL,
  [](const size_t i, const MemoryTierCacheConfig& config){
    return i + config.getRatio();
  });

  return memoryTierConfigs[tid].calculateTierSize(config_.getCacheSize(), partitions);
}

template <typename CacheTrait>
std::vector<std::unique_ptr<MemoryAllocator>>
CacheAllocator<CacheTrait>::createPrivateAllocator() {
  std::vector<std::unique_ptr<MemoryAllocator>> allocators;

  if (!isOnShm_)
    allocators.emplace_back(std::make_unique<MemoryAllocator>(
                            getAllocatorConfig(config_),
                            tempShm_->getAddr(),
                            config_.getCacheSize()));
  else
    allocators.emplace_back(std::make_unique<MemoryAllocator>(
                            getAllocatorConfig(config_),
                            config_.getCacheSize()));

  return allocators;
}

template <typename CacheTrait>
PrivateSegmentOpts CacheAllocator<CacheTrait>::createPrivateSegmentOpts(TierId tid) {
  PrivateSegmentOpts opts;
  opts.alignment = sizeof(Slab);
  auto memoryTierConfigs = config_.getMemoryTierConfigs();
  if (memoryTierConfigs.size() > 2) {
    throw std::invalid_argument("CacheLib only supports two memory tiers");
  }
  opts.memBindNumaNodes = memoryTierConfigs[tid].getMemBind();

  return opts;
}

template <typename CacheTrait>
std::unique_ptr<MemoryAllocator>
CacheAllocator<CacheTrait>::createNewMemoryAllocator(TierId tid) {
  size_t tierSize = memoryTierSize(tid);
  return std::make_unique<MemoryAllocator>(
      getAllocatorConfig(config_),
      shmManager_
          ->createShm(detail::kShmCacheName + std::to_string(tid),
                      tierSize, config_.slabMemoryBaseAddr,
                      createShmCacheOpts(tid))
          .addr,
      tierSize);
}

template <typename CacheTrait>
std::unique_ptr<MemoryAllocator>
CacheAllocator<CacheTrait>::createNewPrivateMemoryAllocator(TierId tid) {
  size_t tierSize = memoryTierSize(tid);
  return std::make_unique<MemoryAllocator>(
      getAllocatorConfig(config_),
      privMemManager_->createMapping(tierSize,
                                     createPrivateSegmentOpts(tid)),
      tierSize);
}

template <typename CacheTrait>
std::unique_ptr<MemoryAllocator>
CacheAllocator<CacheTrait>::restoreMemoryAllocator(TierId tid) {
  return std::make_unique<MemoryAllocator>(
      deserializer_->deserialize<MemoryAllocator::SerializationType>(),
      shmManager_
          ->attachShm(detail::kShmCacheName + std::to_string(tid),
            config_.slabMemoryBaseAddr, createShmCacheOpts(tid)).addr,
      memoryTierSize(tid),
      config_.disableFullCoredump);
}

template <typename CacheTrait>
std::vector<std::unique_ptr<MemoryAllocator>>
CacheAllocator<CacheTrait>::createAllocators() {
  std::vector<std::unique_ptr<MemoryAllocator>> allocators;
  for (int tid = 0; tid < getNumTiers(); tid++) {
    allocators.emplace_back(createNewMemoryAllocator(tid));
  }
  return allocators;
}

template <typename CacheTrait>
std::vector<std::unique_ptr<MemoryAllocator>>
CacheAllocator<CacheTrait>::createPrivateMemoryAllocators() {
  std::vector<std::unique_ptr<MemoryAllocator>> allocators;
  for (int tid = 0; tid < getNumTiers(); tid++) {
    allocators.emplace_back(createNewPrivateMemoryAllocator(tid));
  }
  return allocators;
}

template <typename CacheTrait>
std::vector<std::unique_ptr<MemoryAllocator>>
CacheAllocator<CacheTrait>::restoreAllocators() {
  std::vector<std::unique_ptr<MemoryAllocator>> allocators;
  for (int tid = 0; tid < getNumTiers(); tid++) {
    allocators.emplace_back(restoreMemoryAllocator(tid));
  }
  return allocators;
}

template <typename CacheTrait>
std::unique_ptr<CCacheManager>
CacheAllocator<CacheTrait>::restoreCCacheManager(TierId tid) {
  return std::make_unique<CCacheManager>(
      deserializer_->deserialize<CCacheManager::SerializationType>(),
      *allocator_[tid]);
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::initCommon(bool dramCacheAttached) {
  if (config_.nvmConfig.has_value()) {
    if (config_.nvmCacheAP) {
      nvmAdmissionPolicy_ = config_.nvmCacheAP;
    } else if (config_.rejectFirstAPNumEntries) {
      nvmAdmissionPolicy_ = std::make_shared<RejectFirstAP<CacheT>>(
          config_.rejectFirstAPNumEntries, config_.rejectFirstAPNumSplits,
          config_.rejectFirstSuffixIgnoreLength,
          config_.rejectFirstUseDramHitSignal);
    }
    if (config_.nvmAdmissionMinTTL > 0) {
      if (!nvmAdmissionPolicy_) {
        nvmAdmissionPolicy_ = std::make_shared<NvmAdmissionPolicy<CacheT>>();
      }
      nvmAdmissionPolicy_->initMinTTL(config_.nvmAdmissionMinTTL);
    }
  }
  initStats();
  initNvmCache(dramCacheAttached);

  if (!config_.delayCacheWorkersStart) {
    initWorkers();
  }
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::initNvmCache(bool dramCacheAttached) {
  if (!config_.nvmConfig.has_value()) {
    return;
  }

  // for some usecases that create pools, restoring nvmcache when dram cache
  // is not persisted is not supported.
  const bool shouldDrop = config_.dropNvmCacheOnShmNew && !dramCacheAttached;

  // if we are dealing with persistency, cache directory should be enabled
  const bool truncate = config_.cacheDir.empty() ||
                        nvmCacheState_.shouldStartFresh() || shouldDrop;
  if (truncate) {
    nvmCacheState_.markTruncated();
  }

  nvmCache_ = std::make_unique<NvmCacheT>(*this, *config_.nvmConfig, truncate,
                                          config_.itemDestructor);
  if (!config_.cacheDir.empty()) {
    nvmCacheState_.clearPrevState();
  }
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::initWorkers() {
  if (config_.poolResizingEnabled() && !poolResizer_) {
    startNewPoolResizer(config_.poolResizeInterval,
                        config_.poolResizeSlabsPerIter,
                        config_.poolResizeStrategy);
  }

  if (config_.poolRebalancingEnabled() && !poolRebalancer_) {
    startNewPoolRebalancer(config_.poolRebalanceInterval,
                           config_.defaultPoolRebalanceStrategy,
                           config_.poolRebalancerFreeAllocThreshold);
  }

  if (config_.memMonitoringEnabled() && !memMonitor_) {
    if (!isOnShm_) {
      throw std::invalid_argument(
          "Memory monitoring is not supported for cache on heap. It is "
          "supported "
          "for cache on a shared memory segment only.");
    }
    startNewMemMonitor(config_.memMonitorInterval,
                       config_.memMonitorConfig,
                       config_.poolAdviseStrategy);
  }

  //if (config_.itemsReaperEnabled() && !reaper_) {
  //  startNewReaper(config_.reaperInterval, config_.reaperConfig);
  //}

  if (config_.poolOptimizerEnabled() && !poolOptimizer_) {
    startNewPoolOptimizer(config_.regularPoolOptimizeInterval,
                          config_.compactCacheOptimizeInterval,
                          config_.poolOptimizeStrategy,
                          config_.ccacheOptimizeStepSizePercent);
  }

  if (config_.backgroundEvictorEnabled()) {
      startNewBackgroundEvictor(config_.backgroundEvictorInterval,
                                config_.backgroundEvictorStrategy,
                                config_.backgroundEvictorThreads);
  }

  if (config_.backgroundPromoterEnabled()) {
      startNewBackgroundPromoter(config_.backgroundPromoterInterval,
                                config_.backgroundPromoterStrategy,
                                config_.backgroundPromoterThreads,
                                config_.usePromotionQueue);
  }
}

template <typename CacheTrait>
std::vector<std::unique_ptr<MemoryAllocator>>
CacheAllocator<CacheTrait>::initAllocator(
    InitMemType type) {
  if (type == InitMemType::kNone) {
    XDCHECK(!isOnShm_);
    return createPrivateMemoryAllocators();
  } else if (type == InitMemType::kMemNew) {
    return createAllocators();
  } else if (type == InitMemType::kMemAttach) {
    return restoreAllocators();
  }

  // Invalid type
  throw std::runtime_error(folly::sformat(
      "Cannot initialize memory allocator, unknown InitMemType: {}.",
      static_cast<int>(type)));
}

template <typename CacheTrait>
std::unique_ptr<typename CacheAllocator<CacheTrait>::AccessContainer>
CacheAllocator<CacheTrait>::initAccessContainer(InitMemType type,
                                                const std::string name,
                                                AccessConfig config) {
  if (type == InitMemType::kNone) {
    return std::make_unique<AccessContainer>(
        config, compressor_,
        [this](Item* it) -> WriteHandle { return acquire(it); });
  } else if (type == InitMemType::kMemNew) {
    return std::make_unique<AccessContainer>(
        config,
        shmManager_
            ->createShm(
                name,
                AccessContainer::getRequiredSize(config.getNumBuckets()),
                nullptr,
                ShmSegmentOpts(config.getPageSize()))
            .addr,
        compressor_,
        [this](Item* it) -> WriteHandle { return acquire(it); });
  } else if (type == InitMemType::kMemAttach) {
    return std::make_unique<AccessContainer>(
        deserializer_->deserialize<AccessSerializationType>(),
        config,
        shmManager_->attachShm(name),
        compressor_,
        [this](Item* it) -> WriteHandle { return acquire(it); });
  }

  // Invalid type
  throw std::runtime_error(folly::sformat(
      "Cannot initialize access container, unknown InitMemType: {}.",
      static_cast<int>(type)));
}

template <typename CacheTrait>
std::unique_ptr<Deserializer> CacheAllocator<CacheTrait>::createDeserializer() {
  auto infoAddr = shmManager_->attachShm(detail::kShmInfoName);
  return std::make_unique<Deserializer>(
      reinterpret_cast<uint8_t*>(infoAddr.addr),
      reinterpret_cast<uint8_t*>(infoAddr.addr) + infoAddr.size);
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::allocate(PoolId poolId,
                                     typename Item::Key key,
                                     uint32_t size,
                                     uint32_t ttlSecs,
                                     uint32_t creationTime) {
  if (creationTime == 0) {
    creationTime = util::getCurrentTimeSec();
  }
  return allocateInternal(poolId, key, size, creationTime,
                          ttlSecs == 0 ? 0 : creationTime + ttlSecs);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::shouldWakeupBgEvictor(TierId tid, PoolId pid, ClassId cid) {
  // TODO: should we also work on lower tiers? should we have separate set of params?
  if (tid == 1) return false;
  return (1-getACStats(tid, pid, cid).usageFraction())*100 <= config_.lowEvictionAcWatermark;
}
 
template <typename CacheTrait>
size_t CacheAllocator<CacheTrait>::backgroundWorkerId(TierId tid, PoolId pid, ClassId cid, size_t numWorkers) {
  XDCHECK(numWorkers);

  // TODO: came up with some better sharding (use some hashing)
  return (tid + pid + cid) % numWorkers;
}

template <typename CacheTrait>
std::vector<typename CacheAllocator<CacheTrait>::WriteHandle>
CacheAllocator<CacheTrait>::allocateInternalTierBatch(TierId tid,
                                                 PoolId pid,
                                                 ClassId cid,
                                                 std::vector<Item*> oldItems) {
  //util::LatencyTracker tracker{stats().allocateLatency_};

  SCOPE_FAIL { stats_.invalidAllocs.inc(); };

  //util::RollingLatencyTracker rollTracker{
  //    (*stats_.classAllocLatency)[tid][pid][cid]};

  auto batch = oldItems.size();
  std::vector<WriteHandle> handles;
  handles.reserve(batch);
  (*stats_.allocAttempts)[tid][pid][cid].inc();

  std::vector<void*> allocs(batch);
  std::vector<bool> allocsIncl(batch);
  std::vector<bool> active(batch);
  //first check which ones are inclusive and have copy
  bool lastTier = tid+1 >= getNumTiers();
  uint32_t inclusives = 0;
  if (lastTier) {
    for (int i = 0; i < batch; i++) {
        if (oldItems[i]->isInclusive() && 
            oldItems[i]->getNextTierCopy()) {
          allocs[i] = oldItems[i]->getNextTierCopy();
          allocsIncl[i] = true;
          active[i] = true;
          inclusives++;
        } else {
          allocsIncl[i] = false;
          active[i] = false;
          allocs[i] = nullptr;
        }
    }
  }
  if (inclusives != allocs.size()) {
    uint32_t totalAllocs = inclusives;
    auto allocs2 = allocator_[tid]->allocateBatchByClass(pid, cid, batch-inclusives);
    int j = 0;
    for (int i = 0; i < allocs.size(); i++) {
        if (active[i] == 0 && j < allocs2.size()) {
            allocs[i] = allocs2[j++];
            active[i] = true;
            totalAllocs++;
        }
    }
    if (totalAllocs < batch) {
      std::vector<void*> allocs3;
      findEviction(tid, pid, cid, allocs3, batch - totalAllocs);
      int k = 0;
      for (int i = 0; i < allocs.size(); i++) {
          if (active[i] == 0 && k < allocs3.size()) {
              allocs[i] = allocs3[k++];
              active[i] = true;
              totalAllocs++;
          }
      }
      XDCHECK_EQ(totalAllocs,batch);
    }
  }

  XDCHECK_EQ(allocs.size(),batch);

  for (int i = 0; i < allocs.size(); i++) {
    Item *oldItem = oldItems[i];
    if (allocsIncl[i]) {
      auto nextTierItemHdl = acquire(static_cast<Item*>(allocs[i]));
      handles.push_back(std::move(nextTierItemHdl));
    } else {
      WriteHandle handle = acquire(new (allocs[i]) Item(oldItem->getKey(), oldItem->getSize(), 
                                        oldItem->getCreationTime(), oldItem->getExpiryTime()));
      if (handle) {
        handle.markNascent();
        (*stats_.fragmentationSize)[tid][pid][cid].add(
            util::getFragmentation(*this, *handle));
        handles.push_back(std::move(handle));
      } else {
        (*stats_.allocFailures)[tid][pid][cid].inc();
        allocator_[tid]->free(allocs[i]);
      }
    }
  }
  return handles;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::allocateInternalTier(TierId tid,
                                                 PoolId pid,
                                                 typename Item::Key key,
                                                 uint32_t size,
                                                 uint32_t creationTime,
                                                 uint32_t expiryTime,
                                                 bool fromBgThread) {
  util::LatencyTracker tracker{stats().allocateLatency_};

  SCOPE_FAIL { stats_.invalidAllocs.inc(); };

  // number of bytes required for this item
  const auto requiredSize = Item::getRequiredSize(key, size);

  // the allocation class in our memory allocator.
  const auto cid = allocator_[tid]->getAllocationClassId(pid, requiredSize);
  util::RollingLatencyTracker rollTracker{
      (*stats_.classAllocLatency)[tid][pid][cid]};

  (*stats_.allocAttempts)[tid][pid][cid].inc();
  
  void* memory = allocator_[tid]->allocate(pid, requiredSize);
  
  if (backgroundEvictor_.size() && !fromBgThread && (memory == nullptr || shouldWakeupBgEvictor(tid, pid, cid))) {
    backgroundEvictor_[backgroundWorkerId(tid, pid, cid, backgroundEvictor_.size())]->wakeUp();
  }
  
  if (memory == nullptr) {
    memory = findEviction(tid, pid, cid);
  }

  WriteHandle handle;
  if (memory != nullptr) {
    // At this point, we have a valid memory allocation that is ready for use.
    // Ensure that when we abort from here under any circumstances, we free up
    // the memory. Item's handle could throw because the key size was invalid
    // for example.
    SCOPE_FAIL {
      // free back the memory to the allocator since we failed.
      allocator_[tid]->free(memory);
    };

    handle = acquire(new (memory) Item(key, size, creationTime, expiryTime));
    if (handle) {
      handle.markNascent();
      (*stats_.fragmentationSize)[tid][pid][cid].add(
          util::getFragmentation(*this, *handle));
    }

  } else { // failed to allocate memory.
    (*stats_.allocFailures)[tid][pid][cid].inc();
    // wake up rebalancer
    if (poolRebalancer_) {
      poolRebalancer_->wakeUp();
    }
  }

  if (auto eventTracker = getEventTracker()) {
    const auto result =
        handle ? AllocatorApiResult::ALLOCATED : AllocatorApiResult::FAILED;
    eventTracker->record(AllocatorApiEvent::ALLOCATE, key, result, size,
                         expiryTime ? expiryTime - creationTime : 0);
  }

  return handle;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::allocateInternal(PoolId pid,
                                             typename Item::Key key,
                                             uint32_t size,
                                             uint32_t creationTime,
                                             uint32_t expiryTime,
                                             bool fromBgThread) {
  auto tid = 0; /* TODO: consult admission policy */
  for(TierId tid = 0; tid < getNumTiers(); ++tid) {
    auto handle = allocateInternalTier(tid, pid, key, size, creationTime, expiryTime, fromBgThread);
    if (handle) return handle;
  }
  return {};
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::allocateChainedItem(const ReadHandle& parent,
                                                uint32_t size) {
  if (!parent) {
    throw std::invalid_argument(
        "Cannot call allocate chained item with a empty parent handle!");
  }

  auto it = allocateChainedItemInternal(parent, size);
  if (auto eventTracker = getEventTracker()) {
    const auto result =
        it ? AllocatorApiResult::ALLOCATED : AllocatorApiResult::FAILED;
    eventTracker->record(AllocatorApiEvent::ALLOCATE_CHAINED, parent->getKey(),
                         result, size, parent->getConfiguredTTL().count());
  }
  return it;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::allocateChainedItemInternal(
    const ReadHandle& parent, uint32_t size) {
  util::LatencyTracker tracker{stats().allocateLatency_};

  SCOPE_FAIL { stats_.invalidAllocs.inc(); };

  // number of bytes required for this item
  const auto requiredSize = ChainedItem::getRequiredSize(size);

  // TODO: is this correct?
  auto tid = getTierId(*parent);

  const auto pid = allocator_[tid]->getAllocInfo(parent->getMemory()).poolId;
  const auto cid = allocator_[tid]->getAllocationClassId(pid, requiredSize);

  util::RollingLatencyTracker rollTracker{
      (*stats_.classAllocLatency)[tid][pid][cid]};
  
  (*stats_.allocAttempts)[tid][pid][cid].inc();

  void* memory = allocator_[tid]->allocate(pid, requiredSize);
  if (memory == nullptr) {
    memory = findEviction(tid, pid, cid);
  }
  if (memory == nullptr) {
    (*stats_.allocFailures)[tid][pid][cid].inc();
    return WriteHandle{};
  }

  SCOPE_FAIL { allocator_[tid]->free(memory); };

  auto child = acquire(
      new (memory) ChainedItem(compressor_.compress(parent.getInternal()), size,
                               util::getCurrentTimeSec()));

  if (child) {
    child.markNascent();
    (*stats_.fragmentationSize)[tid][pid][cid].add(
        util::getFragmentation(*this, *child));
  }

  return child;
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::addChainedItem(WriteHandle& parent,
                                                WriteHandle child) {
  if (!parent || !child || !child->isChainedItem()) {
    throw std::invalid_argument(
        folly::sformat("Invalid parent or child. parent: {}, child: {}",
                       parent ? parent->toString() : "nullptr",
                       child ? child->toString() : "nullptr"));
  }

  auto l = chainedItemLocks_.lockExclusive(parent->getKey());

  // Insert into secondary lookup table for chained allocation
  auto oldHead = chainedItemAccessContainer_->insertOrReplace(*child);
  if (oldHead) {
    child->asChainedItem().appendChain(oldHead->asChainedItem(), compressor_);
  }

  // Count an item that just became a new parent
  if (!parent->hasChainedItem()) {
    stats_.numChainedParentItems.inc();
  }
  // Parent needs to be marked before inserting child into MM container
  // so the parent-child relationship is established before an eviction
  // can be triggered from the child
  parent->markHasChainedItem();
  // Count a new child
  stats_.numChainedChildItems.inc();

  // Increment refcount since this chained item is now owned by the parent
  // Parent will decrement the refcount upon release. Since this is an
  // internal refcount, we dont include it in active handle tracking.
  auto ret = child->incRef(true);
  XDCHECK(ret == RefcountWithFlags::incResult::incOk);
  XDCHECK_EQ(2u, child->getRefCount());

  insertInMMContainer(*child);

  invalidateNvm(*parent);
  if (auto eventTracker = getEventTracker()) {
    eventTracker->record(AllocatorApiEvent::ADD_CHAINED, parent->getKey(),
                         AllocatorApiResult::INSERTED, child->getSize(),
                         child->getConfiguredTTL().count());
  }
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::popChainedItem(WriteHandle& parent) {
  if (!parent || !parent->hasChainedItem()) {
    throw std::invalid_argument(folly::sformat(
        "Invalid parent {}", parent ? parent->toString() : nullptr));
  }

  WriteHandle head;
  { // scope of chained item lock.
    auto l = chainedItemLocks_.lockExclusive(parent->getKey());

    head = findChainedItem(*parent);
    if (head->asChainedItem().getNext(compressor_) != nullptr) {
      chainedItemAccessContainer_->insertOrReplace(
          *head->asChainedItem().getNext(compressor_));
    } else {
      chainedItemAccessContainer_->remove(*head);
      parent->unmarkHasChainedItem();
      stats_.numChainedParentItems.dec();
    }
    head->asChainedItem().setNext(nullptr, compressor_);

    invalidateNvm(*parent);
  }
  const auto res = removeFromMMContainer(*head);
  XDCHECK(res == true);

  // decrement the refcount to indicate this item is unlinked from its parent
  head->decRef();
  stats_.numChainedChildItems.dec();

  if (auto eventTracker = getEventTracker()) {
    eventTracker->record(AllocatorApiEvent::POP_CHAINED, parent->getKey(),
                         AllocatorApiResult::REMOVED, head->getSize(),
                         head->getConfiguredTTL().count());
  }

  return head;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::Key
CacheAllocator<CacheTrait>::getParentKey(const Item& chainedItem) {
  XDCHECK(chainedItem.isChainedItem());
  if (!chainedItem.isChainedItem()) {
    throw std::invalid_argument(folly::sformat(
        "Item must be chained item! Item: {}", chainedItem.toString()));
  }
  return reinterpret_cast<const ChainedItem&>(chainedItem)
      .getParentItem(compressor_)
      .getKey();
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::transferChainLocked(WriteHandle& parent,
                                                     WriteHandle& newParent) {
  // parent must be in a state to not have concurrent readers. Eviction code
  // paths rely on holding the last item handle. Since we hold on to an item
  // handle here, the chain will not be touched by any eviction code path.
  XDCHECK(parent);
  XDCHECK(newParent);
  XDCHECK_EQ(parent->getKey(), newParent->getKey());
  XDCHECK(parent->hasChainedItem());

  if (newParent->hasChainedItem()) {
    throw std::invalid_argument(folly::sformat(
        "New Parent {} has invalid state", newParent->toString()));
  }

  auto headHandle = findChainedItem(*parent);
  XDCHECK(headHandle);

  // remove from the access container since we are changing the key
  chainedItemAccessContainer_->remove(*headHandle);

  // change the key of the chain to have them belong to the new parent.
  ChainedItem* curr = &headHandle->asChainedItem();
  const auto newParentPtr = compressor_.compress(newParent.get());
  while (curr) {
    XDCHECK_EQ(curr == headHandle.get() ? 2u : 1u, curr->getRefCount());
    XDCHECK(curr->isInMMContainer());
    curr->changeKey(newParentPtr);
    curr = curr->getNext(compressor_);
  }

  newParent->markHasChainedItem();
  auto oldHead = chainedItemAccessContainer_->insertOrReplace(*headHandle);
  if (oldHead) {
    throw std::logic_error(
        folly::sformat("Did not expect to find an existing chain for {}",
                       newParent->toString(), oldHead->toString()));
  }
  parent->unmarkHasChainedItem();
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::transferChainAndReplace(
    WriteHandle& parent, WriteHandle& newParent) {
  if (!parent || !newParent) {
    throw std::invalid_argument("invalid parent or new parent");
  }
  { // scope for chained item lock
    auto l = chainedItemLocks_.lockExclusive(parent->getKey());
    transferChainLocked(parent, newParent);
  }

  if (replaceIfAccessible(*parent, *newParent)) {
    newParent.unmarkNascent();
  }
  invalidateNvm(*parent);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::replaceIfAccessible(Item& oldItem,
                                                     Item& newItem) {
  XDCHECK(!newItem.isAccessible());

  // Inside the access container's lock, this checks if the old item is
  // accessible, and only in that case replaces it. If the old item is not
  // accessible anymore, it may have been replaced or removed earlier and there
  // is no point in proceeding with a move.
  if (!accessContainer_->replaceIfAccessible(oldItem, newItem)) {
    return false;
  }

  // Inside the MM container's lock, this checks if the old item exists to
  // make sure that no other thread removed it, and only then replaces it.
  if (!replaceInMMContainer(oldItem, newItem)) {
    accessContainer_->remove(newItem);
    return false;
  }

  // Replacing into the MM container was successful, but someone could have
  // called insertOrReplace() or remove() before or after the
  // replaceInMMContainer() operation, which would invalidate newItem.
  if (!newItem.isAccessible()) {
    removeFromMMContainer(newItem);
    return false;
  }
  return true;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::replaceChainedItem(Item& oldItem,
                                               WriteHandle newItemHandle,
                                               Item& parent) {
  if (!newItemHandle) {
    throw std::invalid_argument("Empty handle for newItem");
  }
  auto l = chainedItemLocks_.lockExclusive(parent.getKey());

  if (!oldItem.isChainedItem() || !newItemHandle->isChainedItem() ||
      &oldItem.asChainedItem().getParentItem(compressor_) !=
          &newItemHandle->asChainedItem().getParentItem(compressor_) ||
      &oldItem.asChainedItem().getParentItem(compressor_) != &parent ||
      newItemHandle->isInMMContainer() || !oldItem.isInMMContainer()) {
    throw std::invalid_argument(folly::sformat(
        "Invalid args for replaceChainedItem. oldItem={}, newItem={}, "
        "parent={}",
        oldItem.toString(), newItemHandle->toString(), parent.toString()));
  }

  auto oldItemHdl =
      replaceChainedItemLocked(oldItem, std::move(newItemHandle), parent);
  invalidateNvm(parent);
  return oldItemHdl;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::replaceChainedItemLocked(Item& oldItem,
                                                     WriteHandle newItemHdl,
                                                     const Item& parent) {
  XDCHECK(newItemHdl != nullptr);
  XDCHECK_GE(1u, oldItem.getRefCount());

  // grab the handle to the old item so that we can return this. Also, we need
  // to drop the refcount the parent holds on oldItem by manually calling
  // decRef.  To do that safely we need to have a proper outstanding handle.
  auto oldItemHdl = acquire(&oldItem);

  // Replace the old chained item with new item in the MMContainer before we
  // actually replace the old item in the chain

  if (!replaceChainedItemInMMContainer(oldItem, *newItemHdl)) {
    // This should never happen since we currently hold an valid
    // parent handle. None of its chained items can be removed
    throw std::runtime_error(folly::sformat(
        "chained item cannot be replaced in MM container, oldItem={}, "
        "newItem={}, parent={}",
        oldItem.toString(), newItemHdl->toString(), parent.toString()));
  }

  XDCHECK(!oldItem.isInMMContainer());
  XDCHECK(newItemHdl->isInMMContainer());

  auto head = findChainedItem(parent);
  XDCHECK(head != nullptr);
  XDCHECK_EQ(reinterpret_cast<uintptr_t>(
                 &head->asChainedItem().getParentItem(compressor_)),
             reinterpret_cast<uintptr_t>(&parent));

  // if old item is the head, replace the head in the chain and insert into
  // the access container and append its chain.
  if (head.get() == &oldItem) {
    chainedItemAccessContainer_->insertOrReplace(*newItemHdl);
  } else {
    // oldItem is in the middle of the chain, find its previous and fix the
    // links
    auto* prev = &head->asChainedItem();
    auto* curr = prev->getNext(compressor_);
    while (curr != nullptr && curr != &oldItem) {
      prev = curr;
      curr = curr->getNext(compressor_);
    }

    XDCHECK(curr != nullptr);
    prev->setNext(&newItemHdl->asChainedItem(), compressor_);
  }

  newItemHdl->asChainedItem().setNext(
      oldItem.asChainedItem().getNext(compressor_), compressor_);
  oldItem.asChainedItem().setNext(nullptr, compressor_);

  // this should not result in 0 refcount. We are bumping down the internal
  // refcount. If it did, we would leak an item.
  oldItem.decRef();
  XDCHECK_LT(0u, oldItem.getRefCount()) << oldItem.toString();

  // increment refcount to indicate parent owns this similar to addChainedItem
  // Since this is an internal refcount, we dont include it in active handle
  // tracking.

  auto ret = newItemHdl->incRef(true);
  XDCHECK(ret == RefcountWithFlags::incResult::incOk);
  return oldItemHdl;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::ReleaseRes
CacheAllocator<CacheTrait>::releaseBackToAllocator(Item& it,
                                                   RemoveContext ctx,
                                                   bool nascent,
                                                   const Item* toRecycle) {
  if (it.wasPromoted()) {
      it.unmarkPromoted();
  }
  if (it.isRecent()) {
      it.unmarkRecent();
  }
  if (!it.isDrained()) {
    XDCHECK(it.isDrained()) << it.toString();
    throw std::runtime_error(
        folly::sformat("cannot release this item: {}", it.toString()));
  }
  const auto tid = getTierId(it);
  const auto allocInfo = allocator_[tid]->getAllocInfo(it.getMemory());
  if (it.getNextTierCopy()) {
      it.setNextTierCopy(0);
  }

  if (ctx == RemoveContext::kEviction) {
    const auto timeNow = util::getCurrentTimeSec();
    const auto refreshTime = timeNow - it.getLastAccessTime();
    const auto lifeTime = timeNow - it.getCreationTime();
    stats_.ramEvictionAgeSecs_.trackValue(refreshTime);
    stats_.ramItemLifeTimeSecs_.trackValue(lifeTime);
    stats_.perPoolEvictionAgeSecs_[allocInfo.poolId].trackValue(refreshTime);
  }

  (*stats_.fragmentationSize)[tid][allocInfo.poolId][allocInfo.classId].sub(
      util::getFragmentation(*this, it));

  // Chained items can only end up in this place if the user has allocated
  // memory for a chained item but has decided not to insert the chained item
  // to a parent item and instead drop the chained item handle. In this case,
  // we free the chained item directly without calling remove callback.
  if (it.isChainedItem()) {
    if (toRecycle) {
      throw std::runtime_error(
          folly::sformat("Can not recycle a chained item {}, toRecyle",
                         it.toString(), toRecycle->toString()));
    }
    allocator_[tid]->free(&it);
    return ReleaseRes::kReleased;
  }

  // nascent items represent items that were allocated but never inserted into
  // the cache. We should not be executing removeCB for them since they were
  // not initialized from the user perspective and never part of the cache.
  if (!nascent && config_.removeCb) {
    config_.removeCb(RemoveCbData{ctx, it, viewAsChainedAllocsRange(it)});
  }

  // only skip destructor for evicted items that are either in the queue to put
  // into nvm or already in nvm
  bool skipDestructor =
      nascent || (ctx == RemoveContext::kEviction &&
                  // When this item is queued for NvmCache, it will be marked
                  // as clean and the NvmEvicted bit will also be set to false.
                  // Refer to NvmCache::put()
                  it.isNvmClean() && !it.isNvmEvicted());
  if (!skipDestructor) {
    if (ctx == RemoveContext::kEviction) {
      stats().numCacheEvictions.inc();
    }
    // execute ItemDestructor
    if (config_.itemDestructor) {
      try {
        config_.itemDestructor(DestructorData{
            ctx, it, viewAsChainedAllocsRange(it), allocInfo.poolId});
        stats().numRamDestructorCalls.inc();
      } catch (const std::exception& e) {
        stats().numDestructorExceptions.inc();
        XLOG_EVERY_N(INFO, 100)
            << "Catch exception from user's item destructor: " << e.what();
      }
    }
  }

  // If no `toRecycle` is set, then the result is kReleased
  // Because this function cannot fail to release "it"
  ReleaseRes res =
      toRecycle == nullptr ? ReleaseRes::kReleased : ReleaseRes::kNotRecycled;

  // Free chained allocs if there are any
  if (it.hasChainedItem()) {
    // At this point, the parent is only accessible within this thread
    // and thus no one else can add or remove any chained items associated
    // with this parent. So we're free to go through the list and free
    // chained items one by one.
    auto headHandle = findChainedItem(it);
    ChainedItem* head = &headHandle.get()->asChainedItem();
    headHandle.reset();

    if (head == nullptr || &head->getParentItem(compressor_) != &it) {
      throw std::runtime_error(folly::sformat(
          "Mismatch parent pointer. This should not happen. Key: {}",
          it.getKey()));
    }

    if (!chainedItemAccessContainer_->remove(*head)) {
      throw std::runtime_error(folly::sformat(
          "Chained item associated with {} cannot be removed from hash table "
          "This should not happen here.",
          it.getKey()));
    }

    while (head) {
      auto next = head->getNext(compressor_);

      const auto childInfo =
          allocator_[tid]->getAllocInfo(static_cast<const void*>(head));
      (*stats_.fragmentationSize)[tid][childInfo.poolId][childInfo.classId].sub(
          util::getFragmentation(*this, *head));

      removeFromMMContainer(*head);

      // If this chained item is marked as moving, we will not free it.
      // We must capture the moving state before we do the decRef when
      // we know the item must still be valid. Item cannot be marked as
      // exclusive. Only parent can be marked as such and even parent needs
      // to be unmark prior to calling releaseBackToAllocator.
      const bool wasMoving = head->isMoving();
      XDCHECK(!head->isMarkedForEviction());

      // Decref and check if we were the last reference. Now if the item
      // was marked moving, after decRef, it will be free to be released
      // by slab release thread
      const auto childRef = head->decRef();

      // If the item is already moving and we already decremented the
      // refcount, we don't need to free this item. We'll let the slab
      // release thread take care of that
      if (!wasMoving) {
        if (childRef != 0) {
          throw std::runtime_error(folly::sformat(
              "chained item refcount is not zero. We cannot proceed! "
              "Ref: {}, Chained Item: {}",
              childRef, head->toString()));
        }

        // Item is not moving and refcount is 0, we can proceed to
        // free it or recylce the memory
        if (head == toRecycle) {
          XDCHECK(ReleaseRes::kReleased != res);
          res = ReleaseRes::kRecycled;
        } else {
          allocator_[tid]->free(head);
        }
      }

      stats_.numChainedChildItems.dec();
      head = next;
    }
    stats_.numChainedParentItems.dec();
  }

  if (&it == toRecycle) {
    XDCHECK(ReleaseRes::kReleased != res);
    res = ReleaseRes::kRecycled;
  } else {
    XDCHECK(it.isDrained());
    allocator_[tid]->free(&it);
  }

  return res;
}

template <typename CacheTrait>
RefcountWithFlags::incResult CacheAllocator<CacheTrait>::incRef(Item& it, bool failIfMoving) {
   auto ret = it.incRef(failIfMoving);
   if (ret == RefcountWithFlags::incResult::incOk) {
     ++handleCount_.tlStats();
   }
   return ret;
}

template <typename CacheTrait>
RefcountWithFlags::Value CacheAllocator<CacheTrait>::decRef(Item& it) {
  const auto ret = it.decRef();
  // do this after we ensured that we incremented a reference.
  --handleCount_.tlStats();
  return ret;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::acquire(Item* it) {
  if (UNLIKELY(!it)) {
    return WriteHandle{};
  }

  SCOPE_FAIL { stats_.numRefcountOverflow.inc(); };

  // TODO: do not block incRef for child items to avoid deadlock
  const auto failIfMoving = getNumTiers() > 1 && !it->isChainedItem();

  while (true) {
    auto incRes = incRef(*it, failIfMoving);
    if (LIKELY(incRes == RefcountWithFlags::incResult::incOk)) {
      return WriteHandle{it, *this};
    } else if (incRes == RefcountWithFlags::incResult::incFailedEviction){
      // item is being evicted
      return WriteHandle{};
    } else {
      // item is being moved - wait for completion
      WriteHandle handle;
      if (tryGetHandleWithWaitContextForMovingItem(*it, handle))
        return handle;
    }
  }
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::release(Item* it, bool isNascent) {
  // decrement the reference and if it drops to 0, release it back to the
  // memory allocator, and invoke the removal callback if there is one.
  if (UNLIKELY(!it)) {
    return;
  }

  const auto ref = decRef(*it);

  if (UNLIKELY(ref == 0)) {
    const auto res =
        releaseBackToAllocator(*it, RemoveContext::kNormal, isNascent);
    XDCHECK(res == ReleaseRes::kReleased);
  }
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::removeFromMMContainer(Item& item) {
  // remove it from the mm container.
  if (item.isInMMContainer()) {
    auto& mmContainer = getMMContainer(item);
    return mmContainer.remove(item);
  }
  return false;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::replaceInMMContainer(Item& oldItem,
                                                      Item& newItem) {
  auto& oldContainer = getMMContainer(oldItem);
  auto& newContainer = getMMContainer(newItem);
  if (&oldContainer == &newContainer) {
    return oldContainer.replace(oldItem, newItem);
  } else {
    return oldContainer.remove(oldItem) && newContainer.add(newItem);
  }
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::replaceInMMContainer(Item* oldItem,
                                                      Item& newItem) {
  return replaceInMMContainer(*oldItem, newItem);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::replaceInMMContainer(EvictionIterator& oldItemIt,
                                                      Item& newItem) {
  auto& oldContainer = getMMContainer(*oldItemIt);
  auto& newContainer = getMMContainer(newItem);

  // This function is used for eviction across tiers
  XDCHECK(&oldContainer != &newContainer);
  oldContainer.remove(oldItemIt);

  return newContainer.add(newItem);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::replaceChainedItemInMMContainer(
    Item& oldItem, Item& newItem) {
  auto& oldMMContainer = getMMContainer(oldItem);
  auto& newMMContainer = getMMContainer(newItem);
  if (&oldMMContainer == &newMMContainer) {
    return oldMMContainer.replace(oldItem, newItem);
  } else {
    if (!oldMMContainer.remove(oldItem)) {
      return false;
    }

    // This cannot fail because a new item should not have been inserted
    const auto newRes = newMMContainer.add(newItem);
    XDCHECK(newRes);
    return true;
  }
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::insertInMMContainer(Item& item) {
  XDCHECK(!item.isInMMContainer());
  auto& mmContainer = getMMContainer(item);
  if (!mmContainer.add(item)) {
    throw std::runtime_error(folly::sformat(
        "Invalid state. Node {} was already in the container.", &item));
  }
}

/**
 * There is a potential race with inserts and removes that. While T1 inserts
 * the key, there is T2 that removes the key. There can be an interleaving of
 * inserts and removes into the MM and Access Conatainers.It does not matter
 * what the outcome of this race is (ie  key can be present or not present).
 * However, if the key is not accessible, it should also not be in
 * MMContainer. To ensure that, we always add to MMContainer on inserts before
 * adding to the AccessContainer. Both the code paths on success/failure,
 * preserve the appropriate state in the MMContainer. Note that this insert
 * will also race with the removes we do in SlabRebalancing code paths.
 */

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::insert(const WriteHandle& handle) {
  return insertImpl(handle, AllocatorApiEvent::INSERT);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::insertImpl(const WriteHandle& handle,
                                            AllocatorApiEvent event) {
  XDCHECK(handle);
  XDCHECK(event == AllocatorApiEvent::INSERT ||
          event == AllocatorApiEvent::INSERT_FROM_NVM);
  if (handle->isAccessible()) {
    throw std::invalid_argument("Handle is already accessible");
  }

  if (nvmCache_ != nullptr && !handle->isNvmClean()) {
    throw std::invalid_argument("Can't use insert API with nvmCache enabled");
  }

  // insert into the MM container before we make it accessible. Find will
  // return this item as soon as it is accessible.
  insertInMMContainer(*(handle.getInternal()));

  AllocatorApiResult result;
  if (!accessContainer_->insert(*(handle.getInternal()))) {
    // this should destroy the handle and release it back to the allocator.
    removeFromMMContainer(*(handle.getInternal()));
    result = AllocatorApiResult::FAILED;
  } else {
    handle.unmarkNascent();
    result = AllocatorApiResult::INSERTED;
  }

  if (auto eventTracker = getEventTracker()) {
    eventTracker->record(event, handle->getKey(), result, handle->getSize(),
                         handle->getConfiguredTTL().count());
  }

  return result == AllocatorApiResult::INSERTED;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::insertOrReplace(const WriteHandle& handle) {
  XDCHECK(handle);
  if (handle->isAccessible()) {
    throw std::invalid_argument("Handle is already accessible");
  }

  HashedKey hk{handle->getKey()};

  insertInMMContainer(*(handle.getInternal()));
  WriteHandle replaced;
  try {
    auto lock = nvmCache_ ? nvmCache_->getItemDestructorLock(hk)
                          : std::unique_lock<std::mutex>();

    replaced = accessContainer_->insertOrReplace(*(handle.getInternal()));

    if (replaced) {
      replaced->unmarkPromoted();
      if (handle->isInclusive()) {
          handle->markDirty();
          handle->setNextTierCopy(replaced->getNextTierCopy());
          replaced->setNextTierCopy(0);
      }
      if (replaced->isNvmClean() && !replaced->isNvmEvicted()) {
        // item is to be replaced and the destructor will be executed
        // upon memory released, mark it in nvm to avoid destructor
        // executed from nvm
        nvmCache_->markNvmItemRemovedLocked(hk);
      }

    }
  } catch (const std::exception&) {
    removeFromMMContainer(*(handle.getInternal()));
    if (auto eventTracker = getEventTracker()) {
      eventTracker->record(AllocatorApiEvent::INSERT_OR_REPLACE,
                           handle->getKey(),
                           AllocatorApiResult::FAILED,
                           handle->getSize(),
                           handle->getConfiguredTTL().count());
    }
    throw;
  }

  // Remove from LRU as well if we do have a handle of old item
  if (replaced) {
    removeFromMMContainer(*replaced);
  }

  if (UNLIKELY(nvmCache_ != nullptr)) {
    // We can avoid nvm delete only if we have non nvm clean item in cache.
    // In all other cases we must enqueue delete.
    if (!replaced || replaced->isNvmClean()) {
      nvmCache_->remove(hk, nvmCache_->createDeleteTombStone(hk));
    }
  }

  handle.unmarkNascent();

  if (auto eventTracker = getEventTracker()) {
    XDCHECK(handle);
    const auto result =
        replaced ? AllocatorApiResult::REPLACED : AllocatorApiResult::INSERTED;
    eventTracker->record(AllocatorApiEvent::INSERT_OR_REPLACE, handle->getKey(),
                         result, handle->getSize(),
                         handle->getConfiguredTTL().count());
  }

  return replaced;
}

/* Next two methods are used to asynchronously move Item between memory tiers.
 *
 * The thread, which moves Item, allocates new Item in the tier we are moving to
 * and calls moveRegularItemWithSync() method. This method does the following:
 *  1. Update the access container with the new item from the tier we are
 *     moving to. This Item has moving flag set.
 *  2. Copy data from the old Item to the new one.
 *
 * Concurrent threads which are getting handle to the same key:
 *  1. When a handle is created it checks if the moving flag is set
 *  2. If so, Handle implementation creates waitContext and adds it to the
 *     MoveCtx by calling handleWithWaitContextForMovingItem() method.
 *  3. Wait until the moving thread will complete its job.
 */
template <typename CacheTrait>
bool
CacheAllocator<CacheTrait>::tryGetHandleWithWaitContextForMovingItem(Item& item, WriteHandle& handle) {
  auto shard = getShardForKey(item.getKey());
  auto& movesMap = getMoveMapForShard(shard);
  {
    auto lock = getMoveLockForShard(shard);

    // item might have been evicted or moved before the lock was acquired
    if (!item.isMoving())
      return false;

    WriteHandle hdl{*this};
    auto waitContext = hdl.getItemWaitContext();

    auto ret = movesMap.try_emplace(item.getKey(), std::make_unique<MoveCtx>());
    ret.first->second->addWaiter(std::move(waitContext));

    handle = std::move(hdl);
    return true;
  }
}

template <typename CacheTrait>
size_t CacheAllocator<CacheTrait>::wakeUpWaitersLocked(folly::StringPiece key,
  WriteHandle&& handle) {
  std::unique_ptr<MoveCtx> ctx;
  auto shard = getShardForKey(key);
  auto& movesMap = getMoveMapForShard(shard);
  {
    auto lock = getMoveLockForShard(shard);
    movesMap.eraseInto(key, [&](auto &&key, auto &&value) {
      ctx = std::move(value);
    });
  }

  if (ctx) {
    ctx->setItemHandle(std::move(handle));
    return ctx->numWaiters();
  }

  return 0;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::handleInclusiveWriteback(
        Item& oldItem, WriteHandle& newItemHdl, bool fromBgThread) {
  
  XDCHECK(oldItem.isMoving());

  if (!newItemHdl) {
      return false;
  }

  if (newItemHdl->getSize() != oldItem.getSize()) {
      return false;
  }

  /* 
   * we assume that the nextTier item is not in the mmContainer and is not
   * accessible
   */
  XDCHECK(!newItemHdl->isAccessible());
  //XDCHECK(!newItemHdl->isInMMContainer());
  
  auto tid = getTierId(oldItem);
  auto allocInfo = allocator_[tid]->getAllocInfo(oldItem.getMemory());
  auto pid = allocator_[tid]->getAllocInfo(oldItem.getMemory()).poolId;
  auto cid = allocator_[tid]->getAllocInfo(oldItem.getMemory()).classId;
  if (oldItem.isDirty()) {
    std::memcpy(newItemHdl->getMemory(), oldItem.getMemory(), oldItem.getSize());
    (*stats_.numWritebacks)[tid][pid][cid].inc();
  }

  XDCHECK(newItemHdl->isInclusive());
  newItemHdl->unmarkCopy();
  newItemHdl->setNextTierCopy(0);
  oldItem.setNextTierCopy(0);
  oldItem.unmarkCopy();
  //replace in accessContainer
  auto predicate = [&](const Item& item){
    // we rely on moving flag being set (it should block all readers)
    XDCHECK(item.getRefCount() == 0);
    return true;
  };

  if (!fromBgThread) {
    auto& newContainer = getMMContainer(*newItemHdl);
    auto mmContainerAdded = newContainer.add(*newItemHdl);
    XDCHECK(mmContainerAdded);
  }
  
  auto replaced = accessContainer_->replaceIf(oldItem, *newItemHdl,
                         predicate);
  if (!replaced) {
      auto& newContainer = getMMContainer(*newItemHdl);
      newContainer.remove(*newItemHdl);
      //we failed to replace - this means there was an insert or replace
      //so both items can and should be released back to allocator
      return replaced;
  }

  return true;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::moveRegularItemWithSync(
    Item& oldItem, WriteHandle& newItemHdl, bool fromBgThread) {

  XDCHECK(oldItem.isMoving());
  XDCHECK(!oldItem.isExpired());
  // TODO: should we introduce new latency tracker. E.g. evictRegularLatency_
  // ??? util::LatencyTracker tracker{stats_.evictRegularLatency_};

  XDCHECK_EQ(newItemHdl->getSize(), oldItem.getSize());

  // take care of the flags before we expose the item to be accessed. this
  // will ensure that when another thread removes the item from RAM, we issue
  // a delete accordingly. See D7859775 for an example
  if (oldItem.isNvmClean()) {
    newItemHdl->markNvmClean();
  }

  // mark new item as moving to block readers until the data is copied
  // (moveCb is called). Mark item in MMContainer temporarily (TODO: should
  // we remove markMoving requirement for the item to be linked?)

  auto predicate = [&](const Item& item){
    // we rely on moving flag being set (it should block all readers)
    //XDCHECK_EQ(item.getRefCount(),0);
    return true;
  };

  auto srcTid = getTierId(oldItem);
  auto dstTid = getTierId(*newItemHdl);
  if (oldItem.isInclusive()) {
    newItemHdl->markInclusive();
    if (srcTid > dstTid) { //promotion
      newItemHdl->setNextTierCopy(&oldItem);
      oldItem.markCopy();
      oldItem.setNextTierCopy(static_cast<void*>(newItemHdl.get()));
      XDCHECK_EQ(newItemHdl->getNextTierCopy(),&oldItem);
    } else if ( (srcTid < dstTid) ) { //evict to next tier
      newItemHdl->unmarkCopy();
      oldItem.setNextTierCopy(0);
      oldItem.unmarkCopy();
      newItemHdl->setNextTierCopy(0);
    }
    XDCHECK(newItemHdl->isInclusive());
  }

  if (srcTid > dstTid) {
    newItemHdl->markPromoted();
    XDCHECK(newItemHdl->isInMMContainer());
  }

  // what if another thread calls insertOrReplace now when
  // the item is moving and already replaced in the hash table?
  // 1. it succeeds in updating the hash table - so there is
  //    no guarentee that isAccessible() is true
  // 2. it will then try to remove from MM container
  //     - this operation will wait for newItemHdl to
  //       be unmarkedMoving via the waitContext
  // 3. replaced handle is returned and eventually drops
  //    ref to 0 and the item is recycled back to allocator.

  if (config_.moveCb) {
    // Execute the move callback. We cannot make any guarantees about the
    // consistency of the old item beyond this point, because the callback can
    // do more than a simple memcpy() e.g. update external references. If there
    // are any remaining handles to the old item, it is the caller's
    // responsibility to invalidate them. The move can only fail after this
    // statement if the old item has been removed or replaced, in which case it
    // should be fine for it to be left in an inconsistent state.
    config_.moveCb(oldItem, *newItemHdl, nullptr);
  } else {
    std::memcpy(newItemHdl->getMemory(), oldItem.getMemory(),
                oldItem.getSize());
  }

  if (!fromBgThread) {
    // Adding the item to mmContainer has to succeed since no one can remove the item
    auto& newContainer = getMMContainer(*newItemHdl);
    auto mmContainerAdded = newContainer.add(*newItemHdl);
    XDCHECK(mmContainerAdded);
  }

  // no one can add or remove chained items at this point
  if (oldItem.hasChainedItem()) {
    // safe to acquire handle for a moving Item
    auto incRes = incRef(oldItem, false);
    XDCHECK(incRes == RefcountWithFlags::incResult::incOk);
    auto oldHandle = WriteHandle{&oldItem,*this};
    XDCHECK_EQ(1u, oldHandle->getRefCount()) << oldHandle->toString();
    XDCHECK(!newItemHdl->hasChainedItem()) << newItemHdl->toString();
    try {
      auto l = chainedItemLocks_.lockExclusive(oldItem.getKey());
      transferChainLocked(oldHandle, newItemHdl);
    } catch (const std::exception& e) {
      // this should never happen because we drained all the handles.
      XLOGF(DFATAL, "{}", e.what());
      throw;
    }

    XDCHECK(!oldItem.hasChainedItem());
    XDCHECK(newItemHdl->hasChainedItem());
  }
  auto replaced = accessContainer_->replaceIf(oldItem, *newItemHdl,
                                   predicate);
  // another thread may have called insertOrReplace which could have
  // marked this item as unaccessible causing the replaceIf
  // in the access container to fail - in this case we want
  // to abort the move since the item is no longer valid
  if (!replaced) {
      auto& newContainer = getMMContainer(*newItemHdl);
      newContainer.remove(*newItemHdl);
      return false;
  }
  newItemHdl.unmarkNascent();
  return true;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::moveRegularItem(Item& oldItem,
                                                 WriteHandle& newItemHdl) {
  XDCHECK(config_.moveCb);
  util::LatencyTracker tracker{stats_.moveRegularLatency_};

  if (!oldItem.isAccessible() || oldItem.isExpired()) {
    return false;
  }

  XDCHECK_EQ(newItemHdl->getSize(), oldItem.getSize());
  XDCHECK_EQ(reinterpret_cast<uintptr_t>(&getMMContainer(oldItem)),
             reinterpret_cast<uintptr_t>(&getMMContainer(*newItemHdl)));

  // take care of the flags before we expose the item to be accessed. this
  // will ensure that when another thread removes the item from RAM, we issue
  // a delete accordingly. See D7859775 for an example
  if (oldItem.isNvmClean()) {
    newItemHdl->markNvmClean();
  }

  // Execute the move callback. We cannot make any guarantees about the
  // consistency of the old item beyond this point, because the callback can
  // do more than a simple memcpy() e.g. update external references. If there
  // are any remaining handles to the old item, it is the caller's
  // responsibility to invalidate them. The move can only fail after this
  // statement if the old item has been removed or replaced, in which case it
  // should be fine for it to be left in an inconsistent state.
  config_.moveCb(oldItem, *newItemHdl, nullptr);

  // Inside the access container's lock, this checks if the old item is
  // accessible and its refcount is one. If the item is not accessible,
  // there is no point to replace it since it had already been removed
  // or in the process of being removed. If the item is in cache but the
  // refcount is non-one, it means user could be attempting to remove
  // this item through an API such as remove(itemHandle). In this case,
  // it is unsafe to replace the old item with a new one, so we should
  // also abort.
  if (!accessContainer_->replaceIf(oldItem, *newItemHdl,
                                   itemSlabMovePredicate)) {
    return false;
  }

  // Inside the MM container's lock, this checks if the old item exists to
  // make sure that no other thread removed it, and only then replaces it.
  if (!replaceInMMContainer(oldItem, *newItemHdl)) {
    accessContainer_->remove(*newItemHdl);
    return false;
  }

  // Replacing into the MM container was successful, but someone could have
  // called insertOrReplace() or remove() before or after the
  // replaceInMMContainer() operation, which would invalidate newItemHdl.
  if (!newItemHdl->isAccessible()) {
    removeFromMMContainer(*newItemHdl);
    return false;
  }

  // no one can add or remove chained items at this point
  if (oldItem.hasChainedItem()) {
    auto oldItemHdl = acquire(&oldItem);
    XDCHECK_EQ(1u, oldItemHdl->getRefCount()) << oldItemHdl->toString();
    XDCHECK(!newItemHdl->hasChainedItem()) << newItemHdl->toString();
    try {
      auto l = chainedItemLocks_.lockExclusive(oldItem.getKey());
      transferChainLocked(oldItemHdl, newItemHdl);
    } catch (const std::exception& e) {
      // this should never happen because we drained all the handles.
      XLOGF(DFATAL, "{}", e.what());
      throw;
    }

    XDCHECK(!oldItem.hasChainedItem());
    XDCHECK(newItemHdl->hasChainedItem());
  }
  newItemHdl.unmarkNascent();
  return true;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::moveChainedItem(ChainedItem& oldItem,
                                                 WriteHandle& newItemHdl) {
  XDCHECK(config_.moveCb);
  util::LatencyTracker tracker{stats_.moveChainedLatency_};

  // This item has been unlinked from its parent and we're the only
  // owner of it, so we're done here
  if (!oldItem.isInMMContainer() || oldItem.isOnlyMoving()) {
    return false;
  }

  auto& expectedParent = oldItem.getParentItem(compressor_);
  const auto parentKey = expectedParent.getKey();
  auto l = chainedItemLocks_.lockExclusive(parentKey);

  // verify old item under the lock
  auto parentHandle =
      validateAndGetParentHandleForChainedMoveLocked(oldItem, parentKey);
  if (!parentHandle || &expectedParent != parentHandle.get()) {
    return false;
  }

  // once we have the moving sync and valid parent for the old item, check if
  // the original allocation was made correctly. If not, we destroy the
  // allocation to indicate a retry to moving logic above.
  if (reinterpret_cast<uintptr_t>(
          &newItemHdl->asChainedItem().getParentItem(compressor_)) !=
      reinterpret_cast<uintptr_t>(&parentHandle->asChainedItem())) {
    newItemHdl.reset();
    return false;
  }

  XDCHECK_EQ(reinterpret_cast<uintptr_t>(
                 &newItemHdl->asChainedItem().getParentItem(compressor_)),
             reinterpret_cast<uintptr_t>(&parentHandle->asChainedItem()));

  // In case someone else had removed this chained item from its parent by now
  // So we check again to see if the it has been unlinked from its parent
  if (!oldItem.isInMMContainer() || oldItem.isOnlyMoving()) {
    return false;
  }

  auto parentPtr = parentHandle.getInternal();

  XDCHECK_EQ(reinterpret_cast<uintptr_t>(parentPtr),
             reinterpret_cast<uintptr_t>(&oldItem.getParentItem(compressor_)));

  // Invoke the move callback to fix up any user data related to the chain
  config_.moveCb(oldItem, *newItemHdl, parentPtr);

  // Replace the new item in the position of the old one before both in the
  // parent's chain and the MMContainer.
  auto oldItemHandle =
      replaceChainedItemLocked(oldItem, std::move(newItemHdl), *parentHandle);
  XDCHECK(oldItemHandle->isMoving());
  XDCHECK(!oldItemHandle->isInMMContainer());

  return true;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::NvmCacheT::PutToken
CacheAllocator<CacheTrait>::createPutToken(Item& item) {
  const bool evictToNvmCache = shouldWriteToNvmCache(item);
  return evictToNvmCache ? nvmCache_->createPutToken(item.getKey())
                         : typename NvmCacheT::PutToken{};
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::unlinkItemForEviction(Item& it) {
  XDCHECK(it.isMarkedForEviction());
  XDCHECK(it.getRefCount() == 0);
  if (it.isCopy()) {
    XDCHECK(false);
    Item *tier1 = static_cast<Item*>(it.getNextTierCopy());
    if (tier1 != nullptr) tier1->setNextTierCopy(0);
    it.unmarkCopy();
  }
  accessContainer_->remove(it);
  removeFromMMContainer(it);

  if (it.wasPromoted()) {
      it.unmarkPromoted();
  }
  // Since we managed to mark the item for eviction we must be the only
  // owner of the item.
  const auto ref = it.unmarkForEviction();
  XDCHECK(ref == 0u) << it.toString();
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::findEviction(TierId tid, PoolId pid, ClassId cid, 
        std::vector<void*>& allocs, uint64_t batch) {
  auto& mmContainer = getMMContainer(tid, pid, cid);
  bool lastTier = tid+1 >= getNumTiers();
  /*
  if (!lastTier && getMMContainerStat(tid,pid,cid).size < 7000) {
      
      const Item* it;
      while (true) {
        it = reinterpret_cast<const Item*>(allocator_[tid]->getRandomAlloc());
        const auto allocInfo = allocator_[tid]->getAllocInfo(it->getMemory());
        if (allocInfo.classId == cid) {
            break;
        }
      }
      XDCHECK(it->isInMMContainer()) << it->toString();
  }
  */
  // Keep searching for a candidate until we were able to evict it
  // or until the search limit has been exhausted
  unsigned int searchTries = 0;
  std::vector<Item*> toRecycles; 
  std::vector<Item*> candidates;

  toRecycles.reserve(batch);
  candidates.reserve(batch);
  mmContainer.withEvictionIterator([this, batch, tid, pid, cid, &candidates, &toRecycles,
                                    &searchTries, &mmContainer, &lastTier](auto&& itr) {
    if (!itr) {
      ++searchTries;
      (*stats_.evictionAttempts)[tid][pid][cid].inc();
      return;
    }

    while (candidates.size() < batch && itr) {
      ++searchTries;
      (*stats_.evictionAttempts)[tid][pid][cid].inc();

      auto* toRecycle_ = itr.get();
      auto* candidate_ =
          toRecycle_->isChainedItem()
              ? &toRecycle_->asChainedItem().getParentItem(compressor_)
              : toRecycle_;

      XDCHECK_EQ(getTierId(candidate_),tid);
      if ( (lastTier && candidate_->markForEviction()) ||
                  (!lastTier && candidate_->markMoving(true)) ) {
        XDCHECK(candidate_->isMoving() || candidate_->isMarkedForEviction());
        // markForEviction to make sure no other thead is evicting the item
        // nor holding a handle to that item if this is last tier
        // since we won't be moving the item to the next tier
        // Check if parent changed for chained items - if yes, we cannot
        // remove the child from the mmContainer as we will not be evicting
        // it. We could abort right here, but we need to cleanup in case
        // unmarkForEviction() returns 0 - so just go through normal path.
        if (!toRecycle_->isChainedItem() ||
            &toRecycle_->asChainedItem().getParentItem(compressor_) ==
                candidate_) {
          toRecycles.push_back(toRecycle_);
          candidates.push_back(candidate_);
          mmContainer.remove(itr);
        } else if (toRecycle_->isChainedItem()) {
          XDCHECK(false);
          throw std::invalid_argument("batch not support for chained items");
        } else {
          XDCHECK(false);
        }
      } else {
        //unable to mark moving
        (*stats_.numWritebacksFailBadMove)[tid][pid][cid].inc();
        if (candidate_->hasChainedItem()) {
          stats_.evictFailParentAC.inc();
        } else {
          stats_.evictFailAC.inc();
        }
        ++itr;
      }
    }
  });

  if (!lastTier) {
    std::vector<WriteHandle> nextAllocs;
    std::vector<Item*> wbCandidates;
    nextAllocs.reserve(candidates.size());
    wbCandidates.reserve(candidates.size());
    uint32_t validItems = 0;
    for (int i = 0; i < candidates.size(); i++) {
        if (candidates[i]->isInclusive()) {
          Item *nextTierItem = static_cast<Item*>(candidates[i]->getNextTierCopy());
          if (nextTierItem) {
              nextAllocs.push_back(acquire(nextTierItem));
              validItems++;
          } else {
              nextAllocs.push_back(nullptr);
              wbCandidates.push_back(candidates[i]);
          }
        } else {
          nextAllocs.push_back(nullptr);
          wbCandidates.push_back(candidates[i]);
        }
    }
    if (validItems < candidates.size()) {
      auto wbAllocs = allocateInternalTierBatch(tid+1,pid,cid,wbCandidates);
      if (wbAllocs.size() != wbCandidates.size()) {
          throw std::invalid_argument("failed to alloc enough in next tier");
      }
      int j = 0;
      for (int i = 0; i < candidates.size(); i++) {
          if (nextAllocs[i] == nullptr) {
              XDCHECK_EQ(candidates[i]->getKey(),wbAllocs[j]->getKey());
              nextAllocs[i] = std::move(wbAllocs[j]);
              j++;
          }
      }
    }
    //we have enough allocs in next tier
    //step 1 copy the data
    std::vector<Item*> nextItems;
    nextItems.reserve(candidates.size());
    XDCHECK_EQ(candidates.size(),nextAllocs.size());
    for (int i = 0; i < candidates.size(); i++) {
      if (candidates[i]->getNextTierCopy()) {
        if (candidates[i]->isDirty()) {
          std::memcpy(nextAllocs[i]->getMemory(), candidates[i]->getMemory(), 
                      candidates[i]->getSize());
          (*stats_.numWritebacks)[tid][pid][cid].inc();
        }
        nextAllocs[i]->unmarkCopy();
        nextAllocs[i]->setNextTierCopy(0);
        candidates[i]->setNextTierCopy(0);
        candidates[i]->unmarkCopy();
      } else {
        std::memcpy(nextAllocs[i]->getMemory(), candidates[i]->getMemory(), 
                    candidates[i]->getSize());
        (*stats_.numWritebacks)[tid][pid][cid].inc();
      }
      nextItems.push_back(nextAllocs[i].get());
      if (candidates[i]->isInclusive()) {
        nextAllocs[i]->markInclusive();
      }
    }
    //step 2 add in to new mmContainer
    auto& newMMContainer = getMMContainer(tid+1, pid, cid);
    int added = newMMContainer.addBatch(nextItems);
    XDCHECK_EQ(added,-1);
    if (added != -1) {
      throw std::runtime_error("was not abel to add all items");
    }
    for (int i = 0; i < candidates.size(); i++) {
      //replace in accessContainer
      auto predicate = [&](const Item& item){
        // we rely on moving flag being set (it should block all readers)
        XDCHECK(item.getRefCount() == 0);
        return true;
      };
      XDCHECK_EQ(candidates[i]->getKey(),nextAllocs[i]->getKey());
      auto replaced = accessContainer_->replaceIf(*candidates[i],*nextAllocs[i],
                                                  predicate);
      XDCHECK(candidates[i]->isMoving());
      if (!replaced) {
        Item *copy = static_cast<Item*>(candidates[i]->getNextTierCopy());
        if (copy) {
            bool marked = copy->markForEvictionNotAccessible();
            while (!marked) {
              marked = copy->markForEvictionNotAccessible();
            }
            XDCHECK(marked);
            unlinkItemForEviction(*copy);
            const auto res =
                releaseBackToAllocator(*copy, RemoveContext::kNormal, false);
            XDCHECK(res == ReleaseRes::kReleased);
        }
        newMMContainer.remove(*candidates[i]);
        if (candidates[i]->wasPromoted()) {
            candidates[i]->unmarkPromoted();
        }
        //if (candidates[i]->isInclusive()) {
        //    candidates[i]->unmarkInclusive();
        //}
      }

      auto ref = candidates[i]->unmarkMoving();
      if (!replaced) {
        auto& newContainer = getMMContainer(*nextAllocs[i]);
        newContainer.remove(*nextAllocs[i]);
        //next alloc will be released back to its allocator on handle
        //destruction
        //for our current candidate - it is not longer linked, nor valid in hash table
        //candidate should still be moving but it is okay to unmark now
        XDCHECK_EQ(ref,0) << candidates[i]->toString();
        wakeUpWaiters(*candidates[i],{});
      } else {
        //yay this item was moved successfully
        wakeUpWaiters(*candidates[i],std::move(nextAllocs[i]));
      }
      if (candidates[i]->hasChainedItem()) {
        (*stats_.chainedItemEvictions)[tid][pid][cid].inc();
      } else {
        (*stats_.regularItemEvictions)[tid][pid][cid].inc();
      }
      auto ret = releaseBackToAllocator(*candidates[i], RemoveContext::kEviction,
                                         false, toRecycles[i]);
      XDCHECK_EQ(ret,ReleaseRes::kRecycled);
      allocs.push_back(toRecycles[i]);
    }
  } else {
    for (int i = 0; i < candidates.size(); i++) {
      accessContainer_->remove(*candidates[i]);
      auto res = candidates[i]->unmarkForEviction();
      XDCHECK_EQ(res,0u);
      //TODO NVM
      wakeUpWaiters(*candidates[i], {});
      if (candidates[i]->hasChainedItem()) {
        (*stats_.chainedItemEvictions)[tid][pid][cid].inc();
      } else {
        (*stats_.regularItemEvictions)[tid][pid][cid].inc();
      }
      // check if by releasing the item we intend to, we actually
      // recycle the candidate.
      auto ret = releaseBackToAllocator(*candidates[i], RemoveContext::kEviction,
                                        /* isNascent */ false, toRecycles[i]);
      XDCHECK_EQ(ret,ReleaseRes::kRecycled);
      allocs.push_back(toRecycles[i]);
    }
  }
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::Item*
CacheAllocator<CacheTrait>::findEviction(TierId tid, PoolId pid, ClassId cid) {
  auto& mmContainer = getMMContainer(tid, pid, cid);
  bool lastTier = tid+1 >= getNumTiers();
  /*
  if (!lastTier && getMMContainerStat(tid,pid,cid).size < 7000) {
      
      const Item* it;
      while (true) {
        it = reinterpret_cast<const Item*>(allocator_[tid]->getRandomAlloc());
        const auto allocInfo = allocator_[tid]->getAllocInfo(it->getMemory());
        if (allocInfo.classId == cid) {
            break;
        }
      }
      XDCHECK(it->isInMMContainer()) << it->toString();
  }
  */
  // Keep searching for a candidate until we were able to evict it
  // or until the search limit has been exhausted
  unsigned int searchTries = 0;
  while ((config_.evictionSearchTries == 0 ||
          config_.evictionSearchTries > searchTries)) {

    Item* toRecycle = nullptr;
    Item* candidate = nullptr;
    typename NvmCacheT::PutToken token;

    mmContainer.withEvictionIterator([this, tid, pid, cid, &candidate, &toRecycle,
                                      &searchTries, &mmContainer, &lastTier,
                                      &token](auto&& itr) {
      if (!itr) {
        ++searchTries;
        (*stats_.evictionAttempts)[tid][pid][cid].inc();
        return;
      }

      while ((config_.evictionSearchTries == 0 ||
              config_.evictionSearchTries > searchTries) &&
             itr) {
        ++searchTries;
        (*stats_.evictionAttempts)[tid][pid][cid].inc();

        auto* toRecycle_ = itr.get();
        auto* candidate_ =
            toRecycle_->isChainedItem()
                ? &toRecycle_->asChainedItem().getParentItem(compressor_)
                : toRecycle_;

        if (lastTier) {
          // if it's last tier, the item will be evicted
          // need to create put token before marking it exclusive
          token = createPutToken(*candidate_);
        }

        if (lastTier && shouldWriteToNvmCache(*candidate_) && !token.isValid()) {
          stats_.evictFailConcurrentFill.inc();
        } else if ( (lastTier && candidate_->markForEviction()) ||
                    (!lastTier && candidate_->markMoving(true)) ) {
          XDCHECK(candidate_->isMoving() || candidate_->isMarkedForEviction());
          // markForEviction to make sure no other thead is evicting the item
          // nor holding a handle to that item if this is last tier
          // since we won't be moving the item to the next tier
          toRecycle = toRecycle_;
          candidate = candidate_;

          // Check if parent changed for chained items - if yes, we cannot
          // remove the child from the mmContainer as we will not be evicting
          // it. We could abort right here, but we need to cleanup in case
          // unmarkForEviction() returns 0 - so just go through normal path.
          if (!toRecycle_->isChainedItem() ||
              &toRecycle->asChainedItem().getParentItem(compressor_) ==
                  candidate) {
            mmContainer.remove(itr);
          }
          return;
        } else {
          (*stats_.numWritebacksFailBadMove)[tid][pid][cid].inc();
        }

        if (candidate_->hasChainedItem()) {
          stats_.evictFailParentAC.inc();
        } else {
          stats_.evictFailAC.inc();
        }

        ++itr;
        XDCHECK(toRecycle == nullptr);
        XDCHECK(candidate == nullptr);
      }
    });

    if (!toRecycle)
      continue;

    XDCHECK(toRecycle);
    XDCHECK(candidate);

    auto evictedToNext = lastTier ? nullptr
        : tryEvictToNextMemoryTier(*candidate, false);
    if (!evictedToNext) {
      //if insertOrReplace was called during move
      //then candidate will not be accessible (failed replace during tryEvict)
      // - therefore this was why we failed to
      //   evict to the next tier and insertOrReplace
      //   will remove from NVM cache
      //however, if candidate is accessible
      //that means the allocation in the next
      //tier failed - so we will continue to
      //evict the item to NVM cache
      bool failedToReplace = !candidate->isAccessible();
      if (!token.isValid() && !failedToReplace) {
        token = createPutToken(*candidate);
      }
      // tryEvictToNextMemoryTier can fail if:
      //    a) allocation of the new item fails in that case,
      //       it should be still possible to mark item for eviction.
      //    b) another thread calls insertOrReplace and the item
      //       is no longer accessible
      //
      // in case that we are on the last tier, we whould have already marked
      // as exclusive since we will not be moving the item to the next tier
      // but rather just evicting all together, no need to
      // markForEvictionWhenMoving
      auto ret = lastTier ? true : candidate->markForEvictionWhenMoving();
      if (!lastTier) {
          Item *copy = static_cast<Item*>(candidate->getNextTierCopy());
          if (copy) {
              bool marked = copy->markForEvictionNotAccessible();
              while (!marked) {
                marked = copy->markForEvictionNotAccessible();
              }
              XDCHECK(marked);
              unlinkItemForEviction(*copy);
              const auto res =
                  releaseBackToAllocator(*copy, RemoveContext::kNormal, false);
              XDCHECK(res == ReleaseRes::kReleased);
          }
      }
      XDCHECK(ret);

      unlinkItemForEviction(*candidate);
      
      if (token.isValid() && shouldWriteToNvmCacheExclusive(*candidate)
              && !failedToReplace) {
        nvmCache_->put(*candidate, std::move(token));
      }
      // wake up any readers that wait for the move to complete
      // it's safe to do now, as we have the item marked exclusive and
      // no other reader can be added to the waiters list
      wakeUpWaiters(*candidate, {});

    } else {
      XDCHECK(!evictedToNext->isMarkedForEviction()) << evictedToNext->toString();
      XDCHECK(!evictedToNext->isMoving()) << evictedToNext->toString();
      XDCHECK(!candidate->isAccessible());
      XDCHECK(candidate->getKey() == evictedToNext->getKey());
      if (!candidate->isInclusive()) {
        (*stats_.numWritebacks)[tid][pid][cid].inc();
      }
      wakeUpWaiters(*candidate, std::move(evictedToNext));
    }

    XDCHECK(!candidate->isMarkedForEviction() && !candidate->isMoving());

    // recycle the item. it's safe to do so, even if toReleaseHandle was
    // NULL. If `ref` == 0 then it means that we are the last holder of
    // that item.
    if (candidate->hasChainedItem()) {
      (*stats_.chainedItemEvictions)[tid][pid][cid].inc();
    } else {
      (*stats_.regularItemEvictions)[tid][pid][cid].inc();
    }

    if (auto eventTracker = getEventTracker()) {
      eventTracker->record(AllocatorApiEvent::DRAM_EVICT, candidate->getKey(),
                           AllocatorApiResult::EVICTED, candidate->getSize(),
                           candidate->getConfiguredTTL().count());
    }

    // check if by releasing the item we intend to, we actually
    // recycle the candidate.
    auto ret = releaseBackToAllocator(*candidate, RemoveContext::kEviction,
                                      /* isNascent */ false, toRecycle);
    if (ret == ReleaseRes::kRecycled) {
      return toRecycle;
    }
  }
  return nullptr;
}

template <typename CacheTrait>
folly::Range<typename CacheAllocator<CacheTrait>::ChainedItemIter>
CacheAllocator<CacheTrait>::viewAsChainedAllocsRange(const Item& parent) const {
  return parent.hasChainedItem()
             ? folly::Range<ChainedItemIter>{ChainedItemIter{
                                                 findChainedItem(parent).get(),
                                                 compressor_},
                                             ChainedItemIter{}}
             : folly::Range<ChainedItemIter>{};
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::shouldWriteToNvmCache(const Item& item) {
  // write to nvmcache when it is enabled and the item says that it is not
  // nvmclean or evicted by nvm while present in DRAM.
  bool doWrite = nvmCache_ && nvmCache_->isEnabled();
  if (!doWrite) {
    return false;
  }

  doWrite = !item.isExpired();
  if (!doWrite) {
    stats_.numNvmRejectsByExpiry.inc();
    return false;
  }

  doWrite = (!item.isNvmClean() || item.isNvmEvicted());
  if (!doWrite) {
    stats_.numNvmRejectsByClean.inc();
    return false;
  }
  return true;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::shouldWriteToNvmCacheExclusive(
    const Item& item) {
  auto chainedItemRange = viewAsChainedAllocsRange(item);

  if (nvmAdmissionPolicy_ &&
      !nvmAdmissionPolicy_->accept(item, chainedItemRange)) {
    stats_.numNvmRejectsByAP.inc();
    return false;
  }

  return true;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::tryEvictToNextMemoryTier(
    TierId tid, PoolId pid, ClassId cid, Item& item, bool fromBgThread) {
  XDCHECK(item.isMoving());
  XDCHECK(item.getRefCount() == 0);
  if(item.hasChainedItem()) return WriteHandle{}; // TODO: We do not support ChainedItem yet
  if(item.isExpired()) {
    accessContainer_->remove(item);
    item.unmarkMoving();
    return acquire(&item);
  }
  if (item.isInclusive()) {
    Item *nextTierItem = static_cast<Item*>(item.getNextTierCopy());
    if (nextTierItem) {
        auto nextTierItemHdl = acquire(nextTierItem);
        if (fromBgThread) {
            return nextTierItemHdl;
        }
        if (!handleInclusiveWriteback(item, nextTierItemHdl, fromBgThread)) {
            (*stats_.numWritebacksFailBadMove)[tid][pid][cid].inc();
            return WriteHandle{};
        }
        XDCHECK_EQ(nextTierItemHdl->getKey(),item.getKey());
        item.setNextTierCopy(0);
        item.unmarkMoving();
        return nextTierItemHdl;
    } else {
        (*stats_.numWritebacks)[tid][pid][cid].inc();
        //else there is no nextTierCopy so we need to writeback
    }
  }

  TierId nextTier = tid; // TODO - calculate this based on some admission policy
  while (++nextTier < getNumTiers()) { // try to evict down to the next memory tiers
    // allocateInternal might trigger another eviction
    auto newItemHdl = allocateInternalTier(nextTier, pid,
                     item.getKey(),
                     item.getSize(),
                     item.getCreationTime(),
                     item.getExpiryTime(),
                     fromBgThread);

    if (newItemHdl) {
      XDCHECK_EQ(newItemHdl->getSize(), item.getSize());
      if (fromBgThread) {
          return newItemHdl;
      }
      if (!moveRegularItemWithSync(item, newItemHdl, fromBgThread)) {
          (*stats_.numWritebacksFailBadMove)[tid][pid][cid].inc();
          return WriteHandle{};
      }
      XDCHECK_EQ(newItemHdl->getKey(),item.getKey());
      item.unmarkMoving();
      return newItemHdl;
    } else {
      //failed due to no
      (*stats_.numWritebacksFailNoAlloc)[tid][pid][cid].inc();
      return WriteHandle{};
    }
  }

  return {};
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::tryEvictToNextMemoryTier(Item& item, bool fromBgThread) {
  auto tid = getTierId(item);
  auto info = allocator_[tid]->getAllocInfo(item.getMemory());
  return tryEvictToNextMemoryTier(tid, info.poolId, info.classId, item, fromBgThread);
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::tryPromoteToNextMemoryTier(
    TierId tid, PoolId pid, Item& item, bool fromBgThread) {
  if(item.isExpired()) { return {}; }
  TierId nextTier = tid;
  while (nextTier > 0) { // try to evict down to the next memory tiers
    auto toPromoteTier = nextTier - 1;
    --nextTier;

    // allocateInternal might trigger another eviction
    auto newItemHdl = allocateInternalTier(toPromoteTier, pid,
                     item.getKey(),
                     item.getSize(),
                     item.getCreationTime(),
                     item.getExpiryTime(),
                     fromBgThread);

    if (newItemHdl) {
      XDCHECK_EQ(newItemHdl->getSize(), item.getSize());
      if (fromBgThread) {
          return newItemHdl;
      }
      if (!moveRegularItemWithSync(item, newItemHdl, fromBgThread)) {
          return WriteHandle{};
      }
      item.unmarkMoving();
      return newItemHdl;
    } else {
      return WriteHandle{};
    }
  }

  return {};
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::tryPromoteToNextMemoryTier(Item& item, bool fromBgThread) {
    auto tid = getTierId(item);
    auto pid = allocator_[tid]->getAllocInfo(item.getMemory()).poolId;
    return tryPromoteToNextMemoryTier(tid, pid, item, fromBgThread);
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::RemoveRes
CacheAllocator<CacheTrait>::remove(typename Item::Key key) {
  // While we issue this delete, there can be potential races that change the
  // state of the cache between ram and nvm. If we find the item in RAM and
  // obtain a handle, the situation is simpler. The complicated ones are the
  // following scenarios where when the delete checks RAM, we don't find
  // anything in RAM. The end scenario is that in the absence of any
  // concurrent inserts, after delete, there should be nothing in nvm and ram.
  //
  // == Racing async fill from nvm with delete ==
  // 1. T1 finds nothing in ram and issues a nvmcache look that is async. We
  // enqueue the get holding the fill lock and drop it.
  // 2. T2 finds nothing in ram, enqueues delete to nvmcache.
  // 3. T1's async fetch finishes and fills the item in cache, but right
  // before the delete is enqueued above
  //
  // To deal with this race, we first enqueue the nvmcache delete tombstone
  // and  when we finish the async fetch, we check if a tombstone was enqueued
  // meanwhile and cancel the fill.
  //
  // == Racing async fill from nvm with delete ==
  // there is a key in nvmcache and nothing in RAM.
  // 1.  T1 issues delete while nothing is in RAM and enqueues nvm cache
  // remove
  // 2. before the nvmcache remove gets enqueued, T2 does a find() that
  // fetches from nvm.
  // 3. T2 inserts in cache from nvmcache and T1 observes that item and tries
  // to remove it only from RAM.
  //
  //  to fix this, we do the nvmcache remove always the last thing and enqueue
  //  a tombstone to avoid concurrent fills while we are in the process of
  //  doing the nvmcache remove.
  //
  // == Racing eviction with delete ==
  // 1. T1 is evicting an item, trying to remove from the hashtable and is in
  // the process  of enqueing the put to nvmcache.
  // 2. T2 is removing and finds nothing in ram, enqueue the nvmcache delete.
  // The delete to nvmcache gets enqueued after T1 fills in ram.
  //
  // If T2 finds the item in ram, eviction can not proceed and the race does
  // not exist. If T2 does not find anything in RAM, it is likely that T1 is
  // in the process of issuing an nvmcache put. In this case, T1's nvmcache
  // put will check if there was a delete enqueued while the eviction was in
  // flight after removing from the hashtable.
  //
  stats_.numCacheRemoves.inc();
  HashedKey hk{key};

  using Guard = typename NvmCacheT::DeleteTombStoneGuard;
  auto tombStone = nvmCache_ ? nvmCache_->createDeleteTombStone(hk) : Guard{};

  auto handle = findInternal(key);
  if (!handle) {
    if (nvmCache_) {
      nvmCache_->remove(hk, std::move(tombStone));
    }
    if (auto eventTracker = getEventTracker()) {
      eventTracker->record(AllocatorApiEvent::REMOVE, key,
                           AllocatorApiResult::NOT_FOUND);
    }
    return RemoveRes::kNotFoundInRam;
  }

  return removeImpl(hk, *handle, std::move(tombStone));
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::removeFromRamForTesting(
    typename Item::Key key) {
  return removeImpl(HashedKey{key}, *findInternal(key), DeleteTombStoneGuard{},
                    false /* removeFromNvm */) == RemoveRes::kSuccess;
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::removeFromNvmForTesting(
    typename Item::Key key) {
  if (nvmCache_) {
    HashedKey hk{key};
    nvmCache_->remove(hk, nvmCache_->createDeleteTombStone(hk));
  }
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::pushToNvmCacheFromRamForTesting(
    typename Item::Key key) {
  auto handle = findInternal(key);

  if (handle && nvmCache_ && shouldWriteToNvmCache(*handle) &&
      shouldWriteToNvmCacheExclusive(*handle)) {
    nvmCache_->put(*handle, nvmCache_->createPutToken(handle->getKey()));
    return true;
  }
  return false;
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::flushNvmCache() {
  if (nvmCache_) {
    nvmCache_->flushPendingOps();
  }
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::RemoveRes
CacheAllocator<CacheTrait>::remove(AccessIterator& it) {
  stats_.numCacheRemoves.inc();
  if (auto eventTracker = getEventTracker()) {
    eventTracker->record(AllocatorApiEvent::REMOVE, it->getKey(),
                         AllocatorApiResult::REMOVED, it->getSize(),
                         it->getConfiguredTTL().count());
  }
  HashedKey hk{it->getKey()};
  auto tombstone =
      nvmCache_ ? nvmCache_->createDeleteTombStone(hk) : DeleteTombStoneGuard{};
  return removeImpl(hk, *it, std::move(tombstone));
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::RemoveRes
CacheAllocator<CacheTrait>::remove(const ReadHandle& it) {
  stats_.numCacheRemoves.inc();
  if (!it) {
    throw std::invalid_argument("Trying to remove a null item handle");
  }
  HashedKey hk{it->getKey()};
  auto tombstone =
      nvmCache_ ? nvmCache_->createDeleteTombStone(hk) : DeleteTombStoneGuard{};
  return removeImpl(hk, *(it.getInternal()), std::move(tombstone));
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::RemoveRes
CacheAllocator<CacheTrait>::removeImpl(HashedKey hk,
                                       Item& item,
                                       DeleteTombStoneGuard tombstone,
                                       bool removeFromNvm,
                                       bool recordApiEvent) {
  bool success = false;
  {
    auto lock = nvmCache_ ? nvmCache_->getItemDestructorLock(hk)
                          : std::unique_lock<std::mutex>();

    success = accessContainer_->remove(item);

    if (removeFromNvm && success && item.isNvmClean() && !item.isNvmEvicted()) {
      // item is to be removed and the destructor will be executed
      // upon memory released, mark it in nvm to avoid destructor
      // executed from nvm
      nvmCache_->markNvmItemRemovedLocked(hk);
    }
  }
  XDCHECK(!item.isAccessible());

  // remove it from the mm container. this will be no-op if it is already
  // removed.
  removeFromMMContainer(item);

  // Enqueue delete to nvmCache if we know from the item that it was pulled in
  // from NVM. If the item was not pulled in from NVM, it is not possible to
  // have it be written to NVM.
  if (removeFromNvm && item.isNvmClean()) {
    XDCHECK(tombstone);
    nvmCache_->remove(hk, std::move(tombstone));
  }

  auto eventTracker = getEventTracker();
  if (recordApiEvent && eventTracker) {
    const auto result =
        success ? AllocatorApiResult::REMOVED : AllocatorApiResult::NOT_FOUND;
    eventTracker->record(AllocatorApiEvent::REMOVE, item.getKey(), result,
                         item.getSize(), item.getConfiguredTTL().count());
  }

  // the last guy with reference to the item will release it back to the
  // allocator.
  if (success) {
    stats_.numCacheRemoveRamHits.inc();
    return RemoveRes::kSuccess;
  }
  return RemoveRes::kNotFoundInRam;
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::invalidateNvm(Item& item) {
  if (nvmCache_ != nullptr && item.isAccessible() && item.isNvmClean()) {
    HashedKey hk{item.getKey()};
    {
      auto lock = nvmCache_->getItemDestructorLock(hk);
      if (!item.isNvmEvicted() && item.isNvmClean() && item.isAccessible()) {
        // item is being updated and invalidated in nvm. Mark the item to avoid
        // destructor to be executed from nvm
        nvmCache_->markNvmItemRemovedLocked(hk);
      }
      item.unmarkNvmClean();
    }
    nvmCache_->remove(hk, nvmCache_->createDeleteTombStone(hk));
  }
}

template <typename CacheTrait>
TierId
CacheAllocator<CacheTrait>::getTierId(const Item& item) const {
  return getTierId(item.getMemory());
}

template <typename CacheTrait>
TierId
CacheAllocator<CacheTrait>::getTierId(const void* ptr) const {
  for (TierId tid = 0; tid < getNumTiers(); tid++) {
    if (allocator_[tid]->isMemoryInAllocator(ptr))
      return tid;
  }

  throw std::invalid_argument("Item does not belong to any tier!");
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::MMContainer&
CacheAllocator<CacheTrait>::getMMContainer(const Item& item) const noexcept {
  const auto tid = getTierId(item);
  const auto allocInfo =
      allocator_[tid]->getAllocInfo(static_cast<const void*>(&item));
  return getMMContainer(tid, allocInfo.poolId, allocInfo.classId);
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::MMContainer&
CacheAllocator<CacheTrait>::getMMContainer(TierId tid,
                                           PoolId pid,
                                           ClassId cid) const noexcept {
  XDCHECK_LT(static_cast<size_t>(tid), mmContainers_.size());
  XDCHECK_LT(static_cast<size_t>(pid), mmContainers_[tid].size());
  XDCHECK_LT(static_cast<size_t>(cid), mmContainers_[tid][pid].size());
  return *mmContainers_[tid][pid][cid];
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::PromoQueue&
CacheAllocator<CacheTrait>::getPromoQueue(TierId tid,
                                           PoolId pid,
                                           ClassId cid) const noexcept {
  XDCHECK_LT(static_cast<size_t>(tid), promoQueues_.size());
  XDCHECK_LT(static_cast<size_t>(pid), promoQueues_[tid].size());
  XDCHECK_LT(static_cast<size_t>(cid), promoQueues_[tid][pid].size());
  return *promoQueues_[tid][pid][cid];
}

template <typename CacheTrait>
uint64_t CacheAllocator<CacheTrait>::getQueueSize(TierId tid,
                                           PoolId pid,
                                           ClassId cid) const noexcept {
  XDCHECK_LT(static_cast<size_t>(tid), promoQueues_.size());
  XDCHECK_LT(static_cast<size_t>(pid), promoQueues_[tid].size());
  XDCHECK_LT(static_cast<size_t>(cid), promoQueues_[tid][pid].size());
  return promoQueues_[tid][pid][cid]->size();
}

template <typename CacheTrait>
MMContainerStat CacheAllocator<CacheTrait>::getMMContainerStat(
    TierId tid, PoolId pid, ClassId cid) const noexcept {
  if(static_cast<size_t>(tid) >= mmContainers_.size()) {
    return MMContainerStat{};
  }
  if (static_cast<size_t>(pid) >= mmContainers_[tid].size()) {
    return MMContainerStat{};
  }
  if (static_cast<size_t>(cid) >= mmContainers_[tid][pid].size()) {
    return MMContainerStat{};
  }
  return mmContainers_[tid][pid][cid] ? mmContainers_[tid][pid][cid]->getStats()
                                 : MMContainerStat{};
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::ReadHandle
CacheAllocator<CacheTrait>::peek(typename Item::Key key) {
  return findInternal(key);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::couldExistFast(typename Item::Key key) {
  // At this point, a key either definitely exists or does NOT exist in cache
  auto handle = findFastInternal(key, AccessMode::kRead);
  if (handle) {
    if (handle->isExpired()) {
      return false;
    }
    return true;
  }

  if (!nvmCache_) {
    return false;
  }

  // When we have to go to NvmCache, we can only probalistically determine
  // if a key could possibly exist in cache, or definitely NOT exist.
  return nvmCache_->couldExistFast(HashedKey{key});
}

template <typename CacheTrait>
std::pair<typename CacheAllocator<CacheTrait>::ReadHandle,
          typename CacheAllocator<CacheTrait>::ReadHandle>
CacheAllocator<CacheTrait>::inspectCache(typename Item::Key key) {
  std::pair<ReadHandle, ReadHandle> res;
  res.first = findInternal(key);
  res.second = nvmCache_ ? nvmCache_->peek(key) : nullptr;
  return res;
}

// findFast and find() are the most performance critical parts of
// CacheAllocator. Hence the sprinkling of UNLIKELY/LIKELY to tell the
// compiler which executions we don't want to optimize on.
template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::findFastInternal(typename Item::Key key,
                                             AccessMode mode) {
  auto handle = findInternal(key);

  stats_.numCacheGets.inc();
  if (UNLIKELY(!handle)) {
    stats_.numCacheGetMiss.inc();
    return handle;
  }

  markUseful(handle, mode);
  return handle;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::findFastImpl(typename Item::Key key,
                                         AccessMode mode) {
  auto handle = findFastInternal(key, mode);
  auto eventTracker = getEventTracker();
  if (UNLIKELY(eventTracker != nullptr)) {
    if (handle) {
      eventTracker->record(AllocatorApiEvent::FIND_FAST, key,
                           AllocatorApiResult::FOUND,
                           folly::Optional<uint32_t>(handle->getSize()),
                           handle->getConfiguredTTL().count());
    } else {
      eventTracker->record(AllocatorApiEvent::FIND_FAST, key,
                           AllocatorApiResult::NOT_FOUND);
    }
  }
  return handle;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::ReadHandle
CacheAllocator<CacheTrait>::findFast(typename Item::Key key) {
  return findFastImpl(key, AccessMode::kRead);
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::findFastToWrite(typename Item::Key key,
                                            bool doNvmInvalidation) {
  auto handle = findFastImpl(key, AccessMode::kWrite);
  if (handle == nullptr) {
    return nullptr;
  }
  if (doNvmInvalidation) {
    invalidateNvm(*handle);
  }
  return handle;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::findImpl(typename Item::Key key, AccessMode mode) {
  auto handle = findFastInternal(key, mode);

  if (handle) {
    if (UNLIKELY(handle->isExpired())) {
      // update cache miss stats if the item has already been expired.
      stats_.numCacheGetMiss.inc();
      stats_.numCacheGetExpiries.inc();
      auto eventTracker = getEventTracker();
      if (UNLIKELY(eventTracker != nullptr)) {
        eventTracker->record(AllocatorApiEvent::FIND, key,
                             AllocatorApiResult::NOT_FOUND);
      }
      WriteHandle ret;
      ret.markExpired();
      return ret;
    }

    auto eventTracker = getEventTracker();
    if (UNLIKELY(eventTracker != nullptr)) {
      eventTracker->record(AllocatorApiEvent::FIND, key,
                           AllocatorApiResult::FOUND, handle->getSize(),
                           handle->getConfiguredTTL().count());
    }
    return handle;
  }

  auto eventResult = AllocatorApiResult::NOT_FOUND;

  if (nvmCache_) {
    handle = nvmCache_->find(HashedKey{key});
    eventResult = AllocatorApiResult::NOT_FOUND_IN_MEMORY;
  }

  auto eventTracker = getEventTracker();
  if (UNLIKELY(eventTracker != nullptr)) {
    eventTracker->record(AllocatorApiEvent::FIND, key, eventResult);
  }

  return handle;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::findToWrite(typename Item::Key key,
                                        bool doNvmInvalidation) {
  auto handle = findImpl(key, AccessMode::kWrite);
  if (handle == nullptr) {
    return nullptr;
  }
  if (doNvmInvalidation) {
    invalidateNvm(*handle);
  }
  return handle;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::ReadHandle
CacheAllocator<CacheTrait>::find(typename Item::Key key) {
  return findImpl(key, AccessMode::kRead);
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::markUseful(const ReadHandle& handle,
                                            AccessMode mode) {
  if (!handle) {
    return;
  }

  auto& item = *(handle.getInternal());
  XDCHECK(!item.isCopy());
  bool recorded = recordAccessInMMContainer(item, mode);

  /* 
   * CLUsivity logic
   *  - if item is not in first tier then we should mark it for
   *    promtion for the background reaper
   *  - if item is inclusive and in the first tier we should
   *    make the second tier item updated in access container
   */
  const auto tid = getTierId(item);
  const auto allocInfo = getAllocInfo(item.getMemory());
  const auto pid = allocInfo.poolId;
  const auto cid = allocInfo.classId;
  if (tid > 0) {
      //spawn a fiber to do the promotion
      if (mode == AccessMode::kRead) {
        //item.markRecent();
        Item *itemPtr = const_cast<Item*>(handle.get());
        if (config_.useThreadPool && itemPtr->getRefCount() == 1) {
        auto key = itemPtr->getKey();
        threadPool_.add( [this,key,itemPtr,tid,pid,cid]() {
            bool moving = itemPtr->markMoving(true);
            if (!moving) {
              return;
            }
            const auto stid = getTierId(*itemPtr);
            if (stid == 0 || key != itemPtr->getKey()) {
              auto ref = itemPtr->unmarkMoving();
              if (UNLIKELY(ref == 0)) {
	        wakeUpWaiters(*itemPtr,{});
                const auto res =
                    releaseBackToAllocator(*itemPtr, 
                            RemoveContext::kNormal, false);
                XDCHECK(res == ReleaseRes::kReleased);
              } else {
                //wake up waiters with this handle
	        auto hdl = acquire(itemPtr);
	        wakeUpWaiters(*itemPtr,std::move(hdl));
              }
              return;
            }
            auto& mmContainer = getMMContainer(tid, pid, cid);
            bool removed = mmContainer.remove(*itemPtr);
            if (!removed) {
                auto ref = itemPtr->unmarkMoving();
                if (UNLIKELY(ref == 0)) {
	          wakeUpWaiters(*itemPtr,{});
                  const auto res =
                      releaseBackToAllocator(*itemPtr, 
                              RemoveContext::kNormal, false);
                  XDCHECK(res == ReleaseRes::kReleased);
                } else {
                  //wake up waiters with this handle
	          auto hdl = acquire(itemPtr);
	          wakeUpWaiters(*itemPtr,std::move(hdl));
                }
                return;
            }
            XDCHECK(removed);
            XDCHECK(moving);
            const auto allocInfo = allocator_[stid]->getAllocInfo(itemPtr->getMemory());
            XDCHECK_EQ(allocInfo.poolId,pid);
            XDCHECK_EQ(allocInfo.classId,cid);
            XCHECK_EQ(stid,1);
            XCHECK_EQ(stid,tid);
            auto promoted = tryPromoteToNextMemoryTier(*itemPtr, true);
            if (promoted) {
              promoted->markPromoted();
              (*stats_.numPromotions)[tid][pid][cid].inc();
              wakeUpWaiters(*itemPtr, std::move(promoted));
              if (itemPtr->isInclusive()) {
                  XDCHECK(itemPtr->isCopy());
              } else {
                  XDCHECK(!itemPtr->isAccessible());
                  XDCHECK(!itemPtr->isInMMContainer());
                  XDCHECK_EQ(itemPtr->getRefCount(),0);
                const auto res =
                    releaseBackToAllocator(*itemPtr, 
                            RemoveContext::kEviction, false);
                XDCHECK(res == ReleaseRes::kReleased);
              }
            } else {
              // we failed to allocate a new *toPromoteHdl, this *toPromoteHdl is no  longer moving
              auto ref = itemPtr->unmarkMoving();
              if (UNLIKELY(ref == 0)) {
	       wakeUpWaiters(*itemPtr,{});
                const auto res =
                    releaseBackToAllocator(*itemPtr, 
                            RemoveContext::kNormal, false);
                XDCHECK(res == ReleaseRes::kReleased);
              }  else if (itemPtr->isAccessible()) {
                //case where we failed to allocate in lower tier
                //item is still present in accessContainer
                //item is no longer moving - acquire and
                //wake up waiters with this handle
	        auto hdl = acquire(itemPtr);
	        insertInMMContainer(*hdl);
	        wakeUpWaiters(*itemPtr,std::move(hdl));
              } else if (!itemPtr->isAccessible()) {
                //case where we failed to replace in access
                //container due to another thread calling insertOrReplace
                //unmark moving and return null handle
	        wakeUpWaiters(*itemPtr,{});
	        if (UNLIKELY(ref == 0)) {
                    const auto res =
                      releaseBackToAllocator(*itemPtr, RemoveContext::kNormal,
                                              false);
                    XDCHECK(res == ReleaseRes::kReleased);
                }
              } else {
                XDCHECK(false);
              }
            }
          });

        } else if (!config_.useThreadPool && config_.threadPoolThreads == 2) {
          syncPromotion(item);
        } else if (!config_.useThreadPool && backgroundPromoter_.size() > 0 && 
                   !config_.usePromotionQueue && !itemPtr->isRecent()) {
          itemPtr->markRecent();
        } else if (!config_.useThreadPool && backgroundPromoter_.size() > 0 && 
                   config_.usePromotionQueue) {
          bool moving = itemPtr->markMoving(false);
          if (moving) {
            //itemPtr->markRecent();
            auto &promoQueue = getPromoQueue(tid,pid,cid);
            bool added = promoQueue.write(itemPtr);
            if (!added) {
                itemPtr->unmarkMoving();
                //(*stats_.numWritebacksFailNoSpace)[tid][pid][cid].inc();
                //auto ref = itemPtr->unmarkMoving();
                //XDCHECK_NE(ref,0);
                auto hdl = acquire(itemPtr);
                wakeUpWaiters(*itemPtr,std::move(hdl));
            }
          }

            //{
            //  auto plock = getPromoLockForClass(cid);
            //  //use a handle
            //  // safe to acquire handle for a moving Item
            //  //XDCHECK(itemPtr->isInclusive());
            //  //auto incRes = incRef(*itemPtr, false);
            //  //XDCHECK(incRes == RefcountWithFlags::incResult::incOk);
            //  //auto hdl = WriteHandle{itemPtr,*this};
            //  promoQueue.push(itemPtr);
            //}
            //    //queue full
            //} else {
            //  auto shard = getShardForKey(item.getKey());
            //  auto& promoMap = getPromoMapForShard(shard);
            //  {
            //    auto lock = getMoveLockForShard(shard);
            //    auto ret = promoMap.try_emplace(item.getKey(),itemPtr);
            //    XDCHECK_EQ(ret.first->second,itemPtr);
            //  }
            //}
          //}
        }
      }
  } else if (tid == 0 && item.wasPromoted()) {
    (*stats_.numPromotionsHits)[tid][pid][cid].inc();
  }
  // if parent is not recorded, skip children as well when the config is set
  if (LIKELY(!item.hasChainedItem() ||
             (!recorded && config_.isSkipPromoteChildrenWhenParentFailed()))) {
    return;
  }

  forEachChainedItem(item, [this, mode](ChainedItem& chainedItem) {
    recordAccessInMMContainer(chainedItem, mode);
  });
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::syncPromotion(Item& item) {
  if (!config_.useThreadPool && config_.threadPoolThreads == 1) {
    const auto tid = getTierId(item);
    if (tid != 1) {
        return;
    }
    bool moving = item.markMovingIfRefCount(1);
    if (moving) {
      const auto allocInfo = allocator_[tid]->getAllocInfo(item.getMemory());
      const auto pid = allocInfo.poolId;
      const auto cid = allocInfo.classId;
      auto& mmContainer = getMMContainer(tid, pid, cid);
      bool removed = mmContainer.remove(item);
      if (!removed) {
          auto ref = item.unmarkMoving();
          if (UNLIKELY(ref == 0)) {
            XDCHECK(false);
            //wakeUpWaiters(item,{});
            //const auto res =
            //    releaseBackToAllocator(item, 
            //            RemoveContext::kNormal, false);
            //XDCHECK(res == ReleaseRes::kReleased);
          } else {
            //wake up waiters with this handle
            auto hdl = acquire(&item);
            wakeUpWaiters(item,std::move(hdl));
          }
          return;
      }
      XDCHECK(removed);
      XDCHECK(moving);
      XCHECK_EQ(tid,1);
      auto promoted = tryPromoteToNextMemoryTier(item, false);
      if (promoted) {
        promoted->markPromoted();
        (*stats_.numPromotions)[tid][pid][cid].inc();
        wakeUpWaiters(item, std::move(promoted));
        if (item.isInclusive()) {
            XDCHECK(item.isCopy());
        } else {
          XDCHECK(!item.isAccessible());
          XDCHECK(!item.isInMMContainer());
          XDCHECK_EQ(item.getRefCount(),1);
          //const auto res =
          //    releaseBackToAllocator(item, 
          //            RemoveContext::kEviction, false);
          //XDCHECK(res == ReleaseRes::kReleased);
        }
      } else {
        // we failed to allocate a new *toPromoteHdl, this *toPromoteHdl is no  longer moving
        auto ref = item.unmarkMoving();
        if (UNLIKELY(ref == 0)) {
          XDCHECK(false);
          //wakeUpWaiters(item,{});
          //const auto res =
          //    releaseBackToAllocator(item, 
          //            RemoveContext::kNormal, false);
          //XDCHECK(res == ReleaseRes::kReleased);
        }  else if (item.isAccessible()) {
          //case where we failed to allocate in lower tier
          //item is still present in accessContainer
          //item is no longer moving - acquire and
          //wake up waiters with this handle
          auto hdl = acquire(&item);
          insertInMMContainer(*hdl);
          wakeUpWaiters(item,std::move(hdl));
        } else if (!item.isAccessible()) {
          //case where we failed to replace in access
          //container due to another thread calling insertOrReplace
          //unmark moving and return null handle
          wakeUpWaiters(item,{});
          if (UNLIKELY(ref == 0)) {
              XDCHECK(false);
              //const auto res =
              //  releaseBackToAllocator(*item, RemoveContext::kNormal,
              //                          false);
              //XDCHECK(res == ReleaseRes::kReleased);
          }
        } else {
          XDCHECK(false);
        }
      }
    }
  }
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::recordAccessInMMContainer(Item& item,
                                                           AccessMode mode) {
  const auto tid = getTierId(item);
  const auto allocInfo =
      allocator_[tid]->getAllocInfo(static_cast<const void*>(&item));
  (*stats_.cacheHits)[tid][allocInfo.poolId][allocInfo.classId].inc();

  // track recently accessed items if needed
  if (UNLIKELY(config_.trackRecentItemsForDump)) {
    ring_->trackItem(reinterpret_cast<uintptr_t>(&item), item.getSize());
  }

  auto& mmContainer = getMMContainer(tid, allocInfo.poolId, allocInfo.classId);
  return mmContainer.recordAccess(item, mode);
}

template <typename CacheTrait>
uint32_t CacheAllocator<CacheTrait>::getUsableSize(const Item& item) const {
  const auto tid = getTierId(item);
  const auto allocSize =
      allocator_[tid]->getAllocInfo(static_cast<const void*>(&item)).allocSize;
  return item.isChainedItem()
             ? allocSize - ChainedItem::getRequiredSize(0)
             : allocSize - Item::getRequiredSize(item.getKey(), 0);
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::SampleItem
CacheAllocator<CacheTrait>::getSampleItem() {
  // TODO: is using random tier a good idea?
  auto tid = folly::Random::rand32() % getNumTiers();
  static size_t nvmCacheSize = nvmCache_ ? nvmCache_->getUsableSize() : 0;
  static size_t ramCacheSize = allocator_[tid]->getMemorySizeInclAdvised();

  bool fromNvm =
      folly::Random::rand64(0, nvmCacheSize + ramCacheSize) >= ramCacheSize;
  if (fromNvm) {
    return nvmCache_->getSampleItem();
  }

  // Sampling from DRAM cache
  auto item = reinterpret_cast<const Item*>(allocator_[tid]->getRandomAlloc());
  if (!item) {
    return SampleItem{false /* fromNvm */};
  }

  // Check that item returned is the same that was sampled
  auto sharedHdl = std::make_shared<ReadHandle>(findInternal(item->getKey()));
  if (sharedHdl->get() != item) {
    return SampleItem{false /* fromNvm */};
  }

  const auto allocInfo = allocator_[tid]->getAllocInfo(item->getMemory());

  // Convert the Item to IOBuf to make SampleItem
  auto iobuf = folly::IOBuf{
      folly::IOBuf::TAKE_OWNERSHIP, sharedHdl->getInternal(),
      item->getOffsetForMemory() + item->getSize(),
      [](void* /*unused*/, void* userData) {
        auto* hdl = reinterpret_cast<std::shared_ptr<ReadHandle>*>(userData);
        delete hdl;
      } /* freeFunc */,
      new std::shared_ptr<ReadHandle>{sharedHdl} /* userData for freeFunc */};

  iobuf.markExternallySharedOne();

  return SampleItem(std::move(iobuf), allocInfo, false /* fromNvm */);
}

template <typename CacheTrait>
std::vector<std::string> CacheAllocator<CacheTrait>::dumpEvictionIterator(
  PoolId pid, ClassId cid, size_t numItems) {
  if (numItems == 0) {
    return {};
  }

  // Always evict from the lowest layer.
  int tid = getNumTiers() - 1;

  if (static_cast<size_t>(tid) >= mmContainers_.size() ||
      static_cast<size_t>(pid) >= mmContainers_[tid].size() ||
      static_cast<size_t>(cid) >= mmContainers_[tid][pid].size()) {
    throw std::invalid_argument(
        folly::sformat("Invalid TierId: {} and PoolId: {} and ClassId: {}.", tid, pid, cid));
  }

  std::vector<std::string> content;

  while (tid >= 0) {
    auto& mm = *mmContainers_[tid][pid][cid];
    mm.withEvictionIterator([&content, numItems](auto&& itr) {
      while (itr && content.size() < numItems) {
        content.push_back(itr->toString());
        ++itr;
      }
    });
    --tid;
  }
  return content;
}

template <typename CacheTrait>
template <typename Handle>
folly::IOBuf CacheAllocator<CacheTrait>::convertToIOBufT(Handle& handle) {
  if (!handle) {
    throw std::invalid_argument("null item handle for converting to IOBUf");
  }

  Item* item = handle.getInternal();
  const uint32_t dataOffset = item->getOffsetForMemory();

  using ConvertChainedItem = std::function<std::unique_ptr<folly::IOBuf>(
      Item * item, ChainedItem & chainedItem)>;
  folly::IOBuf iobuf;
  ConvertChainedItem converter;

  // based on current refcount and threshold from config
  // determine to use a new Item Handle for each chain items
  // or use shared Item Handle for all chain items
  if (item->getRefCount() > config_.thresholdForConvertingToIOBuf) {
    auto sharedHdl = std::make_shared<Handle>(std::move(handle));

    iobuf = folly::IOBuf{
        folly::IOBuf::TAKE_OWNERSHIP, item,

        // Since we'll be moving the IOBuf data pointer forward
        // by dataOffset, we need to adjust the IOBuf length
        // accordingly
        dataOffset + item->getSize(),

        [](void* /*unused*/, void* userData) {
          auto* hdl = reinterpret_cast<std::shared_ptr<Handle>*>(userData);
          delete hdl;
        } /* freeFunc */,
        new std::shared_ptr<Handle>{sharedHdl} /* userData for freeFunc */};

    if (item->hasChainedItem()) {
      converter = [sharedHdl](Item*, ChainedItem& chainedItem) {
        const uint32_t chainedItemDataOffset = chainedItem.getOffsetForMemory();

        return folly::IOBuf::takeOwnership(
            &chainedItem,

            // Since we'll be moving the IOBuf data pointer forward by
            // dataOffset,
            // we need to adjust the IOBuf length accordingly
            chainedItemDataOffset + chainedItem.getSize(),

            [](void*, void* userData) {
              auto* hdl = reinterpret_cast<std::shared_ptr<Handle>*>(userData);
              delete hdl;
            } /* freeFunc */,
            new std::shared_ptr<Handle>{sharedHdl} /* userData for freeFunc */);
      };
    }

  } else {
    // following IOBuf will take the item's ownership and trigger freeFunc to
    // release the reference count.
    handle.release();
    iobuf = folly::IOBuf{folly::IOBuf::TAKE_OWNERSHIP, item,

                         // Since we'll be moving the IOBuf data pointer forward
                         // by dataOffset, we need to adjust the IOBuf length
                         // accordingly
                         dataOffset + item->getSize(),

                         [](void* buf, void* userData) {
                           Handle{reinterpret_cast<Item*>(buf),
                                  *reinterpret_cast<decltype(this)>(userData)}
                               .reset();
                         } /* freeFunc */,
                         this /* userData for freeFunc */};

    if (item->hasChainedItem()) {
      converter = [this](Item* parentItem, ChainedItem& chainedItem) {
        const uint32_t chainedItemDataOffset = chainedItem.getOffsetForMemory();

        // Each IOBuf converted from a child item will hold one additional
        // refcount on the parent item. This ensures that as long as the user
        // holds any IOBuf pointing anywhere in the chain, the whole chain
        // will not be evicted from cache.
        //
        // We can safely bump the refcount on the parent here only because
        // we already have an item handle on the parent (which has just been
        // moved into the IOBuf above). Normally, the only place we can
        // bump an item handle safely is through the AccessContainer.
        acquire(parentItem).release();

        return folly::IOBuf::takeOwnership(
            &chainedItem,

            // Since we'll be moving the IOBuf data pointer forward by
            // dataOffset,
            // we need to adjust the IOBuf length accordingly
            chainedItemDataOffset + chainedItem.getSize(),

            [](void* buf, void* userData) {
              auto* cache = reinterpret_cast<decltype(this)>(userData);
              auto* child = reinterpret_cast<ChainedItem*>(buf);
              auto* parent = &child->getParentItem(cache->compressor_);
              Handle{parent, *cache}.reset();
            } /* freeFunc */,
            this /* userData for freeFunc */);
      };
    }
  }

  iobuf.trimStart(dataOffset);
  iobuf.markExternallySharedOne();

  if (item->hasChainedItem()) {
    auto appendHelper = [&](ChainedItem& chainedItem) {
      const uint32_t chainedItemDataOffset = chainedItem.getOffsetForMemory();

      auto nextChain = converter(item, chainedItem);

      nextChain->trimStart(chainedItemDataOffset);
      nextChain->markExternallySharedOne();

      // Append immediately after the parent, IOBuf will present the data
      // in the original insertion order.
      //
      // i.e. 1. Allocate parent
      //      2. add A, add B, add C
      //
      //      In memory: parent -> C -> B -> A
      //      In IOBuf:  parent -> A -> B -> C
      iobuf.appendChain(std::move(nextChain));
    };

    forEachChainedItem(*item, std::move(appendHelper));
  }

  return iobuf;
}

template <typename CacheTrait>
folly::IOBuf CacheAllocator<CacheTrait>::wrapAsIOBuf(const Item& item) {
  folly::IOBuf ioBuf{folly::IOBuf::WRAP_BUFFER, item.getMemory(),
                     item.getSize()};

  if (item.hasChainedItem()) {
    auto appendHelper = [&](ChainedItem& chainedItem) {
      auto nextChain = folly::IOBuf::wrapBuffer(chainedItem.getMemory(),
                                                chainedItem.getSize());

      // Append immediately after the parent, IOBuf will present the data
      // in the original insertion order.
      //
      // i.e. 1. Allocate parent
      //      2. add A, add B, add C
      //
      //      In memory: parent -> C -> B -> A
      //      In IOBuf:  parent -> A -> B -> C
      ioBuf.appendChain(std::move(nextChain));
    };

    forEachChainedItem(item, std::move(appendHelper));
  }
  return ioBuf;
}

template <typename CacheTrait>
PoolId CacheAllocator<CacheTrait>::addPool(
    folly::StringPiece name,
    size_t size,
    const std::set<uint32_t>& allocSizes,
    MMConfig config,
    std::shared_ptr<RebalanceStrategy> rebalanceStrategy,
    std::shared_ptr<RebalanceStrategy> resizeStrategy,
    bool ensureProvisionable) {
  folly::SharedMutex::WriteHolder w(poolsResizeAndRebalanceLock_);

  PoolId pid = 0;
  size_t totalCacheSize = 0;

  for (TierId tid = 0; tid < getNumTiers(); tid++) {
    totalCacheSize += allocator_[tid]->getMemorySize();
  }

  for (TierId tid = 0; tid < getNumTiers(); tid++) {
    auto tierSizeRatio =
        static_cast<double>(allocator_[tid]->getMemorySize()) / totalCacheSize;
    size_t tierPoolSize = static_cast<size_t>(tierSizeRatio * size);
    
    // TODO: what if we manage to add pool only in one tier?
    // we should probably remove that on failure
    auto res = allocator_[tid]->addPool(
        name, tierPoolSize, allocSizes, ensureProvisionable);
    XDCHECK(tid == 0 || res == pid);
    pid = res;
  }

  createPromoQueues(pid);
  createMMContainers(pid, std::move(config));
  setRebalanceStrategy(pid, std::move(rebalanceStrategy));
  setResizeStrategy(pid, std::move(resizeStrategy));

  if (backgroundEvictor_.size()) {
    for (size_t id = 0; id < backgroundEvictor_.size(); id++)
      backgroundEvictor_[id]->setAssignedMemory(getAssignedMemoryToBgWorker(id, backgroundEvictor_.size(), 0));
  }

  if (backgroundPromoter_.size()) {
    for (size_t id = 0; id < backgroundPromoter_.size(); id++)
      backgroundPromoter_[id]->setAssignedMemory(getAssignedMemoryToBgWorker(id, backgroundPromoter_.size(), 1));
  }

  return pid;
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::overridePoolRebalanceStrategy(
    PoolId pid, std::shared_ptr<RebalanceStrategy> rebalanceStrategy) {
  if (static_cast<size_t>(pid) >= mmContainers_[0].size()) {
    throw std::invalid_argument(folly::sformat(
        "Invalid PoolId: {}, size of pools: {}", pid, mmContainers_[0].size()));
  }
  setRebalanceStrategy(pid, std::move(rebalanceStrategy));
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::overridePoolResizeStrategy(
    PoolId pid, std::shared_ptr<RebalanceStrategy> resizeStrategy) {
  if (static_cast<size_t>(pid) >= mmContainers_[0].size()) {
    throw std::invalid_argument(folly::sformat(
        "Invalid PoolId: {}, size of pools: {}", pid, mmContainers_[0].size()));
  }
  setResizeStrategy(pid, std::move(resizeStrategy));
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::overridePoolOptimizeStrategy(
    std::shared_ptr<PoolOptimizeStrategy> optimizeStrategy) {
  setPoolOptimizeStrategy(std::move(optimizeStrategy));
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::overridePoolConfig(TierId tid, PoolId pid,
                                                    const MMConfig& config) {
  // TODO: add generic tier id checking
  if (static_cast<size_t>(pid) >= mmContainers_[tid].size()) {
    throw std::invalid_argument(folly::sformat(
        "Invalid PoolId: {}, size of pools: {}", pid, mmContainers_[tid].size()));
  }
  auto& pool = allocator_[tid]->getPool(pid);
  for (unsigned int cid = 0; cid < pool.getNumClassId(); ++cid) {
    MMConfig mmConfig = config;
    mmConfig.addExtraConfig(
        config_.trackTailHits
            ? pool.getAllocationClass(static_cast<ClassId>(cid))
                  .getAllocsPerSlab()
            : 0);
    DCHECK_NOTNULL(mmContainers_[tid][pid][cid].get());
    mmContainers_[tid][pid][cid]->setConfig(mmConfig);
  }
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::createPromoQueues(const PoolId pid) {
  auto& pool = allocator_[0]->getPool(pid);

  for (unsigned int cid = 0; cid < pool.getNumClassId(); ++cid) {
    for (TierId tid = 0; tid < getNumTiers(); tid++) {
      promoQueues_[tid][pid][cid].reset(new folly::MPMCQueue<Item*>(2000000));
      //promoQueues_[tid][pid][cid].reset(new std::queue<Item*>());
    }
  }
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::createMMContainers(const PoolId pid,
                                                    MMConfig config) {
  // pools on each layer should have the same number of class id, etc.
  // TODO: think about deduplication
  auto& pool = allocator_[0]->getPool(pid);

  for (unsigned int cid = 0; cid < pool.getNumClassId(); ++cid) {
    config.addExtraConfig(
        config_.trackTailHits
            ? pool.getAllocationClass(static_cast<ClassId>(cid))
                  .getAllocsPerSlab()
            : 0);
    for (TierId tid = 0; tid < getNumTiers(); tid++) {
      mmContainers_[tid][pid][cid].reset(new MMContainer(config, compressor_));
    }
  }
}

template <typename CacheTrait>
PoolId CacheAllocator<CacheTrait>::getPoolId(
    folly::StringPiece name) const noexcept {
  // each tier has the same pools
  return allocator_[0]->getPoolId(name.str());
}

// The Function returns a consolidated vector of Release Slab
// events from Pool Workers { Pool rebalancer, Pool Resizer and
// Memory Monitor}.
template <typename CacheTrait>
AllSlabReleaseEvents CacheAllocator<CacheTrait>::getAllSlabReleaseEvents(
    PoolId poolId) const {
  AllSlabReleaseEvents res;
  // lock protects against workers being restarted
  {
    std::lock_guard<std::mutex> l(workersMutex_);
    if (poolRebalancer_) {
      res.rebalancerEvents = poolRebalancer_->getSlabReleaseEvents(poolId);
    }
    if (poolResizer_) {
      res.resizerEvents = poolResizer_->getSlabReleaseEvents(poolId);
    }
    if (memMonitor_) {
      res.monitorEvents = memMonitor_->getSlabReleaseEvents(poolId);
    }
  }
  return res;
}

template <typename CacheTrait>
std::set<PoolId> CacheAllocator<CacheTrait>::filterCompactCachePools(
    const PoolIds& poolIds) const {
  PoolIds ret;
  folly::SharedMutex::ReadHolder lock(compactCachePoolsLock_);
  for (auto poolId : poolIds) {
    if (!isCompactCachePool_[poolId]) {
      // filter out slab pools backing the compact caches.
      ret.insert(poolId);
    }
  }
  return ret;
}

template <typename CacheTrait>
std::set<PoolId> CacheAllocator<CacheTrait>::getRegularPoolIds() const {
  folly::SharedMutex::ReadHolder r(poolsResizeAndRebalanceLock_);
  // TODO - get rid of the duplication - right now, each tier
  // holds pool objects with mostly the same info
  return filterCompactCachePools(allocator_[0]->getPoolIds());
}

template <typename CacheTrait>
std::set<PoolId> CacheAllocator<CacheTrait>::getCCachePoolIds() const {
  PoolIds ret;
  folly::SharedMutex::ReadHolder lock(compactCachePoolsLock_);
  for (PoolId id = 0; id < static_cast<PoolId>(MemoryPoolManager::kMaxPools);
       id++) {
    if (isCompactCachePool_[id]) {
      // filter out slab pools backing the compact caches.
      ret.insert(id);
    }
  }
  return ret;
}

template <typename CacheTrait>
std::set<PoolId> CacheAllocator<CacheTrait>::getRegularPoolIdsForResize()
    const {
  folly::SharedMutex::ReadHolder r(poolsResizeAndRebalanceLock_);
  // If Slabs are getting advised away - as indicated by non-zero
  // getAdvisedMemorySize - then pools may be overLimit even when
  // all slabs are not allocated. Otherwise, pools may be overLimit
  // only after all slabs are allocated.
  return (allocator_[currentTier()]->allSlabsAllocated()) ||
                 (allocator_[currentTier()]->getAdvisedMemorySize() != 0)
             ? filterCompactCachePools(allocator_[currentTier()]->getPoolsOverLimit())
             : std::set<PoolId>{};
}

template <typename CacheTrait>
const std::string CacheAllocator<CacheTrait>::getCacheName() const {
  return config_.cacheName;
}

template <typename CacheTrait>
size_t CacheAllocator<CacheTrait>::getPoolSize(PoolId poolId) const {
  size_t poolSize = 0;
  for (auto& allocator: allocator_) {
    const auto& pool = allocator->getPool(poolId);
    poolSize += pool.getPoolSize();
  }
  return poolSize;
}

template <typename CacheTrait>
PoolStats CacheAllocator<CacheTrait>::getPoolStats(PoolId poolId) const {
  //this pool ref is just used to get class ids, which will be the
  //same across tiers
  const auto& pool = allocator_[currentTier()]->getPool(poolId);
  const auto& allocSizes = pool.getAllocSizes();
  auto mpStats = pool.getStats();
  const auto& classIds = mpStats.classIds;

  // check if this is a compact cache.
  bool isCompactCache = false;
  {
    folly::SharedMutex::ReadHolder lock(compactCachePoolsLock_);
    isCompactCache = isCompactCachePool_[poolId];
  }

  std::unordered_map<ClassId, CacheStat> cacheStats;
  uint64_t totalHits = 0;
  // cacheStats is only menaningful for pools that are not compact caches.
  // TODO export evictions, numItems etc from compact cache directly.
  if (!isCompactCache) {
    for (const ClassId cid : classIds) {
      uint64_t allocAttempts, evictionAttempts, allocFailures,
               fragmentationSize, classHits, chainedItemEvictions,
               regularItemEvictions, numWritebacks, numWritebacksFailBadMove, numWritebacksFailNoAlloc, numPromotions, numPromotionsHits = 0;
      MMContainerStat mmContainerStats;
      for (TierId tid = 0; tid < getNumTiers(); tid++) {
        allocAttempts += (*stats_.allocAttempts)[tid][poolId][cid].get();
        evictionAttempts += (*stats_.evictionAttempts)[tid][poolId][cid].get();
        allocFailures += (*stats_.allocFailures)[tid][poolId][cid].get();
        fragmentationSize += (*stats_.fragmentationSize)[tid][poolId][cid].get();
        classHits += (*stats_.cacheHits)[tid][poolId][cid].get();
        chainedItemEvictions += (*stats_.chainedItemEvictions)[tid][poolId][cid].get();
        regularItemEvictions += (*stats_.regularItemEvictions)[tid][poolId][cid].get();
        numWritebacks += (*stats_.numWritebacks)[tid][poolId][cid].get();
        numWritebacksFailBadMove += (*stats_.numWritebacksFailBadMove)[tid][poolId][cid].get();
        numWritebacksFailNoAlloc += (*stats_.numWritebacksFailNoAlloc)[tid][poolId][cid].get();
        numPromotions += (*stats_.numPromotions)[tid][poolId][cid].get();
        numPromotionsHits += (*stats_.numPromotionsHits)[tid][poolId][cid].get();
        mmContainerStats += getMMContainerStat(tid, poolId, cid);
        XDCHECK(mmContainers_[tid][poolId][cid],
                folly::sformat("Tid {}, Pid {}, Cid {} not initialized.", tid, poolId, cid));
      }
      cacheStats.insert(
          {cid,
           {allocSizes[cid],
            allocAttempts,
            evictionAttempts,
            allocFailures,
            fragmentationSize,
            classHits,
            chainedItemEvictions,
            regularItemEvictions,
            numWritebacks,
            numWritebacksFailBadMove,
            numWritebacksFailNoAlloc,
            numPromotions,
            numPromotionsHits,
            mmContainerStats}});
      totalHits += classHits;
    }
  }

  PoolStats ret;
  ret.isCompactCache = isCompactCache;
  //pool name is also shared among tiers
  ret.poolName = allocator_[currentTier()]->getPoolName(poolId);
  ret.poolSize = pool.getPoolSize();
  ret.poolUsableSize = pool.getPoolUsableSize();
  ret.poolAdvisedSize = pool.getPoolAdvisedSize();
  ret.cacheStats = std::move(cacheStats);
  ret.mpStats = std::move(mpStats);
  ret.numPoolGetHits = totalHits;
  ret.evictionAgeSecs = stats_.perPoolEvictionAgeSecs_[poolId].estimate();

  return ret;
}

template <typename CacheTrait>
PoolStats CacheAllocator<CacheTrait>::getPoolStats(TierId tid, PoolId poolId) const {
  const auto& pool = allocator_[tid]->getPool(poolId);
  const auto& allocSizes = pool.getAllocSizes();
  auto mpStats = pool.getStats();
  const auto& classIds = mpStats.classIds;

  // check if this is a compact cache.
  bool isCompactCache = false;
  {
    folly::SharedMutex::ReadHolder lock(compactCachePoolsLock_);
    isCompactCache = isCompactCachePool_[poolId];
  }

  std::unordered_map<ClassId, CacheStat> cacheStats;
  uint64_t totalHits = 0;
  // cacheStats is only menaningful for pools that are not compact caches.
  // TODO export evictions, numItems etc from compact cache directly.
  if (!isCompactCache) {
    for (const ClassId cid : classIds) {
      uint64_t classHits = (*stats_.cacheHits)[tid][poolId][cid].get();
      XDCHECK(mmContainers_[tid][poolId][cid],
              folly::sformat("Tid {}, Pid {}, Cid {} not initialized.", tid, poolId, cid));
      cacheStats.insert(
          {cid,
           {allocSizes[cid],
            (*stats_.allocAttempts)[tid][poolId][cid].get(),
            (*stats_.evictionAttempts)[tid][poolId][cid].get(),
            (*stats_.allocFailures)[tid][poolId][cid].get(),
            (*stats_.fragmentationSize)[tid][poolId][cid].get(),
            classHits,
            (*stats_.chainedItemEvictions)[tid][poolId][cid].get(),
            (*stats_.regularItemEvictions)[tid][poolId][cid].get(),
            (*stats_.numWritebacks)[tid][poolId][cid].get(),
            (*stats_.numWritebacksFailBadMove)[tid][poolId][cid].get(),
            (*stats_.numWritebacksFailNoAlloc)[tid][poolId][cid].get(),
            (*stats_.numPromotions)[tid][poolId][cid].get(),
            (*stats_.numPromotionsHits)[tid][poolId][cid].get(),
            getMMContainerStat(tid, poolId, cid)}});
      totalHits += classHits;
    }
  }

  PoolStats ret;
  ret.isCompactCache = isCompactCache;
  ret.poolName = allocator_[tid]->getPoolName(poolId);
  ret.poolSize = pool.getPoolSize();
  ret.poolUsableSize = pool.getPoolUsableSize();
  ret.poolAdvisedSize = pool.getPoolAdvisedSize();
  ret.cacheStats = std::move(cacheStats);
  ret.mpStats = std::move(mpStats);
  ret.numPoolGetHits = totalHits;
  ret.evictionAgeSecs = stats_.perPoolEvictionAgeSecs_[poolId].estimate();

  return ret;
}

template <typename CacheTrait>
ACStats CacheAllocator<CacheTrait>::getACStats(TierId tid,
                                               PoolId poolId,
                                               ClassId classId) const {
  const auto& pool = allocator_[tid]->getPool(poolId);
  const auto& ac = pool.getAllocationClass(classId);

  auto stats = ac.getStats();
  stats.allocLatencyNs = (*stats_.classAllocLatency)[tid][poolId][classId];
  return stats;
}

template <typename CacheTrait>
PoolEvictionAgeStats CacheAllocator<CacheTrait>::getPoolEvictionAgeStats(
    PoolId pid, unsigned int slabProjectionLength) const {
  PoolEvictionAgeStats stats;
  const auto& pool = allocator_[currentTier()]->getPool(pid);
  const auto& allocSizes = pool.getAllocSizes();
  for (ClassId cid = 0; cid < static_cast<ClassId>(allocSizes.size()); ++cid) {
    auto& mmContainer = getMMContainer(currentTier(), pid, cid);
    const auto numItemsPerSlab =
        allocator_[currentTier()]->getPool(pid).getAllocationClass(cid).getAllocsPerSlab();
    const auto projectionLength = numItemsPerSlab * slabProjectionLength;
    stats.classEvictionAgeStats[cid] =
        mmContainer.getEvictionAgeStat(projectionLength);
  }
  return stats;
}

template <typename CacheTrait>
CacheMetadata CacheAllocator<CacheTrait>::getCacheMetadata() const noexcept {
  return CacheMetadata{kCachelibVersion, kCacheRamFormatVersion,
                       kCacheNvmFormatVersion, config_.getCacheSize()};
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::releaseSlab(PoolId pid,
                                             ClassId cid,
                                             SlabReleaseMode mode,
                                             const void* hint) {
  releaseSlab(pid, cid, Slab::kInvalidClassId, mode, hint);
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::releaseSlab(PoolId pid,
                                             ClassId victim,
                                             ClassId receiver,
                                             SlabReleaseMode mode,
                                             const void* hint) {
  stats_.numActiveSlabReleases.inc();
  SCOPE_EXIT { stats_.numActiveSlabReleases.dec(); };
  switch (mode) {
  case SlabReleaseMode::kRebalance:
    stats_.numReleasedForRebalance.inc();
    break;
  case SlabReleaseMode::kResize:
    stats_.numReleasedForResize.inc();
    break;
  case SlabReleaseMode::kAdvise:
    stats_.numReleasedForAdvise.inc();
    break;
  }

  try {
    auto releaseContext = allocator_[currentTier()]->startSlabRelease(
        pid, victim, receiver, mode, hint,
        [this]() -> bool { return shutDownInProgress_; });

    // No work needed if the slab is already released
    if (releaseContext.isReleased()) {
      return;
    }

    releaseSlabImpl(currentTier(), releaseContext);
    if (!allocator_[currentTier()]->allAllocsFreed(releaseContext)) {
      throw std::runtime_error(
          folly::sformat("Was not able to free all allocs. PoolId: {}, AC: {}",
                         releaseContext.getPoolId(),
                         releaseContext.getClassId()));
    }

    allocator_[currentTier()]->completeSlabRelease(releaseContext);
  } catch (const exception::SlabReleaseAborted& e) {
    stats_.numAbortedSlabReleases.inc();
    throw exception::SlabReleaseAborted(folly::sformat(
        "Slab release aborted while releasing "
        "a slab in pool {} victim {} receiver {}. Original ex msg: ",
        pid, static_cast<int>(victim), static_cast<int>(receiver), e.what()));
  }
}

template <typename CacheTrait>
SlabReleaseStats CacheAllocator<CacheTrait>::getSlabReleaseStats() const noexcept {
  std::lock_guard<std::mutex> l(workersMutex_);
  return SlabReleaseStats{stats_.numActiveSlabReleases.get(),
                          stats_.numReleasedForRebalance.get(),
                          stats_.numReleasedForResize.get(),
                          stats_.numReleasedForAdvise.get(),
                          poolRebalancer_ ? poolRebalancer_->getRunCount()
                                          : 0ULL,
                          poolResizer_ ? poolResizer_->getRunCount() : 0ULL,
                          memMonitor_ ? memMonitor_->getRunCount() : 0ULL,
                          stats_.numMoveAttempts.get(),
                          stats_.numMoveSuccesses.get(),
                          stats_.numEvictionAttempts.get(),
                          stats_.numEvictionSuccesses.get(),
                          stats_.numSlabReleaseStuck.get()};
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::releaseSlabImpl(TierId tid,
    const SlabReleaseContext& releaseContext) {
  auto startTime = std::chrono::milliseconds(util::getCurrentTimeMs());
  bool releaseStuck = false;

  SCOPE_EXIT {
    if (releaseStuck) {
      stats_.numSlabReleaseStuck.dec();
    }
  };

  util::Throttler throttler(
      config_.throttleConfig,
      [this, &startTime, &releaseStuck](std::chrono::milliseconds curTime) {
        if (!releaseStuck &&
            curTime >= startTime + config_.slabReleaseStuckThreshold) {
          stats().numSlabReleaseStuck.inc();
          releaseStuck = true;
        }
      });

  // Active allocations need to be freed before we can release this slab
  // The idea is:
  //  1. Iterate through each active allocation
  //  2. Under AC lock, acquire ownership of this active allocation
  //  3. If 2 is successful, Move or Evict
  //  4. Move on to the next item if current item is freed
  for (auto alloc : releaseContext.getActiveAllocations()) {
    auto startTimeSec = util::getCurrentTimeSec();
    // Need to mark an item for release before proceeding
    // If we can't mark as moving, it means the item is already freed
    const bool isAlreadyFreed =
        !markMovingForSlabRelease(releaseContext, alloc, throttler);
    if (isAlreadyFreed) {
      continue;
    }

    Item& item = *static_cast<Item*>(alloc);

    // Try to move this item and make sure we can free the memory
    const bool isMoved = moveForSlabRelease(releaseContext, item, throttler);

    // if moving fails, evict it
    if (!isMoved) {
      evictForSlabRelease(releaseContext, item, throttler);
    }
    XDCHECK(allocator_[tid]->isAllocFreed(releaseContext, alloc));
  }
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::throttleWith(util::Throttler& t,
                                              std::function<void()> fn) {
  const unsigned int rateLimit = 1024;
  // execute every 1024 times we have actually throttled
  if (t.throttle() && (t.numThrottles() % rateLimit) == 0) {
    fn();
  }
}

template <typename CacheTrait>
typename RefcountWithFlags::Value CacheAllocator<CacheTrait>::unmarkMovingAndWakeUpWaiters(Item &item, WriteHandle handle)
{
  auto ret = item.unmarkMoving();
  wakeUpWaiters(item, std::move(handle));
  return ret;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::moveForSlabRelease(
    const SlabReleaseContext& ctx, Item& oldItem, util::Throttler& throttler) {
  if (!config_.moveCb) {
    return false;
  }

  bool isMoved = false;
  auto startTime = util::getCurrentTimeSec();
  WriteHandle newItemHdl{};

  for (unsigned int itemMovingAttempts = 0;
       itemMovingAttempts < config_.movingTries;
       ++itemMovingAttempts) {
    stats_.numMoveAttempts.inc();

    // Nothing to move and the key is likely also bogus for chained items.
    if (oldItem.isOnlyMoving()) {
      auto ret = unmarkMovingAndWakeUpWaiters(oldItem, {});
      XDCHECK(ret == 0);
      const auto res =
          releaseBackToAllocator(oldItem, RemoveContext::kNormal, false);
      XDCHECK(res == ReleaseRes::kReleased);
      return true;
    }

    throttleWith(throttler, [&] {
      XLOGF(WARN,
            "Spent {} seconds, slab release still trying to move Item: {}. "
            "Pool: {}, Class: {}.",
            util::getCurrentTimeSec() - startTime, oldItem.toString(),
            ctx.getPoolId(), ctx.getClassId());
    });

    if (!newItemHdl) {
      newItemHdl = allocateNewItemForOldItem(oldItem);
    }

    // if we have a valid handle, try to move, if not, we retry.
    if (newItemHdl) {
      isMoved = tryMovingForSlabRelease(oldItem, newItemHdl);
      if (isMoved) {
        break;
      }
    }
  }

  // Return false if we've exhausted moving tries.
  if (!isMoved) {
    return false;
  }

  // Since item has been moved, we can directly free it. We don't need to
  // worry about any stats related changes, because there is another item
  // that's identical to this one to replace it. Here we just need to wait
  // until all users have dropped the item handles before we can proceed.
  startTime = util::getCurrentTimeSec();
  while (!oldItem.isOnlyMoving()) {
    throttleWith(throttler, [&] {
      XLOGF(WARN,
            "Spent {} seconds, slab release still waiting for refcount to "
            "drain Item: {}. Pool: {}, Class: {}.",
            util::getCurrentTimeSec() - startTime, oldItem.toString(),
            ctx.getPoolId(), ctx.getClassId());
    });
  }
  auto tid = getTierId(oldItem);
  auto ref = unmarkMovingAndWakeUpWaiters(oldItem, std::move(newItemHdl));
  XDCHECK(ref == 0);

  const auto allocInfo = allocator_[tid]->getAllocInfo(oldItem.getMemory());
  allocator_[tid]->free(&oldItem);

  (*stats_.fragmentationSize)[tid][allocInfo.poolId][allocInfo.classId].sub(
      util::getFragmentation(*this, oldItem));
  stats_.numMoveSuccesses.inc();
  return true;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::validateAndGetParentHandleForChainedMoveLocked(
    const ChainedItem& item, const Key& parentKey) {
  WriteHandle parentHandle{};
  try {
    parentHandle = findInternal(parentKey);
    // If the parent is not the same as the parent of the chained item,
    // it means someone has replaced our old parent already. So we abort.
    if (!parentHandle ||
        parentHandle.get() != &item.getParentItem(compressor_)) {
      return {};
    }
  } catch (const exception::RefcountOverflow&) {
    return {};
  }

  return parentHandle;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::allocateNewItemForOldItem(const Item& oldItem) {
  XDCHECK(oldItem.isMoving());
  if (oldItem.isChainedItem()) {
    const auto& oldChainedItem = oldItem.asChainedItem();
    const auto parentKey = oldChainedItem.getParentItem(compressor_).getKey();

    // Grab lock to prevent anyone else from modifying the chain
    auto l = chainedItemLocks_.lockExclusive(parentKey);

    auto parentHandle = validateAndGetParentHandleForChainedMoveLocked(
        oldChainedItem, parentKey);
    if (!parentHandle) {
      return {};
    }

    // Set up the destination for the move. Since oldChainedItem would
    // be marked as moving, it won't be picked for eviction.
    auto newItemHdl =
        allocateChainedItemInternal(parentHandle, oldItem.getSize());
    if (!newItemHdl) {
      return {};
    }

    XDCHECK_EQ(newItemHdl->getSize(), oldItem.getSize());
    auto parentPtr = parentHandle.getInternal();
    XDCHECK_EQ(reinterpret_cast<uintptr_t>(parentPtr),
               reinterpret_cast<uintptr_t>(
                   &newItemHdl->asChainedItem().getParentItem(compressor_)));

    return newItemHdl;
  }

  const auto allocInfo =
      allocator_[getTierId(oldItem)]->getAllocInfo(static_cast<const void*>(&oldItem));

  // Set up the destination for the move. Since oldItem would have the moving
  // bit set, it won't be picked for eviction.
  auto newItemHdl = allocateInternalTier(getTierId(oldItem),
                                         allocInfo.poolId,
                                         oldItem.getKey(),
                                         oldItem.getSize(),
                                         oldItem.getCreationTime(),
                                         oldItem.getExpiryTime(),
                                         false);
  if (!newItemHdl) {
    return {};
  }

  XDCHECK_EQ(newItemHdl->getSize(), oldItem.getSize());
  XDCHECK_EQ(reinterpret_cast<uintptr_t>(&getMMContainer(oldItem)),
             reinterpret_cast<uintptr_t>(&getMMContainer(*newItemHdl)));

  return newItemHdl;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::tryMovingForSlabRelease(
    Item& oldItem, WriteHandle& newItemHdl) {
  // By holding onto a user-level synchronization object, we ensure moving
  // a regular item or chained item is synchronized with any potential
  // user-side mutation.
  std::unique_ptr<SyncObj> syncObj;
  if (config_.movingSync && getNumTiers() == 1) {
    // TODO: use moving-bit synchronization for single tier as well
    if (!oldItem.isChainedItem()) {
      syncObj = config_.movingSync(oldItem.getKey());
    } else {
      // Copy the key so we have a valid key to work with if the chained
      // item is still valid.
      const std::string parentKey =
          oldItem.asChainedItem().getParentItem(compressor_).getKey().str();
      if (oldItem.isOnlyMoving()) {
        // If chained item no longer has a refcount, its parent is already
        // being released, so we abort this try to moving.
        return false;
      }
      syncObj = config_.movingSync(parentKey);
    }

    // We need to differentiate between the following three scenarios:
    // 1. nullptr indicates no move sync required for this particular item
    // 2. moveSync.isValid() == true meaning we've obtained the sync
    // 3. moveSync.isValid() == false meaning we need to abort and retry
    if (syncObj && !syncObj->isValid()) {
      return false;
    }
  }

  // TODO: we can unify move*Item and move*ItemWithSync by always
  // using the moving bit to block readers.
  if (getNumTiers() == 1) {
    return oldItem.isChainedItem()
              ? moveChainedItem(oldItem.asChainedItem(), newItemHdl)
              : moveRegularItem(oldItem, newItemHdl);
  } else {
    if (oldItem.isChainedItem() || oldItem.hasChainedItem()) {
      // TODO: add support for chained items
      return false;
    } else {
      //move can fail if another thread calls insertOrReplace
      //in this case oldItem is no longer valid (not accessible, 
      //it gets removed from MMContainer and evictForSlabRelease
      //will send it back to the allocator
      bool ret = moveRegularItemWithSync(oldItem, newItemHdl, false);
      if (!ret) {
          //we failed to move - newItemHdl was released back to allocator
          //by the moveRegularItemWithSync but oldItem is not accessible
          //and no longer valid - we need to clean it up here
          XDCHECK(!oldItem.isAccessible());
          oldItem.markForEvictionWhenMoving();
          unlinkItemForEviction(oldItem);
          wakeUpWaiters(oldItem, {});
      } else {
        removeFromMMContainer(oldItem);
      }
      return ret;
    }
  }
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::wakeUpWaiters(Item& item, WriteHandle handle)
{
  // readers do not block on 'moving' items in case there is only one tier
  if (getNumTiers() > 1) {
    wakeUpWaitersLocked(item.getKey(), std::move(handle));
  }
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::evictForSlabRelease(
    const SlabReleaseContext& ctx, Item& item, util::Throttler& throttler) {
  auto startTime = util::getCurrentTimeSec();

  while (true) {
    XDCHECK(item.isMoving());
    stats_.numEvictionAttempts.inc();

    if (shutDownInProgress_) {
      auto ref = unmarkMovingAndWakeUpWaiters(item, {});
      allocator_[getTierId(item)]->abortSlabRelease(ctx);
      throw exception::SlabReleaseAborted(
          folly::sformat("Slab Release aborted while trying to evict"
                         " Item: {} Pool: {}, Class: {}.",
                         item.toString(), ctx.getPoolId(), ctx.getClassId()));
    }
    throttleWith(throttler, [&] {
      XLOGF(WARN,
            "Spent {} seconds, slab release still trying to evict Item: {}. "
            "Pool: {}, Class: {}.",
            util::getCurrentTimeSec() - startTime, item.toString(),
            ctx.getPoolId(), ctx.getClassId())
          << (item.isChainedItem()
                  ? folly::sformat(" Parent: {}",
                                   item.asChainedItem()
                                       .getParentItem(compressor_)
                                       .toString())
                  : "");
    });
    // if the item is already in a state where only the exclusive bit is set,
    // nothing needs to be done. We simply need to call unmarkMoving and free
    // the item.
    if (item.isOnlyMoving()) {
      auto ref = unmarkMovingAndWakeUpWaiters(item, {});
      XDCHECK(ref == 0);
      const auto res =
          releaseBackToAllocator(item, RemoveContext::kNormal, false);
      XDCHECK(ReleaseRes::kReleased == res);
      return;
    }

    typename NvmCacheT::PutToken token;
    Item* evicted;
    if (item.isChainedItem()) {
      auto& expectedParent = item.asChainedItem().getParentItem(compressor_);

      if (getNumTiers() == 1) {
        // TODO: unify this with multi-tier implementation
        // right now, taking a chained item lock here would lead to deadlock
        const std::string parentKey = expectedParent.getKey().str();
        auto l = chainedItemLocks_.lockExclusive(parentKey);

        // check if the child is still in mmContainer and the expected parent is
        // valid under the chained item lock.
        if (expectedParent.getKey() != parentKey || !item.isInMMContainer() ||
            item.isOnlyMoving() ||
            &expectedParent != &item.asChainedItem().getParentItem(compressor_) ||
            !expectedParent.isAccessible() || !expectedParent.hasChainedItem()) {
          continue;
        }

        // search if the child is present in the chain
        {
          auto parentHandle = findInternal(parentKey);
          if (!parentHandle || parentHandle != &expectedParent) {
            continue;
          }

          ChainedItem* head = nullptr;
          { // scope for the handle
            auto headHandle = findChainedItem(expectedParent);
            head = headHandle ? &headHandle->asChainedItem() : nullptr;
          }

          bool found = false;
          while (head) {
            if (head == &item) {
              found = true;
              break;
            }
            head = head->getNext(compressor_);
          }

          if (!found) {
            continue;
          }
        }
      }

      evicted = &expectedParent;
      token = createPutToken(*evicted);
      if (evicted->markForEviction()) {
        // unmark the child so it will be freed
        item.unmarkMoving();
        unlinkItemForEviction(*evicted);
        // wake up any readers that wait for the move to complete
        // it's safe to do now, as we have the item marked exclusive and
        // no other reader can be added to the waiters list
        wakeUpWaiters(*evicted, {});
      } else {
        // TODO: potential deadlock with markUseful for parent item
        // for now, we do not block any reader on child items but this
        // should probably be fixed
        continue;
      }
    } else {
      evicted = &item;

      token = createPutToken(*evicted);
      if (evicted->markForEvictionWhenMoving()) {
        unlinkItemForEviction(*evicted);
        wakeUpWaiters(*evicted, {});
      } else {
        continue;
      }
    }

    if (token.isValid() && shouldWriteToNvmCacheExclusive(*evicted)) {
      nvmCache_->put(*evicted, std::move(token));
    }

    const auto tid = getTierId(*evicted);
    const auto allocInfo =
        allocator_[tid]->getAllocInfo(static_cast<const void*>(evicted));
    if (evicted->hasChainedItem()) {
      (*stats_.chainedItemEvictions)[tid][allocInfo.poolId][allocInfo.classId].inc();
    } else {
      (*stats_.regularItemEvictions)[tid][allocInfo.poolId][allocInfo.classId].inc();
    }

    stats_.numEvictionSuccesses.inc();

    XDCHECK(evicted->getRefCount() == 0);
    const auto res =
        releaseBackToAllocator(*evicted, RemoveContext::kEviction, false);

    if (getNumTiers() == 1) {
      XDCHECK(res == ReleaseRes::kReleased);
    } else {
      const bool isAlreadyFreed =
          !markMovingForSlabRelease(ctx, &item, throttler);
      if (!isAlreadyFreed) {
        continue;
      }
    }
  
    return;
  }
}

template <typename CacheTrait>
template <typename Fn>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::removeIf(Item& item, Fn&& predicate) {
  auto handle = accessContainer_->removeIf(item, std::forward<Fn>(predicate));

  if (handle) {
    XDCHECK_EQ(reinterpret_cast<uintptr_t>(handle.get()),
             reinterpret_cast<uintptr_t>(&item));
    removeFromMMContainer(item);
  }

  return handle;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::removeIfExpired(const ReadHandle& handle) {
  if (!handle) {
    return false;
  }

  // We remove the item from both access and mm containers.
  // We want to make sure the caller is the only one holding the handle.
  return (bool)removeIf(*(handle.getInternal()), itemExpiryPredicate);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::markMovingForSlabRelease(
    const SlabReleaseContext& ctx, void* alloc, util::Throttler& throttler) {
  // MemoryAllocator::processAllocForRelease will execute the callback
  // if the item is not already free. So there are three outcomes here:
  //  1. Item not freed yet and marked as moving
  //  2. Item not freed yet but could not be marked as moving
  //  3. Item freed already
  //
  // For 1), return true
  // For 2), retry
  // For 3), return false to abort since no action is required

  // At first, we assume this item was already freed
  bool itemFreed = true;
  bool markedMoving = false;
  TierId tid = getTierId(alloc);
  auto numTiers = getNumTiers();
  const auto fn = [&markedMoving, &itemFreed, numTiers](void* memory) {
    // Since this callback is executed, the item is not yet freed
    itemFreed = false;
    Item* item = static_cast<Item*>(memory);
    // TODO: for chained items, moving bit is only used to avoid
    // freeing the item prematurely
    auto failIfRefNotZero = numTiers > 1 && !item->isChainedItem();
    if (item->markMoving(failIfRefNotZero)) {
      markedMoving = true;
    }
  };

  auto startTime = util::getCurrentTimeSec();
  while (true) {
    allocator_[tid]->processAllocForRelease(ctx, alloc, fn);

    // If item is already freed we give up trying to mark the item moving
    // and return false, otherwise if marked as moving, we return true.
    if (itemFreed) {
      return false;
    } else if (markedMoving) {
      return true;
    }

    // Reset this to true, since we always assume an item is freed
    // when checking with the AllocationClass
    itemFreed = true;

    if (shutDownInProgress_) {
      allocator_[tid]->abortSlabRelease(ctx);
      throw exception::SlabReleaseAborted(
          folly::sformat("Slab Release aborted while still trying to mark"
                         " as moving for Item: {}. Pool: {}, Class: {}.",
                         static_cast<Item*>(alloc)->toString(), ctx.getPoolId(),
                         ctx.getClassId()));
    }
    throttleWith(throttler, [&] {
      XLOGF(WARN,
            "Spent {} seconds, slab release still trying to mark as moving for "
            "Item: {}. Pool: {}, Class: {}.",
            util::getCurrentTimeSec() - startTime,
            static_cast<Item*>(alloc)->toString(), ctx.getPoolId(),
            ctx.getClassId());
    });
  }
}

template <typename CacheTrait>
template <typename CCacheT, typename... Args>
CCacheT* CacheAllocator<CacheTrait>::addCompactCache(folly::StringPiece name,
                                                     size_t size,
                                                     Args&&... args) {
  if (getNumTiers() != 1)
    throw std::runtime_error("TODO: compact cache for multi-tier Cache not supported.");

  if (!config_.isCompactCacheEnabled()) {
    throw std::logic_error("Compact cache is not enabled");
  }

  folly::SharedMutex::WriteHolder lock(compactCachePoolsLock_);
  auto poolId = allocator_[0]->addPool(name, size, {Slab::kSize});
  isCompactCachePool_[poolId] = true;

  auto ptr = std::make_unique<CCacheT>(
      compactCacheManager_->addAllocator(name.str(), poolId),
      std::forward<Args>(args)...);
  auto it = compactCaches_.emplace(poolId, std::move(ptr));
  XDCHECK(it.second);
  return static_cast<CCacheT*>(it.first->second.get());
}

template <typename CacheTrait>
template <typename CCacheT, typename... Args>
CCacheT* CacheAllocator<CacheTrait>::attachCompactCache(folly::StringPiece name,
                                                        Args&&... args) {
  auto& allocator = compactCacheManager_->getAllocator(name.str());
  auto poolId = allocator.getPoolId();
  // if a compact cache with this name already exists, return without creating
  // new instance
  folly::SharedMutex::WriteHolder lock(compactCachePoolsLock_);
  if (compactCaches_.find(poolId) != compactCaches_.end()) {
    return static_cast<CCacheT*>(compactCaches_[poolId].get());
  }

  auto ptr = std::make_unique<CCacheT>(allocator, std::forward<Args>(args)...);
  auto it = compactCaches_.emplace(poolId, std::move(ptr));
  XDCHECK(it.second);
  return static_cast<CCacheT*>(it.first->second.get());
}

template <typename CacheTrait>
const ICompactCache& CacheAllocator<CacheTrait>::getCompactCache(
    PoolId pid) const {
  folly::SharedMutex::ReadHolder lock(compactCachePoolsLock_);
  if (!isCompactCachePool_[pid]) {
    throw std::invalid_argument(
        folly::sformat("PoolId {} is not a compact cache", pid));
  }

  auto it = compactCaches_.find(pid);
  if (it == compactCaches_.end()) {
    throw std::invalid_argument(folly::sformat(
        "PoolId {} belongs to an un-attached compact cache", pid));
  }
  return *it->second;
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::setPoolOptimizerFor(PoolId poolId,
                                                     bool enableAutoResizing) {
  optimizerEnabled_[poolId] = enableAutoResizing;
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::resizeCompactCaches() {
  compactCacheManager_->resizeAll();
}

template <typename CacheTrait>
typename CacheTrait::MMType::LruType CacheAllocator<CacheTrait>::getItemLruType(
    const Item& item) const {
  return getMMContainer(item).getLruType(item);
}

// The order of the serialization is as follows:
//
// This is also the order of deserialization in the constructor, when
// we restore the cache allocator.
//
// ---------------------------------
// | accessContainer_              |
// | mmContainers_                 |
// | compactCacheManager_          |
// | allocator_                    |
// | metadata_                     |
// ---------------------------------
template <typename CacheTrait>
folly::IOBufQueue CacheAllocator<CacheTrait>::saveStateToIOBuf() {
  if (stats_.numActiveSlabReleases.get() != 0) {
    throw std::logic_error(
        "There are still slabs being released at the moment");
  }

  *metadata_.allocatorVersion() = kCachelibVersion;
  *metadata_.ramFormatVersion() = kCacheRamFormatVersion;
  *metadata_.cacheCreationTime() = static_cast<int64_t>(cacheCreationTime_);
  *metadata_.mmType() = MMType::kId;
  *metadata_.accessType() = AccessType::kId;

  metadata_.compactCachePools()->clear();
  const auto pools = getPoolIds();
  {
    folly::SharedMutex::ReadHolder lock(compactCachePoolsLock_);
    for (PoolId pid : pools) {
      for (unsigned int cid = 0; cid < (*stats_.fragmentationSize)[pid].size();
           ++cid) {
        uint64_t fragmentationSize = 0;
        for (TierId tid = 0; tid < getNumTiers(); tid++) {
            fragmentationSize += (*stats_.fragmentationSize)[tid][pid][cid].get();
        }
        metadata_.fragmentationSize()[pid][static_cast<ClassId>(cid)] =
            fragmentationSize;

      }
      if (isCompactCachePool_[pid]) {
        metadata_.compactCachePools()->push_back(pid);
      }
    }
  }

  *metadata_.numChainedParentItems() = stats_.numChainedParentItems.get();
  *metadata_.numChainedChildItems() = stats_.numChainedChildItems.get();
  *metadata_.numAbortedSlabReleases() = stats_.numAbortedSlabReleases.get();

  // TODO: implement serialization for multiple tiers
  auto serializeMMContainers = [](MMContainers& mmContainers) {
    MMSerializationTypeContainer state;
    for (unsigned int i = 0; i < 1 /* TODO: */ ; ++i) {
      for (unsigned int j = 0; j < mmContainers[i].size(); ++j) {
        for (unsigned int k = 0; k < mmContainers[i][j].size(); ++k) {
          if (mmContainers[i][j][k]) {
            state.pools_ref()[j][k] = mmContainers[i][j][k]->saveState();
          }
        }
      }
    }
    return state;
  };
  MMSerializationTypeContainer mmContainersState =
      serializeMMContainers(mmContainers_);

  AccessSerializationType accessContainerState = accessContainer_->saveState();
  // TODO: foreach allocator
  MemoryAllocator::SerializationType allocatorState = allocator_[0]->saveState();
  CCacheManager::SerializationType ccState = compactCacheManager_->saveState();

  AccessSerializationType chainedItemAccessContainerState =
      chainedItemAccessContainer_->saveState();

  // serialize to an iobuf queue. The caller can then copy over the serialized
  // results into a single buffer.
  folly::IOBufQueue queue;
  Serializer::serializeToIOBufQueue(queue, metadata_);
  Serializer::serializeToIOBufQueue(queue, allocatorState);
  Serializer::serializeToIOBufQueue(queue, ccState);
  Serializer::serializeToIOBufQueue(queue, mmContainersState);
  Serializer::serializeToIOBufQueue(queue, accessContainerState);
  Serializer::serializeToIOBufQueue(queue, chainedItemAccessContainerState);
  return queue;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::stopWorkers(std::chrono::seconds timeout) {
  bool success = true;
  success &= stopPoolRebalancer(timeout);
  success &= stopPoolResizer(timeout);
  success &= stopMemMonitor(timeout);
  success &= stopReaper(timeout);
  success &= stopBackgroundEvictor(timeout);
  success &= stopBackgroundPromoter(timeout);
  return success;
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::ShutDownStatus
CacheAllocator<CacheTrait>::shutDown() {
  using ShmShutDownRes = typename ShmManager::ShutDownRes;
  XLOG(DBG, "shutting down CacheAllocator");
  if (shmManager_ == nullptr) {
    throw std::invalid_argument(
        "shutDown can only be called once from a cached manager created on "
        "shared memory. You may also be incorrectly constructing your "
        "allocator. Are you passing in "
        "AllocatorType::SharedMem* ?");
  }
  XDCHECK(!config_.cacheDir.empty());

  if (config_.enableFastShutdown) {
    shutDownInProgress_ = true;
  }

  stopWorkers();

  const auto handleCount = getNumActiveHandles();
  if (handleCount != 0) {
    XLOGF(ERR, "Found {} active handles while shutting down cache. aborting",
          handleCount);
    return ShutDownStatus::kFailed;
  }

  const auto nvmShutDownStatusOpt = saveNvmCache();
  saveRamCache();
  const auto shmShutDownStatus = shmManager_->shutDown();
  const auto shmShutDownSucceeded =
      (shmShutDownStatus == ShmShutDownRes::kSuccess);
  shmManager_.reset();

  // TODO: save per-tier state

  if (shmShutDownSucceeded) {
    if (!nvmShutDownStatusOpt || *nvmShutDownStatusOpt)
      return ShutDownStatus::kSuccess;

    if (nvmShutDownStatusOpt && !*nvmShutDownStatusOpt)
      return ShutDownStatus::kSavedOnlyDRAM;
  }

  XLOGF(ERR, "Could not shutdown DRAM cache cleanly. ShutDownRes={}",
        (shmShutDownStatus == ShmShutDownRes::kFailedWrite ? "kFailedWrite"
                                                           : "kFileDeleted"));

  if (nvmShutDownStatusOpt && *nvmShutDownStatusOpt) {
    return ShutDownStatus::kSavedOnlyNvmCache;
  }

  return ShutDownStatus::kFailed;
}

template <typename CacheTrait>
std::optional<bool> CacheAllocator<CacheTrait>::saveNvmCache() {
  if (!nvmCache_) {
    return std::nullopt;
  }

  // throw any exceptions from shutting down nvmcache since we dont know the
  // state of RAM as well.
  if (!nvmCache_->isEnabled()) {
    nvmCache_->shutDown();
    return std::nullopt;
  }

  if (!nvmCache_->shutDown()) {
    XLOG(ERR, "Could not shutdown nvmcache cleanly");
    return false;
  }

  nvmCacheState_.markSafeShutDown();
  return true;
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::saveRamCache() {
  // serialize the cache state
  auto serializedBuf = saveStateToIOBuf();
  std::unique_ptr<folly::IOBuf> ioBuf = serializedBuf.move();
  ioBuf->coalesce();

  void* infoAddr =
      shmManager_->createShm(detail::kShmInfoName, ioBuf->length()).addr;
  Serializer serializer(reinterpret_cast<uint8_t*>(infoAddr),
                        reinterpret_cast<uint8_t*>(infoAddr) + ioBuf->length());
  serializer.writeToBuffer(std::move(ioBuf));
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::MMContainers
CacheAllocator<CacheTrait>::deserializeMMContainers(
    Deserializer& deserializer,
    const typename Item::PtrCompressor& compressor) {
  const auto container =
      deserializer.deserialize<MMSerializationTypeContainer>();

  /* TODO: right now, we create empty containers because deserialization
   * only works for a single (topmost) tier. */
  MMContainers mmContainers{getNumTiers()};

  for (auto& kvPool : *container.pools_ref()) {
    auto i = static_cast<PoolId>(kvPool.first);
    auto& pool = getPool(i);
    for (auto& kv : kvPool.second) {
      auto j = static_cast<ClassId>(kv.first);
      for (TierId tid = 0; tid < getNumTiers(); tid++) {
        MMContainerPtr ptr =
            std::make_unique<typename MMContainerPtr::element_type>(kv.second,
                                                                    compressor);
        auto config = ptr->getConfig();
        config.addExtraConfig(config_.trackTailHits
                                  ? pool.getAllocationClass(j).getAllocsPerSlab()
                                  : 0);
        ptr->setConfig(config);
        mmContainers[tid][i][j] = std::move(ptr);
      }
    }
  }
  // We need to drop the unevictableMMContainer in the desierializer.
  // TODO: remove this at version 17.
  if (metadata_.allocatorVersion() <= 15) {
    deserializer.deserialize<MMSerializationTypeContainer>();
  }
  return mmContainers;
}

template <typename CacheTrait>
serialization::CacheAllocatorMetadata
CacheAllocator<CacheTrait>::deserializeCacheAllocatorMetadata(
    Deserializer& deserializer) {
  auto meta = deserializer.deserialize<serialization::CacheAllocatorMetadata>();
  // TODO:
  // Once everyone is on v8 or later, remove the outter if.
  if (kCachelibVersion > 8) {
    if (*meta.ramFormatVersion() != kCacheRamFormatVersion) {
      throw std::runtime_error(
          folly::sformat("Expected cache ram format version {}. But found {}.",
                         kCacheRamFormatVersion, *meta.ramFormatVersion()));
    }
  }

  if (*meta.accessType() != AccessType::kId) {
    throw std::invalid_argument(
        folly::sformat("Expected {}, got {} for AccessType", *meta.accessType(),
                       AccessType::kId));
  }

  if (*meta.mmType() != MMType::kId) {
    throw std::invalid_argument(folly::sformat("Expected {}, got {} for MMType",
                                               *meta.mmType(), MMType::kId));
  }
  return meta;
}

template <typename CacheTrait>
int64_t CacheAllocator<CacheTrait>::getNumActiveHandles() const {
  return handleCount_.getSnapshot();
}

template <typename CacheTrait>
int64_t CacheAllocator<CacheTrait>::getHandleCountForThread() const {
  return handleCount_.tlStats();
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::resetHandleCountForThread_private() {
  handleCount_.tlStats() = 0;
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::adjustHandleCountForThread_private(
    int64_t delta) {
  handleCount_.tlStats() += delta;
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::initStats() {
  stats_.init();

  // deserialize the fragmentation size of each thread.
  for (const auto& pid : *metadata_.fragmentationSize()) {
    for (const auto& cid : pid.second) {
      //in multi-tier we serialized as the sum - no way
      //to get back so just divide the two for now
      //TODO: proper multi-tier serialization
      uint64_t total = static_cast<uint64_t>(cid.second);
      uint64_t part = total / getNumTiers();
      uint64_t sum = 0;
      for (TierId tid = 1; tid < getNumTiers(); tid++) {
        (*stats_.fragmentationSize)[tid][pid.first][cid.first].set(part);
        sum += part;
      }
      uint64_t leftover = total - sum;
      (*stats_.fragmentationSize)[0][pid.first][cid.first].set(leftover);

    }
  }

  // deserialize item counter stats
  stats_.numChainedParentItems.set(*metadata_.numChainedParentItems());
  stats_.numChainedChildItems.set(*metadata_.numChainedChildItems());
  stats_.numAbortedSlabReleases.set(
      static_cast<uint64_t>(*metadata_.numAbortedSlabReleases()));
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::forEachChainedItem(
    const Item& parent, std::function<void(ChainedItem&)> func) {
  auto l = chainedItemLocks_.lockShared(parent.getKey());

  auto headHandle = findChainedItem(parent);
  if (!headHandle) {
    return;
  }

  ChainedItem* head = &headHandle.get()->asChainedItem();
  while (head) {
    func(*head);
    head = head->getNext(compressor_);
  }
}

template <typename CacheTrait>
typename CacheAllocator<CacheTrait>::WriteHandle
CacheAllocator<CacheTrait>::findChainedItem(const Item& parent) const {
  const auto cPtr = compressor_.compress(&parent);
  return chainedItemAccessContainer_->find(
      Key{reinterpret_cast<const char*>(&cPtr), ChainedItem::kKeySize});
}

template <typename CacheTrait>
template <typename Handle, typename Iter>
CacheChainedAllocs<CacheAllocator<CacheTrait>, Handle, Iter>
CacheAllocator<CacheTrait>::viewAsChainedAllocsT(const Handle& parent) {
  XDCHECK(parent);
  auto handle = parent.clone();
  if (!handle) {
    throw std::invalid_argument("Failed to clone item handle");
  }

  if (!handle->hasChainedItem()) {
    throw std::invalid_argument(
        folly::sformat("Failed to materialize chain. Parent does not have "
                       "chained items. Parent: {}",
                       parent->toString()));
  }

  auto l = chainedItemLocks_.lockShared(handle->getKey());
  auto head = findChainedItem(*handle);
  return CacheChainedAllocs<CacheAllocator<CacheTrait>, Handle, Iter>{
      std::move(l), std::move(handle), *head, compressor_};
}

template <typename CacheTrait>
GlobalCacheStats CacheAllocator<CacheTrait>::getGlobalCacheStats() const {
  GlobalCacheStats ret{};
  stats_.populateGlobalCacheStats(ret);

  ret.numItems = accessContainer_->getStats().numKeys;

  const uint64_t currTime = util::getCurrentTimeSec();
  ret.cacheInstanceUpTime = currTime - cacheInstanceCreationTime_;
  ret.ramUpTime = currTime - cacheCreationTime_;
  ret.nvmUpTime = currTime - nvmCacheState_.getCreationTime();
  ret.nvmCacheEnabled = nvmCache_ ? nvmCache_->isEnabled() : false;
  ret.reaperStats = getReaperStats();
  ret.evictionStats = getBackgroundMoverStats(MoverDir::Evict);
  ret.promotionStats = getBackgroundMoverStats(MoverDir::Promote);
  ret.numActiveHandles = getNumActiveHandles();

  ret.isNewRamCache = cacheCreationTime_ == cacheInstanceCreationTime_;
  ret.isNewNvmCache =
      nvmCacheState_.getCreationTime() == cacheInstanceCreationTime_;

  return ret;
}

template <typename CacheTrait>
CacheMemoryStats CacheAllocator<CacheTrait>::getCacheMemoryStats() const {
  size_t totalCacheSize = 0;
  size_t configuredTotalCacheSize = 0;
  for(auto& allocator: allocator_) {
    totalCacheSize += allocator->getMemorySize();
    configuredTotalCacheSize += allocator->getMemorySizeInclAdvised();
  }
  auto addSize = [this](size_t a, PoolId pid) {
    return a + allocator_[currentTier()]->getPool(pid).getPoolSize();
  };
  const auto regularPoolIds = getRegularPoolIds();
  const auto ccCachePoolIds = getCCachePoolIds();
  size_t configuredRegularCacheSize = std::accumulate(
      regularPoolIds.begin(), regularPoolIds.end(), 0ULL, addSize);
  size_t configuredCompactCacheSize = std::accumulate(
      ccCachePoolIds.begin(), ccCachePoolIds.end(), 0ULL, addSize);

  return CacheMemoryStats{totalCacheSize,
                          configuredTotalCacheSize,
                          configuredRegularCacheSize,
                          configuredCompactCacheSize,
                          allocator_[currentTier()]->getAdvisedMemorySize(),
                          memMonitor_ ? memMonitor_->getMaxAdvisePct() : 0,
                          allocator_[currentTier()]->getUnreservedMemorySize(),
                          nvmCache_ ? nvmCache_->getSize() : 0,
                          util::getMemAvailable(),
                          util::getRSSBytes()};
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::autoResizeEnabledForPool(PoolId pid) const {
  folly::SharedMutex::ReadHolder lock(compactCachePoolsLock_);
  if (isCompactCachePool_[pid]) {
    // compact caches need to be registered to enable auto resizing
    return optimizerEnabled_[pid];
  } else {
    // by default all regular pools participate in auto resizing
    return true;
  }
}

template <typename CacheTrait>
void CacheAllocator<CacheTrait>::startCacheWorkers() {
  initWorkers();
}

template <typename CacheTrait>
template <typename T>
bool CacheAllocator<CacheTrait>::stopWorker(folly::StringPiece name,
                                            std::unique_ptr<T>& worker,
                                            std::chrono::seconds timeout) {
  std::lock_guard<std::mutex> l(workersMutex_);
  if (!worker) {
    return true;
  }

  bool ret = worker->stop(timeout);
  if (ret) {
    XLOGF(DBG1, "Stopped worker '{}'", name);
  } else {
    XLOGF(ERR, "Couldn't stop worker '{}', timeout: {} seconds", name,
          timeout.count());
  }
  worker.reset();
  return ret;
}

template <typename CacheTrait>
template <typename T, typename... Args>
bool CacheAllocator<CacheTrait>::startNewWorker(
    folly::StringPiece name,
    std::unique_ptr<T>& worker,
    std::chrono::milliseconds interval,
    Args&&... args) {
  if (!stopWorker(name, worker)) {
    return false;
  }

  std::lock_guard<std::mutex> l(workersMutex_);
  worker = std::make_unique<T>(*this, std::forward<Args>(args)...);
  bool ret = worker->start(interval, name);
  if (ret) {
    XLOGF(DBG1, "Started worker '{}'", name);
  } else {
    XLOGF(ERR, "Couldn't start worker '{}', interval: {} milliseconds", name,
          interval.count());
  }
  return ret;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::startNewPoolRebalancer(
    std::chrono::milliseconds interval,
    std::shared_ptr<RebalanceStrategy> strategy,
    unsigned int freeAllocThreshold) {
  if (!startNewWorker("PoolRebalancer", poolRebalancer_, interval, strategy,
                      freeAllocThreshold)) {
    return false;
  }

  config_.poolRebalanceInterval = interval;
  config_.defaultPoolRebalanceStrategy = strategy;
  config_.poolRebalancerFreeAllocThreshold = freeAllocThreshold;

  return true;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::startNewPoolResizer(
    std::chrono::milliseconds interval,
    unsigned int poolResizeSlabsPerIter,
    std::shared_ptr<RebalanceStrategy> strategy) {
  if (!startNewWorker("PoolResizer", poolResizer_, interval,
                      poolResizeSlabsPerIter, strategy)) {
    return false;
  }

  config_.poolResizeInterval = interval;
  config_.poolResizeSlabsPerIter = poolResizeSlabsPerIter;
  config_.poolResizeStrategy = strategy;
  return true;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::startNewPoolOptimizer(
    std::chrono::seconds regularInterval,
    std::chrono::seconds ccacheInterval,
    std::shared_ptr<PoolOptimizeStrategy> strategy,
    unsigned int ccacheStepSizePercent) {
  // For now we are asking the worker to wake up every second to see whether
  // it should do actual size optimization. Probably need to move to using
  // the same interval for both, with confirmation of further experiments.
  const auto workerInterval = std::chrono::seconds(1);
  if (!startNewWorker("PoolOptimizer", poolOptimizer_, workerInterval, strategy,
                      regularInterval.count(), ccacheInterval.count(),
                      ccacheStepSizePercent)) {
    return false;
  }

  config_.regularPoolOptimizeInterval = regularInterval;
  config_.compactCacheOptimizeInterval = ccacheInterval;
  config_.poolOptimizeStrategy = strategy;
  config_.ccacheOptimizeStepSizePercent = ccacheStepSizePercent;

  return true;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::startNewMemMonitor(
    std::chrono::milliseconds interval,
    MemoryMonitor::Config config,
    std::shared_ptr<RebalanceStrategy> strategy) {
  if (!startNewWorker("MemoryMonitor", memMonitor_, interval, config,
                      strategy)) {
    return false;
  }

  config_.memMonitorInterval = interval;
  config_.memMonitorConfig = std::move(config);
  config_.poolAdviseStrategy = strategy;
  return true;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::startNewReaper(
    std::chrono::milliseconds interval,
    util::Throttler::Config reaperThrottleConfig) {
  if (!startNewWorker("Reaper", reaper_, interval, reaperThrottleConfig)) {
    return false;
  }

  config_.reaperInterval = interval;
  config_.reaperConfig = reaperThrottleConfig;
  return true;
}

template <typename CacheTrait>
auto CacheAllocator<CacheTrait>::getAssignedMemoryToBgWorker(size_t evictorId, size_t numWorkers, TierId tid)
{
  std::vector<MemoryDescriptorType> asssignedMemory;
  // TODO: for now, only evict from tier 0
  auto pools = filterCompactCachePools(allocator_[tid]->getPoolIds());
  for (const auto pid : pools) {
    const auto& mpStats = getPoolByTid(pid,tid).getStats();
    for (const auto cid : mpStats.classIds) {
      if (backgroundWorkerId(tid, pid, cid, numWorkers) == evictorId) {
        asssignedMemory.emplace_back(tid, pid, cid);
      }
    }
  }
  return asssignedMemory;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::startNewBackgroundEvictor(
    std::chrono::milliseconds interval,
    std::shared_ptr<BackgroundMoverStrategy> strategy,
    size_t threads) {
  XDCHECK(threads > 0);
  backgroundEvictor_.resize(threads);
  bool result = true;

  for (size_t i = 0; i < threads; i++) {
    auto ret = startNewWorker("BackgroundEvictor" + std::to_string(i), backgroundEvictor_[i], interval, strategy, MoverDir::Evict);
    result = result && ret;

    if (result) {
      backgroundEvictor_[i]->setAssignedMemory(getAssignedMemoryToBgWorker(i, backgroundEvictor_.size(), 0));
    }
  }
  return result;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::startNewBackgroundPromoter(
    std::chrono::milliseconds interval,
    std::shared_ptr<BackgroundMoverStrategy> strategy,
    size_t threads,
    bool useQueue) {
  XDCHECK(threads > 0);
  XDCHECK(getNumTiers() > 1);
  backgroundPromoter_.resize(threads);
  bool result = true;
  for (size_t i = 0; i < threads; i++) {
    auto ret = startNewWorker("BackgroundPromoter" + std::to_string(i), backgroundPromoter_[i], interval, strategy, useQueue ? MoverDir::PromoteFromQueue : MoverDir::Promote);
    result = result && ret;

    if (result) {
      backgroundPromoter_[i]->setAssignedMemory(getAssignedMemoryToBgWorker(i, backgroundPromoter_.size(), 1));
    }
  }
  return result;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::stopPoolRebalancer(
    std::chrono::seconds timeout) {
  return stopWorker("PoolRebalancer", poolRebalancer_, timeout);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::stopPoolResizer(std::chrono::seconds timeout) {
  return stopWorker("PoolResizer", poolResizer_, timeout);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::stopPoolOptimizer(
    std::chrono::seconds timeout) {
  return stopWorker("PoolOptimizer", poolOptimizer_, timeout);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::stopMemMonitor(std::chrono::seconds timeout) {
  return stopWorker("MemoryMonitor", memMonitor_, timeout);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::stopReaper(std::chrono::seconds timeout) {
  return stopWorker("Reaper", reaper_, timeout);
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::stopBackgroundEvictor(std::chrono::seconds timeout) {
  bool result = true;
  for (size_t i = 0; i < backgroundEvictor_.size(); i++) {
    auto ret = stopWorker("BackgroundEvictor", backgroundEvictor_[i], timeout);
    result = result && ret;
  }
  return result;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::stopBackgroundPromoter(std::chrono::seconds timeout) {
  bool result = true;
  for (size_t i = 0; i < backgroundPromoter_.size(); i++) {
    auto ret = stopWorker("BackgroundPromoter", backgroundPromoter_[i], timeout);
    result = result && ret;
  }
  return result;
}

template <typename CacheTrait>
bool CacheAllocator<CacheTrait>::cleanupStrayShmSegments(
    const std::string& cacheDir, bool posix) {
  if (util::getStatIfExists(cacheDir, nullptr) && util::isDir(cacheDir)) {
    try {
      // cache dir exists. clean up only if there are no other processes
      // attached. if another process was attached, the following would fail.
      ShmManager::cleanup(cacheDir, posix);

      // TODO: cleanup per-tier state
    } catch (const std::exception& e) {
      XLOGF(ERR, "Error cleaning up {}. Exception: ", cacheDir, e.what());
      return false;
    }
  } else {
    // cache dir did not exist. Try to nuke the segments we know by name.
    // Any other concurrent process can not be attached to the segments or
    // even if it does, we want to mark it for destruction.
    ShmManager::removeByName(cacheDir, detail::kShmInfoName, posix);
    ShmManager::removeByName(cacheDir, detail::kShmCacheName
                             + std::to_string(0 /* TODO: per tier */), posix);
    ShmManager::removeByName(cacheDir, detail::kShmHashTableName, posix);
    ShmManager::removeByName(cacheDir, detail::kShmChainedItemHashTableName,
                             posix);
  }
  return true;
}

template <typename CacheTrait>
uint64_t CacheAllocator<CacheTrait>::getItemPtrAsOffset(const void* ptr) {
  // Return unt64_t instead of uintptr_t to accommodate platforms where
  // the two differ (e.g. Mac OS 12) - causing templating instantiation
  // errors downstream.

  auto tid = getTierId(ptr);

  // if this succeeeds, the address is valid within the cache.
  allocator_[tid]->getAllocInfo(ptr);

  if (!isOnShm_ || !shmManager_) {
    throw std::invalid_argument("Shared memory not used");
  }

  const auto& shm = shmManager_->getShmByName(detail::kShmCacheName + std::to_string(tid));

  return reinterpret_cast<uint64_t>(ptr) -
         reinterpret_cast<uint64_t>(shm.getCurrentMapping().addr);
}

template <typename CacheTrait>
util::StatsMap CacheAllocator<CacheTrait>::getNvmCacheStatsMap() const {
  auto ret = nvmCache_ ? nvmCache_->getStatsMap() : util::StatsMap{};
  if (nvmAdmissionPolicy_) {
    nvmAdmissionPolicy_->getCounters(ret.createCountVisitor());
  }
  return ret;
}

} // namespace cachelib
} // namespace facebook
