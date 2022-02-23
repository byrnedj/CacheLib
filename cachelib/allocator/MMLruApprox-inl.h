/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
namespace detail {
template <typename T>
bool areBytesSame(const T& one, const T& two) {
  return std::memcmp(&one, &two, sizeof(T)) == 0;
}
} // namespace detail

/* Container Interface Implementation */
template <typename T, MMLruApproxApprox::Hook<T> T::*HookPtr>
MMLruApprox::Container<T, HookPtr>::Container(serialization::MMLruApproxObject object,
                                        PtrCompressor compressor)
    : compressor_(std::move(compressor)),
      lru_(*object.lru_ref(), compressor_),
      insertionPoint_(compressor_.unCompress(
          CompressedPtr{*object.compressedInsertionPoint_ref()})),
      tailSize_(*object.tailSize_ref()),
      config_(*object.config_ref()) {
  lruRefreshTime_ = config_.lruRefreshTime;
  nextReconfigureTime_ = config_.mmReconfigureIntervalSecs.count() == 0
                             ? std::numeric_limits<Time>::max()
                             : static_cast<Time>(util::getCurrentTimeSec()) +
                                   config_.mmReconfigureIntervalSecs.count();
}

template <typename T, MMLruApprox::Hook<T> T::*HookPtr>
bool MMLruApprox::Container<T, HookPtr>::recordAccess(T& node,
                                                AccessMode mode) noexcept {
  if ((mode == AccessMode::kWrite && !config_.updateOnWrite) ||
      (mode == AccessMode::kRead && !config_.updateOnRead)) {
    return false;
  }

  const auto curr = static_cast<Time>(util::getCurrentTimeSec());
  // check if the node is still being memory managed
  if (node.isInMMContainer() &&
      ((curr >= getUpdateTime(node) +
                    lruRefreshTime_.load(std::memory_order_relaxed)) ||
       !isAccessed(node))) {
    if (!isAccessed(node)) {
      markAccessed(node);
    }

    auto func = [this, &node, curr]() {
      if (node.isInMMContainer()) {
        setUpdateTime(node, curr);
      }
    };

    return true;
  }
  return false;
}

template <typename T, MMLruApprox::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat MMLruApprox::Container<T, HookPtr>::getEvictionAgeStat(
    uint64_t projectedLen) const noexcept {
}

template <typename T, MMLruApprox::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat
MMLruApprox::Container<T, HookPtr>::getEvictionAgeStatLocked(
    uint64_t projectedLength) const noexcept {
  EvictionAgeStat stat{};
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());
}

template <typename T, MMLruApprox::Hook<T> T::*HookPtr>
void MMLruApprox::Container<T, HookPtr>::setConfig(const Config& newConfig) {
}

template <typename T, MMLruApprox::Hook<T> T::*HookPtr>
typename MMLruApprox::Config MMLruApprox::Container<T, HookPtr>::getConfig() const {
    return;
}

//template <typename T, MMLruApprox::Hook<T> T::*HookPtr>
//bool MMLruApprox::Container<T, HookPtr>::add(T& node) noexcept {
//  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());
//
//  if (node.isInMMContainer()) {
//    return false;
//  }
//  node.markInMMContainer();
//  setUpdateTime(node, currTime);
//  unmarkAccessed(node);
//  return true;
//}

template <typename T, MMLruApprox::Hook<T> T::*HookPtr>
typename MMLruApprox::Container<T, HookPtr>::Iterator
MMLruApprox::Container<T, HookPtr>::getEvictionIterator() const noexcept {
  //LockHolder l(*lruMutex_);
  //return Iterator{std::move(l), lru_.rbegin()};
  //generate a set of candidates for eviction

}


template <typename T, MMLruApprox::Hook<T> T::*HookPtr>
serialization::MMLruApproxObject MMLruApprox::Container<T, HookPtr>::saveState()
    const noexcept {
  serialization::MMLruApproxConfig configObject;
  *configObject.lruRefreshTime_ref() =
      lruRefreshTime_.load(std::memory_order_relaxed);
  *configObject.lruRefreshRatio_ref() = config_.lruRefreshRatio;
  *configObject.updateOnWrite_ref() = config_.updateOnWrite;
  *configObject.updateOnRead_ref() = config_.updateOnRead;
  *configObject.tryLockUpdate_ref() = config_.tryLockUpdate;
  *configObject.lruInsertionPointSpec_ref() = config_.lruInsertionPointSpec;

  serialization::MMLruApproxObject object;
  *object.config_ref() = configObject;
  *object.compressedInsertionPoint_ref() =
      compressor_.compress(insertionPoint_).saveState();
  *object.tailSize_ref() = tailSize_;
  *object.lru_ref() = lru_.saveState();
  return object;
}

template <typename T, MMLruApprox::Hook<T> T::*HookPtr>
MMContainerStat MMLruApprox::Container<T, HookPtr>::getStats() const noexcept {
}


// Iterator Context Implementation
template <typename T, MMLruApprox::Hook<T> T::*HookPtr>
MMLruApprox::Container<T, HookPtr>::Iterator::Iterator(
    LockHolder l, const typename LruList::Iterator& iter) noexcept
    : LruList::Iterator(iter), l_(std::move(l)) {}
} // namespace cachelib
} // namespace facebook
