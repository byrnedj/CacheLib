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

#include <folly/Conv.h>
#include <folly/ProducerConsumerQueue.h>
#include <folly/ThreadLocal.h>
#include <folly/lang/Aligned.h>
#include <folly/logging/xlog.h>
#include <folly/system/ThreadName.h>
#include <folly/synchronization/Latch.h>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

// BinaryKVReplayGenerator generates the cachelib requests based on the
// requests read from the given binary trace file made with KVReplayGenerator
// In order to minimize the contentions for the request submission queues
// which might need to be dispatched by multiple stressor threads,
// the requests are sharded to each stressor by doing hashing over the key.
class BinaryKVReplayGenerator : public ReplayGeneratorBase {
 public:
  explicit BinaryKVReplayGenerator(const StressorConfig& config)
      : ReplayGeneratorBase(config), binaryStream_(config) {
    for (uint32_t i = 0; i < numShards_; ++i) {
      stressorCtxs_.emplace_back(std::make_unique<StressorCtx>(i));
    }

    XLOGF(INFO,
          "Started BinaryKVReplayGenerator (amp factor {}, # of stressor threads {}, fast foward {})",
          ampFactor_, numShards_, fastForwardCount_);
  }

  virtual ~BinaryKVReplayGenerator() {
    XCHECK(shouldShutdown());
  }

  // getReq generates the next request from the trace file.
  const Request& getReq(
      uint8_t,
      std::mt19937_64&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  void renderStats(uint64_t, std::ostream& out) const override {
    out << std::endl << "== BinaryKVReplayGenerator Stats ==" << std::endl;

    out << folly::sformat("{}: {:.2f} million (parse error: {})",
                          "Total Processed Samples",
                          (double)parseSuccess.load() / 1e6, parseError.load())
        << std::endl;
  }

  void notifyResult(uint64_t requestId, OpResultType result) override;

  void markFinish() override { getStressorCtx().markFinish(); }

 private:
  // Interval at which the submission queue is polled when it is either
  // full (producer) or empty (consumer).
  // We use polling with the delay since the ProducerConsumerQueue does not
  // support blocking read or writes with a timeout
  static constexpr uint64_t checkIntervalUs_ = 100;
  static constexpr size_t kMaxRequests = 50000000; //just stores pointers to mmap'd data
  static constexpr size_t PG_RELEASE = 100000000;
  uint64_t reqsCompleted = 0;

  // StressorCtx keeps track of the state including the submission queues
  // per stressor thread. Since there is only one request generator thread,
  // lock-free ProducerConsumerQueue is used for performance reason.
  // Also, separate queue which is dispatched ahead of any requests in the
  // submission queue is used for keeping track of the requests which need to be
  // resubmitted (i.e., a request having remaining repeat count); there could
  // be more than one requests outstanding for async stressor while only one
  // can be outstanding for sync stressor
  struct StressorCtx {
    explicit StressorCtx(uint32_t id)
        : id_(id), reqIdx_(id) {
      std::string_view s{"abc"};
      requestPtr_ = new Request(s,reinterpret_cast<size_t*>(0),OpType::kGet,0);
    }

    bool isFinished() { return finished_.load(std::memory_order_relaxed); }
    void markFinish() { finished_.store(true, std::memory_order_relaxed); }

    Request* requestPtr_;
    uint64_t reqIdx_{0};
    uint32_t id_{0};
    // Thread that finish its operations mark it here, so we will skip
    // further request on its shard
    std::atomic<bool> finished_{false};
  };

  // Used to assign stressorIdx_
  std::atomic<uint32_t> incrementalIdx_{0};

  // A sticky index assigned to each stressor threads that calls into
  // the generator.
  folly::ThreadLocalPtr<uint32_t> stressorIdx_;

  // Vector size is equal to the # of stressor threads;
  // stressorIdx_ is used to index.
  std::vector<std::unique_ptr<StressorCtx>> stressorCtxs_;

  // Class that holds a vector of pointers to the
  // binary data
  BinaryFileStream binaryStream_;

  // Used to signal end of file as EndOfTrace exception
  std::atomic<bool> eof{false};

  // Stats
  std::atomic<uint64_t> parseError = 0;
  std::atomic<uint64_t> parseSuccess = 0;

  void setEOF() { eof.store(true, std::memory_order_relaxed); }
  bool isEOF() { return eof.load(std::memory_order_relaxed); }

  inline StressorCtx& getStressorCtx(size_t shardId) {
    XCHECK_LT(shardId, numShards_);
    return *stressorCtxs_[shardId];
  }

  inline StressorCtx& getStressorCtx() {
    if (!stressorIdx_.get()) {
      stressorIdx_.reset(new uint32_t(incrementalIdx_++));
    }

    return getStressorCtx(*stressorIdx_);
  }
  
};

const Request& BinaryKVReplayGenerator::getReq(uint8_t,
                                         std::mt19937_64&,
                                         std::optional<uint64_t>) {

  auto& stressorCtx = getStressorCtx();
  auto& r = *stressorCtx.requestPtr_;
  BinaryRequest* prevReq = reinterpret_cast<BinaryRequest*>(*(r.requestId));
  if (prevReq != nullptr && prevReq->repeats_ > 1) {
    prevReq->repeats_ = prevReq->repeats_ - 1;
  } else {
    BinaryRequest *req = nullptr;
    try {
      req = binaryStream_.getNextPtr(stressorCtx.reqIdx_);
    } catch (const EndOfTrace& e) {
      throw cachelib::cachebench::EndOfTrace("Test stopped or EOF reached");
    }
    
    XDCHECK_NE(req,nullptr);
    XDCHECK_NE(reinterpret_cast<uint64_t>(req),0);
    XDCHECK_LT(req->op_,12);
    auto key = req->getKey(binaryStream_.getKeyOffset());
    OpType op;
    switch (req->op_) {
      case 1:
        op = OpType::kGet;
        break;
      case 2:
        op = OpType::kSet;
        break;
      case 3:
        op = OpType::kDel;
        break;
    }
    r.update(key,
             const_cast<size_t*>(reinterpret_cast<size_t*>(&req->valueSize_)),
             op,
             req->ttl_,
             reinterpret_cast<uint64_t>(req));
    stressorCtx.reqIdx_ += numShards_;
  }
  return r;
}

void BinaryKVReplayGenerator::notifyResult(uint64_t requestId, OpResultType) {
  reqsCompleted++;
  //every 100M requests we can dump the old pages
  //here we are at PG_REQUESTS*1 (200M) we safely can release the first 100M requests
  if ((reqsCompleted % PG_RELEASE == 0) && reqsCompleted >= PG_RELEASE*2) {
      binaryStream_.releaseOldData(PG_RELEASE,reqsCompleted);
  }
  
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
