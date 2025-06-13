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

#include "cachelib/navy/block_cache/Region.h"

#include "cachelib/navy/common/NavyThread.h"

#include <openssl/evp.h>
#include <openssl/err.h>
// ────────────────────────────────────────────────────────────────────
//   Global AES-256-CTR test key + IV (in an anonymous namespace)
// ────────────────────────────────────────────────────────────────────
namespace {


/** 
 * AES-256-CTR “test” key (32 bytes). 
 * In production, you must generate a fresh random key and never hard-code it. 
 */
static const unsigned char kAesKey[32] = {
    0x60, 0x3d, 0xeb, 0x10, 0x15, 0xca, 0x71, 0xbe,
    0x2b, 0x73, 0xae, 0xf0, 0x85, 0x7d, 0x77, 0x81,
    0x1f, 0x35, 0x2c, 0x07, 0x3b, 0x61, 0x08, 0xd7,
    0x2d, 0x98, 0x10, 0xa3, 0x09, 0x14, 0xdf, 0xf4
};

/**
 * AES-CTR IV (nonce). Always use a unique IV per encryption in production.
 * Here we use a fixed IV for demonstration only.
 */
static const unsigned char kAesIv[16] = {
    0x00, 0x01, 0x02, 0x03,
    0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b,
    0x0c, 0x0d, 0x0e, 0x0f
};
// In an anonymous namespace (Region.cpp)
thread_local EVP_CIPHER_CTX* tlsEncCtx = nullptr;
thread_local EVP_CIPHER_CTX* tlsDecCtx = nullptr;

void initTlsEncCtx() {
  if (!tlsEncCtx) {
    tlsEncCtx = EVP_CIPHER_CTX_new();
    EVP_EncryptInit_ex(tlsEncCtx, EVP_aes_256_ctr(), nullptr, kAesKey, kAesIv);
  }
}
void resetTlsEncCtx() {
  EVP_EncryptInit_ex(tlsEncCtx, EVP_aes_256_ctr(), nullptr, kAesKey, kAesIv);
}

void initTlsDecCtx() {
  if (!tlsDecCtx) {
    tlsDecCtx = EVP_CIPHER_CTX_new();
    EVP_DecryptInit_ex(tlsDecCtx, EVP_aes_256_ctr(), nullptr, kAesKey, kAesIv);
  }
}
void resetTlsDecCtx() {
  EVP_DecryptInit_ex(tlsDecCtx, EVP_aes_256_ctr(), nullptr, kAesKey, kAesIv);
}

} // anonymous namespace

namespace facebook::cachelib::navy {

bool Region::readyForReclaim(bool wait) {
  std::unique_lock<TimedMutex> l{lock_};
  flags_ |= kBlockAccess;
  bool ready = false;
  while (!(ready = (activeOpenLocked() == 0UL)) && wait) {
    cond_.wait(l);
  }

  return ready;
}

uint32_t Region::activeOpenLocked() {
  return activePhysReaders_ + activeInMemReaders_ + activeWriters_;
}

std::tuple<RegionDescriptor, RelAddress> Region::openAndAllocate(
    uint32_t size) {
  std::lock_guard<TimedMutex> l{lock_};
  XDCHECK(!(flags_ & kBlockAccess));
  if (!canAllocateLocked(size)) {
    return std::make_tuple(RegionDescriptor{OpenStatus::Error}, RelAddress{});
  }
  activeWriters_++;
  return std::make_tuple(
      RegionDescriptor::makeWriteDescriptor(OpenStatus::Ready, regionId_),
      allocateLocked(size));
}

RegionDescriptor Region::openForRead() {
  std::unique_lock<TimedMutex> l{lock_};
  if (flags_ & kBlockAccess) {
    // Region is currently in reclaim, retry later
    if (getCurrentNavyThread()) {
      // If we are on fiber, we can just sleep here
      cond_.wait(l);
    }
    return RegionDescriptor{OpenStatus::Retry};
  }
  bool physReadMode = false;
  if (isFlushedLocked() || !buffer_) {
    physReadMode = true;
    activePhysReaders_++;
  } else {
    activeInMemReaders_++;
  }
  return RegionDescriptor::makeReadDescriptor(
      OpenStatus::Ready, regionId_, physReadMode);
}

std::unique_ptr<Buffer> Region::detachBuffer() {
  std::unique_lock<TimedMutex> l{lock_};
  XDCHECK_NE(buffer_, nullptr);
  while (activeInMemReaders_ != 0) {
    cond_.wait(l);
  }

  XDCHECK_EQ(activeWriters_, 0UL);
  auto retBuf = std::move(buffer_);
  buffer_ = nullptr;
  return retBuf;
}

// This function flushes the attached buffer if there are no active writers
// by calling the callBack function that is expected to write the buffer to
// underlying device. If there are active writers, the caller is expected
// to call this function again.
Region::FlushRes Region::flushBuffer(
    std::function<bool(RelAddress, BufferView)> callBack) {
  std::unique_lock<TimedMutex> lock{lock_};
  if (activeWriters_ != 0) {
    return FlushRes::kRetryPendingWrites;
  }
  if (!isFlushedLocked()) {
    lock.unlock();
    if (callBack(RelAddress{regionId_, 0}, buffer_->view())) {
      lock.lock();
      flags_ |= kFlushed;
      return FlushRes::kSuccess;
    }
    return FlushRes::kRetryDeviceFailure;
  }
  return FlushRes::kSuccess;
}

void Region::cleanupBuffer(std::function<void(RegionId, BufferView)> callBack) {
  std::unique_lock<TimedMutex> lock{lock_};
  while (activeWriters_ != 0) {
    cond_.wait(lock);
  }
  if (!isCleanedupLocked()) {
    lock.unlock();
    callBack(regionId_, buffer_->view());
    lock.lock();
    flags_ |= kCleanedup;
  }
}

void Region::reset() {
  std::lock_guard<TimedMutex> l{lock_};
  XDCHECK_EQ(activeOpenLocked(), 0U);
  priority_ = 0;
  flags_ = 0;
  activeWriters_ = 0;
  activePhysReaders_ = 0;
  activeInMemReaders_ = 0;
  lastEntryEndOffset_ = 0;
  numItems_ = 0;
  cond_.notifyAll();
}

void Region::close(RegionDescriptor&& desc) {
  std::lock_guard<TimedMutex> l{lock_};
  switch (desc.mode()) {
  case OpenMode::Write:
    XDCHECK_GT(activeWriters_, 0u);
    if (--activeWriters_ == 0) {
      cond_.notifyAll();
    }
    break;
  case OpenMode::Read:
    if (desc.isPhysReadMode()) {
      XDCHECK_GT(activePhysReaders_, 0u);
      if (--activePhysReaders_ == 0) {
        cond_.notifyAll();
      }
    } else {
      XDCHECK_GT(activeInMemReaders_, 0u);
      if (--activeInMemReaders_ == 0) {
        cond_.notifyAll();
      }
    }
    break;
  default:
    XDCHECK(false);
  }
}

RelAddress Region::allocateLocked(uint32_t size) {
  XDCHECK(canAllocateLocked(size));
  auto offset = lastEntryEndOffset_;
  lastEntryEndOffset_ += size;
  numItems_++;
  return RelAddress{regionId_, offset};
}

void Region::writeToBuffer(uint32_t offset, BufferView buf) {
  std::lock_guard l{lock_};

  XDCHECK_NE(buffer_, nullptr);
  auto size = buf.size();
  XDCHECK_LE(offset + size, buffer_->size());
  initTlsEncCtx();
  resetTlsEncCtx();
  int outLen = 0, finalLen = 0;
  EVP_EncryptUpdate(tlsEncCtx, buffer_->data() + offset, &outLen, buf.data(), size);
  EVP_EncryptFinal_ex(tlsEncCtx, buffer_->data() + offset + outLen, &finalLen);
  XDCHECK_EQ(static_cast<size_t>(outLen + finalLen), size);
  //memcpy(buffer_->data() + offset, coalseced->data(), size);
}

void Region::readFromBuffer(uint32_t fromOffset,
                            MutableBufferView outBuf) const {
  std::lock_guard l{lock_};
 
  XDCHECK_NE(buffer_, nullptr);
  XDCHECK_LE(fromOffset + outBuf.size(), buffer_->size());
  
  initTlsDecCtx();
  resetTlsDecCtx();
  int outLen = 0, finalLen = 0;
  const size_t cipherLen = outBuf.size();
  const unsigned char* inPtr = buffer_->data() + fromOffset;
  unsigned char* outPtr = outBuf.data();
  int res = EVP_DecryptUpdate(tlsDecCtx, outPtr , &outLen, inPtr, cipherLen);
  if (res != 1) {
    unsigned long err = ERR_get_error();
    XDCHECK_EQ(err, 0UL) << "Decryption failed with error: " << ERR_reason_error_string(err);
  }

  res = EVP_DecryptFinal_ex(tlsDecCtx, outPtr, &finalLen);
  if (res != 1) {
    unsigned long err = ERR_get_error();
    XDCHECK_EQ(err, 0UL) << "Final decryption failed with error: " << ERR_reason_error_string(err);
  }
  XDCHECK_EQ(static_cast<size_t>(outLen + finalLen), outBuf.size());
  //memcpy(outBuf.data(), buffer_->data() + fromOffset, outBuf.size());
}

} // namespace facebook::cachelib::navy
