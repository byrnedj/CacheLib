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

#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include "cachelib/allocator/MMLru3.h"
#include "cachelib/allocator/tests/MMTypeTest.h"

namespace facebook {
namespace cachelib {
using MMLruTest = MMTypeTest<MMLru3>;

TEST_F(MMLru3Test, AddBasic) { testAddBasic(MMLru::Config{}); }

TEST_F(MMLru3Test, RemoveBasic) { testRemoveBasic(MMLru::Config{}); }

TEST_F(MMLru3Test, RecordAccessBasic) {
  MMLru::Config c;
  // Change lruRefreshTime to make sure only the first recordAccess bumps
  // the node and subsequent recordAccess invocations do not.
  c.lruRefreshTime = 100;
  testRecordAccessBasic(std::move(c));
}

TEST_F(MMLru3Test, RecordAccessWrites) {
  using Nodes = std::vector<std::unique_ptr<Node>>;
  // access the nodes in the container randomly with the given access mode and
  // ensure that nodes are updated in lru with access mode write (read) only
  // when updateOnWrite (updateOnRead) is enabled.

  auto testWithAccessMode = [this](Container& c_, const Nodes& nodes_,
                                   AccessMode mode, bool updateOnWrites,
                                   bool updateOnReads) {
    // accessing must at least update the update time. to do so, first set the
    // updateTime of the node to be in the past.
    const uint32_t timeInPastStart = 100;
    std::vector<uint32_t> prevNodeTime;
    int i = 0;
    for (auto& node : nodes_) {
      auto time = timeInPastStart + i;
      node->setUpdateTime(time);
      ASSERT_EQ(node->getUpdateTime(), time);
      prevNodeTime.push_back(time);
      i++;
    }

    std::vector<int> nodeOrderPrev;
    for (auto itr = c_.getEvictionIterator(); itr; ++itr) {
      nodeOrderPrev.push_back(itr->getId());
    }
    verifyIterationVariants(c_);

    int nAccess = 1000;
    std::set<int> accessedNodes;
    while (nAccess-- || accessedNodes.size() < nodes_.size()) {
      auto& node = *(nodes_.begin() + folly::Random::rand32() % nodes_.size());
      accessedNodes.insert(node->getId());
      c_.recordAccess(*node, mode);
    }

    i = 0;
    const auto now = util::getCurrentTimeSec();
    for (const auto& node : nodes_) {
      if ((mode == AccessMode::kWrite && updateOnWrites) ||
          (mode == AccessMode::kRead && updateOnReads)) {
        ASSERT_GT(node->getUpdateTime(), prevNodeTime[i++]);
        ASSERT_LE(node->getUpdateTime(), now);
      } else {
        ASSERT_EQ(node->getUpdateTime(), prevNodeTime[i++]);
      }
    }

    // after a random set of recordAccess, test the order of the nodes in the
    // lru.
    std::vector<int> nodeOrderCurr;
    for (auto itr = c_.getEvictionIterator(); itr; ++itr) {
      nodeOrderCurr.push_back(itr->getId());
    }
    verifyIterationVariants(c_);

    if ((mode == AccessMode::kWrite && updateOnWrites) ||
        (mode == AccessMode::kRead && updateOnReads)) {
      ASSERT_NE(nodeOrderCurr, nodeOrderPrev);
    } else {
      ASSERT_EQ(nodeOrderCurr, nodeOrderPrev);
    }
  };

  auto createNodes = [](Container& c, Nodes& nodes) {
    // put some nodes in the container and ensure that the recordAccess does not
    // change the fact that the node is still in container.
    const int numNodes = 10;
    for (int i = 0; i < numNodes; i++) {
      nodes.emplace_back(new Node{i});
      auto& node = nodes.back();
      ASSERT_TRUE(c.add(*node));
    }
  };

  MMLru::Config config1{/* lruRefreshTime */ 0,
                        /* lruRefreshRatio */ 0.,
                        /* updateOnWrite */ false,
                        /* updateOnRead */ false,
                        /* tryLockUpdate */ false,
                        /* lruInsertionPointSpec */ 0};
  Container c1{config1, {}};

  MMLru::Config config2{/* lruRefreshTime */ 0,
                        /* lruRefreshRatio */ 0.,
                        /* updateOnWrite */ false,
                        /* updateOnRead */ true,
                        /* tryLockUpdate */ false,
                        /* lruInsertionPointSpec */ 0};
  Container c2{config2, {}};

  MMLru::Config config3{/* lruRefreshTime */ 0,
                        /* lruRefreshRatio */ 0.,
                        /* updateOnWrite */ true,
                        /* updateOnRead */ false,
                        /* tryLockUpdate */ false,
                        /* lruInsertionPointSpec */ 0};
  Container c3{config3, {}};

  MMLru::Config config4{/* lruRefreshTime */ 0,
                        /* lruRefreshRatio */ 0.,
                        /* updateOnWrite */ true,
                        /* updateOnRead */ true,
                        /* tryLockUpdate */ false,
                        /* lruInsertionPointSpec */ 0};
  Container c4{config4, {}};

  Nodes nodes1, nodes2, nodes3, nodes4;
  createNodes(c1, nodes1);
  createNodes(c2, nodes2);
  createNodes(c3, nodes3);
  createNodes(c4, nodes4);

  testWithAccessMode(c1, nodes1, AccessMode::kWrite, config1.updateOnWrite,
                     config1.updateOnRead);
  testWithAccessMode(c1, nodes1, AccessMode::kRead, config1.updateOnWrite,
                     config1.updateOnRead);
  testWithAccessMode(c2, nodes2, AccessMode::kWrite, config2.updateOnWrite,
                     config2.updateOnRead);
  testWithAccessMode(c2, nodes2, AccessMode::kRead, config2.updateOnWrite,
                     config2.updateOnRead);
  testWithAccessMode(c3, nodes3, AccessMode::kWrite, config3.updateOnWrite,
                     config3.updateOnRead);
  testWithAccessMode(c3, nodes3, AccessMode::kRead, config3.updateOnWrite,
                     config3.updateOnRead);
  testWithAccessMode(c4, nodes4, AccessMode::kWrite, config4.updateOnWrite,
                     config4.updateOnRead);
  testWithAccessMode(c4, nodes4, AccessMode::kRead, config4.updateOnWrite,
                     config4.updateOnRead);
}


TEST_F(MMLru3Test, Serialization) { testSerializationBasic(MMLru::Config{}); }

TEST_F(MMLru3Test, Reconfigure) {
  Container container(MMLru::Config{}, {});
  auto config = container.getConfig();
  config.defaultLruRefreshTime = 1;
  config.lruRefreshTime = 1;
  config.lruRefreshRatio = 0.8;
  config.mmReconfigureIntervalSecs = std::chrono::seconds(4);
  container.setConfig(config);
  std::vector<std::unique_ptr<Node>> nodes;
  nodes.emplace_back(new Node{0});
  container.add(*nodes[0]);
  sleep(1);
  nodes.emplace_back(new Node{1});
  container.add(*nodes[1]);
  sleep(2);

  // node 0 (age 3) gets promoted
  // current time (age 3) < reconfigure interval (4),
  // so refresh time not updated in reconfigureLocked()
  EXPECT_TRUE(container.recordAccess(*nodes[0], AccessMode::kRead));
  EXPECT_FALSE(nodes[0]->isTail());

  sleep(2);
  // current time (age 5) > reconfigure interval (4),
  // so refresh time set to 4 * 0.8 = 3.2 = 3 in reconfigureLocked()
  EXPECT_TRUE(container.recordAccess(*nodes[0], AccessMode::kRead));
  nodes.emplace_back(new Node{2});
  container.add(*nodes[2]);

  // node 0 (age 2) does not get promoted
  EXPECT_FALSE(container.recordAccess(*nodes[0], AccessMode::kRead));
}

TEST_F(MMLruTest, CombinedLockingIteration) {
  MMLruTest::Config config{};
  config.useCombinedLockForIterators = true;
  config.lruRefreshTime = 0;
  Container c(config, {});
  std::vector<std::unique_ptr<Node>> nodes;
  createSimpleContainer(c, nodes);

  // access to move items from cold to warm
  for (auto& node : nodes) {
    ASSERT_TRUE(c.recordAccess(*node, AccessMode::kRead));
  }

  // trying to remove through iterator should work as expected.
  // no need of iter++ since remove will do that.
  verifyIterationVariants(c);
  for (auto iter = c.getEvictionIterator(); iter;) {
    auto& node = *iter;
    ASSERT_TRUE(node.isInMMContainer());

    // this will move the iter.
    c.remove(iter);
    ASSERT_FALSE(node.isInMMContainer());
    if (iter) {
      ASSERT_NE((*iter).getId(), node.getId());
    }
  }
  verifyIterationVariants(c);

  ASSERT_EQ(c.getStats().size, 0);
  for (const auto& node : nodes) {
    ASSERT_FALSE(node->isInMMContainer());
  }
}
} // namespace cachelib
} // namespace facebook
