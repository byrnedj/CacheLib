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
BackgroundManager<CacheT>::BackgroundManager(  std::vector<std::unique_ptr<BackgroundEvictor<CacheT>>> &backgroundEvictors ,
  std::vector<std::unique_ptr<BackgroundPromoter<CacheT>>> &backgroundPromoters ) 
    : evictors_(backgroundEvictors),
      promoters_(backgroundPromoters)
{
}

template <typename CacheT>
BackgroundManager<CacheT>::~BackgroundManager() { stop(std::chrono::seconds(0)); }

template <typename CacheT>
std::map<uint32_t,std::vector<std::tuple<TierId,PoolId,ClassId>>> BackgroundManager<CacheT>::doLinearPartition(
        std::map<std::tuple<TierId, PoolId, ClassId>,uint32_t> batchesMap, 
        size_t kParts) {

    //TODO memory management etc.
    std::vector<uint32_t> batches;
    std::vector<std::tuple<TierId,PoolId,ClassId>> ids;
    for (auto &entry : batchesMap) {
        batches.push_back(entry.second);
        ids.push_back(entry.first);
    }
    std::map<uint32_t,std::vector<std::tuple<TierId, PoolId, ClassId>>> assignments;

    int n = batches.size();
    //just give me bytes
    int M[n+1][kParts+1];
    int D[n+1][kParts+1];
    int P[n+1];
    P[0] = 0; 
    auto batchI = batches.begin();
    for (int i = 0; i <= n; i++) {
        if (i >= 1) {
            P[i] = P[i-1] + *batchI;
            batchI++;
        }
        for (int j = 0; j <= kParts; j++) {
            M[i][j] = 0;
            D[i][j] = 0;
        }
    }
    for (int i = 1; i <= n; i++) {
        M[i][1] = P[i];
    }
    for (int j = 1; j <= kParts; j++) {
        M[1][j] = batches[0];
    }
    //printf("first M\n");
    //for (int ii = 0; ii <= n; ii++) {
    //    for (int jj = 0; jj <= kParts; jj++) {
    //        printf(" %d ",M[ii][jj]);
    //    }
    //    printf("\n");
    //}
    for (int i = 2; i <= n; i++) {
        for (int j = 2; j <= kParts; j++) {
            M[i][j] = std::numeric_limits<int>::max();
            for (int pos = 1; pos < i; pos++) {
                int sum = std::max(M[pos][j-1],P[i]-P[pos]);
                if (M[i][j] > sum) {
                    M[i][j] = sum;
                    D[i][j] = pos;
                }
            }
        }
    }
    //printf("final M\n");
    //for (int ii = 0; ii <= n; ii++) {
    //    for (int jj = 0; jj <= kParts; jj++) {
    //        printf(" %d ",M[ii][jj]);
    //    }
    //    printf("\n");
    //}
    //printf("final D\n");
    //for (int ii = 0; ii <= n; ii++) {
    //    for (int jj = 0; jj <= kParts; jj++) {
    //        printf(" %d ",D[ii][jj]);
    //    }
    //    printf("\n");
    //}

    int K = kParts;
    int N = n;
    std::vector<int> parts;
    while (K > 0) {
        parts.push_back(D[N][K]);
        N = D[N][K];
        K = K - 1;
    }
    //parts tells at which idxs we should break at
    int lastidx = n;
    for (int i = 0; i < kParts; i++) {
        assignments[i] = 
            std::vector<std::tuple<TierId,PoolId,ClassId>>( 
                    ids.begin()+parts[i],ids.begin()+lastidx);
        lastidx = parts[i];
    }
    return assignments;
}

template <typename CacheT>
void BackgroundManager<CacheT>::work() {

    bool allzero = true;
    std::map<std::tuple<TierId, PoolId, ClassId>,uint32_t> batches;
    for (auto &evictor : evictors_) {
        //get batch size for each
        std::map<std::tuple<TierId,PoolId,ClassId>,uint32_t> batchset = evictor->getLastBatch();
        for (auto &entry : batchset) {
            batches[entry.first] = entry.second;
            if (entry.second > 0) {
                allzero = false;
            }
        }
    }

    if (!allzero) {
        // now we have all the last batches - lets get the proper
        // assignment
        std::map<uint32_t,std::vector<std::tuple<TierId, PoolId, ClassId>>> assignments = 
            doLinearPartition(batches,evictors_.size());


        for (auto &entry : assignments) {
            auto eid = entry.first;
            auto &assignment = entry.second;
            evictors_[eid]->setAssignedMemory(std::move(assignment));
        }
    }

}



} // namespace cachelib
} // namespace facebook
