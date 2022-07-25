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
BackgroundManager<CacheT>::BackgroundManager(  std::vector<std::unique_ptr<BackgroundEvictor<CacheT>>> backgroundEvictors,
  std::vector<std::unique_ptr<BackgroundPromoter<CacheT>>> backgroundPromoters ) :
    : evictors_(backgroundEvictors),
      promoters_(backgroundPromoters)
{
}

std::map<uint32_t,std::vector<std::tuple<TierId,PoolId,ClassId>>> doLinearPartition(
        std::map<std::tuple<TierId, PoolId, ClassId>,uint32_t> batches, 
        size_t kParts) {

    std::map<uint32_t,std::vector<std::tuple<TierId, PoolId, ClassId>>> assignments;
    //just give me bytes
    int **M = (int*)malloc(sizeof(int*)*batches.size()+ 1);
    int **D = (int*)malloc(sizeof(int*)*batches.size()+ 1);
    int *P = (int)malloc(sizeof(int)*batches.size() + 1);
    P[0] = 0; 
    auto batchI = batches.begin();
    for (int i = 0; i <= batches.size(); i++) {
        M[i] = malloc(sizeof(int)*kParts);
        D[i] = malloc(sizeof(int)*kParts);
        if (i >= 1) {
            P[i] = P[i-1] + batchI->second;
            batchI++;
        }
    }
    for (int i = 1; i <= batches.size(); i++) {
        M[i,1] = P[i];
    }
    for (int j = 1; j <= batches.size(); j++) {
        M[1,j] = *batches.begin();
    }
    for (int i = 2; i <= batches.size(); i++) {
        for (int j = 2; j <= kParts; j++) {
            M[i,j] = std::max();
            for (int pos = 1; pos < i; pos++) {
                int sum = std::max(M[pos,j-1],P[i]-P[pos]);
                if (M[i,j] > s) {
                    M[i,j] = s;
                    D[i,j] = pos;
                }
            }
        }
    }

    int K = kParts;
    for (int i = batches.size(); i >= 0; i--) {
        int pos[i] = D[i,K];
    }


}
template <typename CacheT>
BackgroundManager<CacheT>::~BackgroundManager() { stop(std::chrono::seconds(0)); }

template <typename CacheT>
void BackgroundManager<CacheT>::work() {

    std::map<std::tuple<TierId, PoolId, ClassId>,uint32_t> batches;
    for (auto evictor : evictors_) {
        //get batch size for each
        std::map<std::tuple<TierId,PoolId,ClassId>,uint32_t> batchset = evictor->getLastBatchSet();
        for (auto entry : batchset) {
            batches[entry.first] = entry.second;
        }
    }

    // now we have all the last batches - lets get the proper
    // assignment
    std::map<uint32_t,std::vector<std::tuple<TierId, PoolId, ClassId>>> assignments = 
        doLinearPartition(batches,evictors_.size());


    for (auto entry : assignments) {
        auto eid = entry.first;
        auto assignment = entry.second;
        evictors_[eid]->setAssignedMemory(assignment,0);
    }

}



} // namespace cachelib
} // namespace facebook
