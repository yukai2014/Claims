/*
 * Copyright [2012-2015] DaSE@ECNU
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * /txn/txn_server.cpp
 *
 *  Created on: 2016年4月10日
 *      Author: imdb
 *		   Email: 
 * 
 * Description:
 *
 */
#include "txn_server.hpp"

namespace claims{
namespace txn{

int TxnCore::BufferSize = kTxnBufferSize;


int TxnServer::Port = kTxnPort;
int TxnServer::Concurrency = kConcurrency;
caf::actor TxnServer::Router;
vector<caf::actor> TxnServer::Cores;
bool TxnServer::Active = false;

std::unordered_map<UInt64, atomic<UInt64>> TxnServer::PosList;
std::unordered_map<UInt64, UInt64> TxnServer::LogicCPList;
std::unordered_map<UInt64, UInt64> TxnServer::PhyCPList;
std::unordered_map<UInt64, atomic<UInt64>> TxnServer::CountList;

RetCode TxnCore::ReMalloc() {
  Size = 0;
  TxnIndex.clear();
  try {
    delete [] Commit;
    delete [] Abort;
    delete [] StripList;
    Commit = new bool[BufferSize];
    Abort = new bool[BufferSize];
    StripList = new vector<Strip>[BufferSize];
  } catch (...) {
    cout << "core:"<<CoreId<<" remalloc fail"<< endl;
    return -1;
  }
 // cout << "core:"<<CoreId<<" remalloc success"<< endl;
  return 0;
}

caf::behavior TxnCore::make_behavior() {
 ReMalloc();
 //this->delayed_send(this, seconds(kGCTime + CoreId), GCAtom::value);
  return {
    [=](IngestAtom, const FixTupleIngestReq * request, Ingest * ingest)->int {
      struct  timeval tv1;
      if (Size >= BufferSize)
        return -1;
      auto id = ingest->Id = GetId();
      TxnIndex[id] = Size;
      Commit[Size] = Abort[Size] = false;
      for (auto & item : request->Content) {
        auto part = item.first;
        auto tupleSize = item.second.first;
        auto tupleCount = item.second.second;
        auto strip = TxnServer::AtomicMalloc(part, tupleSize, tupleCount);
        StripList[Size].push_back(strip);
        ///cout << strip.ToString() << endl;
        ingest->InsertStrip(strip);
      }
      Size ++;

      return 0;
     },
    [=](CommitIngestAtom, const Ingest * ingest)->int{
      if (TxnIndex.find(ingest->Id) == TxnIndex.end())
        return -1;
      Commit[TxnIndex[ingest->Id]] = true;
      return 0;
     },
    [=](AbortIngestAtom, const Ingest * ingest)->int {
       if (TxnIndex.find(ingest->Id) == TxnIndex.end())
         return -1;
       Commit[TxnIndex[ingest->Id]] = true;
       return 0;
     },
    [=](QueryAtom, const QueryReq * request, Query * query)->int {
       for (auto i = 0; i < Size; i++)
         if (Commit[i])
           for (auto & strip : StripList[i]) {
             if (query->CPList.find(strip.Part) != query->CPList.end() &&
                 strip.Pos >= query->CPList[strip.Part])
               query->InsertStrip(strip.Part, strip.Pos, strip.Offset);
               }
       return 1;
     },
    [=] (CheckpointAtom, Checkpoint * cp)->int  {

       for (auto i = 0; i < Size; i++)
         if (Commit[i]) {
           for (auto & strip : StripList[i])
             if ( strip.Part == cp->Part && strip.Pos >= cp->LogicCP )
               cp->CommitStripList.push_back(PStrip(strip.Pos, strip.Offset));
           }
         else if (Abort[i]) {
           for (auto & strip : StripList[i])
             if (strip.Part == cp->Part && strip.Pos >= cp->LogicCP)
               cp->AbortStripList.push_back(PStrip(strip.Pos, strip.Offset));
         }

     },
    [=](GCAtom) {
       auto size_old = Size;
       auto pos = 0;
       for (auto i = 0; i < Size; i++)
         if (!TxnServer::IsStripListGarbage(StripList[i])) {
           TxnIndex[TxnIndex[i]] = pos;
           Commit[pos] = Commit[i];
           Abort[pos] = Abort[i];
           StripList[pos] = StripList[i];
           ++ pos;
             }
       Size = pos;
       cout <<"core:"<<CoreId<< ",gc:" << size_old << "=>"<< pos << endl;
       this->delayed_send(this, seconds(kGCTime), GCAtom::value);
     },
    caf::others >> [] () { cout<<"core unkown message"<<endl;}
  };

}

caf::behavior TxnWorker::make_behavior( ){
  return {
    [=](IngestAtom, const FixTupleIngestReq & request)->caf::message {
      Ingest ingest;
      auto ret = TxnServer::BeginIngest(request, ingest);
      quit();
      return caf::make_message(ingest, ret);
     },
    [=](CommitIngestAtom, const Ingest & ingest)->RetCode {
       quit();
       return TxnServer::CommitIngest(ingest);
     },
    [=](AbortIngestAtom, const Ingest & ingest)->RetCode {
       quit();
       return TxnServer::AbortIngest(ingest);
     },
    [=](QueryAtom, const QueryReq & request)->caf::message {
       Query query;
       auto ret = TxnServer::BeginQuery(request, query);
       quit();
       return caf::make_message(query, ret);
     },
    [=](CheckpointAtom, const UInt64 part)->caf::message{
       Checkpoint cp;
       cp.Part = part;
       auto ret = TxnServer::BeginCheckpoint(cp);
       quit();
       return caf::make_message(cp, ret);
     },
    [=](CommitCPAtom, const Checkpoint & cp)->RetCode {
       quit();
       return TxnServer::CommitCheckpoint(cp);
     },
    caf::others >> [] () { cout<<"work unkown message"<<endl;}
  };
}


caf::behavior TxnServer::make_behavior() {
  try {
    caf::io::publish(Router, Port, nullptr, true);
    cout << "txn server bind to port:"<< Port<< " success" << endl;
  } catch (...) {
    cout << "txn server bind to port:"<< Port<< " fail" << endl;
  }
  return {
    [=](IngestAtom, const FixTupleIngestReq & request) {
      this->forward_to(caf::spawn<TxnWorker>());
     },
    [=](CommitIngestAtom, const Ingest & ingest) {
       this->forward_to(caf::spawn<TxnWorker>());
     },
    [=](AbortIngestAtom, const Ingest & ingest) {
       this->forward_to(caf::spawn<TxnWorker>());
     },
    [=](QueryAtom, const QueryReq & request) {
       this->forward_to(caf::spawn<TxnWorker>());
     },
    [=](CheckpointAtom, const UInt64 part){
       this->forward_to(caf::spawn<TxnWorker>());
     },
    [=](CommitCPAtom, const Checkpoint & cp) {
       this->forward_to(caf::spawn<TxnWorker>());
     },
    caf::others >> [] () { cout<<"unkown message"<<endl;}
  };
}



RetCode TxnServer::Init(int concurrency, int port) {

  Active = true;
  Concurrency = concurrency;
  Port = port;
  Router = caf::spawn<TxnServer>();
  for (auto i = 0; i < Concurrency; i++)
    Cores.push_back(caf::spawn<TxnCore, caf::detached>(i));
  SerializeConfig();
  RecoveryFromCatalog();
  RecoveryFromTxnLog();
  srand((unsigned) time(NULL));

  return 0;
}

RetCode TxnServer::BeginIngest(const FixTupleIngestReq & request, Ingest & ingest) {
  RetCode ret = 0;
  UInt64 core_id = SelectCore();
  caf::scoped_actor self;
  self->sync_send(Cores[core_id], IngestAtom::value, & request, & ingest).
      await([&](int r) {ret = r;});
  if (ret == 0) {
    LogClient::Begin(ingest.Id);
//    for (auto & strip : ingest.StripList)
//      LogClient::Write(ingest.Id, strip.first, strip.second.first, strip.second.second);
  }
  return ret;
}
RetCode TxnServer::CommitIngest(const Ingest & ingest) {
  RetCode ret = 0;
  UInt64 core_id = GetCoreId(ingest.Id);
  caf::scoped_actor self;
  self->sync_send(Cores[core_id], CommitIngestAtom::value, &ingest).
      await([&](int r) { ret = r;});
 if (ret == 0) {
   LogClient::Commit(ingest.Id);
   LogClient::Refresh();
  }
  return ret;
}
RetCode TxnServer::AbortIngest(const Ingest & ingest) {
  RetCode ret;
  UInt64 core_id = GetCoreId(ingest.Id);
  caf::scoped_actor self;
  self->sync_send(Cores[core_id], AbortIngestAtom::value, &ingest).
      await([&](int r) { ret = r;});
  if (ret == 0) {
    LogClient::Abort(ingest.Id);
    LogClient::Refresh();
  }
  return ret;
}
RetCode TxnServer::BeginQuery(const QueryReq & request, Query & query) {
  RetCode ret;
  caf::scoped_actor self;
  for (auto & part : request.PartList)
    query.CPList[part] = TxnServer::LogicCPList[part];
  for (auto & core : Cores)
    self->sync_send(core, QueryAtom::value, & request, & query).
    await([&](int r) {r = ret;});
  for (auto & part : query.Snapshot) {
    Strip::Sort(part.second);
    Strip::Merge(part.second);
  }
  return ret;
}
RetCode TxnServer::BeginCheckpoint(Checkpoint & cp) {
  RetCode ret = 0;
  if (TxnServer::PosList.find(cp.Part) == TxnServer::PosList.end())
    return -1;
  cp.LogicCP = TxnServer::LogicCPList[cp.Part];
  cp.PhyCP = TxnServer::PhyCPList[cp.Part];

  caf::scoped_actor self;
  for (auto & core : Cores)
    self->sync_send(core,CheckpointAtom::value, &cp).
    await([&]( int r) {  r = ret;});
  Strip::Sort(cp.CommitStripList);
  Strip::Merge(cp.CommitStripList);
  Strip::Sort(cp.AbortStripList);
  Strip::Merge(cp.AbortStripList);
  return ret;
}
RetCode TxnServer::CommitCheckpoint(const Checkpoint & cp) {
  RetCode ret = 0;
  if (TxnServer::PosList.find(cp.Part) == TxnServer::PosList.end())
       return -1;
     TxnServer::LogicCPList[cp.Part] = cp.LogicCP;
     TxnServer::PhyCPList[cp.Part] = cp.PhyCP;
  if (ret == 0) {
    LogClient::Checkpoint(cp.Part, cp.LogicCP, cp.PhyCP);
    LogClient::Refresh();
  }
  return ret;
}


Strip TxnServer::AtomicMalloc(UInt64 part, UInt64 TupleSize, UInt64 TupleCount) {
  Strip strip;
  strip.Part = part;
  if (TupleSize * TupleCount == 0)
    return strip;
  do {
    strip.Pos = PosList[part].load();
    strip.Offset = 0;
    UInt64 block_pos = strip.Pos % kBlockSize;
    UInt64 remain_count = TupleCount;
    int count = 0;
    while(remain_count > 0) {
        // 求出一个块内可以存放的最多元组数
      UInt64 use_count = (kBlockSize - block_pos - kTailSize) / TupleSize;
      if (use_count > remain_count)
        use_count = remain_count;

        //使用块内可用区域
      remain_count -= use_count;
      strip.Offset += use_count * TupleSize;
      block_pos += use_count * TupleSize;
       //将不可利用的空间也分配
      if (kBlockSize - block_pos - kTailSize < TupleSize) {
          strip.Offset += kBlockSize - block_pos;
          block_pos = 0;
        }
    }

  } while(!PosList[part].compare_exchange_weak(strip.Pos, strip.Pos + strip.Offset));

  return strip;
}

RetCode TxnServer::RecoveryFromCatalog() {
  for (auto i = 0; i < 10; i++ ) {
    PosList[i] = 0;
    CountList[i] = 0;
    LogicCPList[i] = 0;
  }
}

RetCode TxnServer::RecoveryFromTxnLog() {

}

}
}



