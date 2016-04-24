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
#include "txn_log.hpp"
//#include "../common/error_define.h"
namespace claims{
namespace txn{
//using claims::common::rSuccess;
//using claims::common::rLinkTmTimeout;
//using claims::common::rLinkTmFail;
//using claims::common::rBeginIngestTxnFail;
//using claims::common::rBeginQueryFail;
//using claims::common::rBeginCheckpointFail;
//using claims::common::rCommitIngestTxnFail;
//using claims::common::rAbortIngestTxnFail;
//using claims::common::rCommitCheckpointFail;
int TxnCore::capacity_ = kTxnBufferSize;


int TxnServer::port_ = kTxnPort;
int TxnServer::concurrency_ = kConcurrency;
caf::actor TxnServer::proxy_;
vector<caf::actor> TxnServer::cores_;
bool TxnServer::active_ = false;

std::unordered_map<UInt64, atomic<UInt64>> TxnServer::pos_list_;
std::unordered_map<UInt64, UInt64> TxnServer::logic_cp_list_;
std::unordered_map<UInt64, UInt64> TxnServer::phy_cp_list_;
std::unordered_map<UInt64, atomic<UInt64>> TxnServer::CountList;
caf::actor test;

caf::behavior TxnCore::make_behavior() {
 //this->delayed_send(this, seconds(kGCTime + CoreId), GCAtom::value);
  return {
    [=](IngestAtom, const FixTupleIngestReq & request)->caf::message {
      //cout << "begin" << endl;
      auto ingest = make_shared<Ingest>();
//      RetCode ret = rSuccess;
      RetCode ret = 0;
      if (size_ >= capacity_)
        return caf::make_message(-1/*rBeginIngestTxnFail*/);
      ingest->id_ = GetId();
      txn_index_[ingest->id_ ] = size_;
      commit_[size_] = abort_[size_] = false;
      for (auto & strip_ : request.content_) {
        auto part = strip_.first;
        auto tupleSize = strip_.second.first;
        auto tupleCount = strip_.second.second;
        auto strip = TxnServer::AtomicMalloc(part, tupleSize, tupleCount);
        strip_list_[size_].push_back(strip);
        ingest->InsertStrip(strip);
        }
      size_ ++;
      ///cout << ingest.ToString() << endl;
      if (LogServer::active_) {
        current_message() = caf::make_message(IngestAtom::value, ingest);
        this->forward_to(LogServer::proxy_);
       }
     return caf::make_message(ret, *ingest);
     },
    [=](CommitIngestAtom, const UInt64 id)->caf::message{
      // cout << "commit" << endl;
      if (txn_index_.find(id) == txn_index_.end())
        return caf::make_message(-1/*rCommitIngestTxnFail*/);
      commit_[txn_index_[id]] = true;
      if (LogServer::active_) {
          this->forward_to(LogServer::proxy_);
        }
      return caf::make_message(0/*rSuccess*/);
     },
    [=](AbortIngestAtom, const UInt64 id)->caf::message {
     //  cout << "abort" << endl;
       if (txn_index_.find(id) == txn_index_.end())
         return caf::make_message(-1/*rBeginIngestTxnFail*/);
       abort_[txn_index_[id]] = true;
       if (LogServer::active_) {
           this->forward_to(LogServer::proxy_);
         }
       return caf::make_message(0/*rAbortIngestTxnFail*/);
     },
    [=](QueryAtom, const QueryReq & request, shared_ptr<Query> query) {
    //   cout << "core:"<< core_id_ <<" query" << endl;
     //  cout << query->ToString() << endl;
       for (auto i = 0; i < size_; i++){
        // cout << "commit:" << commit_[i] << endl;
         if (commit_[i])
         for (auto & strip : strip_list_[i]) {
           if (query->cp_list_.find(strip.part_) != query->cp_list_.end() &&
               strip.pos_ >= query->cp_list_[strip.part_])
             query->InsertStrip(strip.part_, strip.pos_, strip.offset_);
             }
       }
       if (core_id_ != TxnServer::cores_.size() - 1)
         this->forward_to(TxnServer::cores_[core_id_ + 1]);
       else {
         current_message() = caf::make_message(MergeAtom::value, request, query);
         this->forward_to(TxnServer::cores_[TxnServer::SelectCoreId()]);
         }
     },
    [=](MergeAtom, const QueryReq & request, shared_ptr<Query> query)->Query {
      // cout << "query merge" << endl;
       for (auto & part : query->snapshot_) {
         Strip::Sort(part.second);
         Strip::Merge(part.second);
         }
       return *query;
     },
    [=](MergeAtom, shared_ptr<Checkpoint> cp)->Checkpoint {
       Strip::Sort(cp->commit_strip_list_);
       Strip::Merge(cp->commit_strip_list_);
       Strip::Sort(cp->abort_strip_list_);
       Strip::Merge(cp->abort_strip_list_);
       return *cp;
     },
    [=] (CheckpointAtom, shared_ptr<Checkpoint> cp) {
       for (auto i = 0; i < size_; i++)
         if (commit_[i]) {
           for (auto & strip : strip_list_[i])
             if ( strip.part_ == cp->part_ && strip.pos_ >= cp->logic_cp_ )
               cp->commit_strip_list_.push_back(PStrip(strip.pos_, strip.offset_));
            }
         else if (abort_[i]) {
           for (auto & strip : strip_list_[i])
             if (strip.part_ == cp->part_ && strip.pos_ >= cp->logic_cp_)
               cp->abort_strip_list_.push_back(PStrip(strip.pos_, strip.offset_));
            }
       if (core_id_ != TxnServer::cores_.size() - 1)
         this->forward_to(TxnServer::cores_[core_id_ + 1]);
       else {
           current_message() = caf::make_message(MergeAtom::value, cp);
           this->forward_to(TxnServer::cores_[TxnServer::SelectCoreId()]);
         }
     },
    [=](GCAtom) {
       auto size_old = size_;
       auto pos = 0;
       for (auto i = 0; i < size_; i++)
         if (!TxnServer::IsStripListGarbage(strip_list_[i])) {
           txn_index_[txn_index_[i]] = pos;
           commit_[pos] = commit_[i];
           abort_[pos] = abort_[i];
           strip_list_[pos] = strip_list_[i];
           ++ pos;
             }
       size_ = pos;
       cout <<"core:"<<core_id_<< ",gc:" << size_old << "=>"<< pos << endl;
       this->delayed_send(this, seconds(kGCTime), GCAtom::value);
     },
    caf::others >> [&](){ cout<<"core:"<< core_id_<<" unkown message"<<endl;}
  };

}

caf::behavior Test::make_behavior() {
  cout << "test init..." << endl;
  return {
    [=](int a)->int {cout << "receive int:" << a <<endl; return -a;},
     caf::others >> [&](){ cout<<"test unkown message"<<endl;}
  };
}

caf::behavior IngestWorker(caf::event_based_actor * self) {
  return {
      [=](IngestAtom, const FixTupleIngestReq & request)->caf::message {
        Ingest ingest;
        auto ret = TxnServer::BeginIngest(request, ingest);
//        auto ret = 10;
//        cout<<"new IngestWorker!!"<<endl;
////        self->sync_send(test, 34).await(
////            [&](int a) { ret = a;});
////
////         caf::scoped_actor self;
////        self->sync_send(test, 34).await(
////            [=](int a) { cout<<a<<endl;}
////          );
        cout<<"new IngestWorker send~~"<<endl;
        //self->quit();
        return caf::make_message(ingest, ret);
       },
     caf::others >> [] () { cout<<"IngestWorker unkown message"<<endl;}};
}

caf::behavior IngestCommitWorker::make_behavior(){
  return {
    [=](CommitIngestAtom, const UInt64 id)->RetCode {
           quit();
           return TxnServer::CommitIngest(id);
         },
     caf::others >> [] () { cout<<"IngestCommitWorker unkown message"<<endl;}
  };
}

caf::behavior AbortWorker::make_behavior(){
  return {
    [=](AbortIngestAtom, const UInt64 id)->RetCode {
           quit();
           return TxnServer::AbortIngest(id);
         },
     caf::others >> [] () { cout<<"AbortWorker unkown message"<<endl;}
  };
}

caf::behavior QueryWorker::make_behavior(){
  return {
    [=](QueryAtom, const QueryReq & request)->caf::message {
          Query query;
          auto ret = TxnServer::BeginQuery(request, query);
          quit();
          return caf::make_message(query, ret);
        },
     caf::others >> [] () { cout<<"QueryWorker unkown message"<<endl;}
  };
}

caf::behavior CheckpointWorker::make_behavior() {
  return {
    [=](CheckpointAtom, const UInt64 part)->caf::message{
         Checkpoint cp;
         cp.part_ = part;
         auto ret = TxnServer::BeginCheckpoint(cp);
         quit();
         return caf::make_message(cp, ret);
       },
     caf::others >> [] () { cout<<"CheckpointWorker unkown message"<<endl;}
  };
}
caf::behavior CommitCPWorker::make_behavior() {
  return {
    [=](CommitCPAtom, const Checkpoint & cp)->RetCode {
       quit();
       return TxnServer::CommitCheckpoint(cp);
     },
     caf::others >> [] () { cout<<"CommitCPWorker unkown message"<<endl;}
  };
}
caf::behavior TxnServer::make_behavior() {
  try {
    caf::io::publish(proxy_, port_, nullptr, true);
    cout << "txn server bind to port:"<< port_<< " success" << endl;
  } catch (...) {
    cout << "txn server bind to port:"<< port_<< " fail" << endl;
  }
  return {
    [=](IngestAtom, const FixTupleIngestReq & request) {
      forward_to(cores_[SelectCoreId()]);
     },
    [=](CommitIngestAtom, const UInt64 id) {
      forward_to(cores_[GetCoreId(id)]);
     },
    [=](AbortIngestAtom, const UInt64 id) {
      forward_to(cores_[GetCoreId(id)]);
     },
    [=](QueryAtom, const QueryReq & request) {
       auto query = make_shared<Query>();
       for (auto & part : request.part_list_)
         query->cp_list_[part] = TxnServer::logic_cp_list_[part];
       current_message() = caf::make_message(QueryAtom::value, request, query);
       forward_to(cores_[0]);
     },
    [=](CheckpointAtom, const UInt64 part){
       auto cp = make_shared<Checkpoint>();
       cp->part_ = part;
       current_message() = caf::make_message(CheckpointAtom::value, cp);
       forward_to(cores_[0]);
     },
    [=](CommitCPAtom, const Checkpoint & cp) {

     },
    caf::others >> [] () { cout<<"server unkown message"<<endl;}
  };
}



RetCode TxnServer::Init(int concurrency, int port) {
  active_ = true;
  concurrency_ = concurrency;
  port_ = port;
  proxy_ = caf::spawn<TxnServer>();
  for (auto i = 0; i < concurrency_; i++)
    cores_.push_back(caf::spawn<TxnCore, caf::detached>(i));
  SerConfig();
  RecoveryFromCatalog();
  RecoveryFromTxnLog();
  srand((unsigned) time(NULL));
  return 0;
}

RetCode TxnServer::BeginIngest(const FixTupleIngestReq & request, Ingest & ingest) {
  RetCode ret = 0;
  UInt64 core_id = SelectCoreId();
  caf::scoped_actor self;
  self->sync_send(cores_[core_id], IngestAtom::value, & request, & ingest).
      await([&](int r) {ret = r;});
  if (ret == 0) {
  //  LogClient::Begin(ingest.Id);
//    for (auto & strip : ingest.StripList)
//      LogClient::Write(ingest.Id, strip.first, strip.second.first, strip.second.second);
  }
  return ret;
}
RetCode TxnServer::CommitIngest(const UInt64 id) {
  RetCode ret = 0;
  UInt64 core_id = GetCoreId(id);
  caf::scoped_actor self;
  self->sync_send(cores_[core_id], CommitIngestAtom::value, id).
      await([&](int r) { ret = r;});
 if (ret == 0) {
//   LogClient::Commit(ingest.Id);
//   LogClient::Refresh();
  }
  return ret;
}
RetCode TxnServer::AbortIngest(const UInt64 id) {
  RetCode ret;
  UInt64 core_id = GetCoreId(id);
  caf::scoped_actor self;
  self->sync_send(cores_[core_id], AbortIngestAtom::value, id).
      await([&](int r) { ret = r;});
  if (ret == 0) {
//    LogClient::Abort(ingest.Id);
//    LogClient::Refresh();
  }
  return ret;
}
RetCode TxnServer::BeginQuery(const QueryReq & request, Query & query) {
  RetCode ret;
  caf::scoped_actor self;
  for (auto & part : request.part_list_)
    query.cp_list_[part] = TxnServer::logic_cp_list_[part];
  for (auto & core : cores_)
    self->sync_send(core, QueryAtom::value, & request, & query).
    await([&](int r) {r = ret;});
  for (auto & part : query.snapshot_) {
    Strip::Sort(part.second);
    Strip::Merge(part.second);
  }
  return ret;
}
RetCode TxnServer::BeginCheckpoint(Checkpoint & cp) {
  RetCode ret = 0;
  if (TxnServer::pos_list_.find(cp.part_) == TxnServer::pos_list_.end())
    return -1;
  cp.logic_cp_ = TxnServer::logic_cp_list_[cp.part_];
  cp.phy_cp_ = TxnServer::phy_cp_list_[cp.part_];

  caf::scoped_actor self;
  for (auto & core : cores_)
    self->sync_send(core,CheckpointAtom::value, &cp).
    await([&]( int r) {  r = ret;});
  Strip::Sort(cp.commit_strip_list_);
  Strip::Merge(cp.commit_strip_list_);
  Strip::Sort(cp.abort_strip_list_);
  Strip::Merge(cp.abort_strip_list_);
  return ret;
}
RetCode TxnServer::CommitCheckpoint(const Checkpoint & cp) {
  RetCode ret = 0;
  if (TxnServer::pos_list_.find(cp.part_) == TxnServer::pos_list_.end())
       return -1;
     TxnServer::logic_cp_list_[cp.part_] = cp.logic_cp_;
     TxnServer::phy_cp_list_[cp.part_] = cp.phy_cp_;
  if (ret == 0) {
//    LogClient::Checkpoint(cp.Part, cp.LogicCP, cp.PhyCP);
//    LogClient::Refresh();
  }
  return ret;
}


Strip TxnServer::AtomicMalloc(UInt64 part, UInt64 TupleSize, UInt64 TupleCount) {
  Strip strip;
  strip.part_ = part;
  if (TupleSize * TupleCount == 0)
    return strip;
  do {
    strip.pos_ = pos_list_[part].load();
    strip.offset_ = 0;
    UInt64 block_pos = strip.pos_ % kBlockSize;
    UInt64 remain_count = TupleCount;
    int count = 0;
    while(remain_count > 0) {
        // 求出一个块内可以存放的最多元组数
      UInt64 use_count = (kBlockSize - block_pos - kTailSize) / TupleSize;
      if (use_count > remain_count)
        use_count = remain_count;

        //使用块内可用区域
      remain_count -= use_count;
      strip.offset_ += use_count * TupleSize;
      block_pos += use_count * TupleSize;
       //将不可利用的空间也分配
      if (kBlockSize - block_pos - kTailSize < TupleSize) {
          strip.offset_ += kBlockSize - block_pos;
          block_pos = 0;
        }
    }

  } while(!pos_list_[part].compare_exchange_weak(strip.pos_, strip.pos_ + strip.offset_));

  return strip;
}

RetCode TxnServer::RecoveryFromCatalog() {
  for (auto i = 0; i < 10; i++ ) {
    pos_list_[i] = 0;
    CountList[i] = 0;
    logic_cp_list_[i] = 0;
  }
}

RetCode TxnServer::RecoveryFromTxnLog() {

}

}
}



