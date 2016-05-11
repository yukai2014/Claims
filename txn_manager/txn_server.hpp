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
 * /txn/txn_server.hpp
 *
 *  Created on: 2016年4月10日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#ifndef TXN_SERVER_HPP_
#define TXN_SERVER_HPP_
#include <vector>
#include <iostream>
#include <functional>
#include <algorithm>
#include <memory>
#include <map>
#include <utility>
#include <unordered_map>
#include <time.h>
#include <stdlib.h>
#include <chrono>
#include <sys/time.h>
#include "unistd.h"
#include "stdlib.h"
#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "txn.hpp"
//#include "txn_log.hpp"
#include <chrono>

namespace claims {
namespace txn {
using std::cin;
using std::cout;
using std::endl;
using std::vector;
using std::string;
using std::map;
using std::pair;
using std::to_string;
using std::function;
using std::sort;
using std::atomic;
using std::chrono::seconds;
using std::chrono::milliseconds;
using std::make_shared;
using std::shared_ptr;
// UInt64 txn_id;
class TxnCore : public caf::event_based_actor {
 public:
  static int capacity_;
  UInt64 core_id_;
  UInt64 txn_id_ = 0;
  UInt64 size_;
  map<UInt64, UInt64> txn_index_;
  bool* commit_ = nullptr;
  bool* abort_ = nullptr;
  vector<Strip>* strip_list_;
  caf::behavior make_behavior() override;
  void ReMalloc();
  TxnCore(int coreId) : core_id_(coreId) { ReMalloc(); }
  UInt64 GetId();
};

class Test : public caf::event_based_actor {
 public:
  caf::behavior make_behavior() override;
};

class IngestCommitWorker : public caf::event_based_actor {
 public:
  caf::behavior make_behavior() override;
};

class AbortWorker : public caf::event_based_actor {
 public:
  caf::behavior make_behavior() override;
};

class QueryWorker : public caf::event_based_actor {
 public:
  caf::behavior make_behavior() override;
};

class CheckpointWorker : public caf::event_based_actor {
 public:
  caf::behavior make_behavior() override;
};

class CommitCPWorker : public caf::event_based_actor {
 public:
  caf::behavior make_behavior() override;
};

class TxnServer : public caf::event_based_actor {
 public:
  static bool active_;
  static int port_;
  static int concurrency_;
  static caf::actor proxy_;
  static vector<caf::actor> cores_;
  static std::unordered_map<UInt64, atomic<UInt64>> pos_list_;
  static std::unordered_map<UInt64, UInt64> logic_cp_list_;
  static std::unordered_map<UInt64, UInt64> phy_cp_list_;
  static std::unordered_map<UInt64, atomic<UInt64>> CountList;
  /**************** User APIs ***************/
  static RetCode Init(int concurrency = kConcurrency, int port = kTxnPort);

  /**************** System APIs ***************/
  static RetCode BeginIngest(const FixTupleIngestReq& request, Ingest& ingest);
  static RetCode CommitIngest(const UInt64 id);
  static RetCode AbortIngest(const UInt64 id);
  static RetCode BeginQuery(const QueryReq& request, Query& snapshot);
  static RetCode BeginCheckpoint(Checkpoint& cp);
  static RetCode CommitCheckpoint(const Checkpoint& cp);
  static UInt64 GetCoreId(UInt64 id) { return id % 1000; }
  static inline UInt64 SelectCoreId() { return rand() % concurrency_; }
  caf::behavior make_behavior() override;

  static RetCode RecoveryFromCatalog();
  static RetCode RecoveryFromTxnLog();
  static inline Strip AtomicMalloc(UInt64 part, UInt64 TupleSize,
                                   UInt64 TupleCount);
  static inline bool IsStripListGarbage(const vector<Strip>& striplist) {
    for (auto& strip : striplist) {
      if (strip.pos_ >= TxnServer::logic_cp_list_[strip.part_]) return false;
    }
    return true;
  }
};
}
}

#endif  //  TXN_SERVER_HPP_
