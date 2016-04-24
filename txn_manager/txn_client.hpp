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
 * /txn/txn_client.hpp
 *
 *  Created on: 2016年4月10日
 *      Author: imdb
 *		   Email: 
 * 
 * Description:
 *
 */

#ifndef TXN_CLIENT_HPP_
#define TXN_CLIENT_HPP_
#include <vector>
#include <iostream>
#include <functional>
#include <algorithm>
#include <memory>
#include <map>
#include <utility>
#include <unordered_map>
#include <time.h>
#include <chrono>
#include <sys/time.h>
#include "unistd.h"
#include "stdlib.h"
#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "txn.hpp"
#include "txn_server.hpp"

#include <chrono>

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

namespace claims{
namespace txn{



class TxnClient{
 public:
  static string ip_;
  static int port_;
  static caf::actor proxy_;
  static RetCode Init(string ip = kTxnIp, int port = kTxnPort);
  static RetCode BeginIngest(const FixTupleIngestReq & request, Ingest & ingest);
  static RetCode CommitIngest(const UInt64 id);
  static RetCode AbortIngest(const UInt64 id);
  static RetCode BeginQuery(const QueryReq & request, Query & query);
  static RetCode BeginCheckpoint(Checkpoint & cp);
  static RetCode CommitCheckpoint(const UInt64 logic_cp, const UInt64 phy_cp);
};

}
}

#endif //  TXN_CLIENT_HPP_ 
