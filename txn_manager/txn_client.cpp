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
 * /txn/txn_client.cpp
 *
 *  Created on: 2016年4月10日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#include "txn_client.hpp"
#include "txn_log.hpp"
//#include "../common/error_define.h"
namespace claims {
namespace txn {

// using claims::txn::TxnClient;
// using claims::txn::RetCode;
// using claims::txn::FixTupleIngestReq;
// using claims::txn::Ingest;
// using claims::common::rSuccess;
// using claims::common::rLinkTmTimeout;
// using claims::common::rLinkTmFail;
// using claims::common::rBeginIngestTxnFail;
// using claims::common::rBeginQueryFail;
// using claims::common::rBeginCheckpointFail;
// using claims::common::rCommitIngestTxnFail;
// using claims::common::rCommitCheckpointFail;

string TxnClient::ip_ = kTxnIp;
int TxnClient::port_ = kTxnPort;
caf::actor TxnClient::proxy_;
RetCode TxnClient::Init(string ip, int port) {
  ip_ = ip;
  port_ = port;
  SerConfig();
  try {
    proxy_ = caf::io::remote_actor(ip_, port);
  } catch (...) {
    //    return rLinkTmFail;
    return -1;
  }
  //  return rSuccess;
  return 0;
}

RetCode TxnClient::BeginIngest(const FixTupleIngestReq& request,
                               Ingest& ingest) {
  //  RetCode ret = rSuccess;
  RetCode ret = 0;
  try {
    caf::scoped_actor self;
    self->sync_send(TxnServer::active_ ? TxnServer::proxy_ : proxy_,
                    IngestAtom::value, request)
        .await([&](RetCode r, const Ingest& reply) {
                 ret = r;
                 ingest = reply;
               },
               [&](RetCode r) { ret = r; },
               caf::others >> []() { cout << " unkown message" << endl; },
               caf::after(seconds(kTimeout)) >> [&] {
                                                  //              ret =
                                                  //              rLinkTmTimeout;
                                                  ret = -1;
                                                  cout << "time out" << endl;
                                                });
  } catch (...) {
    cout << "link fail" << endl;
    //    return rLinkTmFail;
    return -1;
  }
  return ret;
}

RetCode TxnClient::CommitIngest(const UInt64 id) {
  //  RetCode ret = rSuccess;
  RetCode ret = 0;
  try {
    caf::scoped_actor self;

    self->sync_send(TxnServer::active_ ? TxnServer::proxy_ : proxy_,
                    CommitIngestAtom::value, id)
        .await([&](RetCode r) { ret = r; },
               caf::others >> []() { cout << " unkown message" << endl; },
               caf::after(seconds(kTimeout)) >> [&] {
                                                  //           ret =
                                                  //           rLinkTmTimeout;
                                                  ret = -1;
                                                  cout << "time out" << endl;
                                                });
  } catch (...) {
    cout << "link fail" << endl;
    //    return rLinkTmFail;
    return -1;
  }
  return ret;
}

RetCode TxnClient::AbortIngest(const UInt64 id) {
  //  RetCode ret = rSuccess;
  RetCode ret = 0;
  try {
    caf::scoped_actor self;
    self->sync_send(TxnServer::active_ ? TxnServer::proxy_ : proxy_,
                    AbortIngestAtom::value, id)
        .await([&](RetCode r) { ret = r; },
               caf::others >> []() { cout << " unkown message" << endl; },
               caf::after(seconds(kTimeout)) >> [&] {
                                                  //                ret =
                                                  //                rLinkTmTimeout;
                                                  ret = -1;
                                                  cout << "time out" << endl;
                                                });
  } catch (...) {
    cout << "link fail" << endl;
    //    return rLinkTmFail;
    return -1;
  }
  return ret;
}

RetCode TxnClient::BeginQuery(const QueryReq& request, Query& query) {
  //  RetCode ret = rSuccess;
  RetCode ret = 0;
  try {
    caf::scoped_actor self;
    self->sync_send(TxnServer::active_ ? TxnServer::proxy_ : proxy_,
                    QueryAtom::value, request)
        .await([&](const Query& q) { query = q; },
               caf::after(seconds(kTimeout)) >> [&] {
                                                  //              ret =
                                                  //              rLinkTmTimeout;
                                                  ret = -1;
                                                  cout << "time out" << endl;
                                                });
  } catch (...) {
    cout << "link fail" << endl;
    //    return rLinkTmFail;
    return -1;
  }
  return ret;
}

RetCode TxnClient::BeginCheckpoint(Checkpoint& cp) {
  //  RetCode ret = rSuccess;
  RetCode ret = 0;
  try {
    caf::scoped_actor self;
    self->sync_send(TxnServer::active_ ? TxnServer::proxy_ : proxy_,
                    CheckpointAtom::value, cp.part_)
        .await([&](const Checkpoint& checkpoint, RetCode r) {
                 cp = checkpoint;
                 ret = r;
               },
               caf::after(seconds(kTimeout)) >> [&] {
                                                  //                  ret =
                                                  //                  rLinkTmTimeout;
                                                  ret = -1;
                                                  cout << "time out" << endl;
                                                });
  } catch (...) {
    cout << "link fail" << endl;
    //    return rLinkTmFail;
    return -1;
  }
  return ret;
}

RetCode TxnClient::CommitCheckpoint(const UInt64 logic_cp,
                                    const UInt64 phy_cp) {
  //  RetCode ret = rSuccess;
  RetCode ret = 0;
  try {
    caf::scoped_actor self;
    self->sync_send(TxnServer::active_ ? TxnServer::proxy_ : proxy_,
                    CommitCPAtom::value, logic_cp, phy_cp)
        .await([&](RetCode r) { ret = r; },
               caf::after(seconds(kTimeout)) >> [&] {
                                                  //                ret =
                                                  //                rLinkTmTimeout;
                                                  ret = -1;
                                                  cout << "time out" << endl;
                                                });
  } catch (...) {
    cout << "link fail" << endl;
    //    return rLinkTmFail;
    return -1;
  }
  return ret;
}
}
}
