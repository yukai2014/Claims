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
namespace claims{
namespace txn{

//using claims::txn::TxnClient;
//using claims::txn::RetCode;
//using claims::txn::FixTupleIngestReq;
//using claims::txn::Ingest;

string TxnClient::Ip = kTxnIp;
int TxnClient::Port = kTxnPort;
caf::actor TxnClient::Proxy;
RetCode TxnClient::Init(string ip, int port){
  Ip = ip;
  Port = port;
  SerializeConfig();
  try {
    Proxy = caf::io::remote_actor(Ip, port);
  } catch (...) {
    return -1;
  }
  return 0;
}

RetCode TxnClient::BeginIngest(const FixTupleIngestReq & request, Ingest & ingest){
  RetCode ret = -1;
  if (TxnServer::Active)
     return TxnServer::BeginIngest(request, ingest);
  else {
    try{
      caf::scoped_actor self;;
      self->sync_send(Proxy, IngestAtom::value, request).
          await([&](Ingest & reply, RetCode r) { ingest = reply; ret = r;},
                caf::after(seconds(kTimeout)) >> []{ cout << "time out" << endl;});
    } catch (...){
      cout << "link fail" << endl;
      return -1;
    }
  }
  return ret;
}

RetCode TxnClient::CommitIngest(const Ingest & ingest) {
  RetCode ret = -1;
  if (TxnServer::Active)
    return TxnServer::CommitIngest(ingest);
  else {
    try {
      caf::scoped_actor self;
      self->sync_send(Proxy, CommitIngestAtom::value, ingest).
          await([&](RetCode r) { ret = r;},
                caf::after(seconds(kTimeout)) >> []{ cout << "time out" << endl;});
    } catch (...) {
      cout << "link fail" << endl;
      return -1;
    }
  }
  return ret;
}

RetCode TxnClient::AbortIngest(const Ingest & ingest) {
  RetCode ret = -1;
  if (TxnServer::Active)
    return TxnServer::AbortIngest(ingest);
  else {
    try {
      caf::scoped_actor self;
      self->sync_send(Proxy, AbortIngestAtom::value, ingest).
          await([&](RetCode r) { ret = r;},
                caf::after(seconds(kTimeout)) >> []{ cout << "time out" << endl;});
    } catch (...) {
      cout << "link fail" << endl;
      return -1;
    }
  }
  return ret;
}

RetCode TxnClient::BeginQuery(const QueryReq & request, Query & query) {
  RetCode ret = -1;
  if (TxnServer::Active)
    return TxnServer::BeginQuery(request, query);
  else {
    try {
      caf::scoped_actor self;
      self->sync_send(Proxy, QueryAtom::value, request).
          await([&](const QueryReq & request, RetCode r) { ret = r;},
                caf::after(seconds(kTimeout)) >> []{ cout << "time out" << endl;});
    } catch (...) {
      cout << "link fail" << endl;
      return -1;
    }
  }
  return ret;
}

RetCode TxnClient::BeginCheckpoint(Checkpoint & cp) {
  RetCode ret = -1;
  if (TxnServer::Active)
    return TxnServer::BeginCheckpoint(cp);
  else {
    try {
      caf::scoped_actor self;
      self->sync_send(Proxy, CheckpointAtom::value, cp.Part).
          await([&](const Checkpoint & checkpoint, RetCode r) {cp = checkpoint; ret = r;},
                caf::after(seconds(kTimeout)) >> []{ cout << "time out" << endl;});
    } catch (...) {
      cout << "link fail" << endl;
      return -1;
    }
  }
  return ret;
}

RetCode TxnClient::CommitCheckpoint(const Checkpoint & cp) {
  RetCode ret = -1;
  if (TxnServer::Active)
    return TxnServer::CommitCheckpoint(cp);
  else {
    try {
      caf::scoped_actor self;
      self->sync_send(Proxy, CommitCPAtom::value, cp).
          await([&](RetCode r) { ret = r;},
                caf::after(seconds(kTimeout)) >> []{ cout << "time out" << endl;});
    } catch (...) {
      cout << "link fail" << endl;
      return -1;
    }
  }
  return ret;
}

}
}


