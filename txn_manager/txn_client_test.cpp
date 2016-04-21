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
 * /txn/client.cpp
 *
 *  Created on: 2016年4月7日
 *      Author: imdb
 *		   Email: 
 * 
 * Description:
 *
 */


#include <vector>
#include <iostream>
#include <functional>
#include <algorithm>
#include <memory>
#include <map>
#include <utility>
#include <unordered_map>
#include <tuple>
#include <time.h>
#include <chrono>
#include <sys/time.h>
#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "txn.hpp"
#include "unistd.h"
#include "txn_client.hpp"
using std::cin;
using std::cout;
using std::endl;
using std::vector;
using std::string;
using std::map;
using std::pair;
using std::unordered_map;
using std::to_string;
using std::function;
using std::sort;
using std::tuple;
using std::make_tuple;
using std::make_pair;
using std::get;
using UInt64 = unsigned long long;
using UInt32 = unsigned int;
using UInt16 = unsigned short;
using UInt8 = char;
using RetCode = int;
using OkAtom = caf::atom_constant<caf::atom("ok")>;
using FailAtom = caf::atom_constant<caf::atom("fail")>;


using namespace claims::txn;

class Foo {
 public:
  vector<UInt64> request1;
  unordered_map<UInt64, pair<UInt64, UInt64>> request2;
  vector<pair<int,int>> request3;
  void set_request1(const vector<UInt64> & req) { request1 = req;}
  void set_request2(const unordered_map<UInt64, pair<UInt64, UInt64>> & req) {
    request2 = req;
  }
  void set_request3(const vector<pair<int,int>>  &req) { request3 = req;}
  vector<UInt64> get_request1() const {return request1;}
  unordered_map<UInt64, pair<UInt64, UInt64>> get_request2() const {return request2;}
  vector<pair<int,int>> get_request3() const { return request3;}
};


inline bool operator == (const Foo & a, const Foo & b) {
  return a.request1 == b.request1 && a.request2 == b.request2;
}


int main(){
  TxnClient::Init();
  FixTupleIngestReq request1;
  Ingest ingest;

  struct  timeval tv1, tv2;
  gettimeofday(&tv1,NULL);
//  request1.Content = {{0, {45, 10}}, {1, {54, 10}}};
//  TxnClient::BeginIngest(request1, ingest);
  Checkpoint cp;
  cp.Part = 0;
  TxnClient::BeginCheckpoint(cp);
  cout << cp.ToString() << endl;
  cp.LogicCP = 10000;
  cp.PhyCP = 10000;
  TxnClient::CommitCheckpoint(cp);
  TxnClient::BeginCheckpoint(cp);
  cout << cp.ToString() << endl;
  gettimeofday(&tv2,NULL);
  cout << tv2.tv_sec - tv1.tv_sec << "-" << (tv2.tv_usec - tv1.tv_usec)/1000 <<endl;

  //TxnClient::CommitIngest(ingest);
  //cout << ingest.ToString() << endl;
  caf::await_all_actors_done();
}
