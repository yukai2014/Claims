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
#include "txn_log.hpp"
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
using std::string;
using UInt64 = unsigned long long;
using UInt32 = unsigned int;
using UInt16 = unsigned short;
using UInt8 = char;
using RetCode = int;
using OkAtom = caf::atom_constant<caf::atom("ok")>;
using IngestAtom = caf::atom_constant<caf::atom("ingest")>;
using QueryAtom = caf::atom_constant<caf::atom("query")>;
using FailAtom = caf::atom_constant<caf::atom("fail")>;
using QuitAtom = caf::atom_constant<caf::atom("quit")>;

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
char v[1024+10];

caf::actor proxy;
class A{
 public:
  vector<int> list_ ;
  int c = 0;
  void set_list_(const vector<int> list) { list_ = list;}
  vector<int> get_list_() const { return list_;}
};
inline bool operator == (const A & a1, const A & a2) {
  return a1.list_ == a2.list_;
}

void ConfigA(){
  caf::announce<A>("A", make_pair(&A::get_list_, &A::set_list_));
}
void task(int index){
for (auto i=0;i<index;i++) {
    caf::scoped_actor self;
    self->sync_send(proxy, IngestAtom::value, i).await(
        [=](int ret) { /*cout <<"receive:" << ret << endl;*/},
        caf::after(std::chrono::seconds(2)) >> [] {
            cout << "ingest time out" << endl;
         }
     );
//    self->sync_send(proxy, QueryAtom::value).await(
//        [=](int t) {
//          cout << t<< endl;
//          },
//        [=](A a) {
//              cout << "success" << endl;
//              for (auto &it : a.list_){
//                cout << it << endl;
//              }
//          },
//        caf::after(std::chrono::seconds(2)) >> [] {
//            cout << "query time out" << endl;
//         }
//    );
}
}

using claims::txn::FixTupleIngestReq;
using claims::txn::Ingest;
using claims::txn::QueryReq;
using claims::txn::Query;
using claims::txn::TxnServer;
using claims::txn::TxnClient;
using claims::txn::LogServer;
using claims::txn::LogClient;
char buffer[20*1024+10];
void task2(int id, int times){
  std::default_random_engine e;
  std::uniform_int_distribution<int> rand_tuple_size(50, 150);
  std::uniform_int_distribution<int> rand_tuple_count(10, 100);
  std::uniform_int_distribution<int> rand_part_count(1, 10);
  for (auto i=0; i<times; i++) {
      FixTupleIngestReq req;
      Ingest ingest;
      auto part_count = rand_part_count(e);
      auto tuple_size = rand_tuple_size(e);
      auto tuple_count = rand_tuple_size(e);
      for (auto i = 0; i < part_count; i++)
        req.InsertStrip(i, part_count, tuple_count/part_count>0 ?tuple_count/part_count :1);
      TxnClient::BeginIngest(req, ingest);
      for (auto & strip : ingest.strip_list_)
        LogClient::Data(strip.first,strip.second.first,strip.second.second,
                        buffer, tuple_size*tuple_count);
      TxnClient::CommitIngest(ingest.id_);
      LogClient::Refresh();
    }

}
int main(int argc, const char **argv){
  int n = stoi(string(argv[1]));
  int times = stoi(string(argv[2]));
  string ip = string(argv[3]);
  int port = stoi(string(argv[4]));
  TxnClient::Init(ip, port);
  LogServer::Init("data-log");
  struct  timeval tv1, tv2;
  vector<std::thread> threads;
  for (auto i=0;i<n;i++)
    threads.push_back(std::thread(task2, i, times));
  gettimeofday(&tv1,NULL);
  for (auto i=0;i<n;i++)
    threads[i].join();
  gettimeofday(&tv2,NULL);
  UInt64 time_u = (tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec);
  cout << "Time:" << time_u / 1000000 << "." << time_u / 1000 << "s" << endl;
  cout << "Delay:" << (time_u / times)/1000.0 << "ms" << endl;
  cout << "TPS:" << (n * times * 1000000.0) / time_u << endl;;
  caf::await_all_actors_done();
  caf::shutdown();
}
