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
#include <thread>
#include <utility>
#include <unordered_map>
#include <tuple>
#include <time.h>
#include <chrono>
#include "unistd.h"
#include <sys/time.h>
#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "txn.hpp"
#include "txn_server.hpp"
#include "txn_client.hpp"
#include "txn_log.cpp"
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
using std::make_pair;
using std::make_tuple;
using std::get;

using namespace claims::txn;

using UInt64 = unsigned long long;
using UInt32 = unsigned int;
using UInt16 = unsigned short;
using UInt8 = char;
using RetCode = int;
using OkAtom = caf::atom_constant<caf::atom("ok")>;
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


class AA:public caf::event_based_actor {
  caf::behavior make_behavior() override {
    return{

      [] (FixTupleIngestReq & request){
          cout << request.ToString() << endl;
        },
      [] (int a) {cout << a << endl;},
      caf::others >> []() {
        cout << "no matched" << endl;
        }
    };
  }
};
class C:public caf::event_based_actor {
  caf::behavior make_behavior() override {
    return {
      [=] (int a)->int { quit(); aout(this)<< a*1000 << endl;},
      caf::others >> []() {cout << "no matched" << endl;}
    };
  }
};

class Foo2 {
 public:
  int a = 0;
  int b = 0;
};
class Foo3{
 public:
  int c = 0;
};
using Foo2Atom = caf::atom_constant<caf::atom("foo2")>;
using Foo3Atom = caf::atom_constant<caf::atom("foo3")>;

class B:public caf::event_based_actor {
 public:
  caf::actor Router;
  B() {}
  B(caf::actor router):Router(router) {}
  caf::behavior make_behavior() override {
   return {
     [=](int a) {
         forward_to(caf::spawn<C>());
       },
     [=](Foo2Atom, Foo2 * foo2)->int {
         foo2->a = 97;
         foo2->b = 98;
         cout << "foo2" << endl;
         return 101;
       },
     [=](Foo3Atom, Foo3 * foo3)->int {
         foo3->c = 99;
         cout << "foo3" << endl;
         return 102;
       },
       caf::others >> []() { cout << "unkown" << endl;}
    };
  }
};
using claims::txn::TxnServer;
using claims::txn::FixTupleIngestReq;
using claims::txn::Ingest;
char v[1024+10];



void task(int time){
  for (auto i = 0; i< time; i++) {

      FixTupleIngestReq request1;
      Ingest ingest;
      request1.Content = {{0, {45, 10}},
                                   {1, {35, 20}},
                                   {2,{15,100}}};
      TxnClient::BeginIngest(request1, ingest);
      LogClient::Data(1, 1, 1111,(void*)v, 1024);
      LogClient::Data(1, 1, 1111,(void*)v, 1024);
      LogClient::Data(1, 1, 1111,(void*)v, 1024);

      TxnClient::CommitIngest(ingest);
//    }
  }
}


void task2(int time) {
  for (auto i = 0; i< time; i++) {

    LogClient::Begin(i);
    LogClient::Write(i, 1, 0, 100 );
    LogClient::Write(i, 2, 0, 100 );
    LogClient::Write(i, 3, 0, 100 );

    //LogClient::PushToDisk() ;
  }

}

int main(){
//  auto server = caf::spawn<A>();
//  SerializeConfig();
//  caf::announce<Foo>("foo",
//                     make_pair(&Foo::get_request1, &Foo::set_request1),
//                     make_pair(&Foo::get_request2, &Foo::set_request2),
//                     make_pair(&Foo::get_request3, &Foo::set_request3));
//
//  try {
//    caf::io::publish(server, 8088);
//  } catch (...) {
//     cout << "bind fail" << endl;
//  }

//
//  TxnServer::Init();

//  for (auto j = 0;j < 100  ;j++) {
////        request1.Content[0] = {45, 10};
////        request1.Content[1] = {54, 10};
//    FixTupleIngestReq request1;
//    Ingest ingest;
//    request1.Content = {{0, {45, 10}}, {1, {54, 10}}};
//    TxnServer::BeginIngest(request1, ingest);
//    TxnServer::CommitIngest(ingest);
//   }
//  sleep(1);
  memset(v, 1024, '*');
  string path;
  cout << "input path" << endl;
  cin >> path;
  TxnServer::Init();
  LogServer::init(path);
  struct  timeval tv1, tv2;
  vector<std::thread> v;
  int n = 1, time =1;
  cout << "input #thread, #time" << endl;
  cin >> n >> time;
  gettimeofday(&tv1,NULL);
  for (auto i=0;i<n;i++)
    v.push_back(std::thread(task, time));
  for (auto i=0;i<n;i++)
    v[i].join();
  //cout << "count2:" << LogServer::count2 << endl;
  gettimeofday(&tv2,NULL);
  cout << tv2.tv_sec - tv1.tv_sec << "-" << (tv2.tv_usec - tv1.tv_usec)/1000 <<endl;



  caf::await_all_actors_done();
}
