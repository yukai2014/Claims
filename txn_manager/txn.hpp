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
 * /txn/txn_utility.hpp
 *
 *  Created on: 2016年3月28日
 *      Author: imdb
 *		   Email: 
 * 
 * Description:
 *
 */

#ifndef TXN_HPP_
#define TXN_HPP_
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
#include "caf/all.hpp"
#include "caf/io/all.hpp"

namespace claims {
namespace txn{

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
using std::make_pair;
using UInt64 = unsigned long long;
using UInt32 = unsigned int;
using UInt16 = unsigned short;
using UInt8 = char;
using RetCode = int;
using BeginAtom = caf::atom_constant<caf::atom("Begin")>;
using CommitAtom = caf::atom_constant<caf::atom("Commit")>;
using AbortAtom = caf::atom_constant<caf::atom("Abort")>;
using DataAtom = caf::atom_constant<caf::atom("Data")>;

using OkAtom = caf::atom_constant<caf::atom("Ok")>;
using FailAtom = caf::atom_constant<caf::atom("Fail")>;
using IngestAtom = caf::atom_constant<caf::atom("Ingest")>;
using WriteAtom = caf::atom_constant<caf::atom("Write")>;

using QueryAtom = caf::atom_constant<caf::atom("Query")>;
using CheckpointAtom = caf::atom_constant<caf::atom("Checkpoint")>;
using GCAtom = caf::atom_constant<caf::atom("GC")>;
using CommitIngestAtom = caf::atom_constant<caf::atom("CommitIngt")>;
using AbortIngestAtom = caf::atom_constant<caf::atom("AbortInge")>;
using CommitCPAtom = caf::atom_constant<caf::atom("CommitCP")>;
using AbortCPAtom = caf::atom_constant<caf::atom("AbortCP")>;
using QuitAtom = caf::atom_constant<caf::atom("Quit")>;
using LinkAtom = caf::atom_constant<caf::atom("Link")>;
using RefreshAtom = caf::atom_constant<caf::atom("Refresh")>;
using MergeAtom = caf::atom_constant<caf::atom("merge")>;


static const int kTxnPort = 8089;
static const string kTxnIp = "127.0.0.1";
static const int kConcurrency = 4;
static const int kTxnBufferSize = 1024 * 10000;
static const int kTxnLowLen = 10;
static const int kTxnHighLen = 54;
static const int kGCTime = 5;
static const int kTimeout = 3;
static const int kBlockSize = 64 * 1024;
static const int kTailSize = sizeof(unsigned);

/********Strip******/
using PStrip = pair<UInt64, UInt64>;
class Strip{
 public:
  UInt64 part_;
  UInt64 pos_;
  UInt64 offset_;
  Strip() {}
  Strip(UInt64 pId, UInt64 pos, UInt32 offset):
    part_(pId), pos_(pos), offset_(offset) {}
  UInt64 get_part() const { return part_;}
  UInt64 get_pos() const { return pos_;}
  UInt64 get_offset() const { return offset_;}
  void set_part(UInt64 part) { part_ = part;}
  void set_pos(UInt64 pos) { pos_ = pos;}
  void set_offset(UInt64 offset) { offset_ = offset;}
  string ToString();
  static void Map(vector<Strip> & input, map<UInt64,vector<Strip>> & output);
  static void Sort(vector<Strip> & input);
  static void Sort(vector<PStrip> & input);
  static void Merge(vector<Strip> & input);
  static void Merge(vector<PStrip> & input);
  static void Filter(vector<Strip> & input, function<bool(const Strip &)> predicate);
};
inline bool operator ==  (const Strip & a, const Strip & b) {
  return a.part_ == b.part_ && a.pos_ == b.pos_ && a.offset_ == b.offset_;
}


/***********FixTupleIngestReq************/

class FixTupleIngestReq{
 public:
  /*fix tuple part -> <tuple_size, tuple_count> */
  map<UInt64, PStrip> content_;
  void InsertStrip(UInt64 part, UInt64 tuple_size, UInt64 tuple_count) {
    content_[part] = make_pair(tuple_size, tuple_count);
  }
  map<UInt64, PStrip> get_content() const{
    return content_;
  }
  void set_content(const map<UInt64, PStrip> & content) {
    content_ = content;
  }
  string ToString ();
};
inline bool operator == (const FixTupleIngestReq & a, const FixTupleIngestReq & b) {
  return a.content_ == b.content_;
}


/****************Ingest***************/
class Ingest {
 public:
  UInt64 id_;
  map<UInt64, PStrip> strip_list_;
  void InsertStrip (UInt64 part, UInt64 pos, UInt64 offset) {
    strip_list_[part] = make_pair(pos, offset);
  }
  void InsertStrip (const Strip & strip) {
    strip_list_[strip.part_] = make_pair(strip.pos_, strip.offset_);
  }
  UInt64 get_id() const { return id_;}
  map<UInt64, PStrip> get_strip_list() const { return strip_list_;}
  void set_id(const UInt64 & id){ id_ = id;}
  void set_strip_list(const map<UInt64, PStrip> & stripList) {
    strip_list_ = stripList;
  }
  string ToString();
};
inline bool operator == (const Ingest & a, const Ingest & b) {
  return a.id_ == b.id_;
}

/************QueryReq************/
class QueryReq{
 public:
  vector<UInt64> part_list_;
  void InsertPart(UInt64 part) { part_list_.push_back(part);}
  vector<UInt64> get_part_list() const { return part_list_;}
  void set_part_list(const vector<UInt64> & partList) { part_list_ = partList;}
  string ToString();
};
inline bool operator == (const QueryReq & a, const QueryReq & b) {
  return a.part_list_ == b.part_list_;
}

/***********Snapshot***********/
class Query{
 public:
   map<UInt64, vector<PStrip>> snapshot_;
   map<UInt64, UInt64> cp_list_;
   void InsertStrip (UInt64 part, UInt64 pos, UInt64 offset){
    // if (Snapshot.find(part) == Snapshot.end())
    //   Snapshot[part] = vector<pair<UInt64, UInt64>>();
    // else
       snapshot_[part].push_back(make_pair(pos, offset));
   }
   void InsertCP(UInt64 part, UInt64 cp) {
     cp_list_[part] = cp;
   }
   map<UInt64, vector<PStrip>> get_snapshot() const {
     return snapshot_;
   }
   map<UInt64, UInt64> get_cp_list() const { return cp_list_;}
   void set_snapshot(const map<UInt64, vector<PStrip>> & sp){
     snapshot_ = sp;
   }
   void set_cp_list(const map<UInt64, UInt64> & cplist) {
     cp_list_ = cplist;
   }
   string ToString();
};
inline bool operator == (const Query & a, const Query & b) {
  return a.snapshot_ == b.snapshot_;
}

/*********Checkpoint***********/
class Checkpoint{
 public:
  UInt64 id_;
  UInt64 part_;
  UInt64 logic_cp_;
  UInt64 phy_cp_;
  vector<PStrip> commit_strip_list_;
  vector<PStrip> abort_strip_list_;
  Checkpoint() {}
  Checkpoint(UInt64 part, UInt64 newLogicCP, UInt64 oldPhyCP):
    part_(part), logic_cp_(newLogicCP),phy_cp_(oldPhyCP) {}
  UInt64 get_id() const { return id_;}
  UInt64 get_part() const { return part_;}
  UInt64 get_logic_cp() const { return logic_cp_;}
  UInt64 get_phy_cp() const { return phy_cp_;}
  vector<PStrip> get_commit_strip_list() const { return commit_strip_list_;};
  vector<PStrip> get_abort_strip_list() const { return abort_strip_list_;};
  void set_part(UInt64  part) { part_ = part;}
  void set_Logic_cp(UInt64 logicCP) { logic_cp_ = logicCP;}
  void set_Phy_cp(UInt64 phyCP) { phy_cp_ = phyCP;}
  void set_commit_strip_list(const vector<PStrip> & commitstripList) {
    commit_strip_list_ = commitstripList;
  }
  void set_abort_strip_list(const vector<PStrip> & abortstripList) {
    abort_strip_list_ = abortstripList;
  }
  string ToString();
};
inline bool operator == (const Checkpoint & a, const Checkpoint & b) {
  return a.id_ == b.id_;
}

inline void SerConfig() {
  caf::announce<FixTupleIngestReq>("FixTupleIngestReq",
    make_pair(&FixTupleIngestReq::get_content, &FixTupleIngestReq::set_content));
  caf::announce<Ingest>("Ingest",
                        make_pair(&Ingest::get_id,&Ingest::set_id),
                        make_pair(&Ingest::get_strip_list,&Ingest::set_strip_list));
  caf::announce<QueryReq>("QueryReq",
                        make_pair(&QueryReq::get_part_list, &QueryReq::set_part_list));
  caf::announce<Query>("Query",
                          make_pair(&Query::get_snapshot,&Query::set_snapshot),
                          make_pair(&Query::get_cp_list, &Query::set_cp_list));
  caf::announce<Checkpoint>("Checkpoint",
                            make_pair(&Checkpoint::get_part, &Checkpoint::set_part),
                            make_pair(&Checkpoint::get_logic_cp, &Checkpoint::set_Logic_cp),
                            make_pair(&Checkpoint::get_phy_cp, &Checkpoint::set_Phy_cp),
                            make_pair(&Checkpoint::get_commit_strip_list,
                                      &Checkpoint::set_commit_strip_list),
                            make_pair(&Checkpoint::get_abort_strip_list,
                                      &Checkpoint::set_abort_strip_list));
}



}
}
#endif //  TXN_HPP_ 
