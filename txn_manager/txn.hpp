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
using std::make_pair;;
namespace claims {
namespace txn{
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

static const int kTxnPort = 8089;
static const string kTxnIp = "127.0.0.1";
static const int kConcurrency = 4;
static const int kTxnBufferSize = 1024 * 10000;
static const int kTxnLowLen = 10;
static const int kTxnHighLen = 54;
static const int kGCTime = 5;
static const int kTimeout = 1;
static const int kBlockSize = 64 * 1024;
static const int kTailSize = sizeof(unsigned);

/********Strip******/
using PStrip = pair<UInt64, UInt64>;
class Strip{
 public:
  UInt64 Part;
  UInt64 Pos;
  UInt64 Offset;
  Strip() {}
  Strip(UInt64 pId, UInt64 pos, UInt32 offset):
    Part(pId), Pos(pos), Offset(offset) {}
  UInt64 get_Part() const { return Part;}
  UInt64 get_Pos() const { return Pos;}
  UInt64 get_Offset() const { return Offset;}
  void set_Part(UInt64 part) { Part = part;}
  void set_Pos(UInt64 pos) { Pos = pos;}
  void set_Offset(UInt64 offset) { Offset = offset;}
  string ToString();
  static void Map(vector<Strip> & input, map<UInt64,vector<Strip>> & output);
  static void Sort(vector<Strip> & input);
  static void Sort(vector<PStrip> & input);
  static void Merge(vector<Strip> & input);
  static void Merge(vector<PStrip> & input);
  static void Filter(vector<Strip> & input, function<bool(const Strip &)> predicate);
};
inline bool operator ==  (const Strip & a, const Strip & b) {
  return a.Part == b.Part && a.Pos == b.Pos && a.Offset == b.Offset;
}


/***********FixTupleIngestReq************/

class FixTupleIngestReq{
 public:
  /*fix tuple part -> <tuple_size, tuple_count> */
  map<UInt64, PStrip> Content;
  void Insert(UInt64 part, UInt64 tuple_size, UInt64 tuple_count) {
    Content[part] = make_pair(tuple_size, tuple_count);
  }
  map<UInt64, PStrip> get_Content() const{
    return Content;
  }
  void set_Content(const map<UInt64, PStrip> & content) {
    Content = content;
  }
  string ToString ();
};
inline bool operator == (const FixTupleIngestReq & a, const FixTupleIngestReq & b) {
  return a.Content == b.Content;
}


/****************Ingest***************/
class Ingest {
 public:
  UInt64 Id;
  map<UInt64, PStrip> StripList;
  void InsertStrip (UInt64 part, UInt64 pos, UInt64 offset) {
    StripList[part] = make_pair(pos, offset);
  }
  void InsertStrip (const Strip & strip) {
    StripList[strip.Part] = make_pair(strip.Pos, strip.Offset);
  }
  UInt64 get_Id() const { return Id;}
  map<UInt64, PStrip> get_StripList() const { return StripList;}
  void set_Id(const UInt64 & id){ Id = id;}
  void set_StripList(const map<UInt64, PStrip> & stripList) {
    StripList = stripList;
  }
  string ToString();
};
inline bool operator == (const Ingest & a, const Ingest & b) {
  return a.Id == b.Id;
}

/************QueryReq************/
class QueryReq{
 public:
  vector<UInt64> PartList;
  void InsertPart(UInt64 part) { PartList.push_back(part);}
  vector<UInt64> get_PartList() const { return PartList;}
  void set_PartList(const vector<UInt64> & partList) { PartList = partList;}
  string ToString();
};
inline bool operator == (const QueryReq & a, const QueryReq & b) {
  return a.PartList == b.PartList;
}

/***********Snapshot***********/
class Query{
 public:
   map<UInt64, vector<PStrip>> Snapshot;
   map<UInt64, UInt64> CPList;
   void InsertStrip (UInt64 part, UInt64 pos, UInt64 offset){
    // if (Snapshot.find(part) == Snapshot.end())
    //   Snapshot[part] = vector<pair<UInt64, UInt64>>();
    // else
       Snapshot[part].push_back(make_pair(pos, offset));
   }
   void InsertCP(UInt64 part, UInt64 cp) {
     CPList[part] = cp;
   }
   map<UInt64, vector<PStrip>> get_Snapshot() const {
     return Snapshot;
   }
   map<UInt64, UInt64> get_CPList() const { return CPList;}
   void set_Snapshot(const map<UInt64, vector<PStrip>> & sp){
     Snapshot = sp;
   }
   void set_CPList(const map<UInt64, UInt64> & cplist) {
     CPList = cplist;
   }
   string ToString();
};
inline bool operator == (const Query & a, const Query & b) {
  return a.Snapshot == b.Snapshot;
}

/*********Checkpoint***********/
class Checkpoint{
 public:
  UInt64 Id;
  UInt64 Part;
  UInt64 LogicCP;
  UInt64 PhyCP;
  vector<PStrip> CommitStripList;
  vector<PStrip> AbortStripList;
  Checkpoint() {}
  Checkpoint(UInt64 part, UInt64 newLogicCP, UInt64 oldPhyCP):
    Part(part), LogicCP(newLogicCP),PhyCP(oldPhyCP) {}
  UInt64 get_Id() const { return Id;}
  UInt64 get_Part() const { return Part;}
  UInt64 get_LogicCP() const { return LogicCP;}
  UInt64 get_PhyCP() const { return PhyCP;}
  vector<PStrip> get_CommitStripList() const { return CommitStripList;};
  vector<PStrip> get_AbortStripList() const { return AbortStripList;};
  void set_Part(UInt64  part) { Part = part;}
  void set_LogicCP(UInt64 logicCP) { LogicCP = logicCP;}
  void set_PhyCP(UInt64 phyCP) { PhyCP = phyCP;}
  void set_CommitStripList(const vector<PStrip> & commitstripList) {
    CommitStripList = commitstripList;
  }
  void set_AbortStripList(const vector<PStrip> & abortstripList) {
    AbortStripList = abortstripList;
  }
  string ToString();
};
inline bool operator == (const Checkpoint & a, const Checkpoint & b) {
  return a.Id == b.Id;
}

inline void SerializeConfig() {
  caf::announce<FixTupleIngestReq>("FixTupleIngestReq",
    make_pair(&FixTupleIngestReq::get_Content, &FixTupleIngestReq::set_Content));
  caf::announce<Ingest>("Ingest",
                        make_pair(&Ingest::get_Id,&Ingest::set_Id),
                        make_pair(&Ingest::get_StripList,&Ingest::set_StripList));
  caf::announce<QueryReq>("QueryReq",
                        make_pair(&QueryReq::get_PartList, &QueryReq::set_PartList));
  caf::announce<Query>("Query",
                          make_pair(&Query::get_Snapshot,&Query::set_Snapshot),
                          make_pair(&Query::get_CPList, &Query::set_CPList));
  caf::announce<Checkpoint>("Checkpoint",
                            make_pair(&Checkpoint::get_Part, &Checkpoint::set_Part),
                            make_pair(&Checkpoint::get_LogicCP, &Checkpoint::set_LogicCP),
                            make_pair(&Checkpoint::get_PhyCP, &Checkpoint::set_PhyCP),
                            make_pair(&Checkpoint::get_CommitStripList,
                                      &Checkpoint::set_CommitStripList),
                            make_pair(&Checkpoint::get_AbortStripList,
                                      &Checkpoint::set_AbortStripList));
}









}
}
#endif //  TXN_HPP_ 
