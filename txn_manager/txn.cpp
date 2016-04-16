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
 * /txn/txn_utility.cpp
 *
 *  Created on: 2016年3月28日
 *      Author: imdb
 *		   Email: 
 * 
 * Description:
 *
 */
#include "txn.hpp"
namespace claims {
namespace txn {

using claims::txn::Strip;
void Strip::Map(vector<Strip> & input, map<UInt64,vector<Strip>> & output) {
  output.clear();
  for (auto & strip:input) {
    if (output.find(strip.Part) != output.end())
      output[strip.Part].push_back(strip);
    else
      output[strip.Part] = vector<Strip>();
  }
}

void Strip::Sort(vector<Strip> & input) {
  sort(input.begin(), input.end(),
       [](const Strip & a, const Strip &b){ return a.Pos < b.Pos;});
}

void Strip::Sort(vector<PStrip> & input) {

  sort(input.begin(), input.end(),
       [](const PStrip & a, const PStrip & b)
         { return a.first < b.first;});
}

void Strip::Merge(vector<Strip> & input){
  vector<Strip> buffer(input);
  input.clear();
  if (buffer.size() == 0) return;
  auto pid = buffer[0].Part;
  auto begin = buffer[0].Pos;
  auto end = buffer[0].Pos + buffer[0].Offset;
  for (auto i = 1; i < buffer.size(); i ++) {
    if (end == buffer[i].Pos)
      end = buffer[i].Pos + buffer[i].Offset;
    else {
      input.emplace_back(pid, begin, end - begin);
      begin = buffer[i].Pos;
      end = begin + buffer[i].Offset;
    }
  }
  input.emplace_back(pid, begin, end - begin);
}

void Strip::Merge(vector<PStrip> & input) {
   if (input.size() == 0) return;
   vector<PStrip> buffer;
   auto begin = input[0].first;
   auto end = input[0].first + input[0].second;
   for (auto i = 1; i < input.size(); i++) {
     if (end == input[i].first)
       end = input[i].first + input[i].second;
     else {
      buffer.emplace_back(begin, end - begin);
      begin = input[i].first;
      end = input[i].first + input[i].second;
      }
   }
   buffer.emplace_back(begin, end - begin);
   input = buffer;
}


void Strip::Filter(vector<Strip> & input, function<bool(const Strip &)> predicate) {
  vector<Strip> buffer(input);
  input.clear();
  for (auto & strip : buffer)
    if (predicate(strip))
      input.push_back(strip);
}

string Strip::ToString() {
  string str = "*******Strip******\n";
  str += "part:" + to_string(Part) +
      ",pos:" + to_string(Pos) +
      ",Offset:" + to_string(Offset) + "\n";
  return str;
}

string FixTupleIngestReq::ToString() {
  string str = "*******FixTupleIngestReq********\n";
  for (auto & item : Content)
    str += "part:" + to_string(item.first) +
        ",tuple_size:" + to_string(item.second.first) +
        ",tuple_count:"+ to_string(item.second.second)+"\n";
  return str;
}
string Ingest::ToString() {
  UInt64 core_id = Id % 1000;
  core_id << 54;
  core_id >> 54;
  string str = "*******Ingest*********\n";
  str += "id:" + to_string(Id) + ",core:" + to_string(core_id)+ "\n";
  for (auto & item : StripList)
    str += "part:" + to_string(item.first) +
        ",pos:" + to_string(item.second.first) +
        ",offset:"+ to_string(item.second.second)+"\n";
  return str;
}
string QueryReq::ToString() {
  string str = "*******QueryReq********\n";
  for (auto & part : PartList)
    str += "part:" + to_string(part) +"\n";
  return str;
}

string Query::ToString() {
  string str = "******Query*******\n";
  for (auto & part : Snapshot){
   str += "part:" + to_string(part.first)+"\n";
   for (auto & strip : part.second)
     str += "Pos:" + to_string(strip.first) +
          ",Offset:" + to_string(strip.second) + "\n";
   }
  return str;
}

string Checkpoint::ToString() {
  string str = "******checkpoint******\n";

  str += "part:" + to_string(Part) +"\n";
  str += "commit strip\n";
  for (auto & strip : CommitStripList)
    str += "Pos:" + to_string(strip.first) +
            ",Offset:" + to_string(strip.second) + "\n";

  str += "abort strip\n";
  for (auto & strip : AbortStripList)
    str += "Pos:" + to_string(strip.first) +
            ",Offset:" + to_string(strip.second) + "\n";
  str += "logic cp:" + to_string(LogicCP) + "\n";
  str += "phy cp:" + to_string(PhyCP) + "\n";
  return str;
}

}
}

