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
 * /Claims/loader/master_loader.h
 *
 *  Created on: Apr 7, 2016
 *      Author: yukai
 *		   Email: yukai2014@gmail.com
 *
 * Description:
 *
 */

#ifndef LOADER_MASTER_LOADER_H_
#define LOADER_MASTER_LOADER_H_

#include <boost/unordered/unordered_map.hpp>
#include <glog/logging.h>
#include <functional>
#include <string>
#include <vector>
#include "caf/all.hpp"
#include <unordered_map>

#include "./validity.h"
#include "../common/error_define.h"
#include "../common/ids.h"
#include "../txn_manager/txn.hpp"
#include "../utility/lock.h"

using std::function;
using std::unordered_map;

namespace claims {
namespace catalog {
class TableDescriptor;
}
namespace loader {

using std::map;
using std::string;
using std::vector;
using caf::behavior;
using caf::event_based_actor;
using claims::catalog::TableDescriptor;

class MasterLoader {
  // public:
  //  enum DataIngestSource { kActiveMQ };

 public:
  struct IngestionRequest {
    string table_name_;
    string col_sep_;
    string row_sep_;
    vector<string> tuples_;
    void Show() {
      LOG(INFO) << "table name:" << table_name_
                << ", column separator:" << col_sep_
                << ", row separator:" << row_sep_
                << ", tuples size is:" << tuples_.size();
    }
  };
  struct WorkerPara {
    WorkerPara(MasterLoader* mloader, const std::string& brokerURI,
               const std::string& destURI, bool use_topic = false,
               bool client_ack = false)
        : use_topic_(use_topic),
          brokerURI_(brokerURI),
          destURI_(destURI),
          client_ack_(client_ack),
          master_loader_(mloader) {}
    const std::string& brokerURI_;
    const std::string& destURI_;
    bool use_topic_ = false;
    bool client_ack_ = false;
    MasterLoader* master_loader_;
  };

  struct CommitInfo {
    explicit CommitInfo(uint64_t total_part_num)
        : total_part_num_(total_part_num),
          commited_part_num_(0),
          wait_period_(0) {}
    uint64_t total_part_num_;
    uint64_t commited_part_num_;
    // initial value is 0, add by 1 every time check thread traverses
    // if wait period exceeds the specified value, this transaction fails
    uint64_t wait_period_;
  };

  struct PartitionBuffer {
    PartitionBuffer(void* buf, uint64_t len) : buffer_(buf), length_(len) {}
    void* buffer_;
    uint64_t length_;
  };

 public:
  MasterLoader();
  ~MasterLoader();

  RetCode ConnectWithSlaves();

  RetCode Ingest(const string& message, function<int()> ack_function);

 private:
  string GetMessage();

  RetCode GetRequestFromMessage(const string& message, IngestionRequest* req);

  RetCode CheckAndToValue(const IngestionRequest& req, void* tuple_buffer,
                          vector<Validity>& column_validities);

  RetCode GetPartitionTuples(
      const IngestionRequest& req, const TableDescriptor* table,
      vector<vector<vector<void*>>>& tuple_buffer_per_part,
      vector<Validity>& columns_validities);

  /**
   * copy and merge all tuples buffer of the same partition into one buffer,
   * and release all memory in tuple_buffer_per_part
   */
  RetCode MergePartitionTupleIntoOneBuffer(
      const TableDescriptor* table,
      vector<vector<vector<void*>>>& tuple_buffer_per_part,
      vector<vector<PartitionBuffer>>& partition_buffers);

  RetCode ApplyTransaction(
      const TableDescriptor* table,
      const vector<vector<PartitionBuffer>>& partition_buffers,
      claims::txn::Ingest& ingest);

  RetCode WriteLog(const TableDescriptor* table,
                   const vector<vector<PartitionBuffer>>& partition_buffers,
                   const claims::txn::Ingest& ingest);

  RetCode ReplyToMQ(const IngestionRequest& req);

  RetCode SendPartitionTupleToSlave(
      const TableDescriptor* table,
      const vector<vector<PartitionBuffer>>& partition_buffers,
      const claims::txn::Ingest& ingest);

  RetCode SelectSocket(const TableDescriptor* table, const uint64_t prj_id,
                       const uint64_t part_id, int& socket_fd);

  RetCode SendPacket(const int socket_fd, const void* const packet_buffer,
                     const uint64_t packet_length);

  RetCode GetSlaveNetAddr();
  RetCode SetSocketWithSlaves();
  RetCode GetSocketFdConnectedWithSlave(string ip, int port, int* connected_fd);
  bool CheckValidity();
  void DistributeSubIngestion();

  static behavior ReceiveSlaveReg(event_based_actor* self,
                                  MasterLoader* mloader);

 public:
  static void* Work(void* para);
  static void* StartMasterLoader(void* arg);

 private:
  string master_loader_ip_;
  int master_loader_port_;
  //  vector<NetAddr> slave_addrs_;
  //  vector<int> slave_sockets_;
  boost::unordered_map<NodeAddress, int> slave_addr_to_socket_;

  // store id of transactions which are not finished
  unordered_map<uint64_t, CommitInfo> txn_commint_info_;
  Lock lock_;
  SpineLock spin_lock_;
};

} /* namespace loader */
} /* namespace claims */

#endif  // LOADER_MASTER_LOADER_H_
