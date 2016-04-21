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
 * /Claims/loader/master_loader.cpp
 *
 *  Created on: Apr 7, 2016
 *      Author: yukai
 *		   Email: yukai2014@gmail.com
 *
 * Description:
 *
 */

#include "./master_loader.h"
#include <iostream>
#include <string>
#include <functional>
#include <vector>
#include <map>
#include <utility>

#include "caf/all.hpp"
#include "caf/io/all.hpp"

#include "./load_packet.h"
#include "./loader_message.h"
#include "./validity.h"
#include "../catalog/catalog.h"
#include "../catalog/partitioner.h"
#include "../catalog/table.h"
#include "../common/data_type.h"
#include "../common/ids.h"
#include "../common/memory_handle.h"
#include "../common/Schema/TupleConvertor.h"
#include "../Config.h"
#include "../Environment.h"
#include "../txn_manager/txn.hpp"
#include "../txn_manager/txn_client.hpp"
#include "../utility/resource_guard.h"
using caf::aout;
using caf::behavior;
using caf::event_based_actor;
using caf::io::publish;
using caf::io::remote_actor;
using caf::mixin::sync_sender_impl;
using caf::spawn;
using std::endl;
using claims::catalog::Catalog;
using claims::catalog::Partitioner;
using claims::catalog::TableDescriptor;
using claims::common::Malloc;
using claims::common::rSuccess;
using claims::common::rFailure;
using namespace claims::txn;  // NOLINT

namespace claims {
namespace loader {

MasterLoader::MasterLoader()
    : master_loader_ip(Config::master_loader_ip),
      master_loader_port(Config::master_loader_port) {}

MasterLoader::~MasterLoader() {}

static behavior MasterLoader::ReceiveSlaveReg(event_based_actor* self,
                                              MasterLoader* mloader) {
  return {
      [=](IpPortAtom, std::string ip, int port) {  // NOLINT
        LOG(INFO) << "receive slave network address(" << ip << ":" << port
                  << ")" << endl;
        int new_slave_fd = -1;
        if (rSuccess !=
            mloader->GetSocketFdConnectedWithSlave(ip, port, &new_slave_fd)) {
          LOG(ERROR) << "failed to get connected fd with slave";
        } else {
          LOG(INFO) << "succeed to get connected fd with slave";
        }
        assert(new_slave_fd > 3);
        //        mloader->slave_addrs_.push_back(NetAddr(ip, port));
        //        mloader->slave_sockets_.push_back(new_slave_fd);
        //        assert(mloader->slave_sockets_.size() ==
        //        mloader->slave_addrs_.size());

        mloader->slave_addr_to_socket.insert(
            pair<NodeAddress, int>(NodeAddress(ip, port), new_slave_fd));
        DLOG(INFO) << "start to send test message to slave";

        // test whether socket works well
        ostringstream oss;
        oss << "hello, i'm master, whose address is "
            << mloader->master_loader_ip << ":"
            << to_string(mloader->master_loader_port) << ". \0";

        int message_length = oss.str().length();
        DLOG(INFO) << "message length is " << message_length;

        if (-1 ==
            write(new_slave_fd, reinterpret_cast<char*>(&message_length), 4)) {
          PLOG(ERROR) << "failed to send message length to slave(" << ip << ":"
                      << port << ")";
        } else {
          DLOG(INFO) << "message length is sent";
        }
        if (-1 == write(new_slave_fd, oss.str().c_str(), message_length)) {
          PLOG(ERROR) << "failed to send message to slave(" << ip << ":" << port
                      << ")";
        } else {
          DLOG(INFO) << "message buffer is sent";
        }
      },
      caf::others >> [] { LOG(ERROR) << "nothing matched!!!"; }};
}

RetCode MasterLoader::ConnectWithSlaves() {
  int ret = rSuccess;
  try {
    auto listening_actor = spawn(&MasterLoader::ReceiveSlaveReg, this);
    publish(listening_actor, master_loader_port, master_loader_ip.c_str(),
            true);
    DLOG(INFO) << "published in " << master_loader_ip << ":"
               << master_loader_port;
  } catch (exception& e) {
    LOG(ERROR) << e.what();
    return rFailure;
  }
  return ret;
}

RetCode MasterLoader::Ingest() {
  RetCode ret = rSuccess;
  string message = GetMessage();

  // get message from MQ
  IngestionRequest req;
  EXEC_AND_LOG(ret, GetRequestFromMessage(message, &req), "got request!",
               "failed to get request");

  // parse message and get all tuples of all partitions, then
  // check the validity of all tuple in message
  TableDescriptor* table =
      Environment::getInstance()->getCatalog()->getTable(req.table_name_);
  assert(table != NULL && "table is not exist!");
  vector<vector<vector<void*>>> tuple_buffers_per_part(
      table->getNumberOfProjection());
  for (auto proj : (*(table->GetProjectionList()))) {
    tuple_buffers_per_part.push_back(vector<vector<void*>>(
        proj->getPartitioner()->getNumberOfPartitions(), vector<void*>()));
  }
  vector<Validity> columns_validities;
  EXEC_AND_LOG(ret, GetPartitionTuples(req, table, tuple_buffers_per_part,
                                       columns_validities),
               "got all tuples of every partition",
               "failed to get all tuples of every partition");
  if (ret != rSuccess && ret != claims::common::rNoMemory) {
    // TODO(YUKAI): error handle, like sending error message to client
    LOG(ERROR) << "the tuple is not valid";
    return rFailure;
  }

  // merge all tuple buffers of partition into one partition buffer
  vector<vector<PartitionBuffer>> partition_buffers(
      table->getNumberOfProjection());
  EXEC_AND_LOG(ret, MergePartitionTupleIntoOneBuffer(
                        table, tuple_buffers_per_part, partition_buffers),
               "merged all tuple of same partition into one buffer",
               "failed to merge tuples buffers into one buffer");

  // start transaction from here
  claims::txn::Ingest ingest;
  EXEC_AND_LOG(ret, ApplyTransaction(table, partition_buffers, ingest),
               "applied transaction", "failed to apply transaction");

  // write data log
  EXEC_AND_LOG(ret, WriteLog(req, table, partition_buffers), "written log ",
               "failed to write log");

  // reply ACK to MQ
  EXEC_AND_LOG(ret, ReplyToMQ(req), "replied to MQ", "failed to reply to MQ");

  // distribute partition load task
  EXEC_AND_LOG(ret, SendPartitionTupleToSlave(table, partition_buffers, ingest),
               "sent every partition data to its slave",
               "failed to send every partition data to its slave");

  return ret;
}

string MasterLoader::GetMessage() {
  string ret;
  return ret;
}

bool MasterLoader::CheckValidity() {}

void MasterLoader::DistributeSubIngestion() {}

RetCode MasterLoader::GetSocketFdConnectedWithSlave(string ip, int port,
                                                    int* connected_fd) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);

  //  port = 23667;

  struct sockaddr_in slave_addr;
  slave_addr.sin_family = AF_INET;
  slave_addr.sin_port = htons(port);
  slave_addr.sin_addr.s_addr = inet_addr(ip.c_str());

  if (-1 == connect(fd, (struct sockaddr*)(&slave_addr), sizeof(sockaddr_in))) {
    PLOG(ERROR) << "failed to connect socket(" << ip << ":" << port << ")";
    return rFailure;
  }
  *connected_fd = fd;
  return rSuccess;
}

// get every tuples and add row id for it
RetCode MasterLoader::GetRequestFromMessage(const string& message,
                                            IngestionRequest* req) {
  //  AddRowIdColumn()
  RetCode ret = rSuccess;
  return ret;
}

RetCode MasterLoader::CheckAndToValue(const IngestionRequest& req,
                                      void* tuple_buffer,
                                      vector<Validity>& column_validities) {}

// map every tuple into associate part
RetCode MasterLoader::GetPartitionTuples(
    const IngestionRequest& req, const TableDescriptor* table,
    vector<vector<vector<void*>>>& tuple_buffer_per_part,
    vector<Validity>& columns_validities) {
  RetCode ret = rSuccess;
  vector<void*> correct_tuple_buffer;
  STLGuardWithRetCode<vector<void*>> guard(correct_tuple_buffer,
                                           ret);  // attention!
  // must set RetCode 'ret' before returning error code!!!!
  ThreeLayerSTLGuardWithRetCode<vector<vector<vector<void*>>>>
      return_tuple_buffer_guard(tuple_buffer_per_part, ret);  // attention!

  // check all tuples to be inserted
  int line = 0;
  for (auto tuple_string : req.tuples_) {
    void* tuple_buffer = Malloc(table->getSchema()->getTupleMaxSize());
    if (tuple_buffer == NULL) return claims::common::rNoMemory;
    MemoryGuardWithRetCode<void> guard(tuple_buffer, ret);
    if (rSuccess != (ret = table->getSchema()->CheckAndToValue(
                         tuple_string, tuple_buffer, req.col_sep_,
                         RawDataSource::kSQL, columns_validities))) {
      // handle error which stored in the end
      Validity err = columns_validities.back();
      columns_validities.pop_back();
      string validity_info =
          Validity::GenerateDataValidityInfo(err, table, line, "");
      LOG(ERROR) << validity_info;
    }
    // handle all warnings
    for (auto it : columns_validities) {
      string validity_info =
          Validity::GenerateDataValidityInfo(it, table, line, "");
      LOG(WARNING) << "append warning info:" << validity_info;
    }
    if (rSuccess != ret) {
      // clean work is done by guard
      return ret;
    }
    ++line;
    correct_tuple_buffer.push_back(tuple_buffer);
  }

  // map every tuple in different partition
  for (int i = 0; i < table->getNumberOfProjection(); i++) {
    ProjectionDescriptor* prj = table->getProjectoin(i);
    Schema* prj_schema = prj->getSchema();
    vector<Attribute> prj_attrs = prj->getAttributeList();
    vector<unsigned> prj_index;
    for (int j = 0; j < prj_attrs.size(); j++) {
      prj_index.push_back(prj_attrs[j].index);
    }
    SubTuple sub_tuple(table->getSchema(), prj_schema, prj_index);

    const int partition_key_local_index =
        prj->getAttributeIndex(prj->getPartitioner()->getPartitionKey());
    unsigned tuple_max_length = prj_schema->getTupleMaxSize();

    for (auto tuple_buffer : correct_tuple_buffer) {
      // extract the sub tuple according to the projection schema
      void* target = Malloc(prj_schema->getTupleMaxSize());  // newmalloc
      if (target == NULL) {
        return (ret = claims::common::rNoMemory);
      }
      sub_tuple.getSubTuple(tuple_buffer, target);

      // determine the partition to write the tuple "target"
      void* partition_key_addr =
          prj_schema->getColumnAddess(partition_key_local_index, target);
      int part = prj_schema->getcolumn(partition_key_local_index)
                     .operate->getPartitionValue(
                         partition_key_addr,
                         prj->getPartitioner()->getPartitionFunction());

      tuple_buffer_per_part[i][part].push_back(target);
    }
  }
  return ret;
}

RetCode MasterLoader::ApplyTransaction(
    const TableDescriptor* table,
    const vector<vector<PartitionBuffer>>& partition_buffers,
    claims::txn::Ingest& ingest) {
  RetCode ret = rSuccess;
  uint64_t table_id = table->get_table_id();

  FixTupleIngestReq req;
  for (int i = 0; i < table->getNumberOfProjection(); ++i) {
    ProjectionDescriptor* prj = table->getProjectoin(i);
    uint64_t tuple_length = prj->getSchema()->getTupleMaxSize();
    for (int j = 0; j < prj->getPartitioner()->getNumberOfPartitions(); ++j) {
      req.Insert(GetGlobalPartId(table_id, i, j), tuple_length,
                 partition_buffers[i][j].length_ / tuple_length);
    }
  }
  TxnClient::BeginIngest(req, ingest);

  return ret;
}

RetCode MasterLoader::WriteLog(
    const IngestionRequest& req, const TableDescriptor* table,
    const vector<vector<PartitionBuffer>>& partition_buffers) {}

RetCode MasterLoader::ReplyToMQ(const IngestionRequest& req) {}

RetCode MasterLoader::SendPartitionTupleToSlave(
    const TableDescriptor* table,
    const vector<vector<PartitionBuffer>>& partition_buffers,
    claims::txn::Ingest& ingest) {
  RetCode ret = rSuccess;
  uint64_t table_id = table->get_table_id();

  for (int prj_id = 0; prj_id < partition_buffers.size(); ++prj_id) {
    for (int part_id = 0; part_id < partition_buffers[prj_id].size();
         ++part_id) {
      uint64_t global_part_id = GetGlobalPartId(table_id, prj_id, part_id);
      LoadPacket packet(global_part_id, ingest.StripList[global_part_id].first,
                        ingest.StripList[global_part_id].second,
                        partition_buffers[prj_id][part_id].length_,
                        partition_buffers[prj_id][part_id].buffer_);
      void* packet_buffer;
      MemoryGuard<void> guard(packet_buffer);  // auto release by guard
      uint64_t packet_length;
      EXEC_AND_LOG_RETURN(ret, packet.Serialize(packet_buffer, packet_length),
                          "serialized packet into buffer",
                          "failed to serialize packet");

      int socket_fd = -1;
      EXEC_AND_LOG_RETURN(ret, SelectSocket(table, prj_id, part_id, socket_fd),
                          "selected the socket", "failed to select the socket");
      assert(socket_fd > 3);

      EXEC_AND_LOG_RETURN(ret,
                          SendPacket(socket_fd, packet_buffer, packet_length),
                          "sent message to slave :" << socket_fd,
                          "failed to sent message to slave :" << socket_fd);
    }
  }
  return ret;
}

RetCode MasterLoader::MergePartitionTupleIntoOneBuffer(
    const TableDescriptor* table,
    vector<vector<vector<void*>>>& tuple_buffer_per_part,
    vector<vector<PartitionBuffer>>& partition_buffers) {
  RetCode ret = rSuccess;
  for (int i = 0; i < tuple_buffer_per_part.size(); ++i) {
    for (int j = 0; j < tuple_buffer_per_part[i].size(); ++j) {
      int tuple_count = tuple_buffer_per_part[i][j].size();
      int tuple_len = table->getProjectoin(i)->getSchema()->getTupleMaxSize();
      int buffer_len = tuple_count * tuple_len;

      void* new_buffer = Malloc(buffer_len);
      if (NULL == new_buffer) return ret = claims::common::rNoMemory;

      for (int k = 0; k < tuple_count; ++k) {
        memcpy(new_buffer + k * tuple_len, tuple_buffer_per_part[i][j][k],
               tuple_len);
        // release old memory stored tuple buffer
        DELETE_PTR(tuple_buffer_per_part[i][j][k]);
      }
      // push new partition buffer
      partition_buffers[i].push_back(PartitionBuffer(new_buffer, buffer_len));
      tuple_buffer_per_part[i][j].clear();
    }
    tuple_buffer_per_part[i].clear();
  }
  tuple_buffer_per_part.clear();
  return ret;
}

RetCode MasterLoader::SelectSocket(const TableDescriptor* table,
                                   const uint64_t prj_id,
                                   const uint64_t part_id, int& socket_fd) {
  RetCode ret = rSuccess;
  NodeID node_id_in_rmm =
      table->getProjectoin(prj_id)->getPartitioner()->getPartitionLocation(
          part_id);
  NodeAddress addr;
  EXEC_AND_LOG_RETURN(
      ret, NodeTracker::GetInstance()->GetNodeAddr(node_id_in_rmm, addr),
      "got node address", "failed to get node address");
  socket_fd = slave_addr_to_socket[addr];
  return ret;
}

RetCode MasterLoader::SendPacket(const int socket_fd,
                                 const void* const packet_buffer,
                                 const uint64_t packet_length) {
  size_t total_write_num = 0;
  while (total_write_num < packet_length) {
    ssize_t write_num = write(
        socket_fd, static_cast<const char*>(packet_buffer) + total_write_num,
        packet_length - total_write_num);
    if (-1 == write_num) {
      PLOG(ERROR) << "failed to send buffer to slave(" << socket_fd << "): ";
      return claims::common::rSentMessageError;
    }
    total_write_num += write_num;
  }
  return rSuccess;
}

void* MasterLoader::StartMasterLoader(void* arg) {
  Config::getInstance();
  LOG(INFO) << "start master loader...";

  TxnClient::Init();

  int ret = rSuccess;
  MasterLoader* master_loader = Environment::getInstance()->get_master_loader();
  EXEC_AND_ONLY_LOG_ERROR(ret, master_loader->ConnectWithSlaves(),
                          "failed to connect all slaves");

  while (true)
    EXEC_AND_ONLY_LOG_ERROR(ret, master_loader->Ingest(),
                            "failed to ingest data");

  return NULL;
}

} /* namespace loader */
} /* namespace claims */
