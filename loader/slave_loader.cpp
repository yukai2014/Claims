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
 * /Claims/loader/slave_loader.cpp
 *
 *  Created on: Apr 8, 2016
 *      Author: yukai
 *		   Email: yukai2014@gmail.com
 *
 * Description:
 *
 */

#include "./slave_loader.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <exception>
#include "caf/all.hpp"
#include "caf/io/all.hpp"

#include "./load_packet.h"
#include "../Config.h"
#include "../Environment.h"
#include "../common/error_define.h"
#include "../common/ids.h"
#include "../common/memory_handle.h"
#include "../storage/ChunkStorage.h"
#include "../storage/MemoryStore.h"
#include "../storage/PartitionStorage.h"
#include "../txn_manager/txn.hpp"
#include "../utility/resource_guard.h"
using caf::event_based_actor;
using caf::io::remote_actor;
using caf::spawn;
using claims::common::Malloc;
using claims::common::rSuccess;
using claims::common::rFailure;
using claims::txn::GetPartitionIdFromGlobalPartId;
using claims::txn::GetProjectionIdFromGlobalPartId;
using claims::txn::GetTableIdFromGlobalPartId;

namespace claims {
namespace loader {

SlaveLoader::SlaveLoader() {
  //  try {
  //    master_actor_ =
  //        remote_actor(Config::master_loader_ip, Config::master_loader_port);
  //  } catch (const exception& e) {
  //    cout << "master loader actor failed." << e.what() << endl;
  //  }
}
SlaveLoader::~SlaveLoader() {}

RetCode SlaveLoader::ConnectWithMaster() {
  int ret = rSuccess;
  int retry_time = 10;
  for (int i = 0; Clean(), i < retry_time; ++i) {  // if failed, call Clean()
    EXEC_AND_LOG(ret, EstablishListeningSocket(),
                 "established listening socket",
                 "failed to establish listening socket in " << i << " times");
    if (rSuccess == ret) break;
  }
  if (rSuccess != ret) {
    Clean();
    return ret;
  }

  for (int i = 1; i <= retry_time; ++i) {
    EXEC_AND_LOG(ret, SendSelfAddrToMaster(), "sent self ip/port to master",
                 "failed to send self ip/port to master in " << i << " times");
    if (rSuccess == ret) break;
    sleep(1);
  }
  if (rSuccess != ret) {
    Clean();
    return ret;
  }

  for (int i = 0; i < retry_time; ++i) {
    EXEC_AND_LOG(ret, GetConnectedSocket(), "got connected socket with master",
                 "failed to get connected socket with master in " << i
                                                                  << " times");
    if (rSuccess == ret) break;
  }
  if (rSuccess != ret) Clean();
  return ret;
}

RetCode SlaveLoader::EstablishListeningSocket() {
  int ret = rSuccess;
  listening_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == listening_fd_) {
    PLOG(ERROR) << "failed to create socket";
  }

  struct sockaddr_in sock_addr;
  sock_addr.sin_family = AF_INET;
  sock_addr.sin_port = 0;
  sock_addr.sin_addr.s_addr =
      inet_addr(Environment::getInstance()->getIp().c_str());

  if (-1 ==
      bind(listening_fd_, (struct sockaddr*)(&sock_addr), sizeof(sock_addr))) {
    PLOG(ERROR) << "failed to bind socket";
    return rFailure;
  }

  OutputFdIpPort(listening_fd_);

  if (-1 == listen(listening_fd_, 5)) {
    PLOG(ERROR) << "failed to listen socket";
    return rFailure;
  }

  OutputFdIpPort(listening_fd_);

  struct sockaddr_in temp_addr;
  socklen_t addr_len = sizeof(sockaddr_in);
  if (-1 ==
      getsockname(listening_fd_, (struct sockaddr*)(&temp_addr), &addr_len)) {
    PLOG(ERROR) << "failed to get socket name ";
    return rFailure;
  }

  self_port = ntohs(temp_addr.sin_port);
  self_ip = inet_ntoa(temp_addr.sin_addr);

  LOG(INFO) << "slave socket IP:" << self_ip << ", port:" << self_port;
  return ret;
}

RetCode SlaveLoader::SendSelfAddrToMaster() {
  //  auto send_actor = spawn([&](event_based_actor* self) {
  //    auto master_actor =
  //        remote_actor(Config::master_loader_ip, Config::master_loader_port);
  //    self->sync_send(master_actor, IpPortAtom::value, self_ip, self_port);
  //  });
  DLOG(INFO) << "going to send self (" << self_ip << ":" << self_port << ")"
             << "to (" << Config::master_loader_ip << ":"
             << Config::master_loader_port << ")";
  try {
    auto master_actor =
        remote_actor(Config::master_loader_ip, Config::master_loader_port);
    caf::scoped_actor self;
    self->sync_send(master_actor, IpPortAtom::value, self_ip, self_port)
        .await([&](int r) {  // NOLINT
          LOG(INFO) << "sent ip&port and received response";
        });
  } catch (exception& e) {
    LOG(ERROR) << "can't send self ip&port to master loader. " << e.what();
    return rFailure;
  }
  return rSuccess;
}

RetCode SlaveLoader::GetConnectedSocket() {
  assert(listening_fd_ > 3);
  OutputFdIpPort(listening_fd_);
  DLOG(INFO) << "fd is accepting...";

  struct sockaddr_in master_addr;
  socklen_t len = sizeof(sockaddr_in);
  int master_fd = accept(listening_fd_, (struct sockaddr*)(&master_addr), &len);
  if (-1 == master_fd) {
    PLOG(ERROR) << "failed to accept socket";
    return rFailure;
  }
  master_fd_ = master_fd;
  return rSuccess;
}
void SlaveLoader::OutputFdIpPort(int fd) {
  struct sockaddr_in temp_addr;
  socklen_t addr_len = sizeof(sockaddr_in);
  if (-1 == getsockname(fd, (struct sockaddr*)(&temp_addr), &addr_len)) {
    PLOG(ERROR) << "failed to get socket name ";
  }
  DLOG(INFO) << "fd ----> (" << inet_ntoa(temp_addr.sin_addr) << ":"
             << ntohs(temp_addr.sin_port) << ")";
}

RetCode SlaveLoader::ReceiveAndWorkLoop() {
  assert(master_fd_ > 3);
  char head_buffer[LoadPacket::kHeadLength];
  DLOG(INFO) << "slave is receiving ...";
  while (1) {
    RetCode ret = rSuccess;

    // get load packet
    int real_read_num;
    if (-1 == (real_read_num = recv(master_fd_, head_buffer,
                                    LoadPacket::kHeadLength, MSG_WAITALL))) {
      PLOG(ERROR) << "failed to receive message length from master";
      continue;
    } else if (real_read_num < LoadPacket::kHeadLength) {
      LOG(ERROR) << "received message error! only read " << real_read_num
                 << " bytes";
      continue;
    }
    uint64_t data_length =
        *reinterpret_cast<uint64_t*>(head_buffer + LoadPacket::kHeadLength -
                                     sizeof(uint64_t));
    uint64_t real_packet_length = data_length + LoadPacket::kHeadLength;
    LOG(INFO) << "real packet length is :" << real_packet_length
              << ". date length is " << data_length;
    assert(data_length >= 4 && data_length <= 10000000);

    char* data_buffer = Malloc(data_length);
    MemoryGuard<char> guard(data_buffer);  // auto-release
    if (NULL == data_buffer) {
      ELOG((ret = claims::common::rNoMemory),
           "no memory to hold data of message from master");
      return ret;
    }

    if (-1 == recv(master_fd_, data_buffer, data_length, MSG_WAITALL)) {
      PLOG(ERROR) << "failed to receive message from master";
      return claims::common::rReceiveMessageError;
    }
    //    LOG(INFO) << "data of message from master is:" << buffer;

    // deserialization of packet
    LoadPacket packet;
    packet.Deserialize(head_buffer, data_buffer);

    EXEC_AND_LOG(ret, StoreDataInMemory(packet), "stored data",
                 "failed to store");

    // return result to master loader
    EXEC_AND_LOG(ret, SendAckToMasterLoader(packet.txn_id_, rSuccess == ret),
                 "sent commit result to master loader",
                 "failed to send commit res to master loader");
  }
}

RetCode SlaveLoader::StoreDataInMemory(const LoadPacket& packet) {
  RetCode ret = rSuccess;
  const uint64_t table_id = GetTableIdFromGlobalPartId(packet.global_part_id_);
  const uint64_t prj_id =
      GetProjectionIdFromGlobalPartId(packet.global_part_id_);
  const uint64_t part_id =
      GetPartitionIdFromGlobalPartId(packet.global_part_id_);

  PartitionStorage* part_storage =
      BlockManager::getInstance()->getPartitionHandle(
          PartitionID(ProjectionID(table_id, prj_id), part_id));
  assert(part_storage != NULL);

  /// set HDFS because the memory is not applied actually
  /// it will be set to MEMORY in function
  uint64_t last_chunk_id = (packet.pos_ + packet.offset_) / CHUNK_SIZE;
  //  assert(last_chunk_id <=
  //             (1024UL * 1024 * 1024 * 1024 * 1024) / (64 * 1024 * 1024) &&
  //         " memory for chunk should not larger than 1PB");
  DLOG(INFO) << "position+offset is:" << packet.pos_ + packet.offset_
             << " CHUNK SIZE is:" << CHUNK_SIZE
             << " last chunk id is:" << last_chunk_id;
  EXEC_AND_LOG_RETURN(
      ret, part_storage->AddChunkWithMemoryToNum(last_chunk_id + 1, HDFS),
      "added chunk to " << last_chunk_id + 1, "failed to add chunk");

  /// copy data into applied memory
  const uint64_t tuple_size = Catalog::getInstance()
                                  ->getTable(table_id)
                                  ->getProjectoin(prj_id)
                                  ->getSchema()
                                  ->getTupleMaxSize();
  const uint64_t offset = packet.offset_;
  uint64_t cur_chunk_id = packet.pos_ / CHUNK_SIZE;
  uint64_t cur_block_id = (packet.pos_ % CHUNK_SIZE) / BLOCK_SIZE;
  uint64_t pos_in_block = packet.pos_ % BLOCK_SIZE;
  uint64_t total_written_length = 0;
  HdfsInMemoryChunk chunk_info;
  while (total_written_length < offset) {
    /// get start position of current chunk
    if (BlockManager::getInstance()->getMemoryChunkStore()->getChunk(
            ChunkID(PartitionID(ProjectionID(table_id, prj_id), part_id),
                    cur_chunk_id),
            chunk_info)) {
      InMemoryChunkWriterIterator writer(chunk_info.hook, CHUNK_SIZE,
                                         cur_block_id, BLOCK_SIZE, pos_in_block,
                                         tuple_size);
      do {  // write to every block
        uint64_t written_length =
            writer.Write(packet.data_buffer_ + total_written_length,
                         offset - total_written_length);
        total_written_length += written_length;
        LOG(INFO) << "written " << written_length
                  << " bytes into chunk:" << cur_chunk_id
                  << ". Now total written " << total_written_length << " bytes";
        if (total_written_length == offset) {
          // all tuple is written into memory
          return rSuccess;
        } else if (total_written_length > offset) {
          assert(false);
        }
      } while (writer.NextBlock());

      ++cur_chunk_id;  // get next chunk to write
      LOG(INFO) << "Now chunk id is " << cur_chunk_id
                << ", the number of chunk is" << part_storage->GetChunkNum();
      assert(cur_chunk_id < part_storage->GetChunkNum());
      cur_block_id = 0;  // the block id of next chunk is 0
      pos_in_block = 0;

    } else {
      cout << "chunk id is " << cur_chunk_id << endl;
      assert(false && "no chunk with this chunk id");
    }
  }
  return ret;
}

RetCode SlaveLoader::SendAckToMasterLoader(const uint64_t& txn_id,
                                           bool is_commited) {
  int time = 0;
  int retry_max_time = 10;
  while (1) {
    try {
      auto master_actor =
          remote_actor(Config::master_loader_ip, Config::master_loader_port);
      caf::scoped_actor self;
      self->sync_send(master_actor, LoadAckAtom::value, txn_id, is_commited)
          .await([&](int r) {  // NOLINT
            LOG(INFO) << "sent commit result:" << is_commited
                      << " to master and received response";
          });
      return rSuccess;
    } catch (exception& e) {
      LOG(ERROR) << "failed to send commit result to master loader in "
                 << ++time << "time." << e.what();
      if (time >= retry_max_time) return rFailure;
    }
  }
  return rSuccess;
}

void* SlaveLoader::StartSlaveLoader(void* arg) {
  Config::getInstance();
  LOG(INFO) << "start slave loader...";

  SlaveLoader* slave_loader = Environment::getInstance()->get_slave_loader();
  int ret = rSuccess;
  EXEC_AND_LOG(ret, slave_loader->ConnectWithMaster(),
               "succeed to connect with master",
               "failed to connect with master ");

  assert(rSuccess == ret && "can't connect with master");

  cout << "connected with master loader" << endl;
  // TODO(YK): error handle

  slave_loader->ReceiveAndWorkLoop();
  assert(false);
  return NULL;
}

} /* namespace loader */
} /* namespace claims */
