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
 * /Claims/loader/load_packet.h
 *
 *  Created on: Apr 17, 2016
 *      Author: yukai
 *		   Email: yukai2014@gmail.com
 *
 * Description:
 *
 */

#ifndef LOADER_LOAD_PACKET_H_
#define LOADER_LOAD_PACKET_H_
#include "../common/error_define.h"
#include "../txn_manager/txn.hpp"

namespace claims {
namespace loader {

using IpPortAtom = caf::atom_constant<caf::atom("ip_port")>;
using LoadAckAtom = caf::atom_constant<caf::atom("load_ack")>;
using RegNodeAtom = caf::atom_constant<caf::atom("reg_node")>;
using BindPartAtom = caf::atom_constant<caf::atom("bind_part")>;
using OkAtom = caf::atom_constant<caf::atom("ok")>;

/**************  LoadPacket format  *****************/
/** field             type          length **********/
/****************************************************/
/** transaction_id    uint64_t      4              **/
/** global_part_id    uint64_t      4              **/
/** position          uint64_t      4              **/
/** offset            uint64_t      4              **/
/** date_length       uint64_t      4              **/
/** data              void*         data_length    **/
/****************************************************/
struct LoadPacket {
 public:
  LoadPacket() {}
  LoadPacket(const uint64_t txn_id, const uint64_t g_part_id, uint64_t pos,
             uint64_t offset, uint64_t data_length, const void* data_buffer)
      : txn_id_(txn_id),
        global_part_id_(g_part_id),
        pos_(pos),
        offset_(offset),
        data_buffer_(data_buffer),
        data_length_(data_length) {}
  ~LoadPacket();
  RetCode Serialize(void*& packet_buffer, uint64_t& packet_length) const;

  RetCode Deserialize(const void* const head_buffer, void* data_buffer);

 public:
  static const int kHeadLength = 5 * sizeof(uint64_t);

 public:
  uint64_t txn_id_;
  uint64_t global_part_id_;
  uint64_t pos_;
  uint64_t offset_;
  uint64_t data_length_;
  void* data_buffer_;
};

} /* namespace loader */
} /* namespace claims */

#endif  // LOADER_LOAD_PACKET_H_
