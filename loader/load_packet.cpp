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
 * /Claims/loader/load_packet.cpp
 *
 *  Created on: Apr 17, 2016
 *      Author: yukai
 *		   Email: yukai2014@gmail.com
 *
 * Description:
 *
 */

#include "./load_packet.h"

#include <glog/logging.h>
#include "../common/memory_handle.h"

using namespace claims::common;  // NOLINT

namespace claims {
namespace loader {

LoadPacket::~LoadPacket() {}

RetCode LoadPacket::Serialize(void*& packet_buffer,
                              uint64_t& packet_length) const {
  packet_length = kHeadLength + data_length_;
  packet_buffer = Malloc(packet_length);
  if (NULL == packet_length) {
    ELOG(rNoMemory, "no memory for packet buffer");
    return rNoMemory;
  }

  *reinterpret_cast<uint64_t*>(packet_buffer) = txn_id_;
  *reinterpret_cast<uint64_t*>(packet_buffer + 1 * sizeof(uint64_t)) =
      global_part_id_;
  *reinterpret_cast<uint64_t*>(packet_buffer + 2 * sizeof(uint64_t)) = pos_;
  *reinterpret_cast<uint64_t*>(packet_buffer + 3 * sizeof(uint64_t)) = offset_;
  *reinterpret_cast<uint64_t*>(packet_buffer + 4 * sizeof(uint64_t)) =
      data_length_;
  DLOG(INFO) << "Serialize packet: " << txn_id_ << " " << global_part_id_ << " "
             << pos_ << " " << offset_ << " " << data_length_;

  memcpy(packet_buffer + kHeadLength, data_buffer_, data_length_);
  return rSuccess;
}

RetCode LoadPacket::Deserialize(const void* const head_buffer,
                                void* data_buffer) {
  txn_id_ = *reinterpret_cast<const uint64_t*>(head_buffer);
  global_part_id_ =
      *reinterpret_cast<const uint64_t*>(head_buffer + sizeof(uint64_t));
  pos_ = *reinterpret_cast<const uint64_t*>(head_buffer + 2 * sizeof(uint64_t));
  offset_ =
      *reinterpret_cast<const uint64_t*>(head_buffer + 3 * sizeof(uint64_t));
  data_length_ =
      *reinterpret_cast<const uint64_t*>(head_buffer + 4 * sizeof(uint64_t));
  DLOG(INFO) << "Deserialize packet: " << txn_id_ << " " << global_part_id_
             << " " << pos_ << " " << offset_ << " " << data_length_;
  data_buffer_ = data_buffer;
  return rSuccess;
}

} /* namespace loader */
} /* namespace claims */
