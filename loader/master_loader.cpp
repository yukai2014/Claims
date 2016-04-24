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
#include "unistd.h"
#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include <functional>
#include <vector>
#include "../common/error_define.h"
#include "../catalog/catalog.h"
#include "../Config.h"
#include "../Environment.h"
#include "./loader_message.h"
using caf::aout;
using caf::behavior;
using caf::event_based_actor;
using caf::io::publish;
using caf::io::remote_actor;
using caf::mixin::sync_sender_impl;
using caf::spawn;
using std::endl;
using claims::catalog::Catalog;
using claims::common::rSuccess;
using claims::common::rFailure;

namespace claims {
namespace loader {

MasterLoader::MasterLoader()
    : master_loader_ip(Config::master_loader_ip),
      master_loader_port(Config::master_loader_port) {
  // TODO Auto-generated constructor stub
}

MasterLoader::~MasterLoader() {
  // TODO Auto-generated destructor stub
}

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
        mloader->slave_addrs_.push_back(NetAddr(ip, port));
        mloader->slave_sockets_.push_back(new_slave_fd);
        assert(mloader->slave_sockets_.size() == mloader->slave_addrs_.size());
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

  IngestionRequest req;
  EXEC_AND_LOG(ret, GetRequestFromMessage(message, &req), "got request!",
               "failed to get request");

  //  CheckAndToValue();

  return ret;
}

string MasterLoader::GetMessage() {}

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

RetCode MasterLoader::GetRequestFromMessage(const string& message,
                                            IngestionRequest* req) {
}

void* MasterLoader::StartMasterLoader(void* arg) {
  Config::getInstance();
  LOG(INFO) << "start master loader...";

  int ret = rSuccess;
  MasterLoader* master_loader = Environment::getInstance()->get_master_loader();
  EXEC_AND_ONLY_LOG_ERROR(ret, master_loader->ConnectWithSlaves(),
                          "failed to connect all slaves");

  while(true)
    sleep(10);
//  EXEC_AND_ONLY_LOG_ERROR(ret, master_loader->Ingest(),
//                          "failed to ingest data");

  return NULL;
}

} /* namespace loader */
} /* namespace claims */
