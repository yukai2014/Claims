/*
 * ResourceManagerSlave.cpp
 *
 *  Created on: Oct 31, 2013
 *      Author: wangli
 */

#include "ResourceManagerSlave.h"

#include <string>
#include <exception>

#include "../Config.h"
#include "../Environment.h"
#include "../common/ids.h"
#include "../common/TimeOutReceiver.h"
#include "../loader/load_packet.h"
#include "caf/io/all.hpp"
#include "caf/all.hpp"

using caf::io::remote_actor;
using claims::loader::RegNodeAtom;
#define ResourceManagerMasterName "ResourceManagerMaster"
InstanceResourceManager::InstanceResourceManager() {
  framework_ =
      new Theron::Framework(*Environment::getInstance()->getEndPoint());
  logging_ = new ResourceManagerMasterLogging();
}

InstanceResourceManager::~InstanceResourceManager() {
  delete framework_;
  delete logging_;
}
NodeID InstanceResourceManager::Register() {
  NodeID ret = 10;
  TimeOutReceiver receiver(Environment::getInstance()->getEndPoint());
  Theron::Catcher<NodeID> resultCatcher;
  receiver.RegisterHandler(&resultCatcher, &Theron::Catcher<NodeID>::Push);

  std::string ip = Environment::getInstance()->getIp();
  unsigned port = Environment::getInstance()->getPort();
  NodeRegisterMessage message(ip, port);

  DLOG(INFO) << "resourceManagerSlave is going to register (" << ip << ","
             << port << ")to master";
  framework_->Send(message, receiver.GetAddress(),
                   Theron::Address("ResourceManagerMaster"));
  Theron::Address from;
  if (receiver.TimeOutWait(1, 1000) == 1) {
    resultCatcher.Pop(ret, from);
    logging_->log(
        "Successfully registered to the master, the allocated id =%d.", ret);

    // send all node info to master loader
    DLOG(INFO) << "going to send node info to (" << Config::master_loader_ip
               << ":" << Config::master_loader_port << ")";

    int retry_max_time = 10;
    int time = 0;
    while (1) {
      try {
        caf::actor master_actor =
            remote_actor(Config::master_loader_ip, Config::master_loader_port);
        caf::scoped_actor self;
        self->sync_send(master_actor, RegNodeAtom::value,
                        NodeAddress(ip, to_string(port)),
                        ret).await([&](int r) {
          LOG(INFO) << "sent node info and received response";
        });
        break;
      } catch (exception& e) {
        cout << "new remote actor " << Config::master_loader_ip << ","
             << Config::master_loader_port << "failed for " << ++time
             << " time. " << e.what() << endl;
        usleep(100 * 1000);
        if (time >= retry_max_time) return false;
      }
    }

    return ret;
  } else {
    logging_->elog("Failed to get NodeId from the master.");
    return -1;
  }
}
void InstanceResourceManager::ReportStorageBudget(
    StorageBudgetMessage& message) {
  framework_->Send(message, Theron::Address(),
                   Theron::Address(ResourceManagerMasterName));
}

void InstanceResourceManager::setStorageBudget(uint64_t memory, uint64_t disk) {
}
