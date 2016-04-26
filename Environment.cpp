/*
 * Environment.cpp
 *
 *  Created on: Aug 10, 2013
 *      Author: wangli
 */

#include "Environment.h"

#include <assert.h>
#include "caf/all.hpp"

#include "txn_manager/txn_server.hpp"
#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#undef GLOG_NO_ABBREVIATED_SEVERITIES
#include <libconfig.h++>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>  // NOLINT
#include "loader/master_loader.h"
#include "loader/slave_loader.h"
#include "./Debug.h"
#include "./Config.h"
#include "common/Logging.h"
#include "common/TypePromotionMap.h"
#include "common/TypeCast.h"
#include "common/error_define.h"
#include "codegen/CodeGenerator.h"
#include "common/expression/data_type_oper.h"
#include "common/expression/expr_type_cast.h"
#include "common/expression/type_conversion_matrix.h"
// #define DEBUG_MODE
#include "catalog/catalog.h"

using claims::common::InitAggAvgDivide;
using claims::common::InitOperatorFunc;
using claims::common::InitTypeCastFunc;
using claims::common::InitTypeConversionMatrix;
using claims::common::rSuccess;
using claims::loader::MasterLoader;
using claims::loader::SlaveLoader;
using claims::txn::TxnServer;

Environment* Environment::_instance = 0;

Environment::Environment(bool ismaster) : ismaster_(ismaster) {
  _instance = this;
  Config::getInstance();
  CodeGenerator::getInstance();
  logging_ = new EnvironmentLogging();
  readConfigFile();
  initializeExpressionSystem();
  portManager = PortManager::getInstance();

  if (ismaster) {
    logging_->log("Initializing the Coordinator...");
    initializeCoordinator();
    logging_->log("Initializing the catalog ...");
  }
  catalog_ = claims::catalog::Catalog::getInstance();
  logging_->log("restore the catalog ...");
  if (rSuccess != catalog_->restoreCatalog()) {
    LOG(ERROR) << "failed to restore catalog" << std::endl;
    cerr << "ERROR: restore catalog failed" << endl;
  }

  if (true == g_thread_pool_used) {
    logging_->log("Initializing the ThreadPool...");
    if (false == initializeThreadPool()) {
      logging_->elog("initialize ThreadPool failed");
      assert(false && "can't initialize thread pool");
    }
  }
  logging_->log("Initializing the AdaptiveEndPoint...");
  initializeEndPoint();
  /**
   * TODO:
   * DO something in AdaptiveEndPoint such that the construction function does
          not return until the connection is completed. If so, the following
   sleep()
          dose not needed.

          This is done in Aug.18 by Li :)
   */

  /*Before initializing Resource Manager, the instance ip and port should be
   * decided.*/

  logging_->log("Initializing the ResourceManager...");
  initializeResourceManager();

  logging_->log("Initializing the Storage...");
  initializeStorage();

  logging_->log("Initializing the BufferManager...");
  initializeBufferManager();

  logging_->log("Initializing the loader...");
  if (!InitLoader()) {
    LOG(ERROR) << "failed to initialize loader";
  }

  logging_->log("Initializing txn manager");
  if (!InitTxnManager()) LOG(ERROR) << "failed to initialize txn manager";

  logging_->log("Initializing txn log server");
  if (!InitTxnLog()) LOG(ERROR) << "failed to initialize txn log";

  logging_->log("Initializing the ExecutorMaster...");
  iteratorExecutorMaster = new IteratorExecutorMaster();

  logging_->log("Initializing the ExecutorSlave...");
  iteratorExecutorSlave = new IteratorExecutorSlave();

  exchangeTracker = new ExchangeTracker();
  expander_tracker_ = ExpanderTracker::getInstance();
#ifndef DEBUG_MODE
  if (ismaster) {
    initializeClientListener();
  }
#endif
}

Environment::~Environment() {
  _instance = 0;
  delete expander_tracker_;
  delete logging_;
  delete portManager;
  delete catalog_;
  delete coordinator;
  if (ismaster_) {
    delete iteratorExecutorMaster;
    delete resourceManagerMaster_;
    delete blockManagerMaster_;
#ifndef DEBUG_MODE
    destoryClientListener();
#endif
  }
  delete iteratorExecutorSlave;
  delete exchangeTracker;
  delete resourceManagerSlave_;
  delete blockManager_;
  delete bufferManager_;
  delete endpoint;
}
Environment* Environment::getInstance(bool ismaster) {
  if (_instance == 0) {
    new Environment(ismaster);
  }
  return _instance;
}
std::string Environment::getIp() { return ip; }
unsigned Environment::getPort() { return port; }

ThreadPool* Environment::getThreadPool() const { return thread_pool_; }

void Environment::readConfigFile() {
  libconfig::Config cfg;
  cfg.readFile(Config::config_file.c_str());
  ip = (const char*)cfg.lookup("ip");
}
void Environment::initializeEndPoint() {
  //  libconfig::Config cfg;
  //  cfg.readFile("/home/claims/config/wangli/config");
  //  std::string endpoint_ip=(const char*)cfg.lookup("ip");
  //  std::string endpoint_port=(const char*)cfg.lookup("port");
  std::string endpoint_ip = ip;
  int endpoint_port;
  if ((endpoint_port = portManager->applyPort()) == -1) {
    logging_->elog("The ports in the PortManager is exhausted!");
  }
  port = endpoint_port;
  logging_->log("Initializing the AdaptiveEndPoint as EndPoint://%s:%d.",
                endpoint_ip.c_str(), endpoint_port);
  std::ostringstream name, port_str;
  port_str << endpoint_port;
  name << "EndPoint://" << endpoint_ip << ":" << endpoint_port;

  endpoint =
      new AdaptiveEndPoint(name.str().c_str(), endpoint_ip, port_str.str());
}
void Environment::initializeCoordinator() { coordinator = new Coordinator(); }
void Environment::initializeStorage() {
  if (ismaster_) {
    blockManagerMaster_ = BlockManagerMaster::getInstance();
    blockManagerMaster_->initialize();
  }
  /*both master and slave node run the BlockManager.*/
  //    BlockManagerId *bmid=new BlockManagerId();
  //    string
  // actorname="blockManagerWorkerActor_"+bmid->blockManagerId;
  //    cout<<actorname.c_str()<<endl;
  //    BlockManager::BlockManagerWorkerActor
  //*blockManagerWorkerActor=new
  // BlockManager::BlockManagerWorkerActor(endpoint,framework_storage,actorname.c_str());

  blockManager_ = BlockManager::getInstance();
  blockManager_->initialize();
}

void Environment::initializeResourceManager() {
  if (ismaster_) {
    resourceManagerMaster_ = new ResourceManagerMaster();
    DLOG(INFO) << "ResourceManagerMaster instanced ";
  }
  resourceManagerSlave_ = new InstanceResourceManager();
  DLOG(INFO) << "resourceManagerSlave instanced ";

  nodeid = resourceManagerSlave_->Register();
}

bool Environment::InitLoader() {
  if (Config::is_master_loader) {
    LOG(INFO) << "I'm master loader. Oyeah";
    master_loader_ = new MasterLoader();
    std::thread master_thread(&MasterLoader::StartMasterLoader, nullptr);
    master_thread.detach();
    DLOG(INFO) << "started thread as master loader";

    TxnServer::Init(6);
  }

  usleep(10000);
  DLOG(INFO) << "starting create thread as slave loader";
  slave_loader_ = new SlaveLoader();
  std::thread slave_thread(&SlaveLoader::StartSlaveLoader, nullptr);
  slave_thread.detach();

  //  caf::await_all_actors_done();
  return true;
}

bool Environment::InitTxnManager() {
  if (Config::enable_txn_server) {
    LOG(INFO) << "I'm txn manager server";
    TxnServer::Init(Config::txn_server_cores, Config::txn_server_port);
  }
  TxnClient::Init(Config::txn_server_ip, Config::txn_server_port);
  return true;
}

bool Environment::InitTxnLog() {
  if (Config::enable_txn_log) {
    LOG(INFO) << "I'm txn log server";
    LogServer::init(Config::txn_log_path);
  }
  return true;
}

void Environment::initializeBufferManager() {
  bufferManager_ = BufferManager::getInstance();
}

void Environment::initializeIndexManager() {
  indexManager_ = IndexManager::getInstance();
}

AdaptiveEndPoint* Environment::getEndPoint() { return endpoint; }
ExchangeTracker* Environment::getExchangeTracker() { return exchangeTracker; }
ResourceManagerMaster* Environment::getResourceManagerMaster() {
  return resourceManagerMaster_;
}
InstanceResourceManager* Environment::getResourceManagerSlave() {
  return resourceManagerSlave_;
}
NodeID Environment::getNodeID() const { return nodeid; }
claims::catalog::Catalog* Environment::getCatalog() const { return catalog_; }

void Environment::initializeClientListener() {
  listener_ = new ClientListener(Config::client_listener_port);
  listener_->configure();
}

void Environment::initializeExpressionSystem() {
  InitTypeConversionMatrix();
  InitOperatorFunc();
  InitAggAvgDivide();
  InitTypeCastFunc();
}

void Environment::destoryClientListener() {
  listener_->shutdown();
  delete listener_;
}

bool Environment::initializeThreadPool() {
  thread_pool_ = new ThreadPool();
  return thread_pool_->Init(Config::thread_pool_init_thread_num);
}

IteratorExecutorSlave* Environment::getIteratorExecutorSlave() const {
  return iteratorExecutorSlave;
}
