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
 * /log/log.cpp
 *
 *  Created on: 2016年2月24日
 *      Author: imdb
 *		   Email: 
 * 
 * Description:
 *
 */
#include "txn_log.hpp"

namespace claims{
namespace txn{


string LogServer::log_path = ".";
FILE * LogServer::log_handler = nullptr;
UInt64 LogServer::log_size = 0;
UInt64 LogServer::max_log_size = kMaxLogSize;
char * LogServer::buffer = nullptr;
UInt64 LogServer::buffer_size  = 0;
UInt64 LogServer::max_buffer_size = kMaxLogSize * 10;
caf::actor LogServer::log_server;
bool LogServer::is_active = false;
caf::actor log_s;
RetCode LogServer::init(const string path) {
  cout << "log server init" << endl;
  log_s = caf::spawn<LogServer>();
  log_path = path;
  buffer = (char*)malloc(max_buffer_size);
  if (buffer == nullptr) return -1;
  is_active = true;
  return 0;
}

caf::behavior LogServer::make_behavior() {

  return {
    [=](BeginAtom, UInt64 id)->RetCode {
       // return Append(BeginLog(id));
      cout << "begin" << endl;
      return 0;
      },
    [=](WriteAtom,UInt64 id, UInt64 part, UInt64 pos,
        UInt64 offset)->RetCode {
       // return Append(WriteLog(id, part, pos, offset));
        cout << "write" << endl;
        return 0;
      },
    [=](CommitAtom, UInt64 id)->RetCode {
        //return Append(CommitLog(id));
        cout << "commit" << endl;
        return 0;
      },
    [=](AbortAtom, UInt64 id)->RetCode {
        return Append(AbortLog(id));
      },
    [=](CheckpointAtom, UInt64 part, UInt64 logic_cp, UInt64 phy_cp)
            ->RetCode {
        return Append(CheckpointLog(part, logic_cp, phy_cp));
      },
    [=](DataAtom,UInt64 part, UInt64 pos, UInt64 offset,
        void * buffer, UInt64 size)->RetCode {
        Append(DataLogPrefix(part, pos, offset, size));
        Append(buffer, size);
        return 0;
      },
    [=](RefreshAtom)->RetCode {
       //return Refresh();
        cout << "refresh" << endl;
        return 0;
      },
    caf::others >> [=] () { cout << "unknown log message" << endl; }
  };
}

RetCode LogServer::Append (const string & log) {
  if (buffer_size + log.length() >= max_buffer_size) {
    cout << "append fail" << endl;
    return -1;
  }
  memcpy(buffer + buffer_size, log.c_str(), log.length());
  buffer_size += log.length();
  log_size += log.length();
  return 0;
}

RetCode LogServer::Append(void * data, UInt64 size){
 if (buffer_size + size >= max_buffer_size)
   return -1;

 memcpy(buffer + buffer_size, data, size);
 buffer_size += size;
 buffer[buffer_size++] = '\n';
 log_size += size + 1;

 return 0;
}

RetCode LogServer::Refresh() {
  if (log_handler == nullptr) {
     struct timeval ts;
     gettimeofday (&ts, NULL);
     string file = log_path + "/" + kTxnLogFileName + to_string(ts.tv_sec);
     log_handler = fopen (file.c_str(),"a");
     if (log_handler == nullptr) return -1;
  }

  if (buffer_size == 0)
    return 0;
  //cout << buffer_size << endl;
  fwrite(buffer, sizeof(char), buffer_size, log_handler);
  fflush(log_handler);
  buffer_size = 0;


  /* 日志文件已满 */
  if(log_size >= max_log_size) {
    if (log_handler == nullptr) return -1;
    fclose(log_handler);
    log_handler = nullptr;
    log_size = 0;
  }
  return 0;
}

RetCode LogClient::Begin(UInt64 id) {
  RetCode ret = 0;
  caf::scoped_actor self;
  cout<<"going to send begin atom to log server :"<<LogServer::log_server.id()<<endl;
  self->sync_send( log_s,BeginAtom::value, id).
      await( [&](RetCode ret_code) { cout<<"log:Begin, ret"<<ret_code<<endl;ret = ret_code;});
  return ret;
}
RetCode LogClient::Write(UInt64 id, UInt64 part, UInt64 pos, UInt64 offset) {
  RetCode ret = 0;
  caf::scoped_actor self;
  self->sync_send(LogServer::log_server,
                  WriteAtom::value, id, part, pos, offset).await(
                      [&](RetCode ret_code) { ret = ret_code;}
                        );
  return ret;
}
RetCode LogClient::Commit(UInt64 id) {
  RetCode ret = 0;
  caf::scoped_actor self;
  self->sync_send( LogServer::log_server,
                  CommitAtom::value,id).await(
                      [&](RetCode ret_code) { ret = ret_code;}
                        );
  return ret;
}
RetCode LogClient::Abort(UInt64 id) {
  RetCode ret = 0;
  caf::scoped_actor self;
  self->sync_send( LogServer::log_server,
                  AbortAtom::value, id).await(
                      [&](RetCode ret_code) { ret = ret_code;}
                        );
  return ret;
}
RetCode LogClient::Data(UInt64 part, UInt64 pos, UInt64 offset, void * buffer, UInt64 size) {
  RetCode ret = 0;
  caf::scoped_actor self;
  self->sync_send( LogServer::log_server,
                  DataAtom::value, part, pos, offset, buffer, size).await(
                      [&](RetCode ret_code) { ret = ret_code;}
                      );
  return ret;
}
RetCode LogClient::Checkpoint(UInt64 part, UInt64 logic_cp, UInt64 phy_cp) {
  RetCode ret = 0;
  caf::scoped_actor self;
  self->sync_send(LogServer::log_server,
                  CheckpointAtom::value, part, logic_cp, phy_cp).await(
                      [&](RetCode ret_code) { ret = ret_code;}
                      );
  return ret;
}

RetCode LogClient::Refresh() {
  RetCode ret = 0;
  caf::scoped_actor self;
  self->sync_send(LogServer::log_server, RefreshAtom::value).
      await( [&](RetCode ret_code) { ret = ret_code;});
  return ret;
}


}
}
