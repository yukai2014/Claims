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
 * /CLAIMS/storage/PartitionStorage.cpp
 *
 *  Created on: NOV 14 ,2013
 *  Modified on: NOV 28, 2015
 *      Author: Hanzhang, wangli
 *       Email:
 *
 * Description:
 *
 */

#include "PartitionStorage.h"

#include <assert.h>
#include <vector>

#include "../common/error_define.h"
#include "../Debug.h"
#include "./MemoryManager.h"
#include "../common/memory_handle.h"
#include "../Config.h"
#include "../Resource/BufferManager.h"
#include "../utility/lock_guard.h"

using claims::common::rSuccess;
using claims::utility::LockGuard;

/**
 * According to number_of_chunks, construct chunk from partition and add into
 * the chunk_list_. Meantime, you can get specific information about chunk. when
 * number_of_chunks more than storage_max_bugdege, you should choose the random
 * way to remove the chunk which in the memory. or not, choose LRU.
 */
PartitionStorage::PartitionStorage(const PartitionID& partition_id,
                                   const unsigned& number_of_chunks,

                                   const StorageLevel& storage_level)
    : partition_id_(partition_id),
      number_of_chunks_(number_of_chunks),
      desirable_storage_level_(storage_level) {
  if (number_of_chunks_ * CHUNK_SIZE / 1024 / 1024 >
      BufferManager::getInstance()->getStorageMemoryBudegeInMilibyte() *
          Config::memory_utilization / 100)
    MemoryChunkStore::GetInstance()->SetFreeAlgorithm(0);
  else
    MemoryChunkStore::GetInstance()->SetFreeAlgorithm(1);
  for (unsigned i = 0; i < number_of_chunks_; i++) {
    chunk_list_.push_back(new ChunkStorage(
        ChunkID(partition_id_, i), BLOCK_SIZE, desirable_storage_level_));
  }
}

PartitionStorage::~PartitionStorage() {
  for (unsigned i = 0; i < chunk_list_.size(); i++) {
    DELETE_PTR(chunk_list_[i]);
  }
  chunk_list_.clear();
}

void PartitionStorage::AddNewChunk() { number_of_chunks_++; }

RetCode PartitionStorage::AddChunkWithMemoryToNum(
    unsigned expected_number_of_chunks, const StorageLevel& storage_level) {
  RetCode ret = rSuccess;
  if (number_of_chunks_ >= expected_number_of_chunks) return ret;
  DLOG(INFO) << "now chunk number:" << number_of_chunks_
             << ". expected chunk num:" << expected_number_of_chunks;

  LockGuard<Lock> guard(write_lock_);
  if (number_of_chunks_ >= expected_number_of_chunks) return ret;

  for (unsigned i = number_of_chunks_; i < expected_number_of_chunks; i++) {
    ChunkStorage* chunk =
        new ChunkStorage(ChunkID(partition_id_, i), BLOCK_SIZE, storage_level);
    EXEC_AND_DLOG(ret, chunk->ApplyMemory(), "applied memory for chunk("
                                                 << partition_id_.getName()
                                                 << "," << i << ")",
                  "failed to apply memory for chunk(" << partition_id_.getName()
                                                      << "," << i << ")");
    chunk_list_.push_back(chunk);
  }
  number_of_chunks_ = expected_number_of_chunks;
  assert(chunk_list_.size() == number_of_chunks_);

  return ret;
}

/**
 * when appending data, the last chunk may be dirty so set storage level as HDFS
 * to make sure the data will be reload from HDFS. actually, DISK is not used,
 * only HDFS and MEMORY is used.
 */
void PartitionStorage::UpdateChunksWithInsertOrAppend(
    const PartitionID& partition_id, const unsigned& number_of_chunks,
    const StorageLevel& storage_level) {
  LockGuard<Lock> guard(write_lock_);
  if (!chunk_list_.empty()) {
    MemoryChunkStore::GetInstance()->ReturnChunk(
        chunk_list_.back()->GetChunkID());
    chunk_list_.back()->SetCurrentStorageLevel(HDFS);
  }
  for (unsigned i = number_of_chunks_; i < number_of_chunks; i++)
    chunk_list_.push_back(
        new ChunkStorage(ChunkID(partition_id, i), BLOCK_SIZE, storage_level));
  number_of_chunks_ = number_of_chunks;
}

/**
 * By searching in chunk_list_ to get chunk address(physical information), and
 * free the memory. This function is a logical process of delete the chunk, and
 * call back actual method.
 */
void PartitionStorage::RemoveAllChunks(const PartitionID& partition_id) {
  if (!chunk_list_.empty()) {
    vector<ChunkStorage*>::iterator iter = chunk_list_.begin();
    MemoryChunkStore* mcs = MemoryChunkStore::GetInstance();
    for (; iter != chunk_list_.end(); iter++) {
      mcs->ReturnChunk((*iter)->GetChunkID());
    }
    chunk_list_.clear();
    number_of_chunks_ = 0;
  }
}

PartitionStorage::PartitionReaderIterator*
PartitionStorage::CreateReaderIterator() {
  return new PartitionReaderIterator(this);
}
PartitionStorage::PartitionReaderIterator*
PartitionStorage::CreateAtomicReaderIterator() {
  return new AtomicPartitionReaderIterator(this);
}

PartitionStorage::PartitionReaderIterator::PartitionReaderIterator(
    PartitionStorage* partition_storage)
    : ps_(partition_storage), chunk_cur_(0), chunk_it_(NULL) {}

PartitionStorage::PartitionReaderIterator::~PartitionReaderIterator() {}

ChunkReaderIterator* PartitionStorage::PartitionReaderIterator::NextChunk() {
  //  LockGuard<Lock> guard(ps_->write_lock_);
  if (chunk_cur_ < ps_->number_of_chunks_)
    return ps_->chunk_list_[chunk_cur_++]->CreateChunkReaderIterator();
  else
    return NULL;
}

PartitionStorage::AtomicPartitionReaderIterator::
    ~AtomicPartitionReaderIterator() {}

ChunkReaderIterator*
PartitionStorage::AtomicPartitionReaderIterator::NextChunk() {
  LockGuard<Lock> guard(ps_->write_lock_);
  if (chunk_cur_ < ps_->number_of_chunks_)
    return ps_->chunk_list_[chunk_cur_++]->CreateChunkReaderIterator();
  else
    return NULL;
}

bool PartitionStorage::PartitionReaderIterator::NextBlock(
    BlockStreamBase*& block) {
  assert(false);
  if (chunk_it_ > 0 && chunk_it_->NextBlock(block)) {
    return true;
  } else {
    if ((chunk_it_ = NextChunk()) > 0) {
      return NextBlock(block);
    } else {
      return false;
    }
  }
}

bool PartitionStorage::AtomicPartitionReaderIterator::NextBlock(
    BlockStreamBase*& block) {
  lock_.acquire();
  ChunkReaderIterator::block_accessor* ba = NULL;
  if (NULL != chunk_it_ && chunk_it_->GetNextBlockAccessor(ba)) {
    lock_.release();
    ba->GetBlock(block);
    if (NULL != ba) {
      delete ba;
      ba = NULL;
    }
    return true;
  } else {
    if (NULL != chunk_it_) {
      delete chunk_it_;
      chunk_it_ = NULL;
    }
    if ((chunk_it_ = NextChunk()) > 0) {
      lock_.release();
      return NextBlock(block);
    } else {
      lock_.release();
      return false;
    }
  }
}
