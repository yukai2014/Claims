/*
 * ProjectionStorage.cpp
 *
 *  Created on: Nov 14, 2013
 *      Author: wangli
 */

#include "PartitionStorage.h"
#include "../Debug.h"
#include "MemoryStore.h"
#include "../Config.h"
#include "../utility/CpuScheduler.h"
#include "BlockManager.h"
PartitionStorage::PartitionStorage(const PartitionID &partition_id,const unsigned &number_of_chunks,const StorageLevel& storage_level)
:partition_id_(partition_id),number_of_chunks_(number_of_chunks),desirable_storage_level_(storage_level),load_in_memory(false){
	for(unsigned i=0;i<number_of_chunks_;i++){
		chunk_list_.push_back(new ChunkStorage(ChunkID(partition_id_,i),BLOCK_SIZE,desirable_storage_level_));
	}
}

PartitionStorage::~PartitionStorage() {
	for(unsigned i=0;i<chunk_list_.size();i++){
		chunk_list_[i]->~ChunkStorage();
	}
	chunk_list_.clear();
}

void PartitionStorage::addNewChunk(){
	number_of_chunks_++;
}

void PartitionStorage::updateChunksWithInsertOrAppend(const PartitionID &partition_id, const unsigned &number_of_chunks, const StorageLevel& storage_level)
{
	if(!chunk_list_.empty()){
		MemoryChunkStore::getInstance()->returnChunk(chunk_list_.back()->getChunkID());
//		if (Config::local_disk_mode == 0)
			chunk_list_.back()->setCurrentStorageLevel(HDFS);
//		else
//			chunk_list_.back()->setCurrentStorageLevel(DISK);
	}
	for (unsigned i = number_of_chunks_; i < number_of_chunks; i++)
		chunk_list_.push_back(new ChunkStorage(ChunkID(partition_id, i), BLOCK_SIZE, storage_level));
	number_of_chunks_ = number_of_chunks;
}

void PartitionStorage::removeAllChunks(const PartitionID &partition_id)
{
	if (!chunk_list_.empty())
	{
		vector<ChunkStorage*>::iterator iter = chunk_list_.begin();
		MemoryChunkStore* mcs = MemoryChunkStore::getInstance();
		for (; iter != chunk_list_.end(); iter++)
		{
			mcs->returnChunk((*iter)->getChunkID());
		}
		chunk_list_.clear();
		number_of_chunks_ = 0;
	}
	setInMemory(false);
}

PartitionStorage::PartitionReaderItetaor* PartitionStorage::createReaderIterator(){
	return new PartitionReaderItetaor(this);
}
PartitionStorage::PartitionReaderItetaor* PartitionStorage::createAtomicReaderIterator(){
	return new AtomicPartitionReaderIterator(this);
}

PartitionStorage::PartitionReaderItetaor* PartitionStorage::createNumaSensitiveReaderIterator() {
	return new NumaSensitivePartitionReaderIterator(this);
}


PartitionStorage::PartitionReaderItetaor::PartitionReaderItetaor(PartitionStorage* partition_storage)
:ps(partition_storage),chunk_cur_(0),chunk_it_(0){

}

//PartitionStorage::PartitionReaderItetaor::PartitionReaderItetaor():chunk_cur_(0){
//
//}
PartitionStorage::PartitionReaderItetaor::~PartitionReaderItetaor(){

}
ChunkReaderIterator* PartitionStorage::PartitionReaderItetaor::nextChunk(){
	if(chunk_cur_<ps->number_of_chunks_)
		return ps->chunk_list_[chunk_cur_++]->createChunkReaderIterator();
	else
		return 0;
}
//PartitionStorage::AtomicPartitionReaderIterator::AtomicPartitionReaderIterator():PartitionReaderItetaor(){
//
//}
PartitionStorage::AtomicPartitionReaderIterator::~AtomicPartitionReaderIterator(){

}
ChunkReaderIterator* PartitionStorage::AtomicPartitionReaderIterator::nextChunk(){
	lock_.acquire();
	ChunkReaderIterator* ret;
	if(chunk_cur_<ps->number_of_chunks_)
		ret= ps->chunk_list_[chunk_cur_++]->createChunkReaderIterator();
	else
		ret= 0;
	lock_.release();
	return ret;
}

bool PartitionStorage::PartitionReaderItetaor::nextBlock(
		BlockStreamBase*& block) {
	assert(false);
	if(chunk_it_>0&&chunk_it_->nextBlock(block)){
		return true;
	}
	else{
		if((chunk_it_=nextChunk())>0){
			return nextBlock(block);
		}
		else{
			ps->setInMemory(true);
			return false;
		}
	}
}

bool PartitionStorage::AtomicPartitionReaderIterator::nextBlock(
		BlockStreamBase*& block) {
////	lock_.acquire();
//	if(chunk_it_>0&&chunk_it_->nextBlock(block)){
////		lock_.release();
//		return true;
//	}
//	else{
//		lock_.acquire();
//		if((chunk_it_=nextChunk())>0){
//			lock_.release();
//			return nextBlock(block);
//		}
//		else{
//			lock_.release();
//			return false;
//		}
//	}
	//	lock_.acquire();

	lock_.acquire();
	ChunkReaderIterator::block_accessor* ba;
	if(chunk_it_!=0&&chunk_it_->getNextBlockAccessor(ba)){
		lock_.release();
		ba->getBlock(block);
		return true;
	}
	else{
		if((chunk_it_=PartitionReaderItetaor::nextChunk())>0){
			lock_.release();
			return nextBlock(block);
		}
		else{
			lock_.release();
			return false;
		}
	}
}

PartitionStorage::NumaSensitivePartitionReaderIterator::NumaSensitivePartitionReaderIterator(
		PartitionStorage* partitino_storage):
				PartitionReaderItetaor(partitino_storage),
				socket_map_cur_(getNumberOfSockets(), 0),
				socket_iterator_(getNumberOfSockets(), 0) {
	for (int i = 0; i < ps->chunk_list_.size(); ++i) {
		ChunkID chunk_id = ps->chunk_list_[i]->getChunkID();
		HdfsInMemoryChunk chunk;
		BlockManager::getInstance()->getMemoryChunkStore()->getChunk(chunk_id, chunk);
		int node_index = chunk.numa_index;
		assert(node_index < getNumberOfSockets() && "node index must less than number of Sockets");
		ChunkReaderIterator* chunk_reader = ps->chunk_list_[i]->createChunkReaderIterator();
		socket_index_to_chunk_reader_iterator_.insert(pair<int32_t, ChunkReaderIterator*>(node_index, chunk_reader));
	}
//	socket_map_cur_.insert()
	assert(socket_map_cur_.size()==5
			&& socket_map_cur_[0] == 0
			&& socket_map_cur_[getNumberOfSockets()] == 0);
}

PartitionStorage::NumaSensitivePartitionReaderIterator::~NumaSensitivePartitionReaderIterator() {
}

// lock-free
ChunkReaderIterator* PartitionStorage::NumaSensitivePartitionReaderIterator::nextChunk() {
	printf("NumaSensitivePartitionReaderIterator::nextChunk is currently implemented!\n");
	ChunkReaderIterator* ret = NULL;

	// get chunk located in closest socket
	int current_socket_index = getCurrentSocketAffility();
//	socket_index_to_chunk_reader_iterator_.
	int cur = 0;
	auto range = socket_index_to_chunk_reader_iterator_.equal_range(current_socket_index);
	for (auto it = range.first; it != range.second; ++it) {
		if (cur++ == socket_map_cur_[current_socket_index]) {
			__sync_fetch_and_add(&socket_map_cur_[current_socket_index], 1);
			return it->second;
		}
	}
	return NULL;
}

bool PartitionStorage::NumaSensitivePartitionReaderIterator::nextBlock(
		BlockStreamBase*& block) {
	lock_.acquire();
	int current_socket_id = getCurrentSocketAffility();
	ChunkReaderIterator* &socket_iterator = socket_iterator_[current_socket_id];
	ChunkReaderIterator::block_accessor* ba;
	if(socket_iterator!=0&&socket_iterator->getNextBlockAccessor(ba)){
		lock_.release();
		ba->getBlock(block);
		return true;
	}
	else{
		if((socket_iterator=nextChunk())>0){
			lock_.release();
			return nextBlock(block);
		}
		else{
			lock_.release();
			return false;
		}
	}
}
