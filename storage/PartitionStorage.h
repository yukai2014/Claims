/*
 * ProjectionStorage.h
 *
 *  Created on: Nov 14, 2013
 *      Author: wangli
 */

#ifndef PARTITIONSTORAGE_H_
#define PARTITIONSTORAGE_H_
#ifdef DMALLOC
#include "dmalloc.h"
#endif
#include "ChunkStorage.h"
#include "StorageLevel.h"
#include "PartitionReaderIterator.h"
#include "../utility/lock.h"


class PartitionStorage {
public:
	class PartitionReaderItetaor{
	public:
//		PartitionReaderItetaor();
		PartitionReaderItetaor(PartitionStorage* partition_storage);
		virtual ~PartitionReaderItetaor();
		virtual ChunkReaderIterator* nextChunk();
		virtual bool nextBlock(BlockStreamBase* &block);
	protected:
		PartitionStorage* ps;
		unsigned chunk_cur_;
		ChunkReaderIterator* chunk_it_;
	};
	class AtomicPartitionReaderIterator:public PartitionReaderItetaor{
	public:
//		AtomicPartitionReaderIterator();
		AtomicPartitionReaderIterator(PartitionStorage* partition_storage):PartitionReaderItetaor(partition_storage){};
		virtual ~AtomicPartitionReaderIterator();
		ChunkReaderIterator* nextChunk();
		virtual bool nextBlock(BlockStreamBase* &block);
	private:
		Lock lock_;
	};

	class NumaSensitivePartitionReaderIterator:public PartitionReaderItetaor{
	public:
		NumaSensitivePartitionReaderIterator(PartitionStorage* partitino_storage);
		virtual ~NumaSensitivePartitionReaderIterator();
		ChunkReaderIterator* nextChunk();
		bool nextBlock(BlockStreamBase* &block);
	private:
		// optimized for NUMA
		boost::unordered_multimap<int32_t, ChunkReaderIterator*> socket_index_to_chunk_reader_iterator_;
//		boost::unordered_multimap<int32_t, ChunkReaderIterator*>::iterator it_in_multimap_;
		vector<ChunkReaderIterator*> socket_iterator_;
		vector<size_t> socket_map_cur_;
		Lock lock_;
	};

	friend class PartitionReaderItetaor;
	PartitionStorage(const PartitionID &partition_id,const unsigned &number_of_chunks,const StorageLevel&);
	virtual ~PartitionStorage();
	void addNewChunk();
	void updateChunksWithInsertOrAppend(const PartitionID &partition_id, const unsigned &number_of_chunks, const StorageLevel& storage_level);
	void removeAllChunks(const PartitionID &partition_id);
	PartitionReaderItetaor* createReaderIterator();
	PartitionReaderItetaor* createAtomicReaderIterator();
	PartitionReaderItetaor* createNumaSensitiveReaderIterator();
	inline bool is_in_memory() { return load_in_memory; };
	inline void setInMemory(bool is_memory) { load_in_memory = is_memory; };
protected:
	PartitionID partition_id_;
	unsigned number_of_chunks_;
	std::vector<ChunkStorage*> chunk_list_;
	StorageLevel desirable_storage_level_;
private:
	bool load_in_memory;

};

#endif /* PARTITIONSTORAGE_H_ */
