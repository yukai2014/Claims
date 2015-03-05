/*
 * CSBIndexBuilding.h
 *
 *  Created on: Mar 17, 2014
 *      Author: scdong
 */

#ifndef CSBINDEXBUILDING_H_
#define CSBINDEXBUILDING_H_
#include <boost/serialization/base_object.hpp>
#include <map>
#include "../BlockStreamIterator/ExpandableBlockStreamIteratorBase.h"
#include "../common/ids.h"
#include "../common/data_type.h"
#include "../common/Schema/Schema.h"
#include "../common/Block/BlockStream.h"
#include "../storage/PartitionStorage.h"
#include "../storage/ChunkStorage.h"
#include "CSBPlusTree.h"
#include "CSBTree.h"
#include "EnhancedCSBTree.h"

//template<typename T>
//CSBPlusTree<T>* indexBuilding(Schema* schema, vector<void*> chunk_tuples);

class bottomLayerCollecting :public ExpandableBlockStreamIteratorBase {
public:
	struct remaining_block {
		remaining_block() : block(0), iterator(0), chunk_offset(0), block_offset(0), tuple_offset(0) {}
		remaining_block(BlockStreamBase* block, BlockStreamBase::BlockStreamTraverseIterator* iterator, unsigned short chunk_offset, unsigned short block_offset, unsigned short tuple_offset)
		:block(block), iterator(iterator), chunk_offset(chunk_offset), block_offset(block_offset), tuple_offset(tuple_offset) {}
		BlockStreamBase::BlockStreamTraverseIterator* iterator;
		BlockStreamBase* block;
		ChunkOffset chunk_offset;
		unsigned short block_offset;
		unsigned short tuple_offset;
	};
	class State {
		friend class bottomLayerCollecting;
	public:
		State() {}
		State(ProjectionID projection_id, Schema* schema, unsigned key_indexing, unsigned block_size);
	public:
		Schema* schema_;
		ProjectionID projection_id_;
		unsigned key_indexing_;
		unsigned block_size_;
		friend class boost::serialization::access;
		template<class Archive>
		void serialize(Archive & ar, const unsigned int version){
			ar & schema_ & projection_id_ & block_size_& key_indexing_;
		}
	};

public:
	bottomLayerCollecting();
	bottomLayerCollecting(State state);
	virtual ~bottomLayerCollecting();
	bool open(const PartitionOffset& partition_offset=0);
	bool next(BlockStreamBase* block);
	bool close();
	void print(){
		printf("CCSBIndexingBuilding\n");
	}

private:
	State state_;
	PartitionStorage::PartitionReaderItetaor* partition_reader_iterator_;
	ChunkReaderIterator* chunk_reader_iterator_;
	std::list<remaining_block> remaining_block_list_;
	std::list<BlockStreamBase*> block_stream_list_;

	Schema* output_schema_;

	ChunkOffset chunk_offset_;
	unsigned short block_offset_;
//	unsigned short tuple_offset_;

	Lock lock_;

	void atomicPushRemainingBlock(remaining_block rb);
	bool atomicPopRemainingBlock(remaining_block& rb);
	void AtomicPushBlockStream(BlockStreamBase* block);
	BlockStreamBase* AtomicPopBlockStream();

	bool askForNextBlock(BlockStreamBase* & block, remaining_block& rb);

	void computeOutputSchema();
private:
	friend class boost::serialization::access;
	template<class Archive>
	void serialize(Archive & ar, const unsigned int version){
		ar & boost::serialization::base_object<ExpandableBlockStreamIteratorBase>(*this) & state_;
	}
};




class bottomLayerSorting :public ExpandableBlockStreamIteratorBase {
public:
	class State {
		friend class bottomLayerSorting;
	public:
		State() {}
		State(Schema* schema, BlockStreamIteratorBase* child, const unsigned block_size, ProjectionID projection_id, unsigned key_indexing, std::string index_name, index_type _index_type = CSBPLUS);
	public:
		Schema* schema_;
		BlockStreamIteratorBase* child_;
		unsigned block_size_;
		//The following two variable is for registering to the IndexManager
		//similar to its child's projection id and key indexing
		ProjectionID projection_id_;
		unsigned key_indexing_;
		std::string index_name_;
		index_type index_type_;
	private:
		friend class boost::serialization::access;
		template<class Archive>
		void serialize(Archive & ar, const unsigned version) {
			ar & schema_ & child_ & block_size_ & projection_id_ & key_indexing_ & index_name_ & index_type_;
		}
	};

public:
	typedef struct compare_node {
		Schema* vector_schema_;
		void* tuple_;
		Operate* op_;
	} compare_node;

	bottomLayerSorting();
	bottomLayerSorting(State state);
	virtual ~bottomLayerSorting();

	bool open(const PartitionOffset& partition_offset=0);
	bool next(BlockStreamBase* block);
	bool close();

private:
	static bool compare(const compare_node* a, const compare_node* b);

	template<typename T>
	void* indexBuilding(vector<compare_node*> chunk_tuples);

	void computeVectorSchema();

	State state_;

	Schema* vector_schema_;
	std::map <ChunkOffset, vector<compare_node*> > tuples_in_chunk_; //ChunkID->tuples_in_chunk

	PartitionID partition_id_;

private:
	friend class boost::serialization::access;
	template<class Archive>
	void serialize(Archive & ar, const unsigned int version){
		ar & boost::serialization::base_object<ExpandableBlockStreamIteratorBase>(*this) & state_;
	}
};

template<typename T>
void* bottomLayerSorting::indexBuilding(vector<compare_node*> chunk_tuples)
{
	switch (state_.index_type_)
	{
	case CSBPLUS:
	{
		data_offset<T>* aray = new data_offset<T> [chunk_tuples.size()];
///*for testing*/	cout << "chunk data size: " << chunk_tuples.size() << endl << endl;
		for (unsigned i = 0; i < chunk_tuples.size(); i++)
		{
			aray[i]._key = *(T*)(vector_schema_->getColumnAddess(0, chunk_tuples[i]->tuple_));
			aray[i]._block_off = *(unsigned short*)(vector_schema_->getColumnAddess(1, chunk_tuples[i]->tuple_));
			aray[i]._tuple_off = *(unsigned short*)(vector_schema_->getColumnAddess(2, chunk_tuples[i]->tuple_));
///*for testing*/		cout << aray[i]._key << "\t" << aray[i]._block_off << "\t" << aray[i]._tuple_off << endl;
		}
		CSBPlusTree<T>* csb_plus_tree = new CSBPlusTree<T>();
		csb_plus_tree->BulkLoad(aray, chunk_tuples.size());
		cout << "*************************CSB indexing build successfully!*************************\n";
		return csb_plus_tree;
	}
	case CSB:
	{
		if (vector_schema_->getcolumn(0).type != t_u_long)
		{
			cout << "[ERROR FILE: " << __FILE__ << "] In function " << __func__ << " line " << __LINE__ << ": " << "The column type which will be build CSB-Tree index is not unsigned long!" << endl;
			return NULL;
		}
		data_original* aray = new data_original [chunk_tuples.size()];
		for (unsigned i = 0; i < chunk_tuples.size(); i++)
		{
			aray[i]._key = *(unsigned long*)(vector_schema_->getColumnAddess(0, chunk_tuples[i]->tuple_));
			aray[i]._block_off = *(unsigned short*)(vector_schema_->getColumnAddess(1, chunk_tuples[i]->tuple_));
			aray[i]._tuple_off = *(unsigned short*)(vector_schema_->getColumnAddess(2, chunk_tuples[i]->tuple_));
		}
		CSBTree* csb_tree = new CSBTree();
		csb_tree->BulkLoad(aray, chunk_tuples.size());
		return csb_tree;
	}
	case ECSB:
	{
		if (vector_schema_->getcolumn(0).type != t_u_long)
		{
			cout << "[ERROR FILE: " << __FILE__ << "] In function " << __func__ << " line " << __LINE__ << ": " << "The column type which will be build Enhanced-CSB-Tree index is not unsigned long!" << endl;
			return NULL;
		}
		data_original* aray = new data_original [chunk_tuples.size()];
		for (unsigned i = 0; i < chunk_tuples.size(); i++)
		{
			aray[i]._key = *(unsigned long*)(vector_schema_->getColumnAddess(0, chunk_tuples[i]->tuple_));
			aray[i]._block_off = *(unsigned short*)(vector_schema_->getColumnAddess(1, chunk_tuples[i]->tuple_));
			aray[i]._tuple_off = *(unsigned short*)(vector_schema_->getColumnAddess(2, chunk_tuples[i]->tuple_));
		}
		EnhancedCSBTree* e_csb_tree = new EnhancedCSBTree();
		e_csb_tree->BulkLoad(aray, chunk_tuples.size());
		return e_csb_tree;
	}
	default:
	{
		cout << "[ERROR FILE: " << __FILE__ << "] In function " << __func__ << " line " << __LINE__ << ": The index_type is illegal!\n";
		return NULL;
	}
	}

}

#endif /* CSBINDEXBUILDING_H_ */
