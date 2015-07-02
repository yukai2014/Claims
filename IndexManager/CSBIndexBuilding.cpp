/*
 * CSBIndexBuilding.cpp
 *
 *  Created on: Mar 17, 2014
 *      Author: scdong
 */

#include <map>
#include "CSBIndexBuilding.h"
#include "../storage/BlockManager.h"
#include <algorithm>
#include "IndexManager.h"

using std::stable_sort;

bottomLayerCollecting::bottomLayerCollecting(State state) :state_(state), partition_reader_iterator_(0), chunk_reader_iterator_(0), chunk_offset_(0), block_offset_(0) {
	initialize_expanded_status();
}
bottomLayerCollecting::bottomLayerCollecting(){
	initialize_expanded_status();
}
bottomLayerCollecting::~bottomLayerCollecting() {
	// TODO Auto-generated destructor stub
}

bottomLayerCollecting::State::State(ProjectionID projection_id, Schema* schema, unsigned key_indexing, unsigned block_size)
: projection_id_(projection_id), schema_(schema), key_indexing_(key_indexing), block_size_(block_size) {

}

bool bottomLayerCollecting::open(const PartitionOffset& partition_offset)
{

	AtomicPushBlockStream(BlockStreamBase::createBlockWithDesirableSerilaizedSize(state_.schema_, state_.block_size_));
	if(tryEntryIntoSerializedSection()){

		computeOutputSchema();
		/* this is the first expanded thread*/
		PartitionStorage* partition_handle_;
		if((partition_handle_=BlockManager::getInstance()->getPartitionHandle(PartitionID(state_.projection_id_,partition_offset)))==0){
			printf("The partition[%s] does not exists!\n",PartitionID(state_.projection_id_,partition_offset).getName().c_str());
			setReturnStatus(false);
		}
		else{
			partition_reader_iterator_=partition_handle_->createAtomicReaderIterator();
		}
		setReturnStatus(true);
	}
	barrierArrive();
	return getReturnStatus();
}

bool bottomLayerCollecting::next(BlockStreamBase* block) {
	remaining_block rb;
	void* original_tuple;
	void* tuple_new;

	// There are blocks which haven't been completely processed
	if (atomicPopRemainingBlock(rb))
	{
		while ((original_tuple = rb.iterator->currentTuple()) > 0)
		{
			const unsigned bytes = output_schema_->getTupleMaxSize();
			if ((tuple_new = block->allocateTuple(bytes)) > 0)
			{
				// construct tuple_new <chunk_offset, key_index, block_offset, tuple_offset>
				output_schema_->getcolumn(0).operate->assignment((void*)(& rb.chunk_offset), tuple_new);
				output_schema_->getcolumn(1).operate->assignment(state_.schema_->getColumnAddess(state_.key_indexing_, original_tuple), output_schema_->getColumnAddess(1, tuple_new));
				output_schema_->getcolumn(2).operate->assignment((void*)(& rb.block_offset), output_schema_->getColumnAddess(2, tuple_new));
				output_schema_->getcolumn(3).operate->assignment((void*)(& rb.tuple_offset), output_schema_->getColumnAddess(3, tuple_new));
				rb.iterator->increase_cur_();
				rb.tuple_offset++;

///*for testing*/				state_.schema_->displayTuple(original_tuple, " | ");
///*for testing*/				output_schema_->displayTuple(tuple_new, " | ");
///*for testing*/				sleep(1);
			}
			else
			{
				atomicPushRemainingBlock(rb);
				return true;
			}
		}
		AtomicPushBlockStream(rb.block);
	}

	// When the program arrivals here, it means that there is no remaining block or the remaining block is
	// exhausted. What we should do is to ask a new block from the chunk_reader_iterator (or prartition_reader_iterator)
	BlockStreamBase* block_for_asking = AtomicPopBlockStream();
	block_for_asking->setEmpty();
	rb.block=block_for_asking;
	while (askForNextBlock(block_for_asking, rb))
	{
//		BlockStreamBase::BlockStreamTraverseIterator* traverse_iterator = block_for_asking->createIterator();
		rb.iterator=block_for_asking->createIterator();
		while ((original_tuple = rb.iterator->currentTuple()) > 0)
		{
			const unsigned bytes = output_schema_->getTupleMaxSize();
			if ((tuple_new = block->allocateTuple(bytes)) > 0)
			{
				// construct tuple_new <chunk_offset, key_index, block_offset, tuple_offset>
				output_schema_->getcolumn(0).operate->assignment((void*)(& rb.chunk_offset), tuple_new);
				output_schema_->getcolumn(1).operate->assignment(state_.schema_->getColumnAddess(state_.key_indexing_, original_tuple), output_schema_->getColumnAddess(1, tuple_new));
				output_schema_->getcolumn(2).operate->assignment((void*)(& rb.block_offset), output_schema_->getColumnAddess(2, tuple_new));
				output_schema_->getcolumn(3).operate->assignment((void*)(& rb.tuple_offset), output_schema_->getColumnAddess(3, tuple_new));
				rb.iterator->increase_cur_();
				rb.tuple_offset++;

///*for testing*/				state_.schema_->displayTuple(original_tuple, " | ");
///*for testing*/				output_schema_->displayTuple(tuple_new, " | ");
///*for testing*/				sleep(1);
			}
			else
			{
				atomicPushRemainingBlock(rb);
				return true;
			}
		}
//		traverse_iterator->~BlockStreamTraverseIterator();
		block_for_asking->setEmpty();
	}
	AtomicPushBlockStream(block_for_asking);
	if (!block->Empty())
		return true;
	return false;
}

bool bottomLayerCollecting::close() {
	initialize_expanded_status();
	delete partition_reader_iterator_;
	remaining_block_list_.clear();
	block_stream_list_.clear();

	return true;
}

void bottomLayerCollecting::atomicPushRemainingBlock(remaining_block rb)
{
	lock_.acquire();
	remaining_block_list_.push_back(rb);
	lock_.release();
}

bool bottomLayerCollecting::atomicPopRemainingBlock(remaining_block& rb)
{
	lock_.acquire();
	if (remaining_block_list_.size() > 0)
	{
		rb = remaining_block_list_.front();
		remaining_block_list_.pop_front();
		lock_.release();
		return true;
	}
	lock_.release();
	return false;
}

void bottomLayerCollecting::AtomicPushBlockStream(BlockStreamBase* block)
{
	lock_.acquire();
	block_stream_list_.push_back(block);
	lock_.release();
}

BlockStreamBase* bottomLayerCollecting::AtomicPopBlockStream()
{
	assert(!block_stream_list_.empty());
	lock_.acquire();
	BlockStreamBase* block = block_stream_list_.front();
	block_stream_list_.pop_front();
	lock_.release();
	return block;
}

bool bottomLayerCollecting::askForNextBlock(BlockStreamBase* & block, remaining_block& rb)
{
	if (chunk_reader_iterator_==0||chunk_reader_iterator_->nextBlock(block) == false)
	{
		chunk_reader_iterator_ = partition_reader_iterator_->nextChunk();

		if (chunk_reader_iterator_ == NULL){
			printf("Has been falsed!!!!!!!!!!!!!*&S*DF&(SD&F(S<><<<><><><><><>\n");
			return false;
		}
		chunk_reader_iterator_->nextBlock(block);
		lock_.acquire();
		rb.chunk_offset = ++chunk_offset_;
		block_offset_ = 0;
		lock_.release();
		rb.block_offset = 0;
		rb.tuple_offset = 0;
		return true;
	}
	rb.chunk_offset = chunk_offset_;
	lock_.acquire();
	rb.block_offset = ++block_offset_;
	lock_.release();
	rb.tuple_offset = 0;
	return true;
}


void bottomLayerCollecting::computeOutputSchema(){
	std::vector<column_type> column_list;
	column_list.push_back(column_type(t_int));	//chunk offset
	column_list.push_back(state_.schema_->getcolumn(state_.key_indexing_));
	column_list.push_back(column_type(t_u_smallInt));		//block offset
	column_list.push_back(column_type(t_u_smallInt));		//tuple_offset

	output_schema_ = new SchemaFix(column_list);
}






bottomLayerSorting::bottomLayerSorting(){

	initialize_expanded_status();
}

bottomLayerSorting::bottomLayerSorting(State state) :state_(state)
{
	initialize_expanded_status();
}

bottomLayerSorting::~bottomLayerSorting()
{

}

bottomLayerSorting::State::State(Schema* schema, BlockStreamIteratorBase* child, unsigned block_size, ProjectionID projection_id, unsigned key_indexing, std::string index_name, index_type _index_type)
: schema_(schema), child_(child), block_size_(block_size), projection_id_(projection_id), key_indexing_(key_indexing), index_name_(index_name), index_type_(_index_type) {

}
bool bottomLayerSorting::open(const PartitionOffset& partition_offset)
{
	if (tryEntryIntoSerializedSection())
	{
		computeVectorSchema();
		const bool child_open_return = state_.child_->open(partition_offset);
		setReturnStatus(child_open_return);
	}
	barrierArrive();

	//Construct the PartitionID for the next function to make up the ChunkID
	partition_id_.projection_id = state_.projection_id_;
	partition_id_.partition_off = partition_offset;

	// Open finished. Buffer all the child create dataset in different group according to their ChunkIDs
	BlockStreamBase* block_for_asking = BlockStreamBase::createBlock(state_.schema_, state_.block_size_);
	block_for_asking->setEmpty();
	BlockStreamBase::BlockStreamTraverseIterator* iterator = NULL;
	void* current_chunk = new ChunkOffset;
	Operate* op_ = state_.schema_->getcolumn(1).operate->duplicateOperator();
	while (state_.child_->next(block_for_asking))
	{
		iterator = block_for_asking->createIterator();
		void* current_tuple = NULL;
		while((current_tuple = iterator->nextTuple()) != 0)
		{
			state_.schema_->getColumnValue(0, current_tuple, current_chunk);

			if(tuples_in_chunk_.find(*(ChunkOffset*)current_chunk)==tuples_in_chunk_.end()){
				 vector<compare_node*> tmp;
				 tuples_in_chunk_[*(ChunkOffset*)current_chunk] = tmp;
			}
			compare_node* c_node = (compare_node*)malloc(sizeof(compare_node));		//newmalloc
			c_node->vector_schema_ = vector_schema_;
			c_node->tuple_ = malloc(vector_schema_->getTupleMaxSize());		//newmalloc
			vector_schema_->copyTuple(current_tuple+state_.schema_->getcolumn(0).get_length(),c_node->tuple_);  // include chunk_offset??? -Yu
//			c_node->tuple_ = current_tuple+state_.schema_->getcolumn(0).get_length();
//			c_node->op_ = state_.schema_->getcolumn(1).operate->duplicateOperator();
			c_node->op_ = op_;
			tuples_in_chunk_.find(*(ChunkOffset*)current_chunk)->second.push_back(c_node);

//for testing begin
//			if ((*(ChunkOffset*)current_chunk) == 0)
//			{
//				cout << "current chunk: " << *(ChunkOffset*)current_chunk << " tuple: ";
//				vector_schema_->displayTuple(current_tuple+state_.schema_->getcolumn(0).get_length(), " | ");
//				vector_schema_->displayTuple(tuples_in_chunk_.find(*(ChunkOffset*)current_chunk)->second.back()->tuple_, " | ");
//				sleep(1);
//			}
//for testing end

		}
		block_for_asking->setEmpty();
	}

//for testing begin
//	sleep(10000);
//	cout << "Chunk Num: " << tuples_in_chunk_.size() << endl;
//	sleep(1000);
//for testing end

	// Sorting the tuples in each chunk
/*for testing*/	cout << "Chunk num: " << tuples_in_chunk_.size() << endl;
	for (std::map<ChunkOffset, vector<compare_node*> >::iterator iter = tuples_in_chunk_.begin(); iter != tuples_in_chunk_.end(); iter++)
	{
///*for testing*/		cout << "chunk id: " << *(unsigned short*)iter->first << endl;
//for testing begin
		cout << "Chunk size: " << iter->second.size() << endl;
//		for (unsigned i = 0; i < iter->second.size(); i++)
//		{
//			vector_schema_->displayTuple(iter->second[i]->tuple_, "\t");
////			sleep(1);
//		}
//		sleep(1000);
//for testing end

		stable_sort(iter->second.begin(), iter->second.end(), compare);

//for testing begin
//		for (unsigned i = 0; i < iter->second.size(); i++)
//		{
//			vector_schema_->displayTuple(iter->second[i]->tuple_, "\t");
////			sleep(1);
//		}
//		sleep(1000);
//for testing end
	}


	return getReturnStatus();
}

bool bottomLayerSorting::next(BlockStreamBase* block)
{
	map<ChunkID, void* >* csb_index_list = new map<ChunkID, void*>;
	csb_index_list->clear();
	switch (vector_schema_->getcolumn(0).type)
	{
	case t_int:
	{
		for (std::map<ChunkOffset, vector<compare_node*> >::iterator iter = tuples_in_chunk_.begin(); iter != tuples_in_chunk_.end(); iter++)
		{
			ChunkID* chunk_id = new ChunkID();
			chunk_id->partition_id = partition_id_;
			chunk_id->chunk_off = iter->first;
			void* csb_tree = indexBuilding<int>(iter->second);
			assert(csb_tree != NULL);
			(*csb_index_list)[*chunk_id] = csb_tree;
		}
//		IndexManager::getInstance()->addIndexToList(state_.key_indexing_, csb_index_list);
//		IndexManager::getInstance()->insertIndexToList(state_.index_name_, state_.key_indexing_, csb_index_list);
		IndexManager::getInstance()->insertIndexToList(partition_id_, csb_index_list, state_.index_type_);
		break;
	}
	case t_u_long:
	{
		for (std::map<ChunkOffset, vector<compare_node*> >::iterator iter = tuples_in_chunk_.begin(); iter != tuples_in_chunk_.end(); iter++)
		{
			ChunkID* chunk_id = new ChunkID();
			chunk_id->partition_id = partition_id_;
			chunk_id->chunk_off = iter->first;
			void* csb_tree = indexBuilding<unsigned long>(iter->second);
			assert (csb_tree != NULL);
			(*csb_index_list)[*chunk_id] = csb_tree;
		}
//		IndexManager::getInstance()->insertIndexToList(state_.index_name_, state_.key_indexing_, csb_index_list);
		IndexManager::getInstance()->insertIndexToList(partition_id_, csb_index_list, state_.index_type_);
		break;
	}
	default:
	{
		cout << "[ERROR: (CSBIndexBuilding.cpp->bottomLayerSorting->next()]: The data type is not defined!\n";
		break;
	}
	}
	return false;
}

bool bottomLayerSorting::close()
{
	initialize_expanded_status();
	state_.child_->close();
	cout << "bottomLayerSorting close finished!\n";
	return true;
}

bool bottomLayerSorting::compare(const compare_node* a, const compare_node* b)
{
	const void* left = a->vector_schema_->getColumnAddess(0, a->tuple_);
	const void* right = b->vector_schema_->getColumnAddess(0, b->tuple_);
	return a->op_->less(left, right);

}

void bottomLayerSorting::computeVectorSchema(){
	std::vector<column_type> column_list;
	column_list.push_back(state_.schema_->getcolumn(1));
	column_list.push_back(column_type(t_u_smallInt));		//block offset
	column_list.push_back(column_type(t_u_smallInt));		//tuple_offset

	vector_schema_ = new SchemaFix(column_list);

}
