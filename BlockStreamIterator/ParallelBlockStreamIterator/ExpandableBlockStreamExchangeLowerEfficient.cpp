/*
 * ExpandableBlockStreamExchangeLowerEfficient.cpp
 *
 *  Created on: Aug 29, 2013
 *      Author: wangli
 */

#include "ExpandableBlockStreamExchangeLowerEfficient.h"


#include <malloc.h>
#include "../../configure.h"

#include "../../common/rename.h"
#include "../../common/Logging.h"
#include "../../Executor/ExchangeTracker.h"
#include "../../Environment.h"
#include "../../common/Logging.h"
#include "../../utility/ThreadSafe.h"
#include "../../common/ids.h"
#include "../../utility/rdtsc.h"
ExpandableBlockStreamExchangeLowerEfficient::ExpandableBlockStreamExchangeLowerEfficient(State state)
:state_(state){
	logging_=new ExchangeIteratorEagerLowerLogging();
	assert(state.partition_schema_.partition_key_index<100);
}


ExpandableBlockStreamExchangeLowerEfficient::ExpandableBlockStreamExchangeLowerEfficient() {
	logging_=new ExchangeIteratorEagerLowerLogging();
}

ExpandableBlockStreamExchangeLowerEfficient::~ExpandableBlockStreamExchangeLowerEfficient() {
	// TODO Auto-generated destructor stub
	delete logging_;
	delete state_.schema_;
	delete state_.child_;
}

bool ExpandableBlockStreamExchangeLowerEfficient::open(const PartitionOffset&){
	logging_->log("[%lld] Exchange lower is created!",state_.exchange_id_);

	debug_connected_uppers=0;
	debug_connected_uppers_in=0;
	state_.child_->open(state_.partition_offset_);

	nuppers_=state_.upper_id_list_.size();
	partition_function_=PartitionFunctionFactory::createBoostHashFunction(nuppers_);
	socket_fd_upper_list=new int[nuppers_];

	/** initialize the block stream that is used to accumulate the block obtained by calling child iterator's next() **/
	block_stream_for_asking_=BlockStreamBase::createBlock(state_.schema_,state_.block_size_);

	/** buffer stores the tuples received from child iterator. Note the tuples are partitioned and stored. **/
	partitioned_data_buffer_=new PartitionedBlockBuffer(nuppers_,block_stream_for_asking_->getSerializedBlockSize());

	/** the temporary block that is used to transfer a block from partitioned data buffer into sending buffer.**/
	block_for_buffer_=new BlockContainer(block_stream_for_asking_->getSerializedBlockSize());

	/** Initialize the buffer that is used to hold the blocks being sent. There are nuppers block, each corresponding
	 * to a merger. **/
	sending_buffer_=new PartitionedBlockContainer(nuppers_,block_stream_for_asking_->getSerializedBlockSize());

	/** Initialized the temporary block to hold the serialized block stream. **/
	block_for_serialization_=new Block(block_stream_for_asking_->getSerializedBlockSize());

	/** Initialize the blocks that are used to accumulate the tuples from child so that the insertion to the buffer
	 * can be conducted at the granularity of blocks rather than tuples.
	 */
	partitioned_block_stream_= new BlockStreamBase*[nuppers_];
	for(unsigned i=0;i<nuppers_;i++){
		partitioned_block_stream_[i]=BlockStreamBase::createBlock(state_.schema_,state_.block_size_);
	}


	/** connect to all the mergers **/
	for(unsigned upper_offset=0;upper_offset<state_.upper_id_list_.size();upper_offset++){

		logging_->log("[%ld,%d] try to connect to upper (%d) %s\n",state_.exchange_id_,state_.partition_offset_,upper_offset,state_.upper_id_list_[upper_offset]);
		if(ConnectToUpper(ExchangeID(state_.exchange_id_,upper_offset),state_.upper_id_list_[upper_offset],socket_fd_upper_list[upper_offset],logging_)!=true)
			return false;
		printf("[%ld,%d] connected to upper [%d,%d] on Node %d\n",state_.exchange_id_,state_.partition_offset_,state_.exchange_id_,upper_offset,state_.upper_id_list_[upper_offset]);
	}

	/** create the sender thread **/
//	if (true == g_thread_pool_used) {
//		Environment::getInstance()->getThreadPool()->AddTask(sender, this);
//	}
//	else {
		int error;
		error=pthread_create(&sender_tid,NULL,sender,this);
		if(error!=0){
			logging_->elog("Failed to create the sender thread>>>>>>>>>>>>>>>>>>>>>>>>>>>>@@#@#\n\n.");
			return false;
		}
//	}
//	pthread_create(&debug_tid,NULL,debug,this);
/*debug*/
	return true;
}
bool ExpandableBlockStreamExchangeLowerEfficient::next(BlockStreamBase*){
	void* tuple_from_child;
	void* tuple_in_cur_block_stream;
	while(true){
		block_stream_for_asking_->setEmpty();
		if(state_.child_->next(block_stream_for_asking_)){
			/** if a blocks is obtained from child, we partition the tuples in the block. **/
			if(state_.partition_schema_.isHashPartition()){
				BlockStreamBase::BlockStreamTraverseIterator* traverse_iterator=block_stream_for_asking_->createIterator();
				while((tuple_from_child=traverse_iterator->nextTuple())>0){
					/** for each tuple in the newly obtained block, insert the tuple to one partitioned block according to the
					 * partition hash value**/
					const unsigned partition_id=hash(tuple_from_child,state_.schema_,state_.partition_schema_.partition_key_index,nuppers_);

					/** calculate the tuple size for the current tuple **/
					const unsigned bytes=state_.schema_->getTupleActualSize(tuple_from_child);

					/** insert the tuple into the corresponding partitioned block **/
					while(!(tuple_in_cur_block_stream=partitioned_block_stream_[partition_id]->allocateTuple(bytes))){
						/** if the destination block is full, we insert the block into the buffer **/

						partitioned_block_stream_[partition_id]->serialize(*block_for_serialization_);
						partitioned_data_buffer_->insertBlockToPartitionedList(block_for_serialization_,partition_id);
						partitioned_block_stream_[partition_id]->setEmpty();
					}
					/** thread arriving here means that the space for the tuple is successfully allocated, so we copy the tuple **/
					state_.schema_->copyTuple(tuple_from_child,tuple_in_cur_block_stream);
				}
			}
			else if(state_.partition_schema_.isBoardcastPartition()){
				block_stream_for_asking_->serialize(*block_for_serialization_);
				for(unsigned i=0;i<nuppers_;i++){
					partitioned_data_buffer_->insertBlockToPartitionedList(block_for_serialization_,i);
				}
			}
		}
		else{
			/* the child iterator is exhausted. We add the cur block steram block into the buffer*/
			for(unsigned i=0;i<nuppers_;i++){
				partitioned_block_stream_[i]->serialize(*block_for_serialization_);
				partitioned_data_buffer_->insertBlockToPartitionedList(block_for_serialization_,i);

				/* The following lines send an empty block to the upper, indicating that all
				 * the data from current sent has been transmit to the uppers.
				 */
				if(!partitioned_block_stream_[i]->Empty()){
					partitioned_block_stream_[i]->setEmpty();
					partitioned_block_stream_[i]->serialize(*block_for_serialization_);
					partitioned_data_buffer_->insertBlockToPartitionedList(block_for_serialization_,i);
				}
			}

			/*
			 * waiting until all the block in the buffer has been transformed to the uppers.
			 */
			logging_->log("Waiting until all the blocks in the buffer is sent!");
			while(!partitioned_data_buffer_->isEmpty()){
				usleep(1);
			}

			/*
			 * waiting until all the uppers send the close notification which means that
			 * blocks in the uppers' socket buffer have all been consumed.
			 */
			logging_->log("Waiting for close notification!");
			for(unsigned i=0;i<nuppers_;i++){
				WaitingForCloseNotification(socket_fd_upper_list[i]);
			}

			return false;
		}
	}
}

//unsigned ExpandableBlockStreamExchangeLowerEfficient::hash(void* value){
//	const void* hash_key_address=state.schema->getColumnAddess(state.partition_key_index,value);
//	return state.schema->getcolumn(state.partition_key_index).operate->getPartitionValue(hash_key_address,nuppers);
//}
bool ExpandableBlockStreamExchangeLowerEfficient::close(){

	cancelSenderThread();

	state_.child_->close();

	/** free temporary space **/
	delete partitioned_data_buffer_;
	delete block_stream_for_asking_;
	delete block_for_serialization_;
	delete sending_buffer_;
	delete block_for_buffer_;
	for(unsigned i=0;i<nuppers_;i++){
		delete partitioned_block_stream_[i];
	}

	delete [] partitioned_block_stream_;
	delete [] socket_fd_upper_list;

	delete partition_function_;
	return true;
}

void* ExpandableBlockStreamExchangeLowerEfficient::sender(void* arg){
	ExpandableBlockStreamExchangeLowerEfficient* Pthis=(ExpandableBlockStreamExchangeLowerEfficient*)arg;
	Pthis->logging_->log("[%ld,%d] sender thread created!",Pthis->state_.exchange_id_,Pthis->state_.partition_offset_);
	Pthis->sending_buffer_->Initialized();
	try{
		while(true){
			pthread_testcancel();
			bool consumed=false;
			BlockContainer* block_for_sending;
			int partition_id=Pthis->sending_buffer_->getBlockForSending(block_for_sending);
			if(partition_id>=0){
				pthread_testcancel();
				if(block_for_sending->GetRestSize()>0){
					int recvbytes;
					recvbytes=send(Pthis->socket_fd_upper_list[partition_id],(char*)block_for_sending->getBlock()+block_for_sending->GetCurSize(),block_for_sending->GetRestSize(),MSG_DONTWAIT);
					if(recvbytes==-1){
						if (errno == EAGAIN){
							continue;
						}
						printf("Error=%d,fd=%d\n",errno,Pthis->socket_fd_upper_list[partition_id]);
						Pthis->logging_->elog("Send error!\n");
						break;
					}
					else{
						if(recvbytes<block_for_sending->GetRestSize()){
							/* the block is not entirely sent. */
							Pthis->logging_->log("**not entire sent! bytes=%d, rest size=%d",recvbytes,block_for_sending->GetRestSize());
							block_for_sending->IncreaseActualSize(recvbytes);
							continue;
						}
						else{
							/** the block is sent in entirety. **/
							Pthis->logging_->log("[%ld,%d]A block is sent bytes=%d, rest size=%d",Pthis->state_.exchange_id_,Pthis->state_.partition_offset_,recvbytes,block_for_sending->GetRestSize());
							block_for_sending->IncreaseActualSize(recvbytes);
							Pthis->logging_->log("[%ld,%d]Send the new block to [%d]",Pthis->state_.exchange_id_,Pthis->state_.partition_offset_,Pthis->state_.upper_id_list_[partition_id]);
							Pthis->sendedblocks++;
							consumed=true;
						}
					}
				}
				else{
					consumed=true;
				}
			}
			else{
				/* "partition_id<0" means that block_for_sending is empty, so we get one block from the buffer into the block_for_sending_*/
				unsigned index=Pthis->partitioned_data_buffer_->getBlock(*Pthis->block_for_buffer_);
				Pthis->block_for_buffer_->reset();
				Pthis->sending_buffer_->insert(index,Pthis->block_for_buffer_);
			}
			if(consumed==true){
				/* In the current loop, we have sent an entire block to the receiver, so we should get a new block
				 * into the block_for_sender_*/
				pthread_testcancel();
				if(Pthis->partitioned_data_buffer_->getBlock(*Pthis->block_for_buffer_,partition_id)){
					Pthis->block_for_buffer_->reset();
					Pthis->sending_buffer_->insert(partition_id,Pthis->block_for_buffer_);
				}
				else{
					/**TODO: test the effort of the following sleeping statement and consider
					 * whether it should be replaced by conditioned wait **/
					usleep(1);
				}
			}
		}
	}
	catch(std::exception e){
		pthread_testcancel();
	}
}
void* ExpandableBlockStreamExchangeLowerEfficient::debug(void* arg){
	ExpandableBlockStreamExchangeLowerEfficient* Pthis=(ExpandableBlockStreamExchangeLowerEfficient*)arg;
	while(true){
		usleep(100000);
	}
}

void ExpandableBlockStreamExchangeLowerEfficient::cancelSenderThread() {
//	if (true == g_thread_pool_used) {
//	}
//	else{
		pthread_cancel(sender_tid);
		void* res;
		pthread_join(sender_tid,&res);
		if(res!=PTHREAD_CANCELED)
			printf("thread is not canceled!\n");
		sender_tid=0;
//	}
}
