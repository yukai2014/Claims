/*
 * CSB-Tree.h
 *
 *  Created on: Jul 9, 2014
 *      Author: scdong
 */

#ifndef ENHANCED_CSB_TREE_H_
#define ENHANCED_CSB_TREE_H_

#include <assert.h>
#include <math.h>
#include <xmmintrin.h>
#include <iostream>
#include <stack>
#include <vector>
#include "indexStructures.h"
#include "../utility/lock.h"
#include <string.h>
using namespace std;

#define MAX_NODE1_KEYS 40
#define MAX_NODE2_KEYS 20
#define MAX_NODE4_KEYS 10
#define MAX_NODE8_KEYS 5

#define INVALID_NODE1 0
#define INVALID_NODE2 0
#define INVALID_NODE4 0
#define INVALID_NODE8 0

#define MAX_KEY1 UCHAR_MAX
#define MAX_KEY2 USHRT_MAX
#define MAX_KEY4 UINT_MAX
#define MAX_KEY8 ULONG_MAX

typedef u_int8_t	key_type_1;
typedef u_int16_t	key_type_2;
typedef u_int32_t	key_type_4;
typedef u_int64_t	key_type_8;

class EnhancedCSBNodeGroup;

/***********************************  Enhanced CSB Node  ***********************************/
class EnhancedCSBNode {
public:
	EnhancedCSBNode(){}
	virtual ~EnhancedCSBNode() {}
public:
	virtual bool setkey(unsigned short pos, key_type_8 key) { assert(false); }
	virtual key_type_8 getkey(unsigned short pos) { assert(false); }
	virtual void* getkeys() { assert(false); }

	virtual void setChild(EnhancedCSBNodeGroup* child_) { assert(false); }
	virtual EnhancedCSBNodeGroup* getChild() { assert(false); }

	virtual bool setUsedKeys(unsigned short used_keys_) { assert(false); }
	virtual unsigned short getUsedKeys() { assert(false); }

	virtual int SIMDSearch(key_type_8 &key) { assert(false); }

	bool originInsert(int node_type, data_original value, data_pointer* p_data) {
		int max_keys;
		switch(node_type)
		{
		case 0:
			max_keys = MAX_NODE1_KEYS;
			break;
		case 1:
			max_keys = MAX_NODE2_KEYS;
			break;
		case 2:
			max_keys = MAX_NODE4_KEYS;
			break;
		case 3:
			max_keys = MAX_NODE8_KEYS;
			break;
		}
		if (this->getUsedKeys() >= max_keys)
		{
			cout << "[ERROR] int_Enhanced_CSB-Tree->EnhancedCSBNode->Insert(): The node is already full! Number of keys: " << max_keys << "\n";
			return false;
		}
		int pos = this->getUsedKeys()-1;
		for (; (pos >= 0) && (this->getkey(pos) > value._key); pos--)
		{
			this->setkey(pos+1, this->getkey(pos));
			p_data->block_off[pos+1] = p_data->block_off[pos];
			p_data->tuple_off[pos+1] = p_data->tuple_off[pos];
		}
		this->setkey(++pos, value._key);
		p_data->block_off[pos] = value._block_off;
		p_data->tuple_off[pos] = value._tuple_off;
		this->setUsedKeys(this->getUsedKeys()+1);
		return true;
	}
	bool copyInsert(int node_type, data_original value, data_pointer* p_data, EnhancedCSBNode* new_node, data_pointer* new_data, EnhancedCSBNodeGroup* new_child)
	{
		int max_keys;
		switch(node_type)
		{
		case 0:
			max_keys = MAX_NODE1_KEYS;
			break;
		case 1:
			max_keys = MAX_NODE2_KEYS;
			break;
		case 2:
			max_keys = MAX_NODE4_KEYS;
			break;
		case 3:
			max_keys = MAX_NODE8_KEYS;
			break;
		}
		if (this->getUsedKeys() >= max_keys)
		{
			cout << "[ERROR] int_Enhanced_CSB-Tree->EnhancedCSBNode->copyInsert(): The node is already full! Number of keys: " << max_keys << "\n";
			return false;
		}
		int pos = this->getUsedKeys()-1;
		for (; this->getkey(pos) > value._key; pos--)
		{
			new_node->setkey(pos+1, this->getkey(pos));
			new_data->block_off[pos+1] = p_data->block_off[pos];
			new_data->tuple_off[pos+1] = p_data->tuple_off[pos];
		}
		new_node->setkey(++pos, value._key);
		new_data->block_off[pos] = value._block_off;
		new_data->tuple_off[pos] = value._tuple_off;
		for (pos = pos-1; pos >= 0; pos--)
		{
			new_node->setkey(pos, this->getkey(pos));
			new_data->block_off[pos] = p_data->block_off[pos];
			new_data->tuple_off[pos] = p_data->tuple_off[pos];
		}
		new_node->setUsedKeys(this->getUsedKeys()+1);
		new_node->setChild(new_child);
		return true;
	}
	data_original originSplit(int node_type, EnhancedCSBNode* pNode, data_original data, data_pointer* cur_data_node, data_pointer* new_data_node) {
	/*
	 * node_type: compressed type of current index node
	 * pNode: new empty index node to store the split keys from current index node
	 * data: insert data
	 * cur_data_node: current data node
	 * new_data_node: new empty data node to store the split datas from current data node
	 */
		int max_keys;
		int64_t min_key;
		switch(node_type)
		{
		case 0:
			max_keys = MAX_NODE1_KEYS;
			min_key = CHAR_MIN;
			break;
		case 1:
			max_keys = MAX_NODE2_KEYS;
			min_key = SHRT_MIN;
			break;
		case 2:
			max_keys = MAX_NODE4_KEYS;
			min_key = INT_MIN;
			break;
		case 3:
			max_keys = MAX_NODE8_KEYS;
			min_key = LONG_MIN;
			break;
		}
		if (this->getUsedKeys() != max_keys)
		{
			cout << "[ERROR: int_Enhanced_CSB-Tree.h (EnhancedCSBNode->SplitInsert()] The node is unfull! Number of max_keys: " << max_keys << "\n";
			return data;
		}

		if (data._key < this->getkey(max_keys/2)) //insert into the first node
		{
			for (unsigned i = max_keys/2; i < max_keys; i++)
			{
				pNode->setkey(i-max_keys/2, this->getkey(i));
				new_data_node->block_off[i-max_keys/2] = cur_data_node->block_off[i];
				new_data_node->tuple_off[i-max_keys/2] = cur_data_node->tuple_off[i];
				this->setkey(i, min_key);
				cur_data_node->block_off[i] = INVALID;
				cur_data_node->tuple_off[i] = INVALID;
			}
			pNode->setUsedKeys(max_keys-max_keys/2);
			this->setUsedKeys(max_keys/2);
			this->originInsert(node_type, data, cur_data_node);

			data._key = this->getkey(max_keys/2);
			this->setkey(max_keys/2, min_key);
			data._block_off = cur_data_node->block_off[max_keys/2];
			cur_data_node->block_off[max_keys/2] = INVALID;
			data._tuple_off = cur_data_node->tuple_off[max_keys/2];
			cur_data_node->tuple_off[max_keys/2] = INVALID;
			this->setUsedKeys(max_keys/2);
			return data;
		}
		else
		{
			unsigned pos = max_keys/2+1;
			for (unsigned i = pos; i < max_keys; i++)
			{
				pNode->setkey(i-pos, this->getkey(i));
				new_data_node->block_off[i-pos] = cur_data_node->block_off[i];
				new_data_node->tuple_off[i-pos] = cur_data_node->block_off[i];
				this->setkey(i, min_key);
				cur_data_node->block_off[i] = INVALID;
				cur_data_node->tuple_off[i] = INVALID;
			}
			this->setUsedKeys(max_keys/2);
			pNode->setUsedKeys(max_keys-pos);
			pNode->originInsert(node_type, data, new_data_node);

			pos = max_keys/2;
			data._key = this->getkey(pos);
			this->setkey(pos, min_key);
			data._block_off = cur_data_node->block_off[pos];
			cur_data_node->block_off[pos] = INVALID;
			data._tuple_off = cur_data_node->tuple_off[pos];
			cur_data_node->tuple_off[pos] = INVALID;
			return data;
		}
	}
	data_original copySplit(int node_type, EnhancedCSBNode* pNode_left, EnhancedCSBNode* pNode_right, data_original data, data_pointer* cur_data_node, data_pointer* new_data_node_left, data_pointer* new_data_node_right) {
	/*
	 * node_type: compressed type of current index node
	 * pNode_(left/right): two new empty index nodes to split the current index node
	 * data: insert data
	 * cur_data_node: current data node
	 * new_data_node_(left/right): two new empty data node to split the current data node
	 */
		int max_keys;
		int64_t min_key;
		switch(node_type)
		{
		case 0:
			max_keys = MAX_NODE1_KEYS;
			min_key = CHAR_MIN;
			break;
		case 1:
			max_keys = MAX_NODE2_KEYS;
			min_key = SHRT_MIN;
			break;
		case 2:
			max_keys = MAX_NODE4_KEYS;
			min_key = INT_MIN;
			break;
		case 3:
			max_keys = MAX_NODE8_KEYS;
			min_key = LONG_MIN;
			break;
		}
		if (this->getUsedKeys() != max_keys)
		{
			cout << "[ERROR: int_Enhanced_CSB-Tree.h (EnhancedCSBNode->copySplit()] The node is unfull! Number of max_keys: " << max_keys << "\n";
			return data;
		}

		if (data._key < this->getkey(max_keys/2)) //insert into the left node
		{
			for (unsigned i = 0; i < max_keys/2; i++)
			{
				pNode_left->setkey(i, this->getkey(i));
				new_data_node_left->block_off[i] = cur_data_node->block_off[i];
				new_data_node_left->tuple_off[i] = cur_data_node->tuple_off[i];
			}
			pNode_left->setUsedKeys(max_keys/2);
			for (unsigned i = max_keys/2; i < max_keys; i++)
			{
				pNode_right->setkey(i-max_keys/2, this->getkey(i));
				new_data_node_right->block_off[i-max_keys/2] = cur_data_node->block_off[i];
				new_data_node_right->tuple_off[i-max_keys/2] = cur_data_node->tuple_off[i];
			}
			pNode_right->setUsedKeys(max_keys-max_keys/2);
			pNode_left->originInsert(node_type, data, new_data_node_left);

			data._key = pNode_left->getkey(max_keys/2);
			pNode_left->setkey(max_keys/2, min_key);
			data._block_off = new_data_node_left->block_off[max_keys/2];
			new_data_node_left->block_off[max_keys/2] = INVALID;
			data._tuple_off = new_data_node_left->tuple_off[max_keys/2];
			new_data_node_left->tuple_off[max_keys/2] = INVALID;
			pNode_left->setUsedKeys(max_keys/2);
			return data;
		}
		else	//insert into the right node
		{
			for (unsigned i = 0; i < max_keys/2; i++)
			{
				pNode_left->setkey(i, this->getkey(i));
				new_data_node_left->block_off[i] = cur_data_node->block_off[i];
				new_data_node_left->tuple_off[i] = cur_data_node->tuple_off[i];
			}
			pNode_left->setUsedKeys(max_keys/2);

			unsigned pos = max_keys/2+1;
			for (unsigned i = pos; i < max_keys; i++)
			{
				pNode_right->setkey(i-pos, this->getkey(i));
				new_data_node_right->block_off[i-pos] = cur_data_node->block_off[i];
				new_data_node_right->tuple_off[i-pos] = cur_data_node->block_off[i];
			}
			pNode_right->setUsedKeys(max_keys-pos);
			pNode_right->originInsert(node_type, data, new_data_node_right);

			pos = max_keys/2;
			data._key = this->getkey(pos);
			data._block_off = cur_data_node->block_off[pos];
			data._tuple_off = cur_data_node->tuple_off[pos];
			return data;
		}
	}
};

class EnhancedCSBNode1 :public EnhancedCSBNode {
public:
	EnhancedCSBNode1(){
		for (int i = 0; i < MAX_NODE1_KEYS; i++)
			keys[i] = INVALID_NODE1;
		p_child = NULL;
		used_keys = 0;
	}
	virtual ~EnhancedCSBNode1() {
		delete[] keys;
		p_child = NULL;
		used_keys = 0;
	}

	bool setkey(unsigned short pos, key_type_8 key) {
		if (pos < MAX_NODE1_KEYS)
		{
			keys[pos] = key;
			return true;
		}
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode1->setkey())] The pos " << pos << " is invalid!\n";
		return false;
	}
	key_type_8 getkey(unsigned short pos) {
		if (pos < MAX_NODE1_KEYS)
			return (key_type_8)keys[pos];
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode1->getkey())] The pos " << pos << " is invalid!\n";
		return INVALID_NODE1;
	}
	void* getkeys() { return (void*)keys; }

	void setChild(EnhancedCSBNodeGroup* child_) { p_child = child_; }
	EnhancedCSBNodeGroup* getChild() { return p_child; }

	bool setUsedKeys(unsigned short used_keys_) {
		if (used_keys_ <= MAX_NODE1_KEYS)
		{
			used_keys = used_keys_;
			return true;
		}
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode1->setUsedKeys())] The used_kyes_ is invalid!\n";
		return false;
	}
	unsigned short getUsedKeys() { return used_keys; }

	int SIMDSearch(key_type_8 &key) {
		int ret = 0;
		__m128i* node_keys = (__m128i*)(this->keys);
		__m128i search_key = _mm_set1_epi8(key);
		__m128i result[3];

		result[0] = _mm_cmplt_epi8(*node_keys, search_key);
		node_keys = node_keys+1;
		result[1] = _mm_cmplt_epi8(*node_keys, search_key);
		node_keys = node_keys+1;
		result[2] = _mm_cmplt_epi8(*node_keys, search_key);

		int8_t* com_ret = (int8_t*)result;
		for (unsigned short i = 0; i < used_keys; i++)
			ret = ret + (int)com_ret[i];
		ret = 0-ret;
		return ret;
	}

public:
	EnhancedCSBNodeGroup* p_child;
	key_type_1 keys[MAX_NODE1_KEYS];
	unsigned short used_keys;
};

class EnhancedCSBNode2 :public EnhancedCSBNode {
public:
	EnhancedCSBNode2(){
		for (int i = 0; i < MAX_NODE2_KEYS; i++)
			keys[i] = INVALID_NODE2;
		p_child = NULL;
		used_keys = 0;
	}
	virtual ~EnhancedCSBNode2() {
		delete[] keys;
		p_child = NULL;
		used_keys = 0;
	}

	bool setkey(unsigned short pos, key_type_8 key) {
		if (pos < MAX_NODE2_KEYS)
		{
			keys[pos] = key;
			return true;
		}
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode2->setkey())] The pos " << pos << " is invalid!\n";
		return false;
	}
	key_type_8 getkey(unsigned short pos) {
		if (pos < MAX_NODE2_KEYS)
			return (key_type_8)keys[pos];
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode2->getkey())] The pos " << pos << " is invalid!\n";
		return INVALID_NODE2;
	}
	void* getkeys() { return (void*)keys; }

	void setChild(EnhancedCSBNodeGroup* child_) { p_child = child_; }
	EnhancedCSBNodeGroup* getChild() { return p_child; }

	bool setUsedKeys(unsigned short used_keys_) {
		if (used_keys_ <= MAX_NODE2_KEYS)
		{
			used_keys = used_keys_;
			return true;
		}
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode2->setUsedKeys())] The used_kyes_ is invalid!\n";
		return false;
	}
	unsigned short getUsedKeys() { return used_keys; }

	int SIMDSearch(key_type_8 &key) {
		int ret = 0;
		__m128i* node_keys = (__m128i*)keys;
		__m128i search_key = _mm_set1_epi16((int16_t)key);
		__m128i result[3];

		result[0] = _mm_cmplt_epi16(*node_keys, search_key);
		node_keys = node_keys+1;
		result[1] = _mm_cmplt_epi16(*node_keys, search_key);
		node_keys = node_keys+1;
		result[2] = _mm_cmplt_epi16(*node_keys, search_key);

		int16_t* com_ret = (int16_t*)result;
		for (unsigned short i = 0; i < used_keys; i++)
			ret = ret + (int)com_ret[i];
		ret = 0-ret;
		return ret;
	}

public:
	EnhancedCSBNodeGroup* p_child;
	key_type_2 keys[MAX_NODE2_KEYS];
	unsigned short used_keys;
};

class EnhancedCSBNode4 :public EnhancedCSBNode {
public:
	EnhancedCSBNode4(){
		for (int i = 0; i < MAX_NODE8_KEYS; i++)
			keys[i] = INVALID_NODE4;
		p_child = NULL;
		used_keys = 0;
	}
	virtual ~EnhancedCSBNode4() {
		delete[] keys;
		p_child = NULL;
		used_keys = 0;
	}

	bool setkey(unsigned short pos, key_type_8 key) {
		if (pos < MAX_NODE4_KEYS)
		{
			keys[pos] = key;
			return true;
		}
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode4->setkey())] The pos " << pos << " is invalid!\n";
		return false;
	}
	key_type_8 getkey(unsigned short pos) {
		if (pos < MAX_NODE4_KEYS)
			return (key_type_8)keys[pos];
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode4->getkey())] The pos " << pos << " is invalid!\n";
		return INVALID_NODE4;
	}
	void* getkeys() { return (void*)keys; }

	void setChild(EnhancedCSBNodeGroup* child_) { p_child = child_; }
	EnhancedCSBNodeGroup* getChild() { return p_child; }

	bool setUsedKeys(unsigned short used_keys_) {
		if (used_keys_ <= MAX_NODE4_KEYS)
		{
			used_keys = used_keys_;
			return true;
		}
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode4->setUsedKeys())] The used_kyes_ is invalid!\n";
		return false;
	}
	unsigned short getUsedKeys() { return used_keys; }

	int SIMDSearch(key_type_8 &key) {
		int ret = 0;
		__m128i* node_keys = (__m128i*)keys;
		__m128i search_key = _mm_set1_epi32((int32_t)key);
		__m128i result[3];

		result[0] = _mm_cmplt_epi32(*node_keys, search_key);
		node_keys = node_keys+1;
		result[1] = _mm_cmplt_epi32(*node_keys, search_key);
		node_keys = node_keys+1;
		result[2] = _mm_cmplt_epi32(*node_keys, search_key);

		int32_t* com_ret = (int32_t*)result;
		for (unsigned short i = 0; i < used_keys; i++)
			ret = ret + (int)com_ret[i];
		ret = 0-ret;
		return ret;
	}

public:
	EnhancedCSBNodeGroup* p_child;
	key_type_4 keys[MAX_NODE4_KEYS];
	unsigned short used_keys;
};

class EnhancedCSBNode8 :public EnhancedCSBNode {
public:
	EnhancedCSBNode8(){
		for (int i = 0; i < MAX_NODE8_KEYS; i++)
			keys[i] = INVALID_NODE8;
		p_child = NULL;
		used_keys = 0;
	}
	virtual ~EnhancedCSBNode8() {
		delete[] keys;
		p_child = NULL;
		used_keys = 0;
	}

	bool setkey(unsigned short pos, key_type_8 key) {
		if (pos < MAX_NODE8_KEYS)
		{
			keys[pos] = key;
			return true;
		}
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode8->setkey())] The pos " << pos << " is invalid!\n";
		return false;
	}
	key_type_8 getkey(unsigned short pos) {
		if (pos < MAX_NODE8_KEYS)
			return (key_type_8)keys[pos];
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode8->getkey())] The pos " << pos << " is invalid!\n";
		return INVALID_NODE8;
	}
	void* getkeys() { return (void*)keys; }

	void setChild(EnhancedCSBNodeGroup* child_) { p_child = child_; }
	EnhancedCSBNodeGroup* getChild() { return p_child; }

	bool setUsedKeys(unsigned short used_keys_) {
		if (used_keys_ <= MAX_NODE8_KEYS)
		{
			used_keys = used_keys_;
			return true;
		}
		cout << "[ERROR: Enhanced_CSB-Tree.h(EnhancedCSBNode8->setUsedKeys())] The used_kyes_ is invalid!\n";
		return false;
	}
	unsigned short getUsedKeys() { return used_keys; }

	int SIMDSearch(key_type_8 &key) {
		int ret = 0;
//		__m128i* node_keys = (__m128i*)keys;
//		__m128i search_key = _mm_set1_epi64(key);
//		__m128i result[3];
//
//		result[0] = _mm_cmplt_epi64(*node_keys, search_key);
//		node_keys = node_keys+1;
//		result[1] = _mm_cmplt_epi64(*node_keys, search_key);
//		node_keys = node_keys+1;
//		result[2] = _mm_cmplt_epi64(*node_keys, search_key);
//
//		int64_t* com_ret = (int64_t*)result;
//		for (unsigned short i = 0; i < used_keys; i++)
//			ret = ret + (int)com_ret[i];
//		ret = 0-ret;
		return ret;
	}

public:
	EnhancedCSBNodeGroup* p_child;
	key_type_8 keys[MAX_NODE8_KEYS] __attribute__ ((aligned (16)));
	unsigned short used_keys;
};




/***********************************  CSB Node Group  ***********************************/
class EnhancedCSBNodeGroup
{
public:
	EnhancedCSBNodeGroup():used_nodes(0), nodes(NULL), p_datas(NULL) {}
	EnhancedCSBNodeGroup(unsigned n, int max_keys)
	{
		used_nodes = n;
		p_datas = new data_pointer* [n];
		nodes = new EnhancedCSBNode* [n];
		for (unsigned i = 0; i < n; i++)
		{
			switch (max_keys)
			{
			case MAX_NODE1_KEYS:
				nodes[i] = new EnhancedCSBNode1();
				break;
			case MAX_NODE2_KEYS:
				nodes[i] = new EnhancedCSBNode2();
				break;
			case MAX_NODE4_KEYS:
				nodes[i] = new EnhancedCSBNode4();
				break;
			case MAX_NODE8_KEYS:
				nodes[i] = new EnhancedCSBNode8();
				break;
			}
			p_datas[i] = new data_pointer(max_keys);
		}
	}
	virtual ~EnhancedCSBNodeGroup() {
		for(unsigned i = 0; i < used_nodes; i++)
		{
			nodes[i]->~EnhancedCSBNode();
			delete[] p_datas[i]->block_off;
			delete[] p_datas[i]->tuple_off;
			delete p_datas[i];
		}
	}

	void setNode(unsigned i, EnhancedCSBNode* node, data_pointer* data)
	{
		for (unsigned pos = 0; pos < node->getUsedKeys(); pos++)
		{
			nodes[i]->setkey(pos, node->getkey(pos));
			p_datas[i]->block_off[pos] = data->block_off[pos];
			p_datas[i]->tuple_off[pos] = data->tuple_off[pos];
		}
		nodes[i]->setUsedKeys(node->getUsedKeys());
		nodes[i]->setChild(node->getChild());
	}

	EnhancedCSBNodeGroup* originInsert(int node_type, EnhancedCSBNode* insert_node, data_original &insert_data, EnhancedCSBNodeGroup* new_child_group)
	{
		int max_nodes;
		switch(node_type)
		{
		case 0:
			max_nodes = MAX_NODE1_KEYS+1;
			break;
		case 1:
			max_nodes = MAX_NODE2_KEYS+1;
			break;
		case 2:
			max_nodes = MAX_NODE4_KEYS+1;
			break;
		case 3:
			max_nodes = MAX_NODE8_KEYS+1;
			break;
		}
		if (this->used_nodes == max_nodes)
		{
			cout << "[ERROR: int_Enhanced_CSB-Tree.h (EnhancedCSBNodeGroup->Insert())] The node group is already full! Number of nodes: " << max_nodes << "\n";
			return NULL;
		}
		EnhancedCSBNodeGroup* new_group = new EnhancedCSBNodeGroup(this->used_nodes+1, max_nodes-1);
		unsigned cur_off = 0;
		for (; cur_off <= this->used_nodes; cur_off++)
		{
			new_group->setNode(cur_off, this->nodes[cur_off], this->p_datas[cur_off]);
			if (this->nodes[cur_off] == insert_node)
				break;
		}
		insert_data = new_group->nodes[cur_off]->originSplit(node_type, new_group->nodes[cur_off+1], insert_data, new_group->p_datas[cur_off], new_group->p_datas[cur_off+1]);
		new_group->nodes[++cur_off]->setChild(new_child_group);
		for (; cur_off < this->used_nodes; cur_off++)
			new_group->setNode(cur_off+1, this->nodes[cur_off], this->p_datas[cur_off]);
		return new_group;
	}

	EnhancedCSBNodeGroup* copyInsert(int node_type, EnhancedCSBNode* insert_node, data_original &insert_data, EnhancedCSBNodeGroup* new_child_group)
	{
		int max_nodes;
		switch(node_type)
		{
		case 0:
			max_nodes = MAX_NODE1_KEYS+1;
			break;
		case 1:
			max_nodes = MAX_NODE2_KEYS+1;
			break;
		case 2:
			max_nodes = MAX_NODE4_KEYS+1;
			break;
		case 3:
			max_nodes = MAX_NODE8_KEYS+1;
			break;
		}
		if (this->used_nodes == max_nodes)
		{
			cout << "[ERROR File " << __FILE__ << " line " << __LINE__ << " (" << __func__ << ")]: The node group is already full! Number of used nodes: " << max_nodes << endl;
			return NULL;
		}
		EnhancedCSBNodeGroup* new_group = new EnhancedCSBNodeGroup(this->used_nodes+1, max_nodes-1);
		unsigned cur_off = 0;
		for (; cur_off <= this->used_nodes; cur_off++)
		{
			new_group->setNode(cur_off, this->nodes[cur_off], this->p_datas[cur_off]);
			if (this->nodes[cur_off] == insert_node)
				break;
		}
		insert_data = new_group->nodes[cur_off]->originSplit(node_type, new_group->nodes[cur_off+1], insert_data, new_group->p_datas[cur_off], new_group->p_datas[cur_off+1]);
		new_group->nodes[++cur_off]->setChild(new_child_group);
		for (; cur_off < this->used_nodes; cur_off++)
			new_group->setNode(cur_off+1, this->nodes[cur_off], this->p_datas[cur_off]);
		return new_group;
	}

	EnhancedCSBNodeGroup* originSplit(int node_type, EnhancedCSBNode* insert_node, data_original &insert_data, EnhancedCSBNodeGroup* new_child_group)
	{
		int max_nodes;
		EnhancedCSBNode* new_node;
		switch(node_type)
		{
		case 0:
			max_nodes = MAX_NODE1_KEYS+1;
			new_node = new EnhancedCSBNode1();
			break;
		case 1:
			max_nodes = MAX_NODE2_KEYS+1;
			new_node = new EnhancedCSBNode2();
			break;
		case 2:
			max_nodes = MAX_NODE4_KEYS+1;
			new_node = new EnhancedCSBNode4();
			break;
		case 3:
			max_nodes = MAX_NODE8_KEYS+1;
			new_node = new EnhancedCSBNode8();
			break;
		}
		if (this->used_nodes != max_nodes)
		{
			cout << "[ERROR: int_Enhanced_CSB-Tree.h (EnhancedCSBNodeGroup->SplitInsert())] The node group is not full! Used nodes: " << this->used_nodes << "/" << max_nodes << "\n";
			return NULL;
		}
		unsigned node_group_size1 = (max_nodes+1)/2;
		unsigned node_group_size2 = max_nodes+1-node_group_size1;
		EnhancedCSBNodeGroup* new_group = new EnhancedCSBNodeGroup(node_group_size2, max_nodes-1);

		data_pointer* new_data = new data_pointer(max_nodes-1);
		unsigned pos = 0;
		for (; this->nodes[pos] != insert_node; pos++);
		insert_data = insert_node->originSplit(node_type, new_node, insert_data, this->p_datas[pos], new_data);
		new_node->setChild(new_child_group);

		if (pos < node_group_size1-1)
		{
			for (unsigned i = node_group_size1-1; i < this->used_nodes; i++)
				new_group->setNode(i-node_group_size1+1, this->nodes[i], this->p_datas[i]);
			for (unsigned i = node_group_size1-2; i > pos; i--)
				this->setNode(i+1, this->nodes[i], this->p_datas[i]);
			this->setNode(pos+1, new_node, new_data);
		}
		else
		{
			for (unsigned i = node_group_size1; i <= pos; i++)
				new_group->setNode(i-node_group_size1, this->nodes[i], this->p_datas[i]);
			pos += 1;
			new_group->setNode(pos-node_group_size1, new_node, new_data);
			for(; pos < this->used_nodes; pos++)
				new_group->setNode(pos+1-node_group_size1, this->nodes[pos], this->p_datas[pos]);
		}
		this->used_nodes = node_group_size1;
		return new_group;
	}

	EnhancedCSBNodeGroup* copySplit(int node_type, EnhancedCSBNode* insert_node, data_original &insert_data, EnhancedCSBNodeGroup* new_child_group_left, EnhancedCSBNodeGroup* new_child_group_right)
	{

		/**********************************************************************************/
		int max_nodes;
		EnhancedCSBNode* new_node_left, *new_node_right;
		switch(node_type)
		{
		case 0:
			max_nodes = MAX_NODE1_KEYS+1;
			new_node_left = new EnhancedCSBNode1();
			new_node_right = new EnhancedCSBNode1();
			break;
		case 1:
			max_nodes = MAX_NODE2_KEYS+1;
			new_node_left = new EnhancedCSBNode2();
			new_node_right = new EnhancedCSBNode2();
			break;
		case 2:
			max_nodes = MAX_NODE4_KEYS+1;
			new_node_left = new EnhancedCSBNode4();
			new_node_right = new EnhancedCSBNode4();
			break;
		case 3:
			max_nodes = MAX_NODE8_KEYS+1;
			new_node_left = new EnhancedCSBNode8();
			new_node_right = new EnhancedCSBNode8();
			break;
		}
		if (this->used_nodes != max_nodes)
		{
			cout << "[ERROR File " << __FILE__ << " line " << __LINE__ << " (" << __func__ << ")]: The node group is not full! Used nodes: " << this->used_nodes << "/" << max_nodes << endl;
			return NULL;
		}
		unsigned node_group_size1 = (max_nodes+1)/2;
		unsigned node_group_size2 = max_nodes+1-node_group_size1;
		EnhancedCSBNodeGroup* new_group_left = new EnhancedCSBNodeGroup(node_group_size1, max_nodes-1);
		EnhancedCSBNodeGroup* new_group_right = new EnhancedCSBNodeGroup(node_group_size2, max_nodes-1);

		data_pointer* new_data_left = new data_pointer(max_nodes-1);
		data_pointer* new_data_right = new data_pointer(max_nodes-1);
		unsigned pos = 0;
		for (; this->nodes[pos] != insert_node; pos++);
		insert_data = insert_node->copySplit(node_type, new_node_left, new_node_right, insert_data, this->p_datas[pos], new_data_left, new_data_right);
		new_node_left->setChild(new_child_group_left);
		new_node_right->setChild(new_child_group_right);

		if (pos < node_group_size1-1)
		{
			for (unsigned i = node_group_size1-1; i < this->used_nodes; i++)
				new_group_right->setNode(i-node_group_size1+1, this->nodes[i], this->p_datas[i]);

			for (unsigned i = node_group_size1-2; i > pos; i--)
				new_group_left->setNode(i+1, this->nodes[i], this->p_datas[i]);
			new_group_left->setNode(pos+1, new_node_right, new_data_right);
			new_group_left->setNode(pos, new_node_left, new_data_left);
			for (unsigned i = 0; i < pos; i++)
				new_group_left->setNode(i, this->nodes[i], this->p_datas[i]);
		}
		else
		{
			for (unsigned i = 0; i < node_group_size1; i++)
				new_group_left->setNode(i, this->nodes[i], this->p_datas[i]);

			for (unsigned i = node_group_size1; i <= pos; i++)
				new_group_right->setNode(i-node_group_size1, this->nodes[i], this->p_datas[i]);
			pos += 1;
			new_group_right->setNode(pos-node_group_size1, new_node_left, new_data_left);
			new_group_right->setNode(pos+1-node_group_size1, new_node_right, new_data_right);
			pos += 1;
			for(; pos < this->used_nodes; pos++)
				new_group_right->setNode(pos+1-node_group_size1, this->nodes[pos], this->p_datas[pos]);
		}
		new_child_group_left = new_group_left;
		new_child_group_right = new_group_right;
		return new_group_right;
	}

public:
	EnhancedCSBNode** nodes;
	data_pointer** p_datas;
	unsigned short used_nodes;
};


/***********************************  CSB Tree  ***********************************/
class EnhancedCSBTree {
public:
	struct node_info{
		node_info():divide_datas(NULL) { memset(num_of_keys, 0, 16); }
		node_info(unsigned n)
		{
			divide_datas = new data_original* [4];
			for (unsigned i = 0; i < 4; i++)
			{
				divide_datas[i] = new data_original [n];
				num_of_keys[i] = 0;
			}
		}
		data_original** divide_datas;
		unsigned num_of_keys[4];
	};
	struct enhanced_csb_node_data {
		enhanced_csb_node_data():e_node(NULL), p_data(NULL) {}
		enhanced_csb_node_data(EnhancedCSBNode* node, data_pointer* data):e_node(node), p_data(data) {}
		EnhancedCSBNode* e_node;
		data_pointer* p_data;
	};
public:
	EnhancedCSBTree() {
		for (unsigned i = 0; i < 4; i++)
		{
			root[i] = NULL;
			depth[i] = 0;
		}
	}
	virtual ~EnhancedCSBTree() {
		//TODO: delete all nodes in the csb-tree
		for (unsigned i = 0; i < 4; i++)
		{
			root[i] = NULL;
			depth[i] = 0;
		}
	}
	void BulkLoad(data_original* cur_aray, unsigned cur_aray_num);
	data_pointer* Search(key_type_8 &key);
	data_pointer* SearchSIMD(key_type_8 &key);
	bool Insert(data_original &data);
	bool copyInsert(data_original &data);
	enhanced_csb_node_data* SearchInsertNode(key_type_8 key, int node_type, stack <enhanced_csb_node_data*> &insert_path);

	//For testing
	void printTree();
	unsigned* calculateNodes();
public:
	EnhancedCSBNodeGroup* root[4];
	unsigned depth[4];
	SpineLock lock[4];

	static unsigned long counter_layer;

private:
	int makeCurrentNodeGroup(int node_type, data_original* cur_aray, int cur_aray_num, EnhancedCSBNodeGroup** cur_group, data_original* upper_aray, EnhancedCSBNodeGroup** child_group, int child_group_num);
	node_info* calNodeDivision(data_original* cur_aray, int cur_aray_num);
};



unsigned long EnhancedCSBTree::counter_layer = 0;

void EnhancedCSBTree::BulkLoad(data_original* cur_aray, unsigned cur_aray_num)
{
	if (cur_aray_num == 0)
		return;
	else
	{
		node_info* datas = calNodeDivision(cur_aray, cur_aray_num);
		int node_keys[4] = {MAX_NODE1_KEYS, MAX_NODE2_KEYS, MAX_NODE4_KEYS, MAX_NODE8_KEYS};

		for (int i = 0; i < 4; i++)
		{
			cur_aray = datas->divide_datas[i];
			cur_aray_num = datas->num_of_keys[i];
			int cur_group_num = ceil(ceil((double)(cur_aray_num+1)/(double)(node_keys[i]+1))/(double)(node_keys[i]+1));
			EnhancedCSBNodeGroup** cur_group = new EnhancedCSBNodeGroup* [cur_group_num];
			int upper_aray_num = ceil((double)(cur_aray_num+1)/(double)(node_keys[i]+1))-1;
			data_original* upper_aray = new data_original[upper_aray_num];
			EnhancedCSBNodeGroup** child_group = NULL;
			int child_group_num = 0;
			while (cur_aray_num != 0)
			{
				upper_aray_num = makeCurrentNodeGroup(i, cur_aray, cur_aray_num, cur_group, upper_aray, child_group, child_group_num);
				delete[] cur_aray;
				cur_aray = upper_aray;
				cur_aray_num = upper_aray_num;
				child_group = cur_group;
				child_group_num = cur_group_num;
				upper_aray = new data_original [upper_aray_num];
				cur_group_num = ceil((double)child_group_num/(double(node_keys[i]+1)));
				cur_group = new EnhancedCSBNodeGroup* [cur_group_num];
			}
		}
	}
}


int EnhancedCSBTree::makeCurrentNodeGroup(int node_type, data_original* cur_aray, int cur_aray_num, EnhancedCSBNodeGroup** cur_group, data_original* upper_aray, EnhancedCSBNodeGroup** child_group, int child_group_num)
{
	int max_keys = INVALID;
	switch (node_type)
	{
	case 0:
		max_keys = MAX_NODE1_KEYS;
		break;
	case 1:
		max_keys = MAX_NODE2_KEYS;
		break;
	case 2:
		max_keys = MAX_NODE4_KEYS;
		break;
	case 3:
		max_keys = MAX_NODE8_KEYS;
		break;
	}
	depth[node_type] += 1;
	int upper_aray_num = 0;

	if (cur_aray_num <= max_keys)	//the root level, but not leaf
	{
		cur_group[0] = new EnhancedCSBNodeGroup(1, max_keys);
		for (unsigned i = 0; i < cur_aray_num; i++)
		{
			cur_group[0]->nodes[0]->setkey(i, cur_aray[i]._key);
			cur_group[0]->p_datas[0]->block_off[i] = cur_aray[i]._block_off;
			cur_group[0]->p_datas[0]->tuple_off[i] = cur_aray[i]._tuple_off;
		}
		cur_group[0]->nodes[0]->setUsedKeys(cur_aray_num);

		//set the parents-children relationship
		if (depth[node_type] != 1)
			cur_group[0]->nodes[0]->setChild(child_group[0]);

		//set the root node group
		this->root[node_type] = cur_group[0];
		return upper_aray_num;
	}


	if (child_group_num == 0)
		child_group_num = ceil((double)(cur_aray_num+1)/(double)(max_keys+1));

	int cur_group_num = ceil((double)child_group_num/(double)(max_keys+1));
	int cur_group_counter = 0;
	if (cur_group_num >= 2)
	{
		while (cur_group_counter < cur_group_num-2)
		{
			cur_group[cur_group_counter] = new EnhancedCSBNodeGroup(max_keys+1, max_keys);
			cur_group_counter += 1;
		}
		int rest = child_group_num-(cur_group_num-1)*(max_keys+1);
		if (rest >= max_keys/2+1)
		{
			cur_group[cur_group_counter] = new EnhancedCSBNodeGroup(max_keys+1, max_keys);
			cur_group[cur_group_counter+1] = new EnhancedCSBNodeGroup(rest, max_keys);
		}
		else
		{
			rest += (max_keys+1);
			if (rest%2 == 0)
				cur_group[cur_group_counter] = new EnhancedCSBNodeGroup(rest/2, max_keys);
			else
				cur_group[cur_group_counter] = new EnhancedCSBNodeGroup(rest/2+1, max_keys);
			cur_group[cur_group_counter+1] = new EnhancedCSBNodeGroup(rest/2, max_keys);
		}
	}
	else
		cur_group[cur_group_counter] = new EnhancedCSBNodeGroup(child_group_num, max_keys);

	int cur_aray_counter = 0;
	for (cur_group_counter = 0; cur_group_counter < cur_group_num-1; cur_group_counter++)
	{
		for (unsigned j = 0; j < cur_group[cur_group_counter]->used_nodes; j++)
		{
			unsigned k = 0;
			for (k = 0; k < max_keys; k++)
			{
				cur_group[cur_group_counter]->nodes[j]->setkey(k, cur_aray[cur_aray_counter]._key);
				cur_group[cur_group_counter]->p_datas[j]->block_off[k] = cur_aray[cur_aray_counter]._block_off;
				cur_group[cur_group_counter]->p_datas[j]->tuple_off[k] = cur_aray[cur_aray_counter]._tuple_off;
				cur_aray_counter += 1;
			}
			cur_group[cur_group_counter]->nodes[j]->setUsedKeys(k);
			upper_aray[upper_aray_num++] = cur_aray[cur_aray_counter++];
		}
	}
	int j = 0;
	for (j = 0; j < cur_group[cur_group_counter]->used_nodes-2; j++)
	{
		int k = 0;
		for (k = 0; k < max_keys; k++)
		{
			cur_group[cur_group_counter]->nodes[j]->setkey(k, cur_aray[cur_aray_counter]._key);
			cur_group[cur_group_counter]->p_datas[j]->block_off[k] = cur_aray[cur_aray_counter]._block_off;
			cur_group[cur_group_counter]->p_datas[j]->tuple_off[k] = cur_aray[cur_aray_counter]._tuple_off;
			cur_aray_counter += 1;
		}
		cur_group[cur_group_counter]->nodes[j]->setUsedKeys(k);
		upper_aray[upper_aray_num++] = cur_aray[cur_aray_counter++];
	}
	int rest = cur_aray_num%(max_keys+1);
	if (rest >= max_keys/2)
	{
		cur_group[cur_group_counter]->nodes[j]->setUsedKeys(max_keys);
		cur_group[cur_group_counter]->nodes[j+1]->setUsedKeys(rest);
	}
	else
	{
		rest += max_keys;
		cur_group[cur_group_counter]->nodes[j]->setUsedKeys(rest-rest/2);
		cur_group[cur_group_counter]->nodes[j+1]->setUsedKeys(rest/2);
	}

	for(; j < cur_group[cur_group_counter]->used_nodes; j++)
	{
		for (int k = 0; k < cur_group[cur_group_counter]->nodes[j]->getUsedKeys(); k++)
		{
			cur_group[cur_group_counter]->nodes[j]->setkey(k, cur_aray[cur_aray_counter]._key);
			cur_group[cur_group_counter]->p_datas[j]->block_off[k] = cur_aray[cur_aray_counter]._block_off;
			cur_group[cur_group_counter]->p_datas[j]->tuple_off[k] = cur_aray[cur_aray_counter]._tuple_off;
			cur_aray_counter += 1;
		}
		if (cur_aray_counter < cur_aray_num)
			upper_aray[upper_aray_num++] = cur_aray[cur_aray_counter++];
	}

	//set the parent-children relationship
	if (depth[node_type] != 1)
	{
		int child_group_counter = 0;
		for (cur_group_counter = 0; cur_group_counter < cur_group_num; cur_group_counter++)
		{
			for (int j = 0; j < cur_group[cur_group_counter]->used_nodes; j++)
			{
				cur_group[cur_group_counter]->nodes[j]->setChild(child_group[child_group_counter]);
				child_group_counter += 1;
			}
		}
	}

	return upper_aray_num;
}

EnhancedCSBTree::node_info* EnhancedCSBTree::calNodeDivision(data_original* cur_aray, int cur_aray_num)
{
	node_info* ret = new node_info(cur_aray_num);
	for (unsigned cur_off = 0; cur_off < cur_aray_num; cur_off++)
	{
		if (cur_aray[cur_off]._key < INVALID_NODE4 || cur_aray[cur_off]._key > MAX_KEY4)
			ret->divide_datas[3][ret->num_of_keys[3]++] = cur_aray[cur_off];
		else if (cur_aray[cur_off]._key < INVALID_NODE2 || cur_aray[cur_off]._key > MAX_KEY2)
			ret->divide_datas[2][ret->num_of_keys[2]++] = cur_aray[cur_off];
		else if (cur_aray[cur_off]._key < INVALID_NODE1 || cur_aray[cur_off]._key > MAX_KEY1)
			ret->divide_datas[1][ret->num_of_keys[1]++] = cur_aray[cur_off];
		else
			ret->divide_datas[0][ret->num_of_keys[0]++] = cur_aray[cur_off];
	}
//	delete cur_aray;
	return ret;
}


data_pointer* EnhancedCSBTree::Search(key_type_8 &key)
{
	int node_type = INVALID;
	if (key < INVALID_NODE4 || key > MAX_KEY4)
		node_type = 3;
	else if (key < INVALID_NODE2 || key > MAX_KEY2)
		node_type = 2;
	else if (key < INVALID_NODE1 || key > MAX_KEY1)
		node_type = 1;
	else
		node_type = 0;
	if (this->root[node_type] == NULL)
		return NULL;
	data_pointer* ret = new data_pointer(1);

	EnhancedCSBNodeGroup* cur_group = this->root[node_type];	counter_layer += 1;
	unsigned cur_off = 0;
	EnhancedCSBNode* cur_node = cur_group->nodes[cur_off];
	int cur_i = 0;
	for (unsigned cur_depth = 0; cur_depth < this->depth[node_type]; cur_depth++)
	{

		for (cur_i = 0; (cur_i < cur_node->getUsedKeys()) && (key > cur_node->getkey(cur_i)); cur_i++);

		//find equal => return
		if (cur_i < cur_node->getUsedKeys() && key == cur_node->getkey(cur_i))
		{
			ret->block_off[0] = cur_group->p_datas[cur_off]->block_off[cur_i];
			ret->tuple_off[0] = cur_group->p_datas[cur_off]->tuple_off[cur_i];
			return ret;
		}

		//search the child-layer of cur_node
		if (cur_depth < this->depth[node_type]-1)
		{	counter_layer += 1;
			cur_group = cur_node->getChild();
			cur_off = cur_i;
			cur_node = cur_group->nodes[cur_off];
		}
	}
	return NULL;
}


data_pointer* EnhancedCSBTree::SearchSIMD(key_type_8 &key)
{
	int node_type = INVALID;
	if (key < INVALID_NODE4 || key > MAX_KEY4)
		node_type = 3;
	else if (key < INVALID_NODE2 || key > MAX_KEY2)
		node_type = 2;
	else if (key < INVALID_NODE1 || key > MAX_KEY1)
		node_type = 1;
	else
		node_type = 0;
	data_pointer* ret = new data_pointer(1);

	EnhancedCSBNodeGroup* cur_group = this->root[node_type];	counter_layer += 1;
	unsigned cur_off = 0;
	EnhancedCSBNode* cur_node = cur_group->nodes[cur_off];

	for (unsigned cur_depth = 0; cur_depth < this->depth[node_type]; cur_depth++)
	{
		int cur_i = cur_node->SIMDSearch(key);

		//find equal => return
		if ((cur_i < cur_node->getUsedKeys()) && (key == cur_node->getkey(cur_i)))
		{
			ret->block_off[0] = cur_group->p_datas[cur_off]->block_off[cur_i];
			ret->tuple_off[0] = cur_group->p_datas[cur_off]->tuple_off[cur_i];
			return ret;
		}

		//search the child-layer of cur_node
		if (cur_depth < this->depth[node_type]-1)
		{	counter_layer += 1;
			cur_group = cur_node->getChild();
			cur_off = cur_i;
			cur_node = cur_group->nodes[cur_off];
		}
	}
	return NULL;
}


bool EnhancedCSBTree::Insert(data_original &data)
{
//	if (Search(data._key) != NULL)
//		return false;
	int node_type = INVALID;
	int max_keys = INVALID;
	if (data._key < INVALID_NODE4 || data._key > MAX_KEY4)
	{
		node_type = 3;
		max_keys = MAX_NODE8_KEYS;
	}
	else if (data._key < INVALID_NODE2 || data._key > MAX_KEY2)
	{
		node_type = 2;
		max_keys = MAX_NODE4_KEYS;
	}
	else if (data._key < INVALID_NODE1 || data._key > MAX_KEY1)
	{
		node_type = 1;
		max_keys = MAX_NODE2_KEYS;
	}
	else
	{
		node_type = 0;
		max_keys = MAX_NODE1_KEYS;
	}
	if (this->root[node_type] == NULL)
	{
		this->depth[node_type] = 1;
		EnhancedCSBNodeGroup* leaf_group = new EnhancedCSBNodeGroup(1, max_keys);
		leaf_group->nodes[0]->setkey(0, data._key);
		leaf_group->p_datas[0]->block_off[0] = data._block_off;
		leaf_group->p_datas[0]->tuple_off[0] = data._tuple_off;
		leaf_group->nodes[0]->setUsedKeys(1);
		leaf_group->nodes[0]->setChild(NULL);
		this->root[node_type] = leaf_group;
		return true;
	}
	stack <enhanced_csb_node_data*> insert_path;
	enhanced_csb_node_data* insert_node = SearchInsertNode(data._key, node_type, insert_path);
	EnhancedCSBNodeGroup* new_child = NULL;
	enhanced_csb_node_data* p_insert_node;
	while (!insert_path.empty())
	{
		p_insert_node = insert_path.top();
		insert_path.pop();
		if (insert_node->e_node->getUsedKeys() < max_keys)
		{
			insert_node->e_node->setChild(new_child);
			return insert_node->e_node->originInsert(node_type, data, insert_node->p_data);
		}
		if (p_insert_node->e_node->getUsedKeys() < max_keys)
			new_child = p_insert_node->e_node->getChild()->originInsert(node_type, insert_node->e_node, data, new_child);
		else
			new_child = p_insert_node->e_node->getChild()->originSplit(node_type, insert_node->e_node, data, new_child);
			insert_node = p_insert_node;
	}
	if (insert_node->e_node->getUsedKeys() < max_keys)
	{
		insert_node->e_node->setChild(new_child);
		return insert_node->e_node->originInsert(node_type, data, insert_node->p_data);
	}
	new_child = this->root[node_type]->originInsert(node_type, insert_node->e_node, data, new_child);
	EnhancedCSBNodeGroup* root_group = new EnhancedCSBNodeGroup(1, max_keys);
	root_group->nodes[0]->setkey(0, data._key);
	root_group->p_datas[0]->block_off[0] = data._block_off;
	root_group->p_datas[0]->tuple_off[0] = data._tuple_off;
	root_group->nodes[0]->setUsedKeys(1);
	root_group->nodes[0]->setChild(new_child);
	this->root[node_type] = root_group;
	this->depth[node_type] += 1;
	return true;
}

bool EnhancedCSBTree::copyInsert(data_original &data)
{
//	if (Search(data._key) != NULL)
//		return false;
	int node_type = INVALID;
	int max_keys = INVALID;
	if (data._key < INVALID_NODE4 || data._key > MAX_KEY4)
	{
		node_type = 3;
		max_keys = MAX_NODE8_KEYS;
	}
	else if (data._key < INVALID_NODE2 || data._key > MAX_KEY2)
	{
		node_type = 2;
		max_keys = MAX_NODE4_KEYS;
	}
	else if (data._key < INVALID_NODE1 || data._key > MAX_KEY1)
	{
		node_type = 1;
		max_keys = MAX_NODE2_KEYS;
	}
	else
	{
		node_type = 0;
		max_keys = MAX_NODE1_KEYS;
	}
	if (this->root[node_type] == NULL)
	{
		this->depth[node_type] = 1;
		EnhancedCSBNodeGroup* leaf_group = new EnhancedCSBNodeGroup(1, max_keys);
		leaf_group->nodes[0]->setkey(0, data._key);
		leaf_group->p_datas[0]->block_off[0] = data._block_off;
		leaf_group->p_datas[0]->tuple_off[0] = data._tuple_off;
		leaf_group->nodes[0]->setUsedKeys(1);
		leaf_group->nodes[0]->setChild(NULL);
		this->root[node_type] = leaf_group;
		return true;
	}
	stack <enhanced_csb_node_data*> insert_path;
	enhanced_csb_node_data* insert_node = SearchInsertNode(data._key, node_type, insert_path);
	EnhancedCSBNodeGroup* new_child = NULL;
	enhanced_csb_node_data* p_insert_node;
	while (!insert_path.empty())
	{
		p_insert_node = insert_path.top();
		insert_path.pop();
		if (insert_node->e_node->getUsedKeys() < max_keys)
		{
			insert_node->e_node->setChild(new_child);
			return insert_node->e_node->originInsert(node_type, data, insert_node->p_data);
		}
		if (p_insert_node->e_node->getUsedKeys() < max_keys)
			new_child = p_insert_node->e_node->getChild()->originInsert(node_type, insert_node->e_node, data, new_child);
		else
			new_child = p_insert_node->e_node->getChild()->originSplit(node_type, insert_node->e_node, data, new_child);
			insert_node = p_insert_node;
	}
	if (insert_node->e_node->getUsedKeys() < max_keys)
	{
		insert_node->e_node->setChild(new_child);
		return insert_node->e_node->originInsert(node_type, data, insert_node->p_data);
	}
	new_child = this->root[node_type]->originInsert(node_type, insert_node->e_node, data, new_child);
	EnhancedCSBNodeGroup* root_group = new EnhancedCSBNodeGroup(1, max_keys);
	root_group->nodes[0]->setkey(0, data._key);
	root_group->p_datas[0]->block_off[0] = data._block_off;
	root_group->p_datas[0]->tuple_off[0] = data._tuple_off;
	root_group->nodes[0]->setUsedKeys(1);
	root_group->nodes[0]->setChild(new_child);
	this->root[node_type] = root_group;
	this->depth[node_type] += 1;
	return true;
}

EnhancedCSBTree::enhanced_csb_node_data* EnhancedCSBTree::SearchInsertNode(key_type_8 key, int node_type, stack <enhanced_csb_node_data*> &insert_path)
{
	unsigned cur_offset = 0;
	enhanced_csb_node_data* ret = new enhanced_csb_node_data();
	ret->e_node = this->root[node_type]->nodes[0];
	ret->p_data = this->root[node_type]->p_datas[0];
	for (unsigned cur_depth = 1; cur_depth < this->depth[node_type]; cur_depth++)
	{
		enhanced_csb_node_data* record = new enhanced_csb_node_data(ret->e_node, ret->p_data);
		insert_path.push(record);
		for (cur_offset = 0; (cur_offset < ret->e_node->getUsedKeys()) && (key >= ret->e_node->getkey(cur_offset)); cur_offset++);
		ret->p_data = ret->e_node->getChild()->p_datas[cur_offset];
		ret->e_node = ret->e_node->getChild()->nodes[cur_offset];
	}
	return ret;
}

void EnhancedCSBTree::printTree()
{
	cout << "\n\n-------------------------Print the cache sensitive B tree-------------------------\n";
	for (int node_type = 0; node_type < 4; node_type++)
	{
		cout << "\n*******************************Node Type: " << node_type << "*************************************\n";
		if (this->root[node_type] == NULL)
		{
			cout << "[OUTPUT: EnhancedCSBTree.h->printTree()]: The index tree is empty!\n\n";
			continue;
		}

		vector<EnhancedCSBNodeGroup* > current_level;
		vector<EnhancedCSBNodeGroup* > lower_level;
		lower_level.clear();

		int cur_depth = 1;
		lower_level.push_back(this->root[node_type]);

		while (cur_depth <= depth[node_type])
		{
			current_level.clear();
			while (lower_level.size() != NULL)
			{
				current_level.push_back(lower_level.back());
				lower_level.pop_back();
			}
			lower_level.clear();
			if (cur_depth == 1)
				cout << "\n---------------------Root Layer (depth: " << cur_depth++ << ")---------------------\n";
			else
				cout << "\n---------------------Internal Layer (depth: " << cur_depth++ << ")---------------------\n";
			cout << "Node Group Num: " << current_level.size() << endl;

			unsigned i = 0;
			while (current_level.size() != 0)
			{
				cout << "NodeGroup: " << i++ << "\t";
				cout << "Used Nodes: " << current_level.back()->used_nodes << endl;
				for (unsigned j = 0; j < current_level.back()->used_nodes; j++)
				{
					lower_level.push_back(current_level.back()->nodes[j]->getChild());

					cout << "Node: " << j << "\t";
					cout << "Used Keys: " << current_level.back()->nodes[j]->getUsedKeys() << endl;
					for (unsigned k = 0; k < current_level.back()->nodes[j]->getUsedKeys(); k++)
					{
						cout << current_level.back()->nodes[j]->getkey(k) << " ";
						cout << "<" << current_level.back()->p_datas[j]->block_off[k] << ", ";
						cout << current_level.back()->p_datas[j]->tuple_off[k] << ">\t";
					}
					cout << endl;
				}
				current_level.pop_back();
			}
		}
	}
	cout << "\n\n---------------------Print the cache sensitive B tree finished---------------------\n\n";
}

unsigned* EnhancedCSBTree::calculateNodes()
{
	unsigned* ret = new unsigned [4];
	for (int node_type = 0; node_type < 4; node_type++)
	{
		ret[node_type] = 0;
		if (this->root[node_type] == NULL)
			continue;

		vector<EnhancedCSBNodeGroup* > current_level;
		vector<EnhancedCSBNodeGroup* > lower_level;
		lower_level.clear();

		int cur_depth = 1;
		lower_level.push_back(this->root[node_type]);

		while (cur_depth <= depth[node_type])
		{
			current_level.clear();
			while (lower_level.size() != NULL)
			{
				current_level.push_back(lower_level.back());
				lower_level.pop_back();
			}
			lower_level.clear();
			cur_depth++;

			while (current_level.size() != 0)
			{
				for (unsigned j = 0; j < current_level.back()->used_nodes; j++)
					lower_level.push_back(current_level.back()->nodes[j]->getChild());
				ret[node_type] += current_level.back()->used_nodes;
				current_level.pop_back();
			}
		}
	}
	return ret;
}

#endif /* ENHANCED_CSB_TREE_H_ */
