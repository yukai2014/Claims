/*
 * CSB-Tree.h
 *
 *  Created on: Jul 9, 2014
 *      Author: scdong
 */

#ifndef CSB_TREE_H_
#define CSB_TREE_H_

#include <assert.h>
#include <math.h>
#include <stack>
#include <emmintrin.h>
#include <iostream>
#include <vector>
#include "indexStructures.h"
using namespace std;

#define MAX_KEYS 6
#define INVALID_KEY 0

typedef unsigned long key_type;

class CSBNodeGroup;

/***********************************  CSB Node  ***********************************/
class CSBNode
{
public:
	CSBNode():used_keys(0), p_child(NULL) {
		for (int i = 0; i < MAX_KEYS; i++)
			keys[i] = INVALID_KEY;
	}
	~CSBNode() {
		used_keys = 0;
	}

	bool Insert(data_original value, data_pointer* p_data);
	data_original SplitInsert(CSBNode* pNode, data_original data, data_pointer* cur_data_node, data_pointer* new_data_node);

public:
	key_type keys[MAX_KEYS];
	CSBNodeGroup* p_child;
	unsigned short used_keys;
};


/***********************************  CSB Node Group  ***********************************/
class CSBNodeGroup
{
public:
	CSBNodeGroup():used_nodes(0), nodes(NULL), p_datas(NULL) {}
	CSBNodeGroup(unsigned n)
	{
		if (n <= MAX_KEYS+1)
		{
			used_nodes = n;
			p_datas = new data_pointer* [n];
			nodes = new CSBNode* [n];
			for (unsigned i = 0; i < n; i++)
			{
				nodes[i] = new CSBNode();
				p_datas[i] = new data_pointer(MAX_KEYS);
			}
		}
	}
	virtual ~CSBNodeGroup() {
		for(unsigned i = 0; i < used_nodes; i++)
		{
			nodes[i]->~CSBNode();
			delete[] p_datas[i]->block_off;
			delete[] p_datas[i]->tuple_off;
			delete p_datas[i];
		}
	}

	CSBNodeGroup* Insert(CSBNode* insert_node, data_original &insert_data, CSBNodeGroup* new_child_group);
	CSBNodeGroup* SplitInsert(CSBNode* insert_node, data_original &insert_data, CSBNodeGroup* new_child_group);

private:
	void setNode(unsigned i, CSBNode* node, data_pointer* data)
	{
		for (unsigned pos = 0; pos < node->used_keys; pos++)
		{
			nodes[i]->keys[pos] = node->keys[pos];
			p_datas[i]->block_off[pos] = data->block_off[pos];
			p_datas[i]->tuple_off[pos] = data->tuple_off[pos];
		}
		nodes[i]->used_keys = node->used_keys;
		nodes[i]->p_child = node->p_child;
	}

public:
	CSBNode** nodes;
	unsigned short used_nodes;
	data_pointer** p_datas;
};


/***********************************  CSB Tree  ***********************************/
class CSBTree {
public:
	struct csb_node_data {
		csb_node_data():csb_node(NULL), csb_data(NULL) {}
		csb_node_data(CSBNode* node, data_pointer* data): csb_node(node), csb_data(data) {}
		CSBNode* csb_node;
		data_pointer* csb_data;
	};
public:
	CSBTree() {
		root = NULL;
		depth = 0;
	}
	virtual ~CSBTree() {
		//TODO: delete all nodes in the csb-tree
		root = NULL;
		depth = 0;
	}
	void BulkLoad(data_original* cur_aray, unsigned cur_aray_num);
	data_pointer* Search(key_type &key);
	data_pointer* SearchSIMD(key_type &key);
	bool Insert(data_original data);
	csb_node_data* SearchInsertNode(key_type key, stack <csb_node_data* > &insert_path);

	//For testing
	void printTree();
	unsigned calculateNodes();
public:
	CSBNodeGroup* root;
	unsigned depth;

	static unsigned long counter_layer;

private:
	int makeCurrentNodeGroup(data_original* cur_aray, int cur_aray_num, CSBNodeGroup** cur_group, data_original* upper_aray, CSBNodeGroup** child_group, int child_group_num);
};

#endif /* CSB_TREE_H_ */
