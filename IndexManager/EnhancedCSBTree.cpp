/*
 * EnhancedCSBTree.cpp
 *
 *  Created on: Feb 11, 2015
 *      Author: scdong
 */

#include "EnhancedCSBTree.h"

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

map<index_offset, vector<index_offset>* >* EnhancedCSBTree::search(key_type_8 &key)
{
	map<index_offset, vector<index_offset>* >* ret = new map<index_offset, vector<index_offset>* >;
	ret->clear();
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
		return ret;

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
			if (ret->find(cur_group->p_datas[cur_off]->block_off[cur_i]) == ret->end())
				(*ret)[cur_group->p_datas[cur_off]->block_off[cur_i]] = new vector<index_offset>;
			(*ret)[cur_group->p_datas[cur_off]->block_off[cur_i]]->push_back(cur_group->p_datas[cur_off]->tuple_off[cur_i]);
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
	return ret;
}

map<index_offset, vector<index_offset>* >* EnhancedCSBTree::rangeQuery(key_type_8 lower_key, comparison comp_lower, key_type_8 upper_key, comparison comp_upper)
{
	map<index_offset, vector<index_offset>* >* ret = new map<index_offset, vector<index_offset>* >;
	ret->clear();

	//For point query
	if (comp_lower == EQ)
	{
		if (lower_key != upper_key)
		{
			cout << "[ERROR FILE: " << __FILE__ << "] In function " << __func__ << " line " << __LINE__ << ": For the equal point query, the lower_key " << lower_key << " != the upper_key " << upper_key << "!\n";
			return ret;
		}
		return search(lower_key);
	}
	//Range Query
	else if ((!(comp_lower == G || comp_lower == GEQ)) || (!(comp_upper == L || comp_upper == LEQ)))
	{
		cout << "[ERROR FILE: " << __FILE__ << "] In function " << __func__ << " line " << __LINE__ << ": For the range query, the given two compare operator isn't a range!\n";
		return ret;
	}
	else if (lower_key > upper_key)
	{
		cout << "[ERROR FILE: " << __FILE__ << "] In function " << __func__ << " line " << __LINE__ << ": For the range query, the given two key isn't a range!\n";
		return ret;
	}
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

