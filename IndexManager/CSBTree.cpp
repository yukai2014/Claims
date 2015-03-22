/*
 * CSBTree.cpp
 *
 *  Created on: Feb 11, 2015
 *      Author: scdong
 */

#include "CSBTree.h"

/***********************************  CSB Node  ***********************************/
bool CSBNode::Insert(data_original value, data_pointer* p_data)
{
	if (this->used_keys >= MAX_KEYS)
	{
		cout << "[ERROR] CSB-Tree->CSBNode->Insert(): The node is already full!\n";
		return false;
	}
	int pos = this->used_keys-1;
	for (; this->keys[pos] > value._key; pos--)
	{
		this->keys[pos+1] = this->keys[pos];
		p_data->block_off[pos+1] = p_data->block_off[pos];
		p_data->tuple_off[pos+1] = p_data->tuple_off[pos];
	}
	this->keys[++pos] = value._key;
	p_data->block_off[pos] = value._block_off;
	p_data->tuple_off[pos] = value._tuple_off;
	this->used_keys += 1;
	return true;
}

data_original CSBNode::SplitInsert(CSBNode* pNode, data_original data, data_pointer* cur_data_node, data_pointer* new_data_node)
{
	if (this->used_keys != MAX_KEYS)
	{
		cout << "[ERROR: int_CSB-Tree.h (CSBNode->SplitInsert()] The node is unfull!\n";
		return data;
	}

	if (data._key < this->keys[MAX_KEYS/2]) //insert into the first node
	{
		for (unsigned i = MAX_KEYS/2; i < MAX_KEYS; i++)
		{
			pNode->keys[i-MAX_KEYS/2] = this->keys[i];
			new_data_node->block_off[i-MAX_KEYS/2] = cur_data_node->block_off[i];
			new_data_node->tuple_off[i-MAX_KEYS/2] = cur_data_node->tuple_off[i];
			this->keys[i] = INVALID_KEY;
			cur_data_node->block_off[i] = INVALID;
			cur_data_node->tuple_off[i] = INVALID;
		}
		pNode->used_keys = MAX_KEYS-MAX_KEYS/2;
		this->used_keys = MAX_KEYS/2;
		this->Insert(data, cur_data_node);

		data._key = this->keys[MAX_KEYS/2];
		this->keys[MAX_KEYS/2] = INVALID_KEY;
		data._block_off = cur_data_node->block_off[MAX_KEYS/2];
		cur_data_node->block_off[MAX_KEYS/2] = INVALID;
		data._tuple_off = cur_data_node->tuple_off[MAX_KEYS/2];
		cur_data_node->tuple_off[MAX_KEYS/2] = INVALID;
		this->used_keys = MAX_KEYS/2;
		return data;
	}
	else
	{
		unsigned pos = MAX_KEYS/2+1;
		for (unsigned i = pos; i < MAX_KEYS; i++)
		{
			pNode->keys[i-pos] = this->keys[i];
			new_data_node->block_off[i-pos] = cur_data_node->block_off[i];
			new_data_node->tuple_off[i-pos] = cur_data_node->block_off[i];
			this->keys[i] = INVALID_KEY;
			cur_data_node->block_off[i] = INVALID;
			cur_data_node->tuple_off[i] = INVALID;
		}
		this->used_keys = MAX_KEYS/2;
		pNode->used_keys = MAX_KEYS-pos;
		pNode->Insert(data, new_data_node);

		pos = MAX_KEYS/2;
		data._key = this->keys[pos];
		this->keys[pos] = INVALID_KEY;
		data._block_off = cur_data_node->block_off[pos];
		cur_data_node->block_off[pos] = INVALID;
		data._tuple_off = cur_data_node->tuple_off[pos];
		cur_data_node->tuple_off[pos] = INVALID;
		return data;
	}
}


/***********************************  CSB Node Group  ***********************************/

CSBNodeGroup* CSBNodeGroup::Insert(CSBNode* insert_node, data_original &insert_data, CSBNodeGroup* new_child_group)
{
	if (this->used_nodes == MAX_KEYS+1)
	{
		cout << "[ERROR: int_CSB-Tree.h (CSBNodeGroup->Insert())] The node group is already full!\n";
		return NULL;
	}
	CSBNodeGroup* new_group = new CSBNodeGroup(this->used_nodes+1);
	unsigned cur_off = 0;
	for (; cur_off <= this->used_nodes; cur_off++)
	{
		new_group->setNode(cur_off, this->nodes[cur_off], this->p_datas[cur_off]);
		if (this->nodes[cur_off] == insert_node)
			break;
	}
	insert_data = new_group->nodes[cur_off]->SplitInsert(new_group->nodes[cur_off+1], insert_data, new_group->p_datas[cur_off], new_group->p_datas[cur_off+1]);
	new_group->nodes[++cur_off]->p_child = new_child_group;
	for (; cur_off < this->used_nodes; cur_off++)
		new_group->setNode(cur_off+1, this->nodes[cur_off], this->p_datas[cur_off]);
	return new_group;
}

CSBNodeGroup* CSBNodeGroup::SplitInsert(CSBNode* insert_node, data_original &insert_data, CSBNodeGroup* new_child_group)
{
	if (this->used_nodes != MAX_KEYS+1)
	{
		cout << "[ERROR: int_CSB-Tree.h (CSBNodeGroup->SplitInsert())] The node group is not full!\n";
		return NULL;
	}
	unsigned node_group_size1 = (MAX_KEYS+2)/2;
	unsigned node_group_size2 = MAX_KEYS+2-node_group_size1;
	CSBNodeGroup* new_group = new CSBNodeGroup(node_group_size2);

	CSBNode* new_node = new CSBNode();
	data_pointer* new_data = new data_pointer(MAX_KEYS);
	unsigned pos = 0;
	for (; this->nodes[pos] != insert_node; pos++);
	insert_data = insert_node->SplitInsert(new_node, insert_data, this->p_datas[pos], new_data);
	new_node->p_child = new_child_group;

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


/***********************************  CSB Tree  ***********************************/
unsigned long CSBTree::counter_layer = 0;

void CSBTree::BulkLoad(data_original* cur_aray, unsigned cur_aray_num)
{
	if (cur_aray_num == 0)
		return;
	else
	{
		int cur_group_num = ceil(ceil((double)(cur_aray_num+1)/(double)(MAX_KEYS+1))/(double)(MAX_KEYS+1));
		CSBNodeGroup** cur_group = new CSBNodeGroup* [cur_group_num];
		int upper_aray_num = ceil((double)(cur_aray_num+1)/(double)(MAX_KEYS+1))-1;
		data_original* upper_aray = new data_original[upper_aray_num];
		CSBNodeGroup** child_group = NULL;
		int child_group_num = 0;
		while (cur_aray_num != 0)
		{
			upper_aray_num = makeCurrentNodeGroup(cur_aray, cur_aray_num, cur_group, upper_aray, child_group, child_group_num);
			delete[] cur_aray;
			cur_aray = upper_aray;
			cur_aray_num = upper_aray_num;
			child_group = cur_group;
			child_group_num = cur_group_num;
			upper_aray = new data_original [upper_aray_num];
			cur_group_num = ceil((double)child_group_num/(double(MAX_KEYS+1)));
			cur_group = new CSBNodeGroup* [cur_group_num];
		}
	}
}

data_pointer* CSBTree::Search(key_type &key)
{
	if (this->root == NULL)
		return NULL;
	data_pointer* ret = new data_pointer(1);

	CSBNodeGroup* cur_group = this->root;	counter_layer += 1;
	unsigned cur_off = 0;
	CSBNode* cur_node = cur_group->nodes[cur_off];
	int cur_i = 0;
	for (unsigned cur_depth = 0; cur_depth < this->depth; cur_depth++)
	{

		for (cur_i = 0; (cur_i < cur_node->used_keys) && (key > cur_node->keys[cur_i]); cur_i++);

		//find equal => return
		if (cur_i < cur_node->used_keys && key == cur_node->keys[cur_i])
		{
			ret->block_off[0] = cur_group->p_datas[cur_off]->block_off[cur_i];
			ret->tuple_off[0] = cur_group->p_datas[cur_off]->tuple_off[cur_i];
			return ret;
		}

		//search the child-layer of cur_node
		if (cur_depth < this->depth-1)
		{	counter_layer += 1;
			cur_group = cur_node->p_child;
			cur_off = cur_i;
			cur_node = cur_group->nodes[cur_off];
		}
	}
	return NULL;
}

map<index_offset, vector<index_offset>* >* CSBTree::search(key_type &key)
{
	map<index_offset, vector<index_offset>* >* ret = new map<index_offset, vector<index_offset>* >;
	ret->clear();
	int i = 0;

	if (this->depth == 0)
		return ret;

	CSBNodeGroup* cur_group = this->root;	counter_layer += 1;
	unsigned cur_off = 0;
	CSBNode* cur_node = cur_group->nodes[cur_off];
	int cur_i = 0;
	for (unsigned cur_depth = 0; cur_depth < this->depth; cur_depth++)
	{

		for (cur_i = 0; (cur_i < cur_node->used_keys) && (key > cur_node->keys[cur_i]); cur_i++);

		//find equal => return
		if (cur_i < cur_node->used_keys && key == cur_node->keys[cur_i])
		{
			if (ret->find(cur_group->p_datas[cur_off]->block_off[cur_i]) == ret->end())
				(*ret)[cur_group->p_datas[cur_off]->block_off[cur_i]] = new vector<index_offset>;
			(*ret)[cur_group->p_datas[cur_off]->block_off[cur_i]]->push_back(cur_group->p_datas[cur_off]->tuple_off[cur_i]);
			return ret;
		}

		//search the child-layer of cur_node
		if (cur_depth < this->depth-1)
		{	counter_layer += 1;
			cur_group = cur_node->p_child;
			cur_off = cur_i;
			cur_node = cur_group->nodes[cur_off];
		}
	}
	return ret;
}

map<index_offset, vector<index_offset>* >* CSBTree::rangeQuery(key_type lower_key, comparison comp_lower, key_type upper_key, comparison comp_upper)
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

data_pointer* CSBTree::SearchSIMD(key_type &key)
{
	if (sizeof(key_type) != 32)
		return NULL;
	if (this->root == NULL)
		return NULL;
	data_pointer* ret = new data_pointer(1);
	CSBNodeGroup* cur_group = this->root;	counter_layer += 1;
	unsigned cur_off = 0;
	CSBNode* cur_node = cur_group->nodes[cur_off];
	__m128i* m_1 = (__m128i*)(cur_node->keys);
	__m128i m_2 = _mm_set1_epi32(key);
	__m128i m_3[3];
	int* cmp_ret = NULL;
	for (unsigned cur_depth = 0; cur_depth < this->depth; cur_depth++)
	{
		int cur_i = 0;
		m_3[0] = _mm_cmplt_epi32(*m_1, m_2);
		m_1 = m_1+1;
		m_3[1] = _mm_cmplt_epi32(*m_1, m_2);
		m_1 = m_1+1;
		m_3[2] = _mm_cmplt_epi32(*m_1, m_2);
		m_1 = m_1+1;

		cmp_ret = (int*)m_3;
		for (int i = 0; i < cur_node->used_keys; i++)
			cur_i += cmp_ret[i];
		cur_i = 0-cur_i;

		//find equal => return
		if ((cur_i < cur_node->used_keys) && (key == cur_node->keys[cur_i]))
		{
			ret->block_off[0] = cur_group->p_datas[cur_off]->block_off[cur_i];
			ret->tuple_off[0] = cur_group->p_datas[cur_off]->tuple_off[cur_i];
			return ret;
		}

		//search the child-layer of cur_node
		if (cur_depth < this->depth-1)
		{	counter_layer += 1;
			cur_group = cur_node->p_child;
			cur_off = cur_i;
			cur_node = cur_group->nodes[cur_off];
			m_1 = (__m128i*)(cur_node->keys);
		}
	}
	return NULL;
}

bool CSBTree::Insert(data_original data)
{
//	if (Search(data._key) != NULL)
//		return false;
	if (this->root == NULL)
	{
		this->depth = 1;
		CSBNodeGroup* leaf_group = new CSBNodeGroup(1);
		leaf_group->nodes[0]->keys[0] = data._key;
		leaf_group->p_datas[0]->block_off[0] = data._block_off;
		leaf_group->p_datas[0]->tuple_off[0] = data._tuple_off;
		leaf_group->nodes[0]->used_keys = 1;
		leaf_group->nodes[0]->p_child = NULL;
		this->root = leaf_group;
		return true;
	}

	stack <csb_node_data* > insert_path;
	csb_node_data* insert_node = SearchInsertNode(data._key, insert_path);
	CSBNodeGroup* new_child = NULL;
	csb_node_data* p_insert_node;
	while (!insert_path.empty())
	{
		p_insert_node = insert_path.top();
		insert_path.pop();
		if (insert_node->csb_node->used_keys < MAX_KEYS)
		{
			insert_node->csb_node->p_child = new_child;
			return insert_node->csb_node->Insert(data, insert_node->csb_data);
		}
		if (p_insert_node->csb_node->used_keys < MAX_KEYS)
			new_child = p_insert_node->csb_node->p_child->Insert(insert_node->csb_node, data, new_child);
		else
			new_child = p_insert_node->csb_node->p_child->SplitInsert(insert_node->csb_node, data, new_child);
		insert_node = p_insert_node;
	}

	//insert into the root node
	if (insert_node->csb_node->used_keys < MAX_KEYS)
	{
		insert_node->csb_node->p_child = new_child;
		return insert_node->csb_node->Insert(data, insert_node->csb_data);
	}
	new_child = this->root->Insert(insert_node->csb_node, data, new_child);
	CSBNodeGroup* root_group = new CSBNodeGroup(1);
	root_group->nodes[0]->keys[0] = data._key;
	root_group->p_datas[0]->block_off[0] = data._block_off;
	root_group->p_datas[0]->tuple_off[0] = data._tuple_off;
	root_group->nodes[0]->used_keys = 1;
	root_group->nodes[0]->p_child = new_child;
	this->root = root_group;
	this->depth += 1;
	return true;
}

CSBTree::csb_node_data* CSBTree::SearchInsertNode(key_type key, stack <csb_node_data*> &insert_path)
{
	unsigned cur_offset = 0;
	csb_node_data* ret = new csb_node_data();
	ret->csb_node = this->root->nodes[0];
	ret->csb_data = this->root->p_datas[0];
	for (unsigned cur_depth = 1; cur_depth < this->depth; cur_depth++)
	{
		csb_node_data* record = new csb_node_data(ret->csb_node, ret->csb_data);
		insert_path.push(record);
		for (cur_offset = 0; (key >= ret->csb_node->keys[cur_offset]) && (cur_offset < ret->csb_node->used_keys); cur_offset++);
		ret->csb_data = ret->csb_node->p_child->p_datas[cur_offset];
		ret->csb_node = ret->csb_node->p_child->nodes[cur_offset];
	}
	return ret;


//	CSBNodeGroup* ret = this->root;
//	unsigned short cur_offset = 0;
//	CSBNode* cur_node = ret->nodes[cur_offset];
//
//	// Find the insert node
//	for (unsigned cur_depth = 1; cur_depth < this->depth; cur_depth++)
//	{
//		for (cur_offset = 0; (key >= cur_node->keys[cur_offset]) && (cur_offset < cur_node->used_keys); cur_offset++);
//		csb_path* cur_ = new csb_path(ret, cur_offset);
//		insert_path.push(cur_);
//		ret = cur_node->p_child;
//		cur_node = ret->nodes[cur_offset];
//	}
//	return ret;
}

int CSBTree::makeCurrentNodeGroup(data_original* cur_aray, int cur_aray_num, CSBNodeGroup** cur_group, data_original* upper_aray, CSBNodeGroup** child_group, int child_group_num)
{
	depth += 1;
	int upper_aray_num = 0;

	if (cur_aray_num <= MAX_KEYS)	//the root level, but not leaf
	{
		cur_group[0] = new CSBNodeGroup(1);
		for (unsigned i = 0; i < cur_aray_num; i++)
		{
			cur_group[0]->nodes[0]->keys[i] = cur_aray[i]._key;
			cur_group[0]->p_datas[0]->block_off[i] = cur_aray[i]._block_off;
			cur_group[0]->p_datas[0]->tuple_off[i] = cur_aray[i]._tuple_off;
		}
		cur_group[0]->nodes[0]->used_keys = cur_aray_num;

		//set the parents-children relationship
		if (depth != 1)
			cur_group[0]->nodes[0]->p_child = child_group[0];

		//set the root node group
		this->root = cur_group[0];
		return upper_aray_num;
	}


	if (child_group_num == 0)
		child_group_num = ceil((double)(cur_aray_num+1)/(double)(MAX_KEYS+1));

	int cur_group_num = ceil((double)child_group_num/(double)(MAX_KEYS+1));
	int cur_group_counter = 0;
	if (cur_group_num >= 2)
	{
		while (cur_group_counter < cur_group_num-2)
		{
			cur_group[cur_group_counter] = new CSBNodeGroup(MAX_KEYS+1);
			cur_group_counter += 1;
		}
		int rest = child_group_num-(cur_group_num-1)*(MAX_KEYS+1);
		if (rest >= MAX_KEYS/2+1)
		{
			cur_group[cur_group_counter] = new CSBNodeGroup(MAX_KEYS+1);
			cur_group[cur_group_counter+1] = new CSBNodeGroup(rest);
		}
		else
		{
			rest += (MAX_KEYS+1);
			if (rest%2 == 0)
				cur_group[cur_group_counter] = new CSBNodeGroup(rest/2);
			else
				cur_group[cur_group_counter] = new CSBNodeGroup(rest/2+1);
			cur_group[cur_group_counter+1] = new CSBNodeGroup(rest/2);
		}
	}
	else
		cur_group[cur_group_counter] = new CSBNodeGroup(child_group_num);

	int cur_aray_counter = 0;
	for (cur_group_counter = 0; cur_group_counter < cur_group_num-1; cur_group_counter++)
	{
		for (unsigned j = 0; j < cur_group[cur_group_counter]->used_nodes; j++)
		{
			unsigned k = 0;
			for (k = 0; k < MAX_KEYS; k++)
			{
				cur_group[cur_group_counter]->nodes[j]->keys[k] = cur_aray[cur_aray_counter]._key;
				cur_group[cur_group_counter]->p_datas[j]->block_off[k] = cur_aray[cur_aray_counter]._block_off;
				cur_group[cur_group_counter]->p_datas[j]->tuple_off[k] = cur_aray[cur_aray_counter]._tuple_off;
				cur_aray_counter += 1;
			}
			cur_group[cur_group_counter]->nodes[j]->used_keys = k;
			upper_aray[upper_aray_num++] = cur_aray[cur_aray_counter++];
		}
	}
	int j = 0;
	for (j = 0; j < cur_group[cur_group_counter]->used_nodes-2; j++)
	{
		int k = 0;
		for (k = 0; k < MAX_KEYS; k++)
		{
			cur_group[cur_group_counter]->nodes[j]->keys[k] = cur_aray[cur_aray_counter]._key;
			cur_group[cur_group_counter]->p_datas[j]->block_off[k] = cur_aray[cur_aray_counter]._block_off;
			cur_group[cur_group_counter]->p_datas[j]->tuple_off[k] = cur_aray[cur_aray_counter]._tuple_off;
			cur_aray_counter += 1;
		}
		cur_group[cur_group_counter]->nodes[j]->used_keys = k;
		upper_aray[upper_aray_num++] = cur_aray[cur_aray_counter++];
	}
	int rest = cur_aray_num%(MAX_KEYS+1);
	if (rest >= MAX_KEYS/2)
	{
		cur_group[cur_group_counter]->nodes[j]->used_keys = MAX_KEYS;
		cur_group[cur_group_counter]->nodes[j+1]->used_keys = rest;
	}
	else
	{
		rest += MAX_KEYS;
		cur_group[cur_group_counter]->nodes[j]->used_keys = rest-rest/2;
		cur_group[cur_group_counter]->nodes[j+1]->used_keys = rest/2;
	}

	for(; j < cur_group[cur_group_counter]->used_nodes; j++)
	{
		for (int k = 0; k < cur_group[cur_group_counter]->nodes[j]->used_keys; k++)
		{
			cur_group[cur_group_counter]->nodes[j]->keys[k] = cur_aray[cur_aray_counter]._key;
			cur_group[cur_group_counter]->p_datas[j]->block_off[k] = cur_aray[cur_aray_counter]._block_off;
			cur_group[cur_group_counter]->p_datas[j]->tuple_off[k] = cur_aray[cur_aray_counter]._tuple_off;
			cur_aray_counter += 1;
		}
		if (cur_aray_counter < cur_aray_num)
			upper_aray[upper_aray_num++] = cur_aray[cur_aray_counter++];
	}

	//set the parent-children relationship
	if (depth != 1)
	{
		int child_group_counter = 0;
		for (cur_group_counter = 0; cur_group_counter < cur_group_num; cur_group_counter++)
		{
			for (int j = 0; j < cur_group[cur_group_counter]->used_nodes; j++)
			{
				cur_group[cur_group_counter]->nodes[j]->p_child = child_group[child_group_counter];
				child_group_counter += 1;
			}
		}
	}

	return upper_aray_num;
}

void CSBTree::printTree()
{
	cout << "\n\n-------------------------Print the cache sensitive B tree-------------------------\n";
	if (this->root == NULL)
	{
		cout << "[OUTPUT: CSBTree.h->printTree()]: The index tree is empty!\n\n";
		return;
	}

	vector<CSBNodeGroup* > current_level;
	vector<CSBNodeGroup* > lower_level;
	lower_level.clear();

	int cur_depth = 1;
	lower_level.push_back(this->root);

	while (cur_depth <= depth)
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
				lower_level.push_back(current_level.back()->nodes[j]->p_child);

				cout << "Node: " << j << "\t";
				cout << "Used Keys: " << current_level.back()->nodes[j]->used_keys << endl;
				for (unsigned k = 0; k < current_level.back()->nodes[j]->used_keys; k++)
				{
					cout << current_level.back()->nodes[j]->keys[k] << " ";
					cout << "<" << current_level.back()->p_datas[j]->block_off[k] << ", ";
					cout << current_level.back()->p_datas[j]->tuple_off[k] << ">\t";
				}
				cout << endl;
			}
			current_level.pop_back();
		}
	}
	cout << "\n\n---------------------Print the cache sensitive B tree finished---------------------\n\n";
}

unsigned CSBTree::calculateNodes()
{
	unsigned ret = 0;
	if (this->root == NULL)
	{
		return ret;
	}

	vector<CSBNodeGroup* > current_level;
	vector<CSBNodeGroup* > lower_level;
	lower_level.clear();

	int cur_depth = 1;
	lower_level.push_back(this->root);

	while (cur_depth <= depth)
	{
		current_level.clear();
		while (lower_level.size() != NULL)
		{
			current_level.push_back(lower_level.back());
			lower_level.pop_back();
		}
		lower_level.clear();

		while (current_level.size() != 0)
		{
			for (unsigned j = 0; j < current_level.back()->used_nodes; j++)
				lower_level.push_back(current_level.back()->nodes[j]->p_child);
			ret += current_level.back()->used_nodes;
			current_level.pop_back();
		}
		cur_depth++;
	}
	return ret;
}

