/*
 * IndexManager.h
 *
 *  Created on: 2013年12月5日
 *      Author: imdb
 */

#ifndef INDEXMANAGER_H_
#define INDEXMANAGER_H_
#include <map>
#include <vector>
#include "CSBPlusTree.h"
#include "CSBTree.h"
#include "EnhancedCSBTree.h"
#include "../Catalog/Attribute.h"
#include "../common/ids.h"
#include "../common/data_type.h"

enum index_type {CSBPLUS, CSB, ECSB};

struct attr_index_list
{
	attr_index_list() {};
	attr_index_list(Attribute attr) :attribute(attr), attr_index_name("\0") { index_tree_list.clear(); }
	attr_index_list(Attribute attr, std::string index_name) :attribute(attr), attr_index_name(index_name) { index_tree_list.clear(); }
	Attribute attribute;
	std::string attr_index_name;
	std::map<ChunkID, void* > index_tree_list;
};

class IndexManager {
public:
	static IndexManager* getInstance();
	virtual ~IndexManager();

	bool addIndexToList(unsigned key_indexing, map<ChunkID, void* > attr_index);
	bool insertIndexToList(std::string index_name, unsigned key_indexing, map<ChunkID, void* > attr_index);

	template <typename T>
	std::map<ChunkID, CSBPlusTree<T>* > getIndexList(Attribute attribute);
	template <typename T>
	std::map<ChunkID, CSBPlusTree<T>* > getIndexList(unsigned long attr_index_id);

	std::map<ChunkID, void* > getAttrIndex(unsigned long attr_index_id);

	template <typename T>
	bool deleteIndexFromList(unsigned long index_id);

	data_type getIndexType(unsigned long index_id);

	bool isIndexExist(Attribute attr);
	bool isIndexExist(unsigned long index_id);

	unsigned long getIndexID(Attribute attr);

	bool serialize(std::string file_name);
	bool deserialize(std::string file_name);

private:
	IndexManager();

private:
	map<unsigned long, attr_index_list*> csb_plus_index_;
	map<unsigned long, attr_index_list*> csb_index_;
	map<unsigned long, attr_index_list*> enhanced_csb_index_;

	unsigned long attr_index_id_;
	map<Attribute, unsigned long > column_attribute_to_id;
	map<unsigned long, Attribute> id_to_column_attribute;


	static IndexManager* instance_;
};



template <typename T>
std::map<ChunkID, CSBPlusTree<T>* > IndexManager::getIndexList(Attribute attribute)
{
	map<ChunkID, CSBPlusTree<T>* > ret;
	ret.clear();

	map<Attribute, unsigned long >::iterator iter = column_attribute_to_id.find(attribute);
	if (iter == column_attribute_to_id.end())
		cout << "[WARNING: IndexManager.cpp->getIndexList()]: The attribute " << attribute.attrName << "hasn't be indexed by CSB+ tree!\n";
	else
	{
		unsigned long index_id = iter->second;
		map<unsigned long, attr_index_list*>::iterator iter_index = csb_plus_index_.find(index_id);
		for (map<ChunkID, void*>::iterator iter_ = iter_index->second->index_tree_list.begin(); iter_ != iter_index->second->index_tree_list.end(); iter_++)
			ret[iter_->first] = (CSBPlusTree<T>*)(iter_->second);
	}
	return ret;
}

template <typename T>
std::map<ChunkID, CSBPlusTree<T>* > IndexManager::getIndexList(unsigned long attr_index_id)
{
	map<ChunkID, CSBPlusTree<T>* > ret;
	ret.clear();
	if (csb_plus_index_.find(attr_index_id) == csb_plus_index_.end())
		cout << "[WARNING: IndexManager.cpp->getIndexList()]: The index id " << attr_index_id << "hasn't be used to mapping a CSB+ column index!\n";
	else
	{
		map<unsigned long, attr_index_list*>::iterator iter = csb_plus_index_.find(attr_index_id);
		for (map<ChunkID, void*>::iterator iter_ = iter->second->index_tree_list.begin(); iter_ != iter->second->index_tree_list.end(); iter_++)
			ret[iter_->first] = (CSBPlusTree<T>*)(iter_->second);
	}
	return ret;
}


template <typename T>
bool IndexManager::deleteIndexFromList(unsigned long index_id)
{
	if (csb_plus_index_.find(index_id) == csb_plus_index_.end())
		return true;
	else
	{
		attr_index_list* index_ = csb_plus_index_.find(index_id)->second;
		for (map<ChunkID, void*>::iterator iter = index_->index_tree_list.begin(); iter != index_->index_tree_list.end(); iter++)
			((CSBPlusTree<T>*)(iter->second))->~CSBPlusTree();
		csb_plus_index_.erase(index_id);
		column_attribute_to_id.erase(id_to_column_attribute.find(index_id)->second);
		id_to_column_attribute.erase(index_id);
	}
}

#endif /* INDEXMANAGER_H_ */
