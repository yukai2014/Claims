/*
 * CSBPlusTree.h
 *
 *  Created on: 2013年12月27日
 *      Author: SCDONG
 */

#ifndef CSBPLUSTREE_H_
#define CSBPLUSTREE_H_

#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <memory.h>
#include <assert.h>
#include <iostream>
#include <vector>
#include <map>
#include <stack>
#include "indexStructures.h"
#include "../configure.h"
using namespace std;

// pre-declare of the node_group class
template<typename T>
class CCSBNodeGroup;

/*********************************  CSBNode  *********************************/
template<typename T>
class CCSBNode {
 public:
  CCSBNode()
      : used_keys(0) {
  }
  virtual ~CCSBNode() {
    used_keys = 0;
  }

  // the get and set function for node_type,
  //    and node_inner_key_count, node's_father
  int getUsedKeys() {
    return used_keys;
  }
  virtual bool setUsedKeys(int count) {
    assert(false);
  }

  static unsigned getMaxKeys() {
    return (cacheline_size - 8) / sizeof(T);
  }
  static unsigned getMaxDatas() {
    return cacheline_size / sizeof(data_offset<T> );
  }

  // for the leaf nodes: get or set a certain data
  virtual data_offset<T> getElement(unsigned i) {
    assert(false);
  }
  virtual bool setElement(unsigned i, data_offset<T> value) {
    assert(false);
  }

  // for the internal nodes: get or set a certain key
// virtual T getElement(unsigned i) {};
  virtual bool setElement(unsigned i, T value) {
    assert(false);
  }
  // for the internal nodes: get or set the child's pointer
  virtual CCSBNodeGroup<T>* getPointer() {
    assert(false);
  }
  virtual void setPointer(CCSBNodeGroup<T>* pointer) {
    assert(false);
  }

  // delete the child nodes
  virtual void DeleteChildren() {
    assert(false);
  }

  // operations for a index nodes
  virtual bool Insert(data_offset<T> value) {
    assert(false);
  }
  virtual data_offset<T> SplitInsert(CCSBNode<T>* pNode, data_offset<T> data,
                                     CCSBNodeGroup<T>* new_child_group) {
    assert(false);
  }
  virtual bool serialize(FILE* filename) {
    assert(false);
  }
  virtual bool deserialize(FILE* filename) {
    assert(false);
  }

 public:
  int used_keys;  // number of datas/keys in the node
};

/*****************************  CSBInternalNode  *****************************/
template<typename T>
class CCSBInternalNode : public CCSBNode<T> {
 public:
  CCSBInternalNode();
  virtual ~CCSBInternalNode();

  bool setUsedKeys(int count) {
    if (count <= this->getMaxKeys()) {
      this->used_keys = count;
      return true;
    }
    return false;
  }
  // get or set a certain key
  data_offset<T> getElement(unsigned i) {
    data_offset<T> ret;
    ret._block_off = INVALID;
    ret._tuple_off = INVALID;
    if ((i >= 0) && (i < this->getMaxKeys()))
      ret._key = node_keys[i];
    else
      ret._key = INVALID;
    return ret;
  }
  bool setElement(unsigned i, T key) {
    if ((i >= 0) && (i < this->getMaxKeys())) {
      node_keys[i] = key;
      return true;
    }
    else {
      cout << "[ERROR: CSBPlusTree.h<CCSBInternalNode>->setElement()]:"
           " The offset i: "
           << i << " is invalid!\n";
      return false;
    }
  }

  // get or set the child's pointer
  CCSBNodeGroup<T>* getPointer() {
    return p_child_node_group;
  }
  void setPointer(CCSBNodeGroup<T>* pointer) {
    p_child_node_group = pointer;
  }

  bool Insert(data_offset<T> key);
  data_offset<T> SplitInsert(CCSBNode<T>* pNode, data_offset<T> key,
                             CCSBNodeGroup<T>* new_child_group);
  void DeleteChildren();
  bool serialize(FILE* filename);
  bool deserialize(FILE* filename);

 public:
  T* node_keys;  // array for the keys
  // the pointer to the child nodes group
  CCSBNodeGroup<T>* p_child_node_group;
};

template<typename T>
CCSBInternalNode<T>::CCSBInternalNode() {
  CCSBNode<T>();
  node_keys = new T[this->getMaxKeys()];
  for (unsigned i = 0; i < this->getMaxKeys(); i++)
    node_keys[i] = INVALID;
  p_child_node_group = NULL;
}

template<typename T>
CCSBInternalNode<T>::~CCSBInternalNode() {
  delete[] node_keys;
  p_child_node_group = NULL;
  this->~CCSBNode<T>();
}

template<typename T>
bool CCSBInternalNode<T>::Insert(data_offset<T> key) {
  // return false when the node is full
  if (this->getUsedKeys() >= this->getMaxKeys()) {
    return false;
  }

  // determine the position for insert
  int pos = 0;
  for (pos = 0; pos < this->getUsedKeys() && key._key >= node_keys[pos];
      pos++) {
  }
  for (int j = this->getUsedKeys(); j > pos; j--) {
    node_keys[j] = node_keys[j - 1];
  }
  node_keys[pos] = key._key;
  this->setUsedKeys(this->getUsedKeys() + 1);

  return true;
}

/*
 * @param pNode the point to Node which is chosen to be insert
 * @param key the key to be inserted
 * @param new_child_group the new Node Group attached to pNode???
 *
 */
template<typename T>
data_offset<T> CCSBInternalNode<T>::SplitInsert(
    CCSBNode<T>* pNode, data_offset<T> key, CCSBNodeGroup<T>* new_child_group) {
  data_offset<T> ret;
  ret._key = INVALID;
  unsigned max_keys = this->getMaxKeys();
  if (this->getUsedKeys() != max_keys) {
    cout << "[ERROR: CSBPlusTree.cpp (CCSBInternalNode->SplitInsert)]"
         " The unfull internal node can not be split!\n";
    return ret;
  }

  int pos = max_keys - (new_child_group->getUsedNodes() - 1);  // ??? --Yu
  if (key._key > this->node_keys[pos]) {
    // insert key into pNode and move node after pos into pNode
    for (unsigned i = pos + 1; i < max_keys; i++) {
      pNode->setElement(i - pos - 1, this->node_keys[i]);
      this->node_keys[i] = INVALID;
    }
    pNode->setUsedKeys(max_keys - pos - 1);
    pNode->Insert(key);
  }
  else {
    // insert key into this node, move node after pos into pNode
    for (unsigned i = pos; i < max_keys; i++) {
      pNode->setElement(i - pos, this->node_keys[i]);
      this->node_keys[i] = INVALID;
    }
    pNode->setUsedKeys(max_keys - pos);

    this->setUsedKeys(pos);
    this->Insert(key);
  }
  ret._key = this->node_keys[pos];
  this->node_keys[pos] = INVALID;
  this->setUsedKeys(pos);
  return ret;
}

template<typename T>
void CCSBInternalNode<T>::DeleteChildren() {
  if (this->getPointer()) this->getPointer()->DeleteChildren();

  delete[] node_keys;
  delete p_child_node_group;
}

template<typename T>
bool CCSBInternalNode<T>::serialize(FILE* filename) {
  fwrite(static_cast<void*>(&this->used_keys), sizeof(int), 1, filename);
  fwrite(this->node_keys, sizeof(T), this->used_keys, filename);
  return true;
}

template<typename T>
bool CCSBInternalNode<T>::deserialize(FILE* filename) {
  fread(static_cast<void*>(&this->used_keys), sizeof(int), 1, filename);
  fread(this->node_keys, sizeof(T), this->used_keys, filename);
  return true;
}

/*******************************  CSBLeafNode  *******************************/
template<typename T>
class CCSBLeafNode : public CCSBNode<T> {
 public:
  CCSBLeafNode();
  virtual ~CCSBLeafNode();

  bool setUsedKeys(int count) {
    if (count <= this->getMaxDatas()) {
      this->used_keys = count;
      return true;
    }
    return false;
  }

  data_offset<T> getElement(unsigned i) {
    if ((i >= 0) && (i < this->getMaxDatas())) {
      return node_datas[i];
    }
    else {
      cout << "[ERROR: CSBPlusTree.h<CCSBLeafNode>->getElement()]: "
           "The offset i: "
           << i << " is invalid! return NULL\n";
      data_offset<T> * ret = NULL;
      return *ret;
    }
  }
  bool setElement(unsigned i, data_offset<T> data) {
    if ((i >= 0) && (i < this->getMaxDatas())) {
      node_datas[i]._key = data._key;
      node_datas[i]._block_off = data._block_off;
      node_datas[i]._tuple_off = data._tuple_off;
      return true;
    }
    else {
      cout << "[ERROR: CSBPlusTree.h<CCSBLeafNode>->setElement()]:"
           " The offset i: "
           << i << " is invalid!\n";
      return false;
    }
  }

  CCSBNodeGroup<T>* getPointer() {
    return NULL;
  }

  bool Insert(data_offset<T> value);
  data_offset<T> SplitInsert(CCSBNode<T>* pNode, data_offset<T> data,
                             CCSBNodeGroup<T>* new_child_group);
  void DeleteChildren();

  bool serialize(FILE* filename);
  bool deserialize(FILE* filename);

 public:
  data_offset<T>* node_datas;
};

template<typename T>
CCSBLeafNode<T>::CCSBLeafNode() {
  CCSBNode<T>();
  node_datas = new data_offset<T> [this->getMaxDatas()];
  data_offset<T> tmp;
  tmp._key = INVALID;
  tmp._block_off = INVALID;
  tmp._tuple_off = INVALID;
  for (unsigned i = 0; i < this->getMaxDatas(); i++)
    node_datas[i] = tmp;
}

template<typename T>
CCSBLeafNode<T>::~CCSBLeafNode() {
  delete[] node_datas;
  this->~CCSBNode<T>();
}

template<typename T>
bool CCSBLeafNode<T>::Insert(data_offset<T> value) {
  // return false if the leaf node is full
  if (this->getUsedKeys() >= this->getMaxDatas()) return false;

  // determine the position for insert(for the equals keys insert at the last)
  int pos = 0;
  for (pos = 0;
      pos < this->getUsedKeys() && value._key >= (node_datas[pos])._key;
      pos++) {
  }
  for (int j = this->getUsedKeys(); j > pos; j--)
    node_datas[j] = node_datas[j - 1];
  node_datas[pos] = value;
  this->setUsedKeys(this->getUsedKeys() + 1);

  return true;
}

template<typename T>
data_offset<T> CCSBLeafNode<T>::SplitInsert(CCSBNode<T>* pNode,
                                            data_offset<T> data,
                                            CCSBNodeGroup<T>* new_child_group) {
  unsigned max_datas = this->getMaxDatas();
  if (this->getUsedKeys() != max_datas) {
    cout << "[ERROR: CSBPlusTree.cpp (CCSBLeafNode->SplitInsert)]"
         " The unfull leaf node can not be splited!\n";
    return data;
  }

  if (data._key < this->node_datas[max_datas / 2]._key) {
    // insert into the first node
    for (unsigned i = max_datas / 2; i < max_datas; i++) {
      pNode->setElement(i - max_datas / 2, this->node_datas[i]);
      this->node_datas[i]._key = INVALID;
      this->node_datas[i]._block_off = INVALID;
      this->node_datas[i]._tuple_off = INVALID;
    }
    pNode->setUsedKeys(max_datas - max_datas / 2);
    this->setUsedKeys(max_datas / 2);
    this->Insert(data);
    return pNode->getElement(0);
  }
  else {  // insert into pNode
    unsigned tmp = max_datas - max_datas / 2;
    for (unsigned i = tmp; i < max_datas; i++) {
      pNode->setElement(i - tmp, this->node_datas[i]);
      this->node_datas[i]._key = INVALID;
      this->node_datas[i]._block_off = INVALID;
      this->node_datas[i]._tuple_off = INVALID;
    }
    pNode->setUsedKeys(max_datas - tmp);
    this->setUsedKeys(tmp);
    pNode->Insert(data);
    return pNode->getElement(0);
  }
}

template<typename T>
void CCSBLeafNode<T>::DeleteChildren() {
  delete[] node_datas;
}

template<typename T>
bool CCSBLeafNode<T>::serialize(FILE* filename) {
  fwrite(static_cast<void*>(&this->used_keys), sizeof(int), 1, filename);
  fwrite(this->node_datas, sizeof(data_offset<T> ), this->used_keys, filename);
  return true;
}

template<typename T>
bool CCSBLeafNode<T>::deserialize(FILE* filename) {
  fread(static_cast<void*>(&this->used_keys), sizeof(int), 1, filename);
  fread(this->node_datas, sizeof(data_offset<T> ), this->used_keys, filename);
  for (unsigned i = 0; i < this->used_keys; i++)
    if (this->node_datas[i]._block_off > 1023
        || this->node_datas[i]._tuple_off > 2046)
    assert(false);
  return true;
}

/********************************  NodeGroup  ********************************/
template<typename T>
class CCSBNodeGroup {
 public:
  CCSBNodeGroup()
      : used_nodes(0) {
  }
  explicit CCSBNodeGroup(unsigned n)
      : used_nodes(n) {
  }
  ~CCSBNodeGroup() {
    used_nodes = 0;
  }

  unsigned getUsedNodes() {
    return used_nodes;
  }
  void nodeCountIncrement() {
    used_nodes++;
  }

  virtual CCSBNodeGroup<T>* getHeaderNG() {
    assert(false);
  }
  virtual void setHeaderNG(CCSBNodeGroup<T>* header) {
    assert(false);
  }
  virtual CCSBNodeGroup<T>* getTailerNG() {
    assert(false);
  }
  virtual void setTailerNG(CCSBNodeGroup<T>* tailer) {
    assert(false);
  }

  inline virtual bool setUsedNodes(unsigned n) {
    assert(false);
  }
  virtual CCSBNode<T>* getNode(unsigned i) {
    assert(false);
  }
  virtual void setNode(unsigned i, CCSBNode<T>* node) {
    assert(false);
  }

  virtual CCSBNodeGroup<T>* Insert(CCSBNode<T>* insert_node,
                                   data_offset<T> &insert_value,
                                   CCSBNodeGroup<T>* new_child_group) {
    assert(false);
  }
  virtual CCSBNodeGroup<T>* SplitInsert(CCSBNode<T>* insert_node,
                                        data_offset<T>& insert_value,
                                        CCSBNodeGroup<T>* new_child_group) {
    assert(false);
  }
  virtual void DeleteChildren() {
    assert(false);
  }

  virtual bool serialize(FILE* filename) {
    assert(false);
  }
  virtual bool deserialize(FILE* filename) {
    assert(false);
  }

 public:
  unsigned used_nodes;
};

/****************************  InternalNodeGroup  ****************************/
template<typename T>
class CCSBInternalNodeGroup : public CCSBNodeGroup<T> {
 public:
  CCSBInternalNodeGroup()
      : internal_nodes(NULL) {
  }
  explicit CCSBInternalNodeGroup(unsigned n);
  ~CCSBInternalNodeGroup();

  inline bool setUsedNodes(unsigned n) {
    if (n <= CCSBNode<T>::getMaxKeys() + 1) {
      this->used_nodes = n;
      return true;
    }
    return false;
  }
  CCSBNode<T>* getNode(unsigned i) {
    if (i > this->used_nodes) {
      cout << "[ERROR: CSBPlusTree.h<CCSBInternalNodeGroup>->getNode()]:"
           " The offset i: "
           << i << " is invalid!\n";
      return NULL;
    }
    return internal_nodes[i];
  }
  void setNode(unsigned i, CCSBNode<T>* node) {
    for (unsigned j = 0; j < node->getUsedKeys(); j++)
      internal_nodes[i]->setElement(j, node->getElement(j)._key);
    internal_nodes[i]->setUsedKeys(node->getUsedKeys());
    internal_nodes[i]->setPointer(node->getPointer());
  }

  CCSBNodeGroup<T>* Insert(CCSBNode<T>* insert_node,
                           data_offset<T> &insert_value,
                           CCSBNodeGroup<T>* new_child_group);
  CCSBNodeGroup<T>* SplitInsert(CCSBNode<T>* insert_node,
                                data_offset<T>& insert_value,
                                CCSBNodeGroup<T>* new_child_group);
  void DeleteChildren();

  bool serialize(FILE* filename);
  bool deserialize(FILE* filename);

 public:
  CCSBNode<T>** internal_nodes;
};

template<typename T>
CCSBInternalNodeGroup<T>::CCSBInternalNodeGroup(unsigned n) {
  internal_nodes = (CCSBNode<T>**) new CCSBInternalNode<T>*[n];
  for (unsigned i = 0; i < n; i++)
    internal_nodes[i] = new CCSBInternalNode<T>();
  this->setUsedNodes(n);
}

template<typename T>
CCSBInternalNodeGroup<T>::~CCSBInternalNodeGroup() {
  for (unsigned i = 0; i < this->used_nodes; i++)
    ((CCSBInternalNode<T>*) internal_nodes[i])->~CCSBInternalNode();
  this->~CCSBNodeGroup<T>();
}

template<typename T>
CCSBNodeGroup<T>* CCSBInternalNodeGroup<T>::Insert(
    CCSBNode<T>* insert_node, data_offset<T>& insert_value,
    CCSBNodeGroup<T>* new_child_group) {
  if (this->getUsedNodes() <= CCSBNode<T>::getMaxKeys() + 1) {  // ??? question---Yu
    CCSBNodeGroup<T>* new_group = new CCSBInternalNodeGroup(
        this->getUsedNodes() + 1);
    unsigned cur_off = 0;
    for (cur_off = 0; cur_off <= this->getUsedNodes(); cur_off++) {
      new_group->setNode(cur_off, this->getNode(cur_off));
      if (this->getNode(cur_off) == insert_node) break;
    }
    insert_value = new_group->getNode(cur_off++)->SplitInsert(
        new_group->getNode(cur_off), insert_value, new_child_group);
    new_group->getNode(cur_off)->setPointer(new_child_group);
    for (; cur_off < this->getUsedNodes(); cur_off++)
      new_group->setNode(cur_off + 1, this->getNode(cur_off));

    return new_group;
  }
  cout << "[ERROR] CSB+-Tree.h->CCSBInternalNodeGroup->Insert():"
       " The node group is full!\n";
  return NULL;
}

template<typename T>
CCSBNodeGroup<T>* CCSBInternalNodeGroup<T>::SplitInsert(
    CCSBNode<T>* insert_node, data_offset<T>& insert_value,
    CCSBNodeGroup<T>* new_child_group) {
  if (this->getUsedNodes() != CCSBNode<T>::getMaxKeys() + 1) {
    cout << "[ERROR] CSB+-Tree.h->CCSBInternalNodeGroup->SplitInsert():"
         " The node group is not full!\n";
    return NULL;
  }
  unsigned node_group_size2 = (CCSBNode<T>::getMaxKeys() + 2) / 2;
  unsigned node_group_size1 = CCSBNode<T>::getMaxKeys() + 2 - node_group_size2;
  CCSBNodeGroup<T>* new_group = new CCSBInternalNodeGroup<T>(node_group_size2);
  CCSBNode<T>* new_node = new CCSBInternalNode<T>();
  insert_value = insert_node->SplitInsert(new_node, insert_value,
                                          new_child_group);
  new_node->setPointer(new_child_group);

  if (insert_node < this->getNode(node_group_size1 - 1)) {    // ??? --Yu
  // move the bigger key node into new group
    for (unsigned i = node_group_size1 - 1; i < this->getUsedNodes(); i++)
      new_group->setNode(i - node_group_size1 + 1, this->getNode(i));

    // find a place in this node to insert new_node
    for (int i = node_group_size1 - 2; i >= 0; i--) {
      if (insert_node < this->getNode(i)) {
        this->setNode(i + 1, this->getNode(i));
      }
      else if (insert_node == this->getNode(i)) {
        this->setNode(i + 1, new_node);
        break;
      }
    }
  }
  else {
    // move the new_node and bigger node into new_group
    unsigned cur_off = node_group_size1;
    for (;
        cur_off < this->getUsedNodes() && this->getNode(cur_off) <= insert_node;
        cur_off++)
      new_group->setNode(cur_off - node_group_size1, this->getNode(cur_off));

    new_group->setNode(cur_off - node_group_size1, new_node);
    for (; cur_off < this->getUsedNodes(); cur_off++)
      new_group->setNode(cur_off + 1 - node_group_size1,
                         this->getNode(cur_off));
  }
  this->setUsedNodes(node_group_size1);
  return new_group;
}

template<typename T>
void CCSBInternalNodeGroup<T>::DeleteChildren() {
  for (unsigned i = 0; i < this->used_nodes; i++)
    internal_nodes[i]->DeleteChildren();
}

template<typename T>
bool CCSBInternalNodeGroup<T>::serialize(FILE* filename) {
  fwrite(static_cast<void*>(&this->used_nodes), sizeof(unsigned), 1, filename);
  for (unsigned i = 0; i < this->used_nodes; i++)
    internal_nodes[i]->serialize(filename);
  return true;
}

template<typename T>
bool CCSBInternalNodeGroup<T>::deserialize(FILE* filename) {
  fread(static_cast<void*>(&this->used_nodes), sizeof(unsigned), 1, filename);
  internal_nodes = (CCSBNode<T>**) new CCSBInternalNode<T>*[this->used_nodes];
  for (unsigned i = 0; i < this->used_nodes; i++) {
    internal_nodes[i] = new CCSBInternalNode<T>();
    internal_nodes[i]->deserialize(filename);
  }
  return true;
}

/******************************  LeafNodeGroup  ******************************/
template<typename T>
class CCSBLeafNodeGroup : public CCSBNodeGroup<T> {
 public:
  CCSBLeafNodeGroup()
      : leaf_nodes(NULL),
        p_header(NULL),
        p_tailer(NULL) {
    this->setUsedNodes(0);
  }
  explicit CCSBLeafNodeGroup(unsigned n);
  ~CCSBLeafNodeGroup();

  CCSBNodeGroup<T>* getHeaderNG() {
    return p_header;
  }
  void setHeaderNG(CCSBNodeGroup<T>* header) {
    p_header = header;
  }
  CCSBNodeGroup<T>* getTailerNG() {
    return p_tailer;
  }
  void setTailerNG(CCSBNodeGroup<T>* tailer) {
    p_tailer = tailer;
  }

  inline bool setUsedNodes(unsigned n) {
    if (n <= CCSBNode<T>::getMaxKeys() + 1) {
      this->used_nodes = n;
      return true;
    }
    return false;
  }
  CCSBNode<T>* getNode(unsigned i) {
    if (i > this->used_nodes) {
      cout << "[ERROR: CSBPlusTree.h<CCSBLeafNodeGroup>->getNode()]:"
           " The offset i: "
           << i << " is invalid!\n";
      return NULL;
    }
    return leaf_nodes[i];
  }
  void setNode(unsigned i, CCSBNode<T>* node) {
    for (unsigned j = 0; j < node->getUsedKeys(); j++)
      leaf_nodes[i]->setElement(j, node->getElement(j));
    leaf_nodes[i]->setUsedKeys(node->getUsedKeys());
    leaf_nodes[i]->setUsedKeys(node->getUsedKeys());
  }

  CCSBNodeGroup<T>* Insert(CCSBNode<T>* insert_node,
                           data_offset<T> &insert_value,
                           CCSBNodeGroup<T>* new_child_group);
  CCSBNodeGroup<T>* SplitInsert(CCSBNode<T>* insert_node,
                                data_offset<T>& insert_value,
                                CCSBNodeGroup<T>* new_child_group);
  void DeleteChildren();
  bool serialize(FILE* filename);
  bool deserialize(FILE* filename);

 public:
  CCSBNode<T>** leaf_nodes;
  CCSBNodeGroup<T>* p_header;
  CCSBNodeGroup<T>* p_tailer;
};

template<typename T>
CCSBLeafNodeGroup<T>::CCSBLeafNodeGroup(unsigned n) {
  leaf_nodes = (CCSBNode<T>**) new CCSBLeafNode<T>*[n];
  for (unsigned i = 0; i < n; i++)
    leaf_nodes[i] = new CCSBLeafNode<T>();
  this->setUsedNodes(n);
  p_header = NULL;
  p_tailer = NULL;
}

template<typename T>
CCSBLeafNodeGroup<T>::~CCSBLeafNodeGroup() {
  for (unsigned i = 0; i < this->used_nodes; i++)
    ((CCSBLeafNode<T>*) leaf_nodes[i])->~CCSBLeafNode();
  this->~CCSBNodeGroup<T>();
  p_header = NULL;
  p_tailer = NULL;
}

template<typename T>
CCSBNodeGroup<T>* CCSBLeafNodeGroup<T>::Insert(
    CCSBNode<T>* insert_node, data_offset<T>& insert_value,
    CCSBNodeGroup<T>* new_child_group) {
  if (this->getUsedNodes() < CCSBNode<T>::getMaxKeys() + 1) {
    CCSBNodeGroup<T>* new_group = new CCSBLeafNodeGroup(
        this->getUsedNodes() + 1);
    unsigned cur_off = 0;
    for (cur_off = 0; cur_off <= this->getUsedNodes(); cur_off++) {
      new_group->setNode(cur_off, this->getNode(cur_off));
      if (this->getNode(cur_off) == insert_node) break;
    }
    // move half key of insert_node to new allocated node
    insert_value = new_group->getNode(cur_off++)->SplitInsert(
        new_group->getNode(cur_off), insert_value, NULL);
    for (; cur_off < this->getUsedNodes(); cur_off++)
      new_group->setNode(cur_off + 1, this->getNode(cur_off));

    // use new_group replace current node in term of pointer
    new_group->setHeaderNG(this->getHeaderNG());
    new_group->setTailerNG(this->getTailerNG());
    if (this->getHeaderNG() != NULL) this->getHeaderNG()->setTailerNG(
        new_group);
    if (this->getTailerNG() != NULL) this->getTailerNG()->setHeaderNG(
        new_group);
    return new_group;
  }
  cout << "[ERROR] CSB+-Tree.h->CCSBLeafNodeGroup->Insert():"
       " The node group is full!";
  return NULL;
}

template<typename T>
CCSBNodeGroup<T>* CCSBLeafNodeGroup<T>::SplitInsert(
    CCSBNode<T>* insert_node, data_offset<T>& insert_value,
    CCSBNodeGroup<T>* new_child_group) {
  if (this->getUsedNodes() != CCSBNode<T>::getMaxKeys() + 1) {
    cout << "[ERROR] CSB+-Tree.h->CCSBLeafNodeGroup->SplitInsert():"
         " The node group is not full!\n";
    return NULL;
  }
  unsigned node_group_size2 = (CCSBNode<T>::getMaxKeys() + 2) / 2;
  unsigned node_group_size1 = CCSBNode<T>::getMaxKeys() + 2 - node_group_size2;
  CCSBNodeGroup<T>* new_group = new CCSBLeafNodeGroup<T>(node_group_size2);
  CCSBNode<T>* new_node = new CCSBLeafNode<T>();
  insert_value = insert_node->SplitInsert(new_node, insert_value, NULL);

  if (insert_node < this->getNode(node_group_size1 - 1)) {  // according the pointer location, decide the node location
    for (unsigned i = node_group_size1 - 1; i < this->getUsedNodes(); i++)
      new_group->setNode(i - node_group_size1 + 1, this->getNode(i));
    for (unsigned i = node_group_size1 - 2; i >= 0; i--) {
      if (insert_node < this->getNode(i)) {
        this->setNode(i + 1, this->getNode(i));
      }
      else if (insert_node == this->getNode(i)) {
        this->setNode(i + 1, new_node);
        break;
      }
    }
  } else {
    unsigned cur_off = node_group_size1;
    for (;
        cur_off < this->getUsedNodes()
            && (this->getNode(cur_off) <= insert_node); cur_off++)
      new_group->setNode(cur_off - node_group_size1, getNode(cur_off));

    new_group->setNode(cur_off - node_group_size1, new_node);
    for (; cur_off < this->getUsedNodes(); cur_off++)
      new_group->setNode(cur_off + 1 - node_group_size1,
                         this->getNode(cur_off));
  }
  new_group->setHeaderNG(this);
  new_group->setTailerNG(this->getTailerNG());
  this->setTailerNG(new_group);
  if (new_group->getTailerNG() != NULL) new_group->getTailerNG()->setHeaderNG(
      new_group);
  this->setUsedNodes(node_group_size1);
  return new_group;
}

template<typename T>
void CCSBLeafNodeGroup<T>::DeleteChildren() {
  for (unsigned i = 0; i < this->used_nodes; i++)
    leaf_nodes[i]->DeleteChildren();
}

template<typename T>
bool CCSBLeafNodeGroup<T>::serialize(FILE* filename) {
  fwrite(static_cast<void*>(&this->used_nodes), sizeof(unsigned), 1, filename);
  for (unsigned i = 0; i < this->used_nodes; i++)
    leaf_nodes[i]->serialize(filename);
  return true;
}

template<typename T>
bool CCSBLeafNodeGroup<T>::deserialize(FILE* filename) {
  fread(static_cast<void*>(&this->used_nodes), sizeof(unsigned), 1, filename);
  leaf_nodes = (CCSBNode<T>**) new CCSBLeafNode<T>*[this->used_nodes];
  for (unsigned i = 0; i < this->used_nodes; i++) {
    leaf_nodes[i] = new CCSBLeafNode<T>();
    leaf_nodes[i]->deserialize(filename);
  }
  return true;
}

/*******************************  CSBPlusTree  *******************************/
template<typename T>
class CSBPlusTree {
 public:
  CSBPlusTree()
      : csb_root(NULL),
        csb_depth(0),
        leaf_header(NULL),
        leaf_tailer(NULL) {
    max_keys = CCSBNode<T>::getMaxKeys();
    max_datas = CCSBNode<T>::getMaxDatas();
  }
  virtual ~CSBPlusTree();
  // bulkload indexing
  void BulkLoad(data_offset<T>* aray, unsigned arayNo);
  // search certain records according to the key
  map<index_offset, vector<index_offset>*>* Search(T key);
  map<index_offset, vector<index_offset>*>* rangeQuery(T lower_key,
                                                       T upper_key);
  map<index_offset, vector<index_offset>*>* rangeQuery(T lower_key,
                                                       comparison comp_lower,
                                                       T upper_key,
                                                       comparison comp_upper);
  // insert a record
  bool Insert(data_offset<T> insert_data);
  // save the index structure to disk
  bool serialize(FILE* filename);
  bool deserialize(FILE* filename);

  // for testing
  void printTree();
  void printDoubleLinkedList();

 public:
  // the leaf nodes list
  CCSBNodeGroup<T>* leaf_header;
  CCSBNodeGroup<T>* leaf_tailer;
  // root of the index CSB+ Tree
  CCSBNode<T>* csb_root;
  // depth of the index CSB+ Tree
  unsigned csb_depth;

 private:
  unsigned max_keys;
  unsigned max_datas;

 private:
  // 将aray填入leafNodeGroup中，并记录上层索引key
  int makeLeafNodeGroup(data_offset<T>* aray, unsigned aray_num,
                        CCSBNodeGroup<T>** leaf, T* internal_key_array);
  // 建立最底层索引，直接索引leafNodeGroup层，并记录其上层索引key
  int makeInternalNodeGroup(T* internalAray, int iArayNo,
                            CCSBNodeGroup<T>** internal, int leafNGNo,
                            T* in2Aray, CCSBNodeGroup<T>** leaf);
// 	// 循环建立中间索引层
// 	int makeInternal2NodeGroup(
//    T* internalAray, int iArayNo, CCSBNodeGroup<T>** internal,
//    int leafNGNo, T* in2Aray, CCSBNodeGroup<T>** leaf);
  //  为插入而查找叶子结点
  CCSBNode<T>* SearchLeafNode(T key, stack<CCSBNode<T>*> &insert_path);
  //  插入键到中间结点
  bool InsertInternalNode(CCSBInternalNode<T>* pNode, T key,
                          CCSBNode<T>* pRightSon);
  //  在中间结点中删除键
  bool DeleteInternalNode(CCSBInternalNode<T>* pNode, T key);
  //  清除树
  void ClearTree();
};

template<typename T>
CSBPlusTree<T>::~CSBPlusTree() {
  // TODO Auto-generated destructor stub
  ClearTree();
}

template<typename T>
void CSBPlusTree<T>::BulkLoad(data_offset<T>* aray, unsigned aray_num) {
  if (aray_num == 0) return;
  // just one CSBNode
  if (aray_num <= max_datas) {
    this->csb_depth = 1;
    CCSBNodeGroup<T>* leaf = new CCSBLeafNodeGroup<T>(1);

    for (unsigned i = 0; i < aray_num; i++) {
      leaf->getNode(0)->setElement(i, aray[i]);
    }
    leaf->getNode(0)->setUsedKeys(aray_num);

    // set the root and the leaf doubly linked list
    this->csb_root = leaf->getNode(0);
    leaf->setHeaderNG(NULL);
    leaf->setTailerNG(NULL);
    this->leaf_header = leaf;
    this->leaf_tailer = leaf;
  }
  else {  // more than one layer: bottom up construct
    // allocate the leaf_node_group
    // ceil((ceil(1.0 * aray_num / max_datas)) / (max_keys + 1.0));
    // (array_num - 1 ) / max_datas / (max_keys + 1) + 1
    int leaf_node_group_num = ceil(
        (double) (ceil((double) (aray_num) / (double) (max_datas)))
            / (double) (max_keys + 1));
    CCSBNodeGroup<T>** leaf =
        (CCSBNodeGroup<T>**) new CCSBLeafNodeGroup<T>*[leaf_node_group_num];

    // allocate the parent_key_array of the leaf_node_group
    int internal_key_array_num = ceil((double) aray_num / (double) max_datas)
        - 1;
    T* internal_key_array = new T[internal_key_array_num];
    // 建立最底层叶子结点，并返回其索引internalAray
    internal_key_array_num = makeLeafNodeGroup(aray, aray_num, leaf,
                                               internal_key_array);

    // used the internal_key_array to build the internal indexing node;
    // the indexing array is in internal_key_array,
    //internal_key_array_num stands for the number of keys in it
    // set the p_father and p_child_node_group between index nodes
    if (internal_key_array_num <= max_keys) {
      this->csb_depth += 1;
      CCSBNodeGroup<T>* internal = new CCSBInternalNodeGroup<T>(1);

      for (unsigned i = 0; i < internal_key_array_num; i++) {
        internal->getNode(0)->setElement(i, internal_key_array[i]);
      }
      ((CCSBNode<T>*) (internal->getNode(0)))->setUsedKeys(
          internal_key_array_num);

      // set the parent-child relationship between internal and leaf
      ((CCSBNode<T>*) (internal->getNode(0)))->setPointer(
          (CCSBNodeGroup<T>*) (*leaf));

      this->csb_root = internal->getNode(0);
    }
    else {
      // allocate upper node_group (equals to the lower_node_groups/branches)
      int internal_node_group_num = ceil(
          (double) leaf_node_group_num / (double) (max_keys + 1));  // 1 node in internal group node match a leaf group node
      CCSBNodeGroup<T>** internal =
          (CCSBNodeGroup<T>**) new CCSBInternalNodeGroup<T>*[internal_node_group_num];

      // allocate the key_array for the upper internal_key_array
      T* upper_internal_key_array = new T[leaf_node_group_num - 1];
      int upper_internal_key_array_num = makeInternalNodeGroup(
          internal_key_array, internal_key_array_num, internal,
          leaf_node_group_num, upper_internal_key_array, leaf);

      // loop construct the internal node groups until reach the root
      while (upper_internal_key_array_num > max_keys) {
        // 释放已经建立索引的internalAray数据，将其上层索引in2Aray赋给该指针准备再次建立索引
        delete[] internal_key_array;
        internal_key_array = upper_internal_key_array;
        internal_key_array_num = upper_internal_key_array_num;
        leaf_node_group_num = internal_node_group_num;
        CCSBNodeGroup<T>** leaf = internal;

        // 申请足够的中间结点group空间，注：上层索引node的个数即为下层的nodegroup数
        internal_node_group_num = ceil(
            (double) leaf_node_group_num / (double) (max_keys + 1));
        internal =
            (CCSBNodeGroup<T>**) new CCSBInternalNodeGroup<T>*[internal_node_group_num];

        // 申请更上一层内部结点空间，即为本层node数目-1
        upper_internal_key_array = new T[leaf_node_group_num - 1];

        upper_internal_key_array_num = makeInternalNodeGroup(
            internal_key_array, internal_key_array_num, internal,
            leaf_node_group_num, upper_internal_key_array, leaf);
      }
      if (upper_internal_key_array_num <= max_keys) {	 // 建立根结点
        // 释放已经建立索引的internalAray数据，将其上层索引in2Aray赋给该指针准备再次建立索引
        delete[] internal_key_array;
        internal_key_array = upper_internal_key_array;
        internal_key_array_num = upper_internal_key_array_num;
        leaf_node_group_num = internal_node_group_num;
        CCSBNodeGroup<T>** leaf = internal;

        this->csb_depth += 1;
        CCSBInternalNodeGroup<T>* internal = new CCSBInternalNodeGroup<T>(1);

        // 将数据填入internalnode中
        for (int i = 0; i < internal_key_array_num; i++) {
          internal->internal_nodes[0]->setElement(i, internal_key_array[i]);
        }
        internal->internal_nodes[0]->used_keys = internal_key_array_num;

        // 设置与叶子层的父子关系
        ((CCSBNode<T>*) internal->getNode(0))->setPointer(
            (CCSBNodeGroup<T>*) leaf[0]);

        this->csb_root = internal->internal_nodes[0];
      }
    }
  }
}

template<typename T>
int CSBPlusTree<T>::makeLeafNodeGroup(data_offset<T>* aray, unsigned aray_num,
                                      CCSBNodeGroup<T>** leaf,
                                      T* internal_key_array) {
  // m_Depth记录树高度，建立叶子层其自加1
  csb_depth += 1;

  // 计算leafNodeGroup的数目
  int leaf_node_group_num = ceil(
      (double) (ceil((double) (aray_num) / (double) (max_datas)))
          / (double) (max_keys + 1));

  int internal_key_array_off = 0;  // 记录internalAray下标

  // 如果只有一个leafNodeGroup
  if (leaf_node_group_num == 1) {
    // 判断之中有几个leafNode （arayNo <= CSB_MAXNUM_DATA的情况已单独讨论过，至少有两个leafNode结点）
    int leaf_node_num = ceil((double) aray_num / (double) max_datas);
    leaf[0] = new CCSBLeafNodeGroup<T>(leaf_node_num);

    // avoid the last node having less than max_datas/2 key which is threshold
    // 将数据填入leaf[0]中，对前leafNNo-2个结点而言，每个leafNode中均填满CSB_MAXNUM_DATA个数据
    unsigned i = 0, counter = 0;
    while (counter < leaf_node_num - 2) {
      unsigned k = 0;
      for (k = 0; k < max_datas && i < aray_num; k++) {
        leaf[0]->getNode(counter)->setElement(i % max_datas, aray[i]);
        if (counter != 0 && k == 0)  // ??? what i think should be k == max_datas - 1 --Yu
        internal_key_array[internal_key_array_off++] = aray[i]._key;
        i += 1;
      }
      leaf[0]->getNode(counter)->setUsedKeys(k);
      counter += 1;
    }
    if (aray_num - (leaf_node_num - 1) * max_datas >= max_datas / 2) {  // 不需平衡最后两个结点
      int k = 0;
      while (counter < leaf_node_num) {
        for (k = 0; k < max_datas && i < aray_num; k++) {
          ((CCSBNode<T>*) (leaf[0]->getNode(counter)))->setElement(k, aray[i]);
          if (counter != 0 && k == 0) internal_key_array[internal_key_array_off++] =
              aray[i]._key;
          i += 1;
        }
        ((CCSBNode<T>*) (leaf[0]->getNode(counter)))->setUsedKeys(k);
        counter += 1;
      }
    }
    else {  // 最后一个结点data数目小于CSB_ORDER_V，需平衡最后两个结点
      // 剩余的data数目
      int rest = aray_num - max_datas * (leaf_node_num - 2);
      if (rest % 2 == 1)
        (leaf[0]->getNode(counter))->setUsedKeys(rest / 2 + 1);
      else
        (leaf[0]->getNode(counter))->setUsedKeys(rest / 2);

      leaf[0]->getNode(counter + 1)->setUsedKeys(rest / 2);
      while (counter < leaf_node_num) {
        for (int k = 0; k < leaf[0]->getNode(counter)->getUsedKeys(); k++) {
          leaf[0]->getNode(counter)->setElement(k, aray[i]);
          if (counter != 0 && k == 0) internal_key_array[internal_key_array_off++] =
              aray[i]._key;
          i += 1;
        }
        counter += 1;
      }
    }
  }
  else {  // 至少有两个leafNodeGroup
    unsigned i = 0, counter = 0;  // 记录aray下标、leaf下标
    while (counter < leaf_node_group_num - 2) {  // 对前leafNGNo-2个NG而言，每个NG中均填满CSB_MAXNUM_KEY+1个leafNode
      leaf[counter++] = new CCSBLeafNodeGroup<T>(max_keys + 1);
    }
    int rest = ceil(
        (double) (aray_num
            - (leaf_node_group_num - 1) * (max_keys + 1) * max_datas)
            / (double) max_datas);
    if (rest >= max_keys / 2 + 1) {  // 无需平衡最后两个NG
    // 倒数第二个NG仍满，最后一个NG中leafNode的数目需要计算
      leaf[counter] = new CCSBLeafNodeGroup<T>(max_keys + 1);
      leaf[counter + 1] = new CCSBLeafNodeGroup<T>(rest);
    }
    else {  // 最后一个NG中leafNode数目小于CSB_ORDER_V+1，需平衡最后两个NG
      rest = rest + (max_keys + 1);
      if (rest % 2 == 0)
        leaf[counter] = new CCSBLeafNodeGroup<T>(rest / 2);
      else
        leaf[counter] = new CCSBLeafNodeGroup<T>(rest / 2 + 1);
      leaf[counter + 1] = new CCSBLeafNodeGroup<T>(rest / 2);
    }

    // 将data填入之前申请好的leafNG中

    // 注意，最后一个NG中的最后两个leafNode可能需要平衡
    for (counter = 0; counter < leaf_node_group_num - 1; counter++) {
      for (int j = 0; j < leaf[counter]->used_nodes; j++) {
        unsigned k = 0;
        for (k = 0; k < max_datas; k++) {
          leaf[counter]->getNode(j)->setElement(k, aray[i]);
          if (i != 0 && k == 0) internal_key_array[internal_key_array_off++] =
              aray[i]._key;
          i += 1;
        }
        ((CCSBNode<T>*) (leaf[counter]->getNode(j)))->setUsedKeys(k);
      }
    }
    // 最后一个NG的填入
    int restdata = aray_num % max_datas;
    if (restdata == 0) restdata = max_datas;
    // 确定每个leafNode中填充的data数目
    int j = 0;
    for (j = 0; j < leaf[counter]->used_nodes - 2; j++) {
      ((CCSBNode<T>*) (leaf[counter]->getNode(j)))->setUsedKeys(max_datas);
    }
    if (restdata >= max_datas / 2) {  // 无需平衡
      ((CCSBNode<T>*) (leaf[counter]->getNode(j)))->setUsedKeys(max_datas);
      ((CCSBNode<T>*) (leaf[counter]->getNode(j + 1)))->setUsedKeys(restdata);
    }
    else {  // 平衡最后两个leafNode
      restdata += max_datas;
      if (restdata % 2 == 0)
        ((CCSBNode<T>*) (leaf[counter]->getNode(j)))->setUsedKeys(restdata / 2);
      else
        ((CCSBNode<T>*) (leaf[counter]->getNode(j)))->setUsedKeys(
            restdata / 2 + 1);
      ((CCSBNode<T>*) (leaf[counter]->getNode(j + 1)))->setUsedKeys(
          restdata / 2);
    }
    for (j = 0; j < leaf[counter]->used_nodes; j++) {
      for (unsigned k = 0;
          k < ((CCSBNode<T>*) (leaf[counter]->getNode(j)))->getUsedKeys();
          k++) {
        ((CCSBNode<T>*) (leaf[counter]->getNode(j)))->setElement(k, aray[i]);
        if (i != 0 && k == 0) internal_key_array[internal_key_array_off++] =
            aray[i]._key;
        i++;
      }
    }
  }

  // set the doubly linked list in the leaf node group
  leaf_header = leaf[0];
  leaf[0]->setHeaderNG(NULL);
  leaf_tailer = leaf[leaf_node_group_num - 1];
  leaf[leaf_node_group_num - 1]->setTailerNG(NULL);
  for (unsigned i = 0; i < leaf_node_group_num - 1; i++)
    leaf[i]->setTailerNG(leaf[i + 1]);
  for (unsigned i = 1; i < leaf_node_group_num; i++)
    leaf[i]->setHeaderNG(leaf[i - 1]);
  return internal_key_array_off;
}

template<typename T>
int CSBPlusTree<T>::makeInternalNodeGroup(T* aray, int aray_num,
                                          CCSBNodeGroup<T>** internal,
                                          int leaf_node_group_num,
                                          T* upper_key_array,
                                          CCSBNodeGroup<T>** leaf) {
  // m_Depth记录树高度，建立叶子层其自加1
  csb_depth += 1;
  // 计算internalNodeGroup数目
  int internal_node_group_num = ceil(
      (double) leaf_node_group_num / (double) (max_keys + 1));
// 	internal = (CCSBNodeGroup<T>**)new CCSBInternalNodeGroup<T>* [internal_node_group_num];

  int upper_key_array_off = 0;  // 记录in2Aray下标

  int counter = 0;  // 用于记录internal的下标

  if (internal_node_group_num >= 2) {
    while (counter < internal_node_group_num - 2) {  // 除了最后两个NG外，其他的NG均填满CSB_MAXNUM_KEY+1个node
      internal[counter] = new CCSBInternalNodeGroup<T>(max_keys + 1);
      counter += 1;
    }
    int rest = leaf_node_group_num
        - (internal_node_group_num - 1) * (max_keys + 1);
    if (rest >= max_keys / 2 + 1) {  // 不需平衡最后两个NG
      internal[counter] = new CCSBInternalNodeGroup<T>(max_keys + 1);
      internal[counter + 1] = new CCSBInternalNodeGroup<T>(rest);
    }
    else {
      // 最后一个NG中leafNode数目小于CSB_ORDER_V+1，需平衡最后两个NG
      rest = rest + (max_keys + 1);
      if (rest % 2 == 0)
        internal[counter] = new CCSBInternalNodeGroup<T>(rest / 2);
      else
        internal[counter] = new CCSBInternalNodeGroup<T>(rest / 2 + 1);
      internal[counter + 1] = new CCSBInternalNodeGroup<T>(rest / 2);
    }
  }
  else
    // 只有一个NG
    internal[counter] = new CCSBInternalNodeGroup<T>(leaf_node_group_num);

  // 每个internal中包含的node数目申请结束，将internalAray中的数据填入internal中，并生成该层的索引in2Aray
  // 其下一层的NG中只有最后两个可能不满，所以该层NG中的所有node只有最后两个可能不满，即最后一个NG要特殊处理
  int i = 0;  // 用于记录internalAray的下标
  for (counter = 0; counter < internal_node_group_num - 1; counter++) {
    for (int j = 0; j < internal[counter]->getUsedNodes(); j++) {
      int k = 0;
      for (k = 0; k < max_keys; k++) {
        internal[counter]->getNode(j)->setElement(k, aray[i++]);
      }
      ((CCSBNode<T>*) (internal[counter]->getNode(j)))->setUsedKeys(k);
      upper_key_array[upper_key_array_off++] = aray[i++];
    }
  }
  // 最后一个NG
  int j = 0;
  for (j = 0; j < internal[counter]->getUsedNodes() - 2; j++) {
    int k = 0;
    for (k = 0; k < max_keys; k++) {
      ((CCSBNode<T>*) (internal[counter]->getNode(j)))->setElement(k,
                                                                   aray[i++]);
    }
    ((CCSBNode<T>*) (internal[counter]->getNode(j)))->setUsedKeys(k);
    upper_key_array[upper_key_array_off++] = aray[i++];
  }
  // 最后两个node中的key数目为leaf中最后两个nodegroup中node数-1
  ((CCSBNode<T>*) (internal[counter]->getNode(j)))->setUsedKeys(
      leaf[leaf_node_group_num - 2]->getUsedNodes() - 1);
  ((CCSBNode<T>*) (internal[counter]->getNode(j + 1)))->setUsedKeys(
      leaf[leaf_node_group_num - 1]->getUsedNodes() - 1);
  for (; j < internal[counter]->getUsedNodes(); j++) {
    int k = 0;
    for (k = 0; k < internal[counter]->getNode(j)->getUsedKeys(); k++) {
      internal[counter]->getNode(j)->setElement(k, aray[i++]);
    }
    ((CCSBNode<T>*) (internal[counter]->getNode(j)))->setUsedKeys(k);
    if (i < aray_num) upper_key_array[upper_key_array_off++] = aray[i++];
  }

  // 设置父子关系
  int leafi = 0;  // 用于记录leaf层NG的编号
  for (counter = 0; counter < internal_node_group_num; counter++)
    for (int j = 0; j < internal[counter]->getUsedNodes(); j++)
      internal[counter]->getNode(j)->setPointer(leaf[leafi++]);

  return upper_key_array_off;
}

template<typename T>
map<index_offset, vector<index_offset>*>* CSBPlusTree<T>::Search(T key) {
  map<index_offset, vector<index_offset>*>* ret = new map<index_offset,
      vector<index_offset>*>;
  ret->clear();
  int i = 0;

  CCSBNode<T>* search_node = csb_root;
  CCSBNode<T>* p_search_node = NULL;

  // find the leaf node
  for (unsigned depth = 1; depth < this->csb_depth; depth++) {
    // find the first search_node.key >= key
    for (i = 0;
        key > search_node->getElement(i)._key && i < search_node->used_keys;
        i++) {
    }
    p_search_node = search_node;
    search_node = (search_node->getPointer())->getNode(i);
  }
  // not found
  if (NULL == search_node) return ret;

  // finding the first data in leaf_node whose key = search key
  for (i = 0; (i < search_node->used_keys); i++) {
    if (key == search_node->getElement(i)._key) {
      break;
    }
  }

  // collect all tuples whose key equals to search key
  if (p_search_node == NULL) {
    for (; i < search_node->getUsedKeys(); i++) {
      if (key == search_node->getElement(i)._key) {
        index_offset tmp = search_node->getElement(i)._block_off;
        if (ret->find(tmp) == ret->end()) (*ret)[tmp] =
            new vector<index_offset>;
        (*ret)[tmp]->push_back(search_node->getElement(i)._tuple_off);
      }
      else
        return ret;
    }
    return ret;
  }
  CCSBNodeGroup<T>* search_node_group = p_search_node->getPointer();
  unsigned j = 0;
  for (j = 0; j < search_node_group->getUsedNodes(); j++) {
    if (search_node == search_node_group->getNode(j)) break;
  }
  while (search_node_group != NULL) {
    for (; j < search_node_group->getUsedNodes(); j++) {
      for (; i < search_node_group->getNode(j)->getUsedKeys(); i++) {
        if (key == search_node_group->getNode(j)->getElement(i)._key) {
          index_offset tmp =
              search_node_group->getNode(j)->getElement(i)._block_off;
          if (ret->find(tmp) == ret->end())
            (*ret)[tmp] = new vector<index_offset>;

          (*ret)[tmp]->push_back(
              search_node_group->getNode(j)->getElement(i)._tuple_off);
        }
        else
          return ret;
      }
      i = 0;
    }
    i = 0;
    j = 0;
    search_node_group = search_node_group->getTailerNG();
  }
  return ret;
}

template<typename T>
map<index_offset, vector<index_offset>*>* CSBPlusTree<T>::rangeQuery(
    T lower_key, T upper_key) {
  map<index_offset, vector<index_offset>*>* ret = new map<index_offset,
      vector<index_offset>*>;
  ret->clear();
  if (lower_key > upper_key) return ret;
  int i = 0;

  CCSBNode<T>* search_node = csb_root;
  CCSBNode<T>* p_search_node = NULL;

  // find the leaf node
  for (unsigned depth = 1; depth < this->csb_depth; depth++) {
    // find the first search_node.key >= lower_key
    for (i = 0;
        (lower_key > search_node->getElement(i)._key)
            && (i < search_node->used_keys); i++) {
    }
    p_search_node = search_node;
    search_node = (search_node->getPointer())->getNode(i);
  }
  // not found
  if (NULL == search_node) return ret;

  // finding the first data in leaf_node whose key >= lower_key
  for (i = 0; (i < search_node->used_keys); i++) {
    if (lower_key <= search_node->getElement(i)._key) {
      break;
    }
  }

  // collect all tuples whose key is between lower_key and upper_key
  if (p_search_node == NULL) {
    for (; i < search_node->getUsedKeys(); i++) {
      if ((lower_key <= search_node->getElement(i)._key)
          && upper_key >= search_node->getElement(i)._key) {
        index_offset tmp = search_node->getElement(i)._block_off;
        if (ret->find(tmp) == ret->end())
          (*ret)[tmp] = new vector<index_offset>;
        (*ret)[tmp]->push_back(search_node->getElement(i)._tuple_off);
      }
      else
        return ret;
    }
    return ret;
  }
  CCSBNodeGroup<T>* search_node_group = p_search_node->getPointer();
  unsigned j = 0;
  for (j = 0; j < search_node_group->getUsedNodes(); j++) {
    if (search_node == search_node_group->getNode(j)) break;
  }
  while (search_node_group != NULL) {
    for (; j < search_node_group->getUsedNodes(); j++) {
      for (; i < search_node_group->getNode(j)->getUsedKeys(); i++) {
        if ((lower_key <= search_node_group->getNode(j)->getElement(i)._key)
            && (upper_key >= search_node_group->getNode(j)->getElement(i)._key)) {
          index_offset tmp = search_node_group->getNode(j)->getElement(i)
              ._block_off;
          if (ret->find(tmp) == ret->end()) (*ret)[tmp] = new vector<
              index_offset>;
          (*ret)[tmp]->push_back(
              search_node_group->getNode(j)->getElement(i)._tuple_off);
        }
        else {
          return ret;
        }
      }
      i = 0;
    }
    i = 0;
    j = 0;
    search_node_group = search_node_group->getTailerNG();
  }
  return ret;
}

template<typename T>
map<index_offset, vector<index_offset>*>* CSBPlusTree<T>::rangeQuery(
    T lower_key, comparison comp_lower, T upper_key, comparison comp_upper) {
  map<index_offset, vector<index_offset>*>* ret = new map<index_offset, vector<index_offset>*>;
  ret->clear();
  // For point query
  if (comp_lower == EQ) {
    if (lower_key != upper_key) {
      cout << "[ERROR FILE: " << __FILE__ << "] In function " << __func__
          << " line " << __LINE__
          << ": For the equal point query, the lower_key " << lower_key
          << " != the upper_key " << upper_key << "!\n";
      return ret;
    }
    return Search(lower_key);
  }
  else if ((!(comp_lower == G || comp_lower == GEQ))
      || (!(comp_upper == L || comp_upper == LEQ))) {
    //  Range Query
    cout << "[ERROR FILE: " << __FILE__ << "]"
         " In function "
         << __func__ << " line " << __LINE__ << ": For the range query, "
         "the given two compare operator isn't a range!\n";
    return ret;
  }
  if (lower_key > upper_key) {
    cout << "[ERROR FILE: " << __FILE__ << "] In function " << __func__
         << " line " << __LINE__
         << ": For the range query, the given two key isn't a range!\n";
    return ret;
  }

  int i = 0;
  CCSBNode<T>* search_node = csb_root;
  CCSBNode<T>* p_search_node = NULL;

  // find the leaf node
  for (unsigned depth = 1; depth < this->csb_depth; depth++) {
    // find the first search_node.key > lower_key for GEQ and search_node.key >= lower_key for G
    if (comp_lower == GEQ)
      for (i = 0; (lower_key > search_node->getElement(i)._key) && (i < search_node->used_keys); i++);
    else if (comp_lower == G)
      for (i = 0; (lower_key >= search_node->getElement(i)._key) && (i < search_node->used_keys); i++);

    p_search_node = search_node;
    search_node = (search_node->getPointer())->getNode(i);
  }
  // not found
  if (NULL == search_node) return ret;

  // finding the first data in leaf_node whose key >= lower_key
  for (i = 0; (i < search_node->used_keys); i++) {
    if (comp_lower == GEQ && lower_key <= search_node->getElement(i)._key)
        break;

    if (comp_lower == G && lower_key < search_node->getElement(i)._key)
        break;
  }

  // collect all tuples whose key is between lower_key and upper_key
  if (p_search_node == NULL) {
    if (comp_lower == GEQ && comp_upper == LEQ) {
      for (; i < search_node->getUsedKeys(); i++) {
        if ((lower_key <= search_node->getElement(i)._key)
            && upper_key >= search_node->getElement(i)._key) {
          index_offset tmp = search_node->getElement(i)._block_off;
          if (ret->find(tmp) == ret->end()) (*ret)[tmp] = new vector<
              index_offset>;
          (*ret)[tmp]->push_back(search_node->getElement(i)._tuple_off);
        }
        else
          return ret;
      }
    }
    else if (comp_lower == GEQ && comp_upper == L) {
      for (; i < search_node->getUsedKeys(); i++) {
        if ((lower_key <= search_node->getElement(i)._key)
            && upper_key > search_node->getElement(i)._key) {
          index_offset tmp = search_node->getElement(i)._block_off;
          if (ret->find(tmp) == ret->end()) (*ret)[tmp] = new vector<
              index_offset>;
          (*ret)[tmp]->push_back(search_node->getElement(i)._tuple_off);
        }
        else
          return ret;
      }
    }
    else if (comp_lower == G && comp_upper == LEQ) {
      for (; i < search_node->getUsedKeys(); i++) {
        if ((lower_key < search_node->getElement(i)._key)
            && upper_key >= search_node->getElement(i)._key) {
          index_offset tmp = search_node->getElement(i)._block_off;
          if (ret->find(tmp) == ret->end()) (*ret)[tmp] = new vector<
              index_offset>;
          (*ret)[tmp]->push_back(search_node->getElement(i)._tuple_off);
        }
        else
          return ret;
      }
    }
    else if (comp_lower == G && comp_upper == L) {
      for (; i < search_node->getUsedKeys(); i++) {
        if ((lower_key < search_node->getElement(i)._key)
            && upper_key > search_node->getElement(i)._key) {
          index_offset tmp = search_node->getElement(i)._block_off;
          if (ret->find(tmp) == ret->end()) (*ret)[tmp] = new vector<
              index_offset>;
          (*ret)[tmp]->push_back(search_node->getElement(i)._tuple_off);
        }
        else
          return ret;
      }
    }
    return ret;
  }
  CCSBNodeGroup<T>* search_node_group = p_search_node->getPointer();
  unsigned j = 0;
  for (j = 0; j < search_node_group->getUsedNodes(); j++) {
    if (search_node == search_node_group->getNode(j)) break;
  }
  while (search_node_group != NULL) {
    if (comp_lower == GEQ && comp_upper == LEQ) {
      for (; j < search_node_group->getUsedNodes(); j++) {
        for (; i < search_node_group->getNode(j)->getUsedKeys(); i++) {
          if ((lower_key <= search_node_group->getNode(j)->getElement(i)._key)
              && (upper_key >= search_node_group->getNode(j)->getElement(i)._key)) {
            index_offset tmp = search_node_group->getNode(j)->getElement(i)
                ._block_off;
            if (ret->find(tmp) == ret->end()) (*ret)[tmp] = new vector<
                index_offset>;
            (*ret)[tmp]->push_back(
                search_node_group->getNode(j)->getElement(i)._tuple_off);
          }
          else
            return ret;
        }
        i = 0;
      }
      i = 0;
      j = 0;
      search_node_group = search_node_group->getTailerNG();
    }
    else if (comp_lower == GEQ && comp_upper == L) {
      for (; j < search_node_group->getUsedNodes(); j++) {
        for (; i < search_node_group->getNode(j)->getUsedKeys(); i++) {
          if ((lower_key <= search_node_group->getNode(j)->getElement(i)._key)
              && (upper_key > search_node_group->getNode(j)->getElement(i)._key)) {
            index_offset tmp = search_node_group->getNode(j)->getElement(i)
                ._block_off;
            if (ret->find(tmp) == ret->end()) (*ret)[tmp] = new vector<
                index_offset>;
            (*ret)[tmp]->push_back(
                search_node_group->getNode(j)->getElement(i)._tuple_off);
          }
          else
            return ret;
        }
        i = 0;
      }
      i = 0;
      j = 0;
      search_node_group = search_node_group->getTailerNG();
    }
    else if (comp_lower == G && comp_upper == LEQ) {
      for (; j < search_node_group->getUsedNodes(); j++) {
        for (; i < search_node_group->getNode(j)->getUsedKeys(); i++) {
          if ((lower_key < search_node_group->getNode(j)->getElement(i)._key)
              && (upper_key >= search_node_group->getNode(j)->getElement(i)._key)) {
            index_offset tmp = search_node_group->getNode(j)->getElement(i)
                ._block_off;
            if (ret->find(tmp) == ret->end()) (*ret)[tmp] = new vector<
                index_offset>;
            (*ret)[tmp]->push_back(
                search_node_group->getNode(j)->getElement(i)._tuple_off);
          }
          else
            return ret;
        }
        i = 0;
      }
      i = 0;
      j = 0;
      search_node_group = search_node_group->getTailerNG();
    }
    else if (comp_lower == G && comp_upper == L) {
      for (; j < search_node_group->getUsedNodes(); j++) {
        for (; i < search_node_group->getNode(j)->getUsedKeys(); i++) {
          if ((lower_key < search_node_group->getNode(j)->getElement(i)._key)
              && (upper_key > search_node_group->getNode(j)->getElement(i)._key)) {
            index_offset tmp = search_node_group->getNode(j)->getElement(i)
                ._block_off;
            if (ret->find(tmp) == ret->end()) (*ret)[tmp] = new vector<
                index_offset>;
            (*ret)[tmp]->push_back(
                search_node_group->getNode(j)->getElement(i)._tuple_off);
          }
          else
            return ret;
        }
        i = 0;
      }
      i = 0;
      j = 0;
      search_node_group = search_node_group->getTailerNG();
    }
  }
  return ret;
}

/* 插入指定的数据，分几种情况讨论
 * 1. 初始索引树为空，直接生成根结点，并将要插入的数据填入
 * 2. 要插入的目标叶子结点未满，直接填入data
 * 3. 要插入的目标叶子结点已满，判断其上层结点情况
 * 3.1 若该叶子结点无父结点，即其本身为根结点，则将原始的node分裂为同在一个leafNG中的两个node，并建立新的根结点索引该leafNG
 * 3.2 若该叶子结点的父结点未满，则说明该叶子结点所在的leafNG未满，申请新的leafNG=leafNG+1，分裂目标插入结点将data插入，并在其父结点中增加一个索引key
 * 3.3 若该叶子结点的父结点已满，则说明该叶子结点所在的leafNG也已满，申请新的leafNG1，大小为CSB_MAXMUM_KEY/2+1，将原始leafNG中的一半node移动到leafNG1中，插入data；
       并向其父结点中增加一个索引key，导致父结点分裂为两个结点，分别索引leafNG和leafNG1，父结点的分裂策略依其父结点的情况而定
 */
template<typename T>
bool CSBPlusTree<T>::Insert(data_offset<T> insert_data) {
// 	if (Search(data._key) != NULL)	// the key is already exists
// 		return false;

  if (NULL == this->csb_root) {  // the whole index tree is empty
    this->csb_depth = 1;
    CCSBLeafNodeGroup<T>* leaf_group = new CCSBLeafNodeGroup<T>(1);

    // 将数据填入leafnode中
    ((CCSBNode<T>*) (leaf_group->getNode(0)))->setElement(0, insert_data);
    ((CCSBNode<T>*) (leaf_group->getNode(0)))->setUsedKeys(1);
    ((CCSBNode<T>*) (leaf_group->getNode(0)))->setPointer(NULL);
    leaf_group->setHeaderNG(NULL);
    leaf_group->setTailerNG(NULL);
    this->leaf_header = leaf_group;
    this->leaf_tailer = leaf_group;
    this->csb_root = leaf_group->getNode(0);
    return true;
  }
  stack<CCSBNode<T>*> insert_path;
  CCSBNode<T>* insert_node = SearchLeafNode(insert_data._key, insert_path);
  int max_num = max_datas;
  bool isLeaf = true;
  CCSBNodeGroup<T>* new_child = NULL;
  CCSBNode<T>* p_insert_node;
  while (!insert_path.empty()) {
    p_insert_node = insert_path.top();
    insert_path.pop();
    if (insert_node->getUsedKeys() < max_num) {
      if (!isLeaf) insert_node->setPointer(new_child);
      return insert_node->Insert(insert_data);
    }
    else if (p_insert_node->getUsedKeys() < max_keys) {  // parent node is not full
      new_child = p_insert_node->getPointer()
          ->Insert(insert_node, insert_data, new_child);
      // TODO: delete the old node group
      if (p_insert_node->getPointer() == this->leaf_header)
        this->leaf_header = new_child;
      insert_node = p_insert_node;
    }
    else  // parent node is full
    {
      new_child = p_insert_node->getPointer()->SplitInsert(insert_node,
                                                           insert_data,
                                                           new_child);
      // TODO: set p_insert_node child pointer the new_child
      if (this->leaf_tailer->getTailerNG() != NULL)
        this->leaf_tailer = this->leaf_tailer->getTailerNG();
      insert_node = p_insert_node;
    }
    max_num = max_keys;
    isLeaf = false;
  }
  // insert into the root node
  if (insert_node->getUsedKeys() < max_num) {
    if (this->csb_depth != 1) insert_node->setPointer(new_child);
    return insert_node->Insert(insert_data);
  }
  else {
    CCSBNodeGroup<T>* new_group =
        (this->csb_depth == 1) ?
            (CCSBNodeGroup<T>*) new CCSBLeafNodeGroup<T>(2) :
            (CCSBNodeGroup<T>*) new CCSBInternalNodeGroup<T>(2);
    new_group->setNode(0, insert_node);
    insert_data = new_group->getNode(0)->SplitInsert(new_group->getNode(1),
                                                     insert_data, new_child);
    if (this->csb_depth != 1) {
      new_group->getNode(0)->setPointer(insert_node->getPointer());
      new_group->getNode(1)->setPointer(new_child);
    }
    else {
      this->leaf_header = new_group;
      this->leaf_tailer = new_group;
      new_group->setHeaderNG(NULL);
      new_group->setTailerNG(NULL);
    }

    CCSBNode<T>* new_root = new CCSBInternalNode<T>();
    new_root->setElement(0, insert_data._key);
    new_root->setUsedKeys(1);
    new_root->setPointer(new_group);
    this->csb_depth += 1;
    this->csb_root = new_root;
    return true;
  }
}

//  为插入而查找叶子结点
template<typename T>
CCSBNode<T>* CSBPlusTree<T>::SearchLeafNode(T key,
                                            stack<CCSBNode<T>*> &insert_path) {
  int i = 0;
  CCSBNode<T>* search_node = csb_root;

  // find the leaf node
  for (unsigned depth = 1; depth < this->csb_depth; depth++) {
    // find the first search_node.key >= key
    for (i = 0;
        key >= search_node->getElement(i)._key && i < search_node->used_keys;
        i++)
      ;
    insert_path.push(search_node);
    search_node = (search_node->getPointer())->getNode(i);
  }

// 	if (search_node == NULL)
// 		return NULL;
//
// 	// transfer the forward linked list to find the last key = search_key
// 	if (search_node->getFather() == NULL) // root, just one node
// 		return search_node;
//
// 	CCSBNodeGroup<T>* search_node_group = search_node->getFather()->getPointer();
// 	unsigned j = 0;
// 	for (j = 0; j < search_node_group->getUsedNodes(); j++)
// 	{
// 		if (search_node == search_node_group->getNode(j))
// 			break;
// 	}
// 	while (search_node_group != NULL)
// 	{
// 		for (; j < search_node_group->getUsedNodes(); j++)
// 		{
// 			if (key < search_node_group->getNode(j)->getElement(0)._key)
// 			{
// 				search_node = search_node_group->getNode(j-1);
// 				break;
// 			}
// 		}
// 		search_node = search_node_group->getNode(j-1);
// 		j = 0;
// 		search_node_group = search_node_group->getTailerNG();
// 	}
  return search_node;
}

template<typename T>
bool CSBPlusTree<T>::serialize(FILE* filename) {
  if (this->csb_root == NULL) {
    cout << "The index tree is empty! Nothing could be serialized!\n";
    return true;
  }

  vector<CCSBNodeGroup<T>*> current_level;
  vector<CCSBNodeGroup<T>*> lower_level;
  current_level.clear();
  lower_level.clear();
  int depth = 1;

  fwrite((void*) (&csb_depth), sizeof(unsigned), 1, filename);
  csb_root->serialize(filename);
  lower_level.push_back(((CCSBNode<T>*) (this->csb_root))->getPointer());
  depth++;

  while (depth <= csb_depth) {
    current_level.clear();
    while (lower_level.size() != 0) {
      current_level.push_back(lower_level.back());
      lower_level.pop_back();
    }
    lower_level.clear();

    while (current_level.size() != 0) {
      if (depth != csb_depth) {
        for (unsigned i = 0; i < current_level.back()->getUsedNodes(); i++)
          lower_level.push_back(current_level.back()->getNode(i)->getPointer());
      }
      current_level.back()->serialize(filename);
      current_level.pop_back();
    }
    depth++;
  }

  return true;
}

template<typename T>
bool CSBPlusTree<T>::deserialize(FILE* filename) {
  vector<CCSBNodeGroup<T>*> current_level;
  vector<CCSBNodeGroup<T>*> upper_level;
  unsigned depth = 1;
  fread((void*) (&csb_depth), sizeof(unsigned), 1, filename);

  if (csb_depth == 1) {
    CCSBNodeGroup<T>* leaf = (CCSBNodeGroup<T>*) new CCSBLeafNodeGroup<T>(1);
    leaf->getNode(0)->deserialize(filename);

    csb_root = leaf->getNode(0);
    leaf->setHeaderNG(NULL);
    leaf->setTailerNG(NULL);
    leaf_header = leaf;
    leaf_tailer = leaf;
    return true;
  }

  CCSBNodeGroup<T>* root_group = new CCSBInternalNodeGroup<T>(1);
  root_group->getNode(0)->deserialize(filename);
  csb_root = root_group->getNode(0);
  current_level.push_back(root_group);
  depth++;

  while (depth <= csb_depth) {
    upper_level.clear();
    while (current_level.size() != 0) {
      upper_level.push_back(current_level.back());
      current_level.pop_back();
    }
    while (upper_level.size() != 0) {
      for (unsigned i = 0; i < upper_level.back()->getUsedNodes(); i++) {
        CCSBNodeGroup<T>* node_group = NULL;
        if (depth < csb_depth)
          node_group = new CCSBInternalNodeGroup<T>();  // (upper_level[i]->getNode(j)->getUsedKeys()+1);
        else
          node_group = new CCSBLeafNodeGroup<T>();
        node_group->deserialize(filename);

        current_level.push_back(node_group);

        // set the parent-children relationship
        upper_level.back()->getNode(i)->setPointer(node_group);
      }
      upper_level.pop_back();
    }
    depth++;
  }
  //  depth == csb_depth, the current_level collects all the leafNodeGroup of the CSBPlusTree
  //  set the double linked list in the leaf layer
  leaf_header = current_level[0];
  leaf_tailer = current_level[current_level.size() - 1];
  current_level[0]->setHeaderNG(NULL);
  current_level[current_level.size() - 1]->setTailerNG(NULL);
  if (current_level.size() > 1) {
    current_level[0]->setTailerNG(current_level[1]);
    current_level[current_level.size() - 1]->setHeaderNG(
        current_level[current_level.size() - 2]);
    for (unsigned i = 1; i < current_level.size() - 1; i++) {
      current_level[i]->setHeaderNG(current_level[i - 1]);
      current_level[i]->setTailerNG(current_level[i + 1]);
    }

  }
  return true;
}

template<typename T>
void CSBPlusTree<T>::ClearTree() {
  csb_root->DeleteChildren();
  leaf_header = NULL;
  leaf_tailer = NULL;
  csb_root = NULL;
  csb_depth = 0;
}

template<typename T>
void CSBPlusTree<T>::printTree() {
  cout
      << "\n\n---------------------Print the index tree---------------------\n";
  if (this->csb_root == NULL) {
    cout
        << "[OUTPUT: CSBPlusTree.cpp->printTree()]: The index tree is empty!\n";
    return;
  }

  vector<CCSBNodeGroup<T>*> current_level;
  vector<CCSBNodeGroup<T>*> lower_level;
  current_level.clear();
  lower_level.clear();

  int depth = 1;
  // print the root layer and save the child node group in the lower_level
  cout << "---------------------Root Node (depth: " << 1
       << ")---------------------\n";
  cout << "Root: " << "\t";
  cout << "Used keys: " << ((CCSBNode<T>*) (this->csb_root))->getUsedKeys()
      << endl;
  for (unsigned i = 0; i < this->csb_root->getUsedKeys(); i++)
    cout << ((CCSBNode<T>*) (this->csb_root))->getElement(i)._key << " ";
  cout << endl;
  lower_level.push_back(((CCSBNode<T>*) (this->csb_root))->getPointer());
  depth++;

  // print the rest layers
  CCSBNodeGroup<T>* cur_node_group = new CCSBNodeGroup<T>();
  while (depth <= this->csb_depth) {
    current_level.clear();
    while (lower_level.size() != NULL) {
      current_level.push_back(lower_level.back());
      lower_level.pop_back();
    }
    lower_level.clear();
    if (depth < this->csb_depth)
      cout << "\n\n---------------------Internal Layer (depth: " << depth++
           << ")---------------------\n";
    else
      cout << "\n\n---------------------Leaf Layer (depth: " << depth++
           << ")---------------------\n";
    cout << "Node Group Num: " << current_level.size() << endl;
    unsigned i = 0;
    while (current_level.size() != 0) {
      cout << "NodeGroup: " << i++ << "\t";
      cout << "Used nodes: " << current_level.back()->getUsedNodes() << endl;
      for (unsigned j = 0; j < current_level.back()->getUsedNodes(); j++) {
        if (depth <= this->csb_depth) lower_level.push_back(
            current_level.back()->getNode(j)->getPointer());
        cout << "Node: " << j << "\t";
        cout << "Used keys: "
            << ((CCSBNodeGroup<T>*) (current_level.back()))->getNode(j)
                ->getUsedKeys() << endl;
        for (unsigned k = 0;
            k
                < ((CCSBNodeGroup<T>*) (current_level.back()))->getNode(j)
                    ->getUsedKeys(); k++) {
          if (depth > this->csb_depth) {
            cout << current_level.back()->getNode(j)->getElement(k)._key << " ";
            cout << "<"
                << current_level.back()->getNode(j)->getElement(k)._block_off
                << ", ";
            cout << current_level.back()->getNode(j)->getElement(k)._tuple_off
                << ">\t";
          }
          else
            cout << current_level.back()->getNode(j)->getElement(k)._key
                << "\t";
        }
        cout << endl;
      }
      current_level.pop_back();
    }
  }
  cout
      << "---------------------Print the index tree finished---------------------\n";
}

template<typename T>
void CSBPlusTree<T>::printDoubleLinkedList() {
  cout
      << "\n\n---------------------Print the doubly linked list---------------------\n";
  if (this->csb_root == NULL) {
    cout
        << "[OUTPUT: CSBPlusTree.cpp->printDoubleLinkedList()]: The index tree is empty!\n";
    return;
  }

  if (this->leaf_header == NULL || this->leaf_tailer == NULL) {
    cout
        << "[ERROR: CSBPlusTree.cpp->printDoubleLinkedList()]:The double linked list is error!\n";
    return;
  }

  CCSBNodeGroup<T>* header = this->leaf_header;
  cout << "The forward linked list is: \n";
  while (header != NULL) {
    for (unsigned i = 0; i < header->getUsedNodes(); i++) {
      for (unsigned j = 0;
          j < ((CCSBNode<T>*) (header->getNode(i)))->getUsedKeys(); j++) {
        cout << "<" << ((CCSBNode<T>*) (header->getNode(i)))->getElement(j)._key
            << ", ";
        cout << ((CCSBNode<T>*) (header->getNode(i)))->getElement(j)._block_off
            << ", ";
        cout << ((CCSBNode<T>*) (header->getNode(i)))->getElement(j)._tuple_off
            << "> ";
      }
      cout << "\t";
    }
    header = header->getTailerNG();
  }
  cout << endl;
  CCSBNodeGroup<T>* tailer = this->leaf_tailer;
  cout << "The reverse linked list is: \n";
  while (tailer != NULL) {
    for (int i = tailer->getUsedNodes() - 1; i >= 0; i--) {
      for (int j = ((CCSBNode<T>*) (tailer->getNode(i)))->getUsedKeys() - 1;
          j >= 0; j--) {
        cout << "<" << ((CCSBNode<T>*) (tailer->getNode(i)))->getElement(j)._key
            << ", ";
        cout << ((CCSBNode<T>*) (tailer->getNode(i)))->getElement(j)._block_off
            << ", ";
        cout << ((CCSBNode<T>*) (tailer->getNode(i)))->getElement(j)._tuple_off
            << "> ";
      }
      cout << "\t";
    }
    tailer = tailer->getHeaderNG();
  }
  cout << endl;
}

#endif /* CSBPLUSTREE_H_ */
