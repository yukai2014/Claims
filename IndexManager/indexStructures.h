/*
 * indexStructures.h
 *
 *  Created on: Feb 10, 2015
 *      Author: scdong
 */

#ifndef INDEXSTRUCTURES_H_
#define INDEXSTRUCTURES_H_

#include <limits.h>

#define NULL 0
#define INVALID -1
#define FLAG_LEFT 1
#define FLAG_RIGHT 2

#define cacheline_size 64

typedef unsigned short index_offset;

enum comparison {EQ, L, LEQ, G, GEQ};

enum index_type {CSBPLUS, CSB, ECSB};

//original data structure for int
struct data_original
{
	data_original():_key(0), _block_off(INVALID), _tuple_off(INVALID) {};
	unsigned long _key;
	index_offset _block_off;
	index_offset _tuple_off;
};

//original data structure for template
template <typename T>
struct data_offset
{
	data_offset<T>():_key(INVALID), _block_off(INVALID), _tuple_off(INVALID){};
	T _key;
	index_offset _block_off;
	index_offset _tuple_off;
};

//data entry -- struct for data node groups in CSB-Trees
struct data_pointer {
	data_pointer():block_off(NULL), tuple_off(NULL) {};
	data_pointer(unsigned n) {
		block_off = new index_offset[n];
		tuple_off = new index_offset[n];
		for (unsigned i = 0; i < n; i++)
		{
			block_off[i] = INVALID;
			tuple_off[i] = INVALID;
		}
	};
	index_offset* block_off;
	index_offset* tuple_off;
};

#endif /* INDEXSTRUCTURES_H_ */
