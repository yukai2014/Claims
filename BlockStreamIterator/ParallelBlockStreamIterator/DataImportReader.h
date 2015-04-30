/*
 * DataImportReader.h
 *
 *  Created on: Apr 30, 2015
 *      Author: scdong
 */

#ifndef DATAIMPORTREADER_H_
#define DATAIMPORTREADER_H_

#include <vector>
#include <iosfwd>
#include <stdio.h>
#include <boost/serialization/serialization.hpp>
#include "../ExpandableBlockStreamIteratorBase.h"
#include "../../common/Schema/Schema.h"
#include "../../common/Block/BlockStream.h"
#include "../../common/ids.h"
#include "../../utility/lock.h"

class DataImportReader:public ExpandableBlockStreamIteratorBase {
public:
	class State {
	public:
		friend class DataImportReader;
		State(const vector<std::string> &file_path, const char attribute_separator, const char tuple_separator, const Schema* input, const vector<Schema*> &output, const unsigned block_size);
		State() {};
	public:
		vector<std::string> file_path_;
		char attribute_separator_;
		char tuple_separator_;
		Schema* input_;
		vector<Schema*> output_;
		unsigned block_size_;
	private:
		friend class boost::serialization::access;
		template<class Archive>
		void serialize(Archive &ar, const unsigned int version) {
			ar & file_path_ & attribute_separator_ & tuple_separator_ & input_ & output_ & block_size_;
		}
	};

public:
	DataImportReader();
	DataImportReader(State state);
	virtual ~DataImportReader();
	bool open(const PartitionOffset &part_off);
	bool next(BlockStreamBase* block);
	bool close();
	void print();
private:
	State state_;
	FILE* fd_;
	Lock lock_;
private:
	friend class boost::serialization::access;
	template<class Archive>
	void serialize(Archive &ar, const unsigned int version) {
		ar & boost::serialization::base_object<BlockStreamIteratorBase>(*this) & state_;
	}
};

#endif /* DATAIMPORTREADER_H_ */
