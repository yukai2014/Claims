/*
 * DataImportReader.cpp
 *
 *  Created on: Apr 30, 2015
 *      Author: scdong
 */

#include "DataImportReader.h"

DataImportReader::State::State(const vector<std::string>& file_path,
		const char attribute_separator, const char tuple_separator,
		const Schema* input, const vector<Schema*>& output,
		const unsigned block_size)
:file_path_(file_path), attribute_separator_(attribute_separator), tuple_separator_(tuple_separator), input_(input), output_(output), block_size_(block_size){
}

DataImportReader::DataImportReader()
:fd_(NULL) {
	// TODO Auto-generated constructor stub
	initialize_expanded_status();
}

DataImportReader::DataImportReader(State state)
:state_(state), fd_(NULL){
	initialize_expanded_status();
}

DataImportReader::~DataImportReader() {
	// TODO Auto-generated destructor stub
}

bool DataImportReader::open(const PartitionOffset& part_off) {
}

bool DataImportReader::next(BlockStreamBase* block) {
}

bool DataImportReader::close() {
}

void DataImportReader::print() {
}
