/*
 * DataImportWriter.h
 *
 *  Created on: Apr 30, 2015
 *      Author: scdong
 */

#ifndef DATAIMPORTWRITER_H_
#define DATAIMPORTWRITER_H_
#include "../ExpandableBlockStreamIteratorBase.h"

class DataImportWriter:public ExpandableBlockStreamIteratorBase {
public:
	DataImportWriter();
	virtual ~DataImportWriter();
};

#endif /* DATAIMPORTWRITER_H_ */
