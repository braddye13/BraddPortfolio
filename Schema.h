#ifndef _SCHEMA_H
#define _SCHEMA_H

#include <iostream>

#include "Config.h"
#include "Swapify.h"
#include "Vector.cc"

using namespace std;


/* Data structure for schema attributes:
 * name of attribute
 * type of attribute
 * number of distinct values
 */
class Attribute {
public:
	SString name;
	Type type;
	SInt noDistinct;

	// constructors and destructor
	Attribute();
	Attribute(const Attribute& _other);
	Attribute& operator=(const Attribute& _other);
	void Swap(Attribute& _other);

	virtual ~Attribute() {}
};

typedef Vector<Attribute> AttributeVector;


/* Class to manage schema of relations:
 * materialized on disk
 * intermediate result during query execution
 */
class Schema {
private:
	// number of tuples
	SInt noTuples;
	// path to data file
	SString fPath;
	// attributes in schema
	AttributeVector atts;

public:
	// default constructor
	Schema();
	// table constructor
	Schema(SInt& _tuples, SString& _file);
	// attribute constructor
	Schema(StringVector& _attributes, StringVector& _attributeTypes, IntVector& _distincts);
	// full constructor
	Schema(StringVector& _attributes, StringVector& _attributeTypes, IntVector& _distincts,
		SInt& _tuples, SString& _file);
	// copy constructor
	Schema(const Schema& _other);
	// assignment operator
	Schema& operator=(const Schema& _other);
	// swap function
	void Swap(Schema& _other);

	// destructor
	virtual ~Schema();

	// get functions
	unsigned int GetNumAtts();
	AttributeVector& GetAtts();

	SInt& GetNoTuples();
	SString& GetDataFile();

	// set functions
	void SetNoTuples(SInt&);
	void SetDataFile(SString&);

	// append other schema
	int Append(Schema& _other);

	// find index of specified attribute
	// return -1 if attribute is not present
	int Index(SString& _attName);

	// find number of distincts of specified attribute
	// return -1 if attribute is not present
	SInt GetDistincts(SString& _attName);
	int SetDistincts(SString& _attName, SInt& _noDistinct);

	// rename an attribute
	int RenameAtt(SString& _oldName, SString& _newName);

	// project attributes of a schema
	// only attributes indexed in the input vector are kept after projection
	// index begins from 0
	// return -1 if failure, 0 otherwise
	int Project(IntVector& _attsToKeep);

	// find type of the specified attribute
	// return arbitrary type if attribute is not present
	// call only after Index returns valid result
	Type FindType(SString& _attName);

	// operator for printing
	friend ostream& operator<<(ostream& _os, Schema& _c);
};

#endif //_SCHEMA
