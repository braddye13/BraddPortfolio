#include <iostream>

#include "Config.h"
#include "Swap.h"
#include "Schema.h"

using namespace std;


Attribute::Attribute() : name(""), type(Name), noDistinct(0) {}

Attribute::Attribute(const Attribute& _other) :
	name(_other.name), type(_other.type), noDistinct(_other.noDistinct) {}

Attribute& Attribute::operator=(const Attribute& _other) {
	// handle self-assignment first
	if (this == &_other) return *this;

	name = _other.name;
	type = _other.type;
	noDistinct = _other.noDistinct;

	return *this;
}

void Attribute::Swap(Attribute& _other) {
	name.Swap(_other.name);
	SWAP(type, _other.type);
	noDistinct.Swap(_other.noDistinct);
}


Schema::Schema() : noTuples(0), fPath(string("")) {
}

Schema::Schema(SInt& _tuples, SString& _file) : noTuples(_tuples), fPath(_file) {
}

Schema::Schema(StringVector& _attributes, StringVector& _attributeTypes,
	IntVector& _distincts) : noTuples(0), fPath(string("")) {
	for (int i = 0; i < _attributes.Length(); i++) {
		Attribute a;

		a.name = _attributes[i];
		a.noDistinct = _distincts[i];

		if (_attributeTypes[i] == SString("INTEGER") || _attributeTypes[i] == SString("Integer")) a.type = Integer;
		else if (_attributeTypes[i] == SString("FLOAT") || _attributeTypes[i] == SString("Float")) a.type = Float;
		else if (_attributeTypes[i] == SString("STRING") || _attributeTypes[i] == SString("String")) a.type = String;

		atts.Append(a);
	}
}

Schema::Schema(StringVector& _attributes, StringVector& _attributeTypes,
	IntVector& _distincts, SInt& _tuples, SString& _file) : noTuples(_tuples), fPath(_file) {
	for (int i = 0; i < _attributes.Length(); i++) {
		Attribute a;

		a.name = _attributes[i];
		a.noDistinct = _distincts[i];

		if (_attributeTypes[i] == SString("INTEGER") || _attributeTypes[i] == SString("Integer")) a.type = Integer;
		else if (_attributeTypes[i] == SString("FLOAT") || _attributeTypes[i] == SString("Float")) a.type = Float;
		else if (_attributeTypes[i] == SString("STRING") || _attributeTypes[i] == SString("String")) a.type = String;

		atts.Append(a);
	}
}

Schema::Schema(const Schema& _other) {
	noTuples = _other.noTuples;
	fPath = _other.fPath;

	AttributeVector newObject;
	newObject.Swap(const_cast<AttributeVector&>(_other.atts));

	for (int i = 0; i < newObject.Length(); i++) {
		Attribute a; a = newObject[i];
		atts.Append(a);
	}

	newObject.Swap(const_cast<AttributeVector&>(_other.atts));
}

Schema& Schema::operator=(const Schema& _other) {
	// handle self-assignment first
	if (this == &_other) return *this;

	noTuples = _other.noTuples;
	fPath = _other.fPath;

	Attribute tmp;
	while (atts.Length() > 0) {
		atts.Remove(0, tmp);
	}

	AttributeVector newObject;
	newObject.Swap(const_cast<AttributeVector&>(_other.atts));

	for (int i = 0; i < newObject.Length(); i++) {
		Attribute a; a = newObject[i];
		atts.Append(a);
	}

	newObject.Swap(const_cast<AttributeVector&>(_other.atts));

	return *this;
}

Schema::~Schema() {
	AttributeVector t;
	atts.Swap(t);
}

void Schema::Swap(Schema& _other) {
	atts.Swap(_other.atts);
	noTuples.Swap(_other.noTuples);
	fPath.Swap(_other.fPath);
}

unsigned int Schema::GetNumAtts() {
	return atts.Length();
}

AttributeVector& Schema::GetAtts() {
	return atts;
}

SInt& Schema::GetNoTuples() {
	return noTuples;
}

SString& Schema::GetDataFile() {
	return fPath;
}

void Schema::SetNoTuples(SInt& _tuples) {
	noTuples = _tuples;
}

void Schema::SetDataFile(SString& _file) {
	fPath = _file;
}

int Schema::Append(Schema& _other) {
	for (int i = 0; i < _other.atts.Length(); i++) {
		int pos = Index(_other.atts[i].name);
		if (pos != -1) return -1;
	}

	for (int i = 0; i < _other.atts.Length(); i++) {
		Attribute a; a = _other.atts[i];
		atts.Append(a);
	}

	return 0;
}

int Schema::Index(SString& _attName) {
	for (int i = 0; i < atts.Length(); i++) {
		if (_attName == atts[i].name) return i;
	}

	// if we made it here, the attribute was not found
	return -1;
}

Type Schema::FindType(SString& _attName) {
	int pos = Index(_attName);
	if (pos == -1) return Integer;

	return atts[pos].type;
}

SInt Schema::GetDistincts(SString& _attName) {
	int pos = Index(_attName);
	if (pos == -1) return -1;

	return atts[pos].noDistinct;
}

int Schema::SetDistincts(SString& _attName, SInt& _noDistinct){
	int pos = Index(_attName);
	if (pos == -1) return -1;

	atts[pos].noDistinct = _noDistinct;
	return 0;
}

int Schema::RenameAtt(SString& _oldName, SString& _newName) {
	int pos = Index(_newName);
	if (pos != -1) return -1;

	pos = Index(_oldName);
	if (pos == -1) return -1;

	atts[pos].name = _newName;

	return 0;
}

int Schema::Project(IntVector& _attsToKeep) {
	int numAttsToKeep = _attsToKeep.Length();
	int numAtts = atts.Length();
	
	// too many attributes to keep
	if (numAttsToKeep > numAtts) return -1;

	AttributeVector copy; atts.Swap(copy);

	for (int i=0; i<numAttsToKeep; i++) {
		int index = _attsToKeep[i];
		if ((index >= 0) && (index < numAtts)) {
			Attribute a; a = copy[index];
			atts.Append(a);
		}
		else {
			atts.Swap(copy);
			AttributeVector avt; copy.Swap(avt);

			return -1;
		}
	}

	AttributeVector avt; copy.Swap(avt);

	return 0;
}

ostream& operator<<(ostream& _os, Schema& _c) {
	_os << "(";
	for(int i=0; i<_c.atts.Length(); i++) {
		_os << _c.atts[i].name << ':';

		switch(_c.atts[i].type) {
			case Integer:
				_os << "INTEGER";
				break;
			case Float:
				cout << "FLOAT";
				break;
			case String:
				cout << "STRING";
				break;
			default:
				cout << "UNKNOWN";
				break;
		}

		_os << " [" << _c.atts[i].noDistinct << "]";
		if (i < _c.atts.Length()-1) _os << ", ";
	}
	_os << ")";

	_os << "[" << _c.noTuples << "] [" << _c.fPath << "]" << endl;

	return _os;
}