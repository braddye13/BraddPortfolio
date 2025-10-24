#ifndef _VECTOR_H
#define _VECTOR_H

#include "Swapify.h"

using namespace std;


// Basic vector with dynamic size
// Type requires Swap

template <class Type>
class Vector {
protected:
	// size of the allocated space
	unsigned int size;

	// length: this is the last index that has a value
	unsigned int length;

	// actual vector
	Type* vector;

private:
	// making the default constructor and = operator private so the list
	// can only be propagated by Swap. Otherwise subtle bugs can appear
	Vector(Vector&);
	Vector operator= (Vector&);

public:
	// basic constructor function
	Vector ();

	// destructor function
	virtual ~Vector ();

	// swap operator
	void Swap (Vector&);

	// get the content from another list (without destroying it)
	void CopyFrom(Vector&);

	// insert at given indedx
	void Insert (unsigned int, Type&);

	// add at the end of the vector
	void Append (Type&);

	// remove from given index
	void Remove (unsigned int, Type&);

	// return element at given index
	int AtIndex (unsigned int, Type&);

	// overload index [] operator
	Type& operator[] (unsigned int);

	int Length();
};

typedef Vector<SDouble> DoubleVector;
typedef Vector<SInt> IntVector;
typedef Vector<SString> StringVector;

#endif
