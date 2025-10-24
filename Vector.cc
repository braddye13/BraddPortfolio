#ifndef _VECTOR_C
#define _VECTOR_C

#include "Vector.h"
#include "Swapify.cc"
#include "Swap.h"

#include <cstdlib>
#include <iostream>

using namespace std;


// original size of the vector
#define STARTSIZE 64

// basic constructor function
template <class Type>
Vector <Type> :: Vector () : size(STARTSIZE), length(0) {
	vector = new Type[size];
}

// basic destructor function
template <class Type>
Vector <Type> :: ~Vector () {
	delete [] vector;
}

// swap operator
template <class Type> void
Vector <Type> :: Swap (Vector& _vector) {
	Type* tmp = _vector.vector;
	_vector.vector = vector;
	vector = tmp;

	SWAP(size, _vector.size);
	SWAP(length, _vector.length);
}

// copy from operator
template <class Type> void
Vector <Type> :: CopyFrom (Vector& _vector) {
	delete [] vector;

	vector = new Type[_vector.size];
	for (int i = 0; i < _vector.length; i++)
		vector[i].CopyFrom(_vector.vector[i]);

	size = _vector.size;
	length = _vector.length;
}

template <class Type> void
Vector <Type> :: Insert (unsigned int _index, Type& _item) {
	while (_index >= size) {
		// resize the vector: double the size
		Type* newV = new Type[size*2];
		for (int i = 0; i < length; i++)
			newV[i].Swap(vector[i]);

		delete [] vector;
		vector = newV;

		size = size*2;
	}

	vector[_index].Swap(_item);

	if (_index >= length)
		length = _index+1;
}

template <class Type> void
Vector <Type> :: Append (Type& _item) {
	if (length == size) {
		// resize the vector: double the size
		Type* newV = new Type[size*2];
		for (int i = 0; i < length; i++)
			newV[i].Swap(vector[i]);

		size = size*2;

		delete [] vector;
		vector = newV;
	}

	vector[length].Swap(_item);
	length += 1;
}

template <class Type> void
Vector <Type> :: Remove (unsigned int _index, Type& _item) {
	for (int i = _index; i <= length-1; i++) {
		vector[i].Swap(vector[i+1]);
	}

	if ((length > 0) && (_index < length)) {
		Type tmp;
		vector[length-1].Swap(tmp);
		length -= 1;
	}
}

template <class Type> int
Vector <Type> :: AtIndex (unsigned int _index, Type& _item) {
	if (_index >= length)
		return 0;

	_item = vector[_index];

	return 1;
}

template <class Type> Type&
Vector <Type> :: operator[] (unsigned int _index) {
	if ((_index < 0) || (_index >= length))
		return vector[0];
		
	return vector[_index];
}
	
template <class Type> int
Vector <Type> :: Length () {
	return length;
}


// redefine operator << for printing
template <class Type> ostream&
operator<<(ostream& _output, const Vector<Type>& _vector) {
	Vector<Type> newObject;
	newObject.Swap(const_cast<Vector<Type>&>(_vector));

	_output << "[";
	for (int i = 0; i < newObject.Length(); i++) {
		if (i > 0) {
			_output << ", ";
		}

		_output << newObject[i];
	}
	_output << "] : " << newObject.Length();

	newObject.Swap(const_cast<Vector<Type>&>(_vector));

	return _output;
}

#endif
