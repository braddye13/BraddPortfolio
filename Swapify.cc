#ifndef _SWAPIFY_CC_
#define _SWAPIFY_CC_

#include <iostream>

#include "Swapify.h"
#include "Swap.h"

using namespace std;


template <class Type>
Swapify <Type> :: Swapify () {
}

template <class Type>
Swapify <Type> :: Swapify (const Type _castFromMe) {
	data = _castFromMe;
}

template <class Type>
Swapify <Type> :: ~Swapify () {
}


template <class Type>
Swapify <Type> :: operator Type () {
	return data;
}

template <class Type> void
Swapify <Type> :: Swap (Swapify& _withMe) {
	SWAP(data, _withMe.data);
}

template <class Type> void
Swapify <Type> :: CopyFrom (Swapify& _fromMe) {
	data = _fromMe.data;
}

template <class Type> bool 
Swapify <Type> :: operator== (const Swapify& _other) const {
	return data == _other.data;
}


// redefine operator << for printing
template <class Type> ostream&
operator<<(ostream& _output, const Swapify<Type>& _s) {
	Swapify<Type> newObject;
	newObject.Swap(const_cast<Swapify<Type>&>(_s));

	Type st = newObject;
	_output << st;

	newObject.Swap(const_cast<Swapify<Type>&>(_s));

	return _output;
}

#endif //_SWAPIFY_CC_
