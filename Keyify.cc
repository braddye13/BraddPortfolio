#ifndef _KEYIFY_CC_
#define _KEYIFY_CC_

#include <iostream>

#include "Keyify.h"
#include "Swap.h"

using namespace std;


template <class Type>
Keyify <Type> :: Keyify () {
}

template <class Type>
Keyify <Type> :: Keyify (const Type _castFromMe) {
	data = _castFromMe;
}

template <class Type>
Keyify <Type> :: ~Keyify () {
}

template <class Type>
Keyify <Type> :: operator Type () {
	return data;
}

template <class Type> void
Keyify <Type> :: Swap (Keyify& _withMe) {
	SWAP(data, _withMe.data);
}

template <class Type> void
Keyify <Type> :: CopyFrom (Keyify& _withMe) {
	data = _withMe.data;
}

template <class Type> int
Keyify <Type> :: IsEqual(Keyify& _checkMe) {
	return (data == _checkMe.data);
}

template <class Type> int
Keyify <Type> :: LessThan (Keyify& _checkMe) {
	return (data < _checkMe.data);
}

// redefine operator << for printing
template <class Type> ostream&
operator<<(ostream& _output, const Keyify<Type>& _s) {
	Keyify<Type> newObject;
	newObject.Swap(const_cast<Keyify<Type>&>(_s));

	Type st = newObject;
	_output << st;

	newObject.Swap(const_cast<Keyify<Type>&>(_s));

	return _output;
}

#endif //_KEYIFY_CC_
