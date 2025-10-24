#ifndef _SWAPIFY_H_
#define _SWAPIFY_H_

// The following templates are used to quickly and easily create a class that
// wraps around a simple type (such as int) and can be put into Vector, List or Map.
// The "Swapify" template attaches only a Swap operation (so a simple type can
// be used with Vector or List).

// SPECIAL NOTE: ONLY USE THESE TEMPLATES WITH SIMPLE TYPES (int, double, etc.)
// These templates use the = operation, so they are only safe with such types.
// If the type is more complicated, then create a class with Swap and IsEqual.

#include <string>

using namespace std;


template <class Type>
class Swapify {
protected:
	Type data;

public:
	Swapify ();
	Swapify (const Type);
	virtual ~Swapify ();

	operator Type();
	void Swap (Swapify&);
	void CopyFrom(Swapify&);

	bool operator== (const Swapify&) const;
};

typedef Swapify<double> SDouble;
typedef Swapify<int> SInt;
typedef Swapify<string> SString;

#endif //_SWAPIFY_H_
