#ifndef _KEYIFY_H_
#define _KEYIFY_H_

// The following templates are used to quickly and easily create a class that
// wraps around a simple type (such as an int) and can be put into Vector, List, or Map.
// The "Keyify" template attaches both a Swap and an IsEqual (so a simple type
// can be used with Vector, List or Map).
// For example, the following is valid code, which uses Keyify to attach a Swap
// and IsEqual to an int:
//
// void IntSwap (int& _a, int& _b) {
//         int temp;
//         temp = _a;
//         _a = _b;
//         _b = temp;
// }
//
// int IntCheck (int& _a, int& _b) {
//         return (a == b);
// }
//
// int main () {
//
// 	    typedef Keyify <int, IntSwap, IntCheck> keyifiedInt;
//      Map <keyifiedInt, keyifiedInt> foo;
//      keyifiedInt bar1, bar2;
//
//      bar1 = 12;
//      bar2 = 43;
//      foo.Insert (bar1, bar2);
//      ...
//
// SPECIAL NOTE: ONLY USE THESE TEMPLATES WITH SIMPLE TYPES (int, double, etc.)
// These templates use the = operation, so they are only safe with such types.
// If the type is more complicated, then create a class with Swap and IsEqual.

#include <string>

using namespace std;


template <class Type>
class Keyify {
private:
	Type data;

public:
	Keyify ();
	Keyify (const Type);
	virtual ~Keyify ();

	operator Type();
	void Swap (Keyify&);
	void CopyFrom (Keyify&);

	int IsEqual(Keyify&);
	int LessThan(Keyify&);
};

typedef Keyify<double> KDouble;
typedef Keyify<int> KInt;
typedef Keyify<string> KString;

#endif //_KEYIFY_H_
