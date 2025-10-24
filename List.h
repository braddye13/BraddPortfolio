#ifndef _LIST_H
#define _LIST_H

#include "Swapify.h"

using namespace std;


// Basic doubly linked list
// Type requires Swap

template <class Type>
class List {
protected:
	struct Node {
		// data
		Type data;
		Node* next;
		Node* previous;

		// constructor
		Node () : next (0), previous (0) {}
		virtual ~Node () {}
	};

	struct Header {
		// data
		Node* first;
		Node* last;
		Node* current;

		int leftSize;
		int rightSize;

		// constructor
		Header () : first (0), last (0), current (0), leftSize (0), rightSize (0) {}
		virtual ~Header () {}
	};

	// the list itself is pointed to by this pointer
	Header* list;

private:
	// making the default constructor and = operator private so the list
	// can only be propagated by Swap. Otherwise subtle bugs can appear
	List(List&);
	List operator= (List&);

public:
	// basic constructor function
	List ();

	// destructor function
	virtual ~List ();

	// swap operator
	void Swap (List&);

	// get the content from another list (without destroying it)
	void CopyFrom(List&);

	// add to current pointer position
	void Insert (Type&);

	// add at the end of the list
	void Append (Type&);

	// remove from current position
	void Remove (Type&);

	// operations to consult state
	bool AtStart ();
	bool AtEnd ();

	// operations to move the the start of end of a list
	void MoveToStart ();
	void MoveToFinish ();

	// look at the current item
	Type& Current ();

	// move the current pointer position forward through the list
	void Advance ();

	// move the current pointer position backward through the list
	void Retreat ();

	// operations to check the size of both sides
	int LeftLength ();
	int RightLength ();
	int Length();

	// operations to swap the left and right sides of two lists
	void SwapLefts (List&);
	void SwapRights (List&);
};

typedef List<SDouble> DoubleList;
typedef List<SInt> IntList;
typedef List<SString> StringList;

#endif
