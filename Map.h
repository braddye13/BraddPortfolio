#ifndef _MAP_H
#define _MAP_H

#include "Swapify.h"
#include "Keyify.h"

using namespace std;


// This is a template for a lookup table, associating a key with some data.
// It makes use of a reasonably complex skip list data structure to obtain log-n
// inserts, lookups, and removes.
//
// Key requires Swap (), IsEqual (), LessThan (), CopyFrom ()
// Data requires Swap (), CopyFrom ()

template <class Key, class Data>
class Map {
protected:
	struct Node {
		// data
		Key key;
		Data data;

		// backward link
		Node* previous;

		// forward links
		int numLinks;
		Node** next;

		// constructors and destructor
		Node (int _numLinks) : numLinks (_numLinks) {next = new Node*[numLinks];}
		Node () : previous (0), next (0), numLinks (0) {}
		virtual ~Node () {delete [] next;}
	};

	struct Header {
		// data
		Node* first;
		Node* last;
		Node* current;
		int curDepth;

		// constructor
		Header () : first (0), last (0), current (0), curDepth (0) {}
		virtual ~Header () {}
	};

	// the list itself is pointed to by this pointer
	Header* list;

private:
	// making the default constructor and = operator private so the map
	// can only be propagated by Swap. Otherwise subtle bugs can appear
	Map(Map&);
	Map operator= (Map&);

public:
	// constructor and destructor
	Map ();
	virtual ~Map ();

	// swap two of the maps
	void Swap (Map&);

	// get the content from another map (without destroying it)
	void CopyFrom(Map&);

	// eat up another map
	// plays nicely and removes duplicates
	void SuckUp(Map&);

	// inserts the key/data pair into the structure
	void Insert (Key&, Data&);

	// removes one (any) instance of the given key from the map...
	// returns a 1 on success and a zero if the given key was not found
	int Remove (Key&, Key&, Data&);

	// attempts to locate the given key
	// returns 1 if it is, 0 otherwise
	int IsThere (Key&);

	// returns a reference to the data associated with the given search key
	// if the key is not there, then a garbage (newly initialized) Data item is
	// returned.  "Plays nicely" with IsThere in the sense that if IsThere found
	// an item, Find will immediately return that item w/o having to locate it
	int Find (Key&, Data&);

	// the length of the map
	int Length();

	///////////// ITERATOR INTERFAACE //////////////
	// look at the current item
	Key& CurrentKey ();
	Data& CurrentData ();

	// move the current pointer position forward through the list
	void Advance ();

	// move the current pointer position backward through the list
	void Retreat ();

	// operations to consult state
	bool AtStart ();
	bool AtEnd ();

	// operations to move the the start of end of a list
	void MoveToStart ();
	void MoveToFinish ();

protected:
	// versions of the above that work only at a specific level of the skip list
	void insert (Node*, int);
	void remove (Node*&, int);

	Key& currentKey (int);
	Data& currentData (int);

	void advance (int);
	int atEnd (int);
};

#endif
