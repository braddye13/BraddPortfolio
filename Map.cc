#ifndef _MAP_C
#define _MAP_C

#include "Map.h"
#include "Keyify.cc"
#include "Swapify.cc"

#include <cstdlib>
#include <iostream>

using namespace std;


// this constant is relatively unimportant... the data structure will have
// efficient performance up to approximately 2^MAXLEVELS items, but there is
// no reason to have it too large!
#define MAXLEVELS 64

// basic constructor function
template <class Key, class Data>
Map <Key, Data> :: Map () {
	// allocate space for the header
	list = new Header();

	// set up the initial values for an empty list
	list->first = new Node (MAXLEVELS);
	list->last = new Node ();
	list->current = list->first;
	list->curDepth = 0;

	// note that the size of the list at every level is zero
	for (int i = 0; i < MAXLEVELS; i++) {
		list->first->next[i] = list->last;
	}

	list->last->previous = list->first;
}

// basic destructor function
template <class Key, class Data>
Map <Key, Data> :: ~Map () {
	while (list->first != list->last) {
		list->first = list->first->next[0];
		delete list->first->previous;
	}
	delete list->first;

	// delete the header
	delete list;
}

// swap operator
template <class Key, class Data> void
Map <Key, Data> :: Swap (Map& _list) {
	Header* temp = _list.list;
	_list.list = list;
	list = temp;
}

template <class Key, class Data> void
Map <Key, Data> :: CopyFrom(Map <Key, Data>& _other) {
	// clean up our content
	Map empty;
	Swap(empty);

	// scan the other map and insert one element at the time
	for (_other.MoveToStart(); !_other.AtEnd(); _other.Advance()){
		Key myKey;
		myKey.CopyFrom(_other.CurrentKey());
		Data myData;
		myData.CopyFrom(_other.CurrentData());

		Insert(myKey, myData);
	}
}

template <class Key, class Data> void
Map <Key, Data> :: SuckUp(Map <Key, Data>& _other) {
	// scan the other map and insert one element at the time
	// _other will not have any valid data after this operation
	for (_other.MoveToStart(); !_other.AtEnd(); _other.Advance())
		Insert(_other.CurrentKey(), _other.CurrentData());
}

template <class Key, class Data> void
Map <Key, Data> :: Insert (Key& _key, Data& _data) {
	// first, we figure out how many levels are in the new node
	int numLevels = 1;
	double dr = drand48 ();
	while (dr > 0.5) {
		numLevels++;
		if (numLevels == MAXLEVELS) {
			numLevels--;
			break;
		}
		dr = drand48 ();
	}

	// now create the node
	Node* temp = new Node (numLevels);
	temp->key.Swap (_key);
	temp->data.Swap (_data);

	// now, see how many levels we must work thru
	if (list->curDepth < numLevels)
		list->curDepth = numLevels;

	// actually do the insertion
	MoveToStart ();
	for (int i = list->curDepth-1; i >= 0; i--) {
		// find the location to insert at this level
		while (1) {
			// keep looping until either we reach the end
			if (atEnd (i)) {
				break;
			}
			// or we find a larger item
			if (!currentKey(i).LessThan (temp->key))
				break;

			// if we made it here, we have more data
			advance (i);
		}

		// do the insertion, if we are far enough down
		if (i < numLevels) {
			insert (temp, i);
		}
	}
}

template <class Key, class Data> int
Map <Key, Data> :: Remove (Key& _key, Key& _removedKey, Data& _removedData) {
	MoveToStart ();

	// start at the highest level and work down
	for (int i = list->curDepth-1; i >= 0; i--) {
		while (1) {
			// keep looping until either we reach the end
			if (atEnd (i))
				break;

			// or we find a larger item
			if (_key.LessThan (currentKey (i)))
				break;

			// see if we actually found it
			if (_key.IsEqual (currentKey (i))) {
				Node* temp;
				remove (temp, i);

				// if this is the lowest level, then remove the node
				if (i == 0) {
					temp->data.Swap (_removedData);
					temp->key.Swap (_removedKey);

					delete temp;

					return 1;
				}

				break;
			}

			// if we made it here, we have more data to loop thru
			advance (i);
		}
	}

	return 0;
}

template <class Key, class Data> int
Map <Key, Data> :: IsThere (Key& _key) {
	MoveToStart ();

	// start at the highest level and work down
	for (int i = list->curDepth-1; i >= 0; i--) {
		// find the location to go down from this level
		while (1) {
			// keep looping until either we reach the end
			if (atEnd (i))
				break;

			// or we find a larger item
			if (_key.LessThan (currentKey (i)))
				break;

			// see if we actually found it
			if (_key.IsEqual (currentKey (i))) {
				if (i > 0) {
					while (!_key.IsEqual(CurrentKey()))
						Advance();
				}
			
				return 1;
			}

			// if we made it here, we have more data to loop thru
			advance (i);
		}
	}

	// if we made it here, we did not find it
	return 0;
}

template <class Key, class Data> int
Map <Key, Data> :: Find (Key& _key, Data& _data) {
	MoveToStart ();

	// start at the highest level and work down
	for (int i = list->curDepth-1; i >= 0; i--) {
		// find the location to go down from this level
		while (1) {
			// keep looping until either we reach the end
			if (atEnd (i))
				break;

			// or we find a larger item
			if (_key.LessThan (currentKey (i)))
				break;

			// see if we actually found it
			if (_key.IsEqual (currentKey (i))) {
				if (i > 0) {
					while (!_key.IsEqual(CurrentKey()))
						Advance();
				}

				_data = CurrentData ();

				return 1;
			}

			// if we made it here, we have more data to loop thru
			advance (i);
		}
	}

	return 0;
}

template <class Key, class Data> int
Map <Key, Data> :: Length() {
	int length = 0;
	for (MoveToStart(); !AtEnd(); Advance()){
		length += 1;
	}

	return length;
}

template <class Key, class Data> Key&
Map <Key, Data> :: CurrentKey () {
	return list->current->next[0]->key;
}

template <class Key, class Data> Data&
Map <Key, Data> :: CurrentData () {
	return list->current->next[0]->data;
}

template <class Key, class Data> void
Map <Key, Data> :: Advance () {
	list->current = list->current->next[0];
}

template <class Key, class Data> void
Map <Key, Data> :: Retreat () {
	list->current = list->current->previous;
}

template <class Key, class Data> bool
Map <Key, Data> :: AtStart () {
	return (list->current == list->first);
}

template <class Key, class Data> bool
Map <Key, Data> :: AtEnd () {
	return (list->current->next[0] == list->last);
}

// make the first node the current node
template <class Key, class Data> void
Map <Key, Data> :: MoveToStart () {
	list->current = list->first;
}

// make the first node the current node
template <class Key, class Data> void
Map <Key, Data> :: MoveToFinish () {
	list->current = list->last->previous;
}


// insert an item at the current position
template <class Key, class Data> void
Map <Key, Data> :: insert (Node* _newN, int _whichLevel) {
	Node* left = list->current;
	Node* right = list->current->next[_whichLevel];

	left->next[_whichLevel] = _newN;
	_newN->next[_whichLevel] = right;

	if (_whichLevel == 0) {
		_newN->previous = left;
		right->previous = _newN;
	}
}

// remove an item from the current position
template <class Key, class Data> void
Map <Key, Data> :: remove (Node*& _removeMe, int _whichLevel) {
	_removeMe = list->current->next[_whichLevel];
	list->current->next[_whichLevel] = _removeMe->next[_whichLevel];

	if (_whichLevel == 0)
		_removeMe->next[_whichLevel]->previous = list->current;
}

template <class Key, class Data> Key&
Map <Key, Data> :: currentKey (int _whichLevel) {
	return list->current->next[_whichLevel]->key;
}

template <class Key, class Data> Data&
Map <Key, Data> :: currentData (int _whichLevel) {
	return list->current->next[_whichLevel]->data;
}

template <class Key, class Data> void
Map <Key, Data> :: advance (int _whichLevel) {
	list->current = list->current->next[_whichLevel];
}

template <class Key, class Data> int
Map <Key, Data> :: atEnd (int _whichLevel) {
	return (list->current->next[_whichLevel] == list->last);
}


// redefine operator << for printing
template <class Key, class Data> ostream& operator<<(ostream& _output,
	const Map<Key, Data>& _map) {
	Map<Key, Data> newObject;
	newObject.Swap(const_cast<Map<Key, Data>&>(_map));

	_output << "[";
	for (newObject.MoveToStart(); !newObject.AtEnd(); newObject.Advance()) {
		if (!newObject.AtStart()) {
			_output << ", ";
		}

		_output << "(" << newObject.CurrentKey() << ", " << newObject.CurrentData() << ")";
	}
	_output << "] : " << newObject.Length();

	newObject.Swap(const_cast<Map<Key, Data>&>(_map));

	return _output;
}

#endif
