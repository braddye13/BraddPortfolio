#ifndef _LIST_C
#define _LIST_C

#include "List.h"
#include "Swapify.cc"

#include <cstdlib>
#include <iostream>

using namespace std;


// basic constructor function
template <class Type>
List <Type> :: List () {
	// allocate space for the header
	list = new Header();

	// set up the initial values for an empty list
	list->first = new Node();
	list->last = new Node();
	list->current = list->first;
	list->leftSize = 0;
	list->rightSize = 0;
	list->first->next = list->last;
	list->last->previous = list->first;
}

// basic destructor function
template <class Type>
List <Type> :: ~List () {
	MoveToStart ();
	while (RightLength () > 0) {
		Type temp;
		Remove (temp);
	}
	delete list->first;
	delete list->last;

	// kill the header
	delete list;
}

// swap operator
template <class Type> void
List <Type> :: Swap (List& _list) {
	Header* temp = _list.list;
	_list.list = list;
	list = temp;
}

// copy from operator
template <class Type> void
List <Type> :: CopyFrom (List& _list) {
	// first clean the current content of the list
	MoveToStart();
	while (!AtEnd()) {
		Remove(Current());
	}

	// then copy the elements from the input list
	_list.MoveToStart();
	while (!_list.AtEnd()) {
		Type element;
		element.CopyFrom(_list.Current());
		Append(element);

		_list.Advance();
	}
}

// insert an item at the current position
template <class Type> void
List <Type> :: Insert (Type& _item) {
	Node* temp = new Node();
	Node* left = list->current;
	Node* right = list->current->next;

	left->next = temp;
	temp->previous = left;
	temp->next = right;
	right->previous = temp;

	temp->data.Swap (_item);

	list->rightSize += 1;
}

template <class Type> void
List <Type> :: Append (Type& _item) {
	MoveToFinish();
	Insert(_item);
}

// remove an item from the current position
template <class Type> void
List <Type> :: Remove (Type& _item) {
	Node* temp = list->current->next;
	list->current->next = temp->next;
	temp->next->previous = list->current;

	_item.Swap (temp->data);

	delete temp;

	list->rightSize--;
}

template <class Type> bool
List <Type> :: AtStart () {
	return (list->leftSize == 0);
}

template <class Type> bool
List <Type> :: AtEnd () {
	return (list->rightSize == 0);
}

// make the first node the current node
template <class Type> void
List <Type> :: MoveToStart () {
	list->current = list->first;
	list->rightSize += list->leftSize;
	list->leftSize = 0;
}

// make the first node the current node
template <class Type> void
List <Type> :: MoveToFinish () {
	list->current = list->last->previous;
	list->leftSize += list->rightSize;
	list->rightSize = 0;
}

template <class Type> Type&
List <Type> :: Current () {
	return list->current->next->data;
}

// move forwards through the list
template <class Type> void
List <Type> :: Advance () {
	list->rightSize--;
	list->leftSize++;
	list->current = list->current->next;
}

// move backwards through the list
template <class Type> void
List <Type> :: Retreat () {
	list->rightSize++;
	list->leftSize--;
	list->current = list->current->previous;
}

// determine the number of items to the left of the current node
template <class Type> int
List <Type> :: LeftLength () {
	return list->leftSize;
}

// determine the number of items to the right of the current node
template <class Type> int
List <Type> :: RightLength () {
	return list->rightSize;
}

template <class Type> int
List <Type> :: Length () {
	return list->leftSize+list->rightSize;
}

// swap the left sides of the two lists
template <class Type> void
List <Type> :: SwapLefts (List& _list) {
	// swap out everything after the current nodes
	Node* left_1 = list->current;
	Node* right_1 = list->current->next;
	Node* left_2 = _list.list->current;
	Node* right_2 = _list.list->current->next;

	left_1->next = right_2;
	right_2->previous = left_1;
	left_2->next = right_1;
	right_1->previous = left_2;

	// set the new front points
	Node* temp = list->first;
	list->first = _list.list->first;
	_list.list->first = temp;

	// set the new current nodes
	temp = list->current;
	list->current = _list.list->current;
	_list.list->current = temp;

	int tempint = _list.list->leftSize;
	_list.list->leftSize = list->leftSize;
	list->leftSize = tempint;
}

// swap the right sides of two lists
template <class Type> void
List <Type> :: SwapRights (List& _list) {
	// swap out everything after the current nodes
	Node* left_1 = list->current;
	Node* right_1 = list->current->next;
	Node* left_2 = _list.list->current;
	Node* right_2 = _list.list->current->next;

	left_1->next = right_2;
	right_2->previous = left_1;
	left_2->next = right_1;
	right_1->previous = left_2;

	// set the new endpoints
	Node* temp = list->last;
	list->last = _list.list->last;
	_list.list->last = temp;

	int tempint = _list.list->rightSize;
	_list.list->rightSize = list->rightSize;
	list->rightSize = tempint;
}


// redefine operator << for printing
template <class Type> ostream&
operator<<(ostream& _output, const List<Type>& _list) {
	List<Type> newObject;
	newObject.Swap(const_cast<List<Type>&>(_list));

	_output << "(";
	for (newObject.MoveToStart(); !newObject.AtEnd(); newObject.Advance()) {
		if (!newObject.AtStart()) {
			_output << ", ";
		}

		_output << newObject.Current();
	}
	_output << ") : " << newObject.Length();

	newObject.Swap(const_cast<List<Type>&>(_list));

	return _output;
}

#endif
