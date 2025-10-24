#include <string>
#include <cstring>

#include <stack>
// #include <vector>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
    return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file, string _tblName) :
    schema(_schema), file(_file), tblName(_tblName) {

    file.MoveFirst(); // prepare DBFile to read from beginning
}

Scan::~Scan() {

}

// returns true when record was retrieved
bool Scan::GetNext(Record& _record) {
    // cout << "scan get next" << endl;
    if (file.GetNext(_record) > 0) { // assumes positive code means success
        return true;
    }
    return false;
}

ostream& Scan::print(ostream& _os) {
    _os << "SCAN (" << schema.GetNoTuples() << " tuples, " << schema.GetNumAtts() << " atts, table: " << tblName << ")";
    return _os;
}

IndexScan::IndexScan(int key, BTreeIndex& _index, DBFile& _file)
    : searchKey(key), index(&_index), file(_file), fetched(false) {
        file.MoveFirst(); // prepare DBFile to read from beginning

    }


IndexScan::~IndexScan() {}

bool IndexScan::GetNext(Record& _record) {
    cout << "indexscan get next" << endl;

    if (fetched) return false;
    cout << "index scan get next passed fetched" << endl;

    int heapPageNum, recordOffset;
    if (!index->Find(searchKey, heapPageNum, recordOffset)) {
        fetched = true;
        return false;
    }
    cout << "index scan get next passed find -> heapPageNum , recordOffset: " << heapPageNum << ", " << recordOffset << endl;

    file.GetPage(heapPage, heapPageNum);

    // Get the first record to start
    Record temp;
    if (!heapPage.GetFirst(temp)) {
        cout << "Failed to get first record" << endl;
        return false;
    }

    // Then get first record repeatedly until we reach the desired offset
    for (int i = 0; i < recordOffset; ++i) {
        if (!heapPage.GetFirst(temp)) {
            cout << "Failed to get record at offset " << i << endl;
            return false;
        }
    }

    _record.CopyBits(temp.GetBits(), temp.GetSize());
    fetched = true;
    cout << "index scan get next passed record assignment" << endl;
    return true;
}

ostream& IndexScan::print(ostream& _os) {
    _os << "INDEX SCAN (key = " << searchKey << ")";
    return _os;
}

Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
    RelationalOp* _producer)
    {

    schema = _schema;
    predicate = _predicate;
    constants = _constants;
    producer = _producer;

}

Select::~Select() {
    delete producer;
}

bool Select::GetNext(Record& _record) {
    cout << "select get next" << endl;
    
    // loop over producer until getting a record that passes the predicate
    while (producer->GetNext(_record)) {
        // apply predicate to record (see Comparison.h)
        if (true == predicate.Run(_record, constants)) return true;
    }
    return false;
}

ostream& Select::print(ostream& _os) {
    _os << "SELECT (~" << schema.GetNoTuples() << " tuples, " << schema.GetNumAtts() << " atts, " << predicate.numAnds << " conditions)\n└────>";
    producer->print(_os);
    return _os;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
    int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
    schemaIn = _schemaIn;
    schemaOut = _schemaOut;
    numAttsInput = _numAttsInput;
    numAttsOutput = _numAttsOutput;
    keepMe = new int[numAttsOutput];
    memcpy(keepMe, _keepMe, numAttsOutput * sizeof(int));
    producer = _producer;
}

Project::~Project() {
    delete [] keepMe;
    delete producer;
}

bool Project::GetNext(Record& _record) {
    cout << "project get next" << endl;
    if (producer->GetNext(_record)) {
        // use built-in Project method (see Record.h)
        // keepme and numAttsInput must be correct
        _record.Project(keepMe, numAttsOutput, numAttsInput);
        return true;
    }
    return false;
}

ostream& Project::print(ostream& _os) {
    _os << "PROJECT (" << schemaOut.GetNumAtts() << " atts)\n└────>";
    producer->print(_os);
    return _os;
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
    CNF& _predicate, RelationalOp* _left, RelationalOp* _right) :
    schemaLeft(_schemaLeft), schemaRight(_schemaRight), schemaOut(_schemaOut),
    predicate(_predicate), left(_left), right(_right) {

    cout << "constructer" << endl;

}

Join::~Join() {
    cout << "destructer" << endl;
    delete left;
    delete right;
}

bool Join::GetNext(Record& _record) {
    cout << "join get next" << endl;

    return false;
}

ostream& Join::print(ostream& _os) {    
    // cout << "print join" << endl;
    _os << "JOIN (~" << schemaOut.GetNoTuples() << " tuples, " << schemaOut.GetNumAtts() << " atts, " << predicate.numAnds << " conditions)\n├────>";
    left->print(_os);
    _os << "\n└────>";
    right->print(_os);
    return _os;
}


SymmetricHashJoin::SymmetricHashJoin(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
    CNF& _predicate, RelationalOp* _left, RelationalOp* _right)
    : Join(_schemaLeft, _schemaRight, _schemaOut, _predicate, _left, _right){


}

SymmetricHashJoin::~SymmetricHashJoin() {
    delete left;
	delete right;
}

void SymmetricHashJoin::bufferState(int x)
{
    
    cout << "Line: " << x << "  buffer length  " << buffer.Length() << " left length  " << buffer.LeftLength() << " right length  " << buffer.RightLength() <<endl;
}

bool SymmetricHashJoin::GetNext(Record &_record)
{
    bufferState(__LINE__);
    
    if (isFirstTime)
    {
        cout << "SHJ first time" << endl;
        predicate.GetSortOrders(orderLeft, orderRight);
        isFirstTime = false;
    }
    
    if(leftDone && rightDone && buffer.AtEnd())
    {
        bufferState(__LINE__);
        cout << "leftDone && rightDone && buffer.AtEnd() " << __LINE__ << endl;
        // all of the elements of the right hash, left hash, and buffer have been read
        return false;
    }
    
    
    bufferState(__LINE__);
    if(buffer.AtEnd() == false)
    {
        cout << __LINE__ << endl;

        // if buffer is not at end, then record = buffer.current()
        _record = buffer.Current();
        buffer.Advance();
        return true;
    }

    // buffer = {}
    buffer.MoveToStart();
    while(buffer.RightLength() > 0)
    {
        bufferState(__LINE__);
        Record tmp;
        buffer.Remove(tmp);

    }
    int loopCounter = 0;
    while(true)
    {
        bufferState(__LINE__);
        cout << "loop counter " << loopCounter++ << "  " << __LINE__ << endl;
        cout << matchCounter << " match counter "  << __LINE__ << endl;
        if(isLeftSide ==false)
        {
            cout << "right side  " << __LINE__ << endl;
            // on right side
            isLeftSide = true;

            Record rRec = Record();

            if((!rightDone) &&  right->GetNext(rRec))
            {
                bufferState(__LINE__);
                cout << "if(!rightDone &&  right->GetNext(rRec))  " << __LINE__ << endl;
                rRec.print(cout, schemaRight);
                cout << endl;cout << endl;
                SInt val = 0;
                
                
                rRec.SetOrderMaker(&orderRight);
                
                
                cout << "rRec.SetOrderMaker(&orderRight);  " << __LINE__ << endl;
                
                
                rightHash.Insert(rRec, val);


                cout << "rightHash.Insert(rRec, val)  " << __LINE__ << endl;
                rRec.print(cout, schemaRight); cout << endl;
                if (leftHash.IsThere(rRec))
                {
                    cout << "match on right side  " << __LINE__ << endl;
                    
                    

                    while (leftHash.CurrentKey().IsEqual(rRec))
                    {
                        Record oRec;
                        oRec.AppendRecords(leftHash.CurrentKey(), rRec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
                        buffer.Append(oRec);
                        matchCounter++;
                        leftHash.Advance();
                    }
                    buffer.MoveToStart();
                }
            }else{
                rightDone = true;
            }
            
        }else
        {
            cout << "left side  " << __LINE__ << endl;
            isLeftSide = false;

            Record lRec = Record();

            if((!leftDone) &&  left->GetNext(lRec))
            {
                bufferState(__LINE__);
                SInt val = 0;
                cout << "if(!leftDone &&  left->GetNext(lRec))  " << __LINE__ << endl;
                
                
                lRec.SetOrderMaker(&orderLeft);
                
                cout << "lRec.SetOrderMaker(&orderLeft);  " << __LINE__ << endl;
                
                cout << "Record tmp = lRec;  " << __LINE__ << endl;
                
                
                leftHash.Insert(lRec, val);
                
                
                cout << "leftHash.Insert(tmp, val);  " << __LINE__ << endl;
                
                if (rightHash.IsThere(lRec))
                {
                    cout << "match on left side   " << __LINE__ << endl;
                    while (rightHash.CurrentKey().IsEqual(lRec))
                    {
                        Record oRec;
                        oRec.AppendRecords(lRec, rightHash.CurrentKey(), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
                        buffer.Append(oRec);
                        matchCounter++;

                        rightHash.Advance();
                    }

                    buffer.MoveToStart();
                }
                

            }else{
                rightDone = true;
            }
        }

        cout << "if(leftDone && rightDone && buffer.AtEnd())   " << __LINE__ << endl;
        if(leftDone && rightDone && buffer.AtEnd())
        {
            return false;
        }

        cout << __LINE__ << endl;
        if (buffer.AtEnd() == false)
        {
            cout << "buffer.AtEnd() == false  " <<__LINE__ << endl;

            cout << buffer.Length() << " left " << buffer.LeftLength() << " right " << buffer.RightLength() << endl;
            

            _record = buffer.Current();
            buffer.Advance();
            return true;
        }
            

    }


    return true;
}



HashJoin::HashJoin(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
    CNF& _predicate, RelationalOp* _left, RelationalOp* _right)
    : Join(_schemaLeft, _schemaRight, _schemaOut, _predicate, _left, _right){


}

HashJoin::~HashJoin() {
    delete left;
	delete right;
}



bool HashJoin::GetNext(Record &_record) {
    
    
    if (first_time) {
        cout << "HJ first time" << endl;
        
        predicate.GetSortOrders(orderLeft, orderRight);
        
        // cout << orderLeft << endl;
        // cout << orderRight << endl;
        
        
        first_time = false;
        // Read right side and keep in memory
        // cout << "first time" << endl;
        Record tmp = Record();
		int c = 0;
		while(right->GetNext(tmp))
		{
			// cout << "first time while" << endl;
            tmp.SetOrderMaker(&orderRight); // Set order of left record
            // cout << "first time while" << endl;
            SInt val = 0;
            // cout << "first time while" << endl;
            hash.Insert(tmp, val);
            // cout << "first time while" << endl;
            // tmp.print(cout, schemaRight);
            // cout << "first time while" << endl;

			c++;
		}

        // cout << c << endl;

        first_time = false;
        
        if(left->GetNext(lRec) == false)
        {
            
            // cout << "left->GetNext(lRec) == false" << endl;
            return false;
        }

        lRec.SetOrderMaker(&orderLeft);
        // cout << "lRec.SetOrderMaker(&orderLeft)" << endl;

        if(hash.IsThere(lRec))
        {
            // cout << "if(hash.IsThere(lRec))" << endl;
            _record.AppendRecords(lRec, hash.CurrentKey(), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
            counter++;
            // cout << counter <<"  _record.AppendRecords)" << endl;
            hash.Advance();

            // cout << "hash advance 1" << endl;

            return true;
        }

    }

    

	while(true)
	{
		
		if (lRec.IsEqual(hash.CurrentKey()))
        {
            // if (counter == 1499)
            // {
                
            //     lRec.print(cout, schemaLeft);
            //     cout << endl;
            //     cout << endl;
            //     hash.CurrentKey().print(cout, schemaRight);

            //     cout << endl;
            //     cout << endl;
                


            // }
            
            // if (counter == 1500)
            // {
                
            //     lRec.print(cout, schemaLeft);
            //     cout << endl;
            //     cout << endl;
            //     hash.CurrentKey().print(cout, schemaRight);

            //     cout << endl;
            //     cout << endl;
                


            // }
            
            // cout << "lRec.IsEqual(hash.CurrentKey())" << __LINE__ << endl;
            
            
            _record.AppendRecords(lRec, hash.CurrentKey(), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
            counter++;
            // cout << counter << "   _record.AppendRecords " << __LINE__ << endl;
            if (hash.AtEnd() == false)
            {
                // cout << "hash advance 2 " << __LINE__<< endl;
                hash.Advance();
            }else{
                // cout << "at end = true" << endl;
            }
            
            

            return true;
        }

        if(left->GetNext(lRec) == false)
        {
            
            // cout << "left->GetNext(lRec) == false " << __LINE__ << endl;
            return false;
        }
        // cout << "set order maker before" << __LINE__ << endl;
        lRec.SetOrderMaker(&orderLeft);
        // cout << "set order maker after " << __LINE__ << endl;

        if(hash.IsThere(lRec))
        {
            // cout << "hash is there " << __LINE__ << endl;
            _record.AppendRecords(lRec, hash.CurrentKey(), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
            counter++;
            // cout << counter << " _record.AppendRecords" << __LINE__ << endl;
            hash.Advance();

            return true;
        }

    }

    return false;
    
}


NestedLoopJoin::NestedLoopJoin(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
    CNF& _predicate, RelationalOp* _left, RelationalOp* _right)
    : Join(_schemaLeft, _schemaRight, _schemaOut, _predicate, _left, _right) {
    
        

}

NestedLoopJoin::~NestedLoopJoin() {
    delete left;
	delete right;
}

bool NestedLoopJoin::GetNext(Record& _record) {
    // two lists, right and left, we take the first element from left and compare to the right list,
    // if nothing is found, we go to the next left element, this continues, until there
    // are no more left elements
    

    // we take all the tuples from the right table, and load it into memory
	if(first_time)
	{
        cout << "NLJ first time" << endl;

		first_time = false;

		// Read right side and keep in memory
		Record tmp = Record();
		int c = 0;
		while(right->GetNext(tmp))
		{
			// list is defined in Relop.h
            list.Append(tmp);
			c++;
		}

        // when we are done going through the right table, we go to the top of the list as next time we want to start from the top
        list.MoveToStart();
		
	}

	// we have two list, the right table is in memory, and now we want to take the next element 
	// of the left table and compare it with all of the elements of the right table
	while(true)
	{
		

        // before we enter the loop, we dont have a left record
		if(has_left_rec == false)
		{
			

            if(left->GetNext(lRec) == false)
			{
				// there is no more left records, thus we return false
                return false;
			}

			// there is a left record if true
			has_left_rec = true;
			
            // now we are going to compare the left record, with the list of records from the right, stored in list
            list.MoveToStart();
		}


		
		while (list.AtEnd() == false)
		{
            // right records we are going to compare	
            // rside stores a copy of the register that list.current is pointing to 
            // rside = Record(list.Current()); // don't use this!!!!!

            			

            // logical condition 
            if(predicate.Run(lRec, list.Current()) == true)
			{
				// cout << "match!!" << endl;
                _record.AppendRecords(lRec, list.Current(), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());


                // advances the current register in list
                list.Advance();
				
                return true;

			}
            // advances the current register in list
            list.Advance();
			// if not match it will try the next right side record
		}

		// if we exit this loop, then we run out of right side tuples
		// so indicate that we don't have a left side so that code on top will get a new one
		has_left_rec = false;
    }

    return false;
}



DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) : 
    schema(_schema), producer(_producer) {

}

DuplicateRemoval::~DuplicateRemoval() {
    delete producer;
}

bool DuplicateRemoval::GetNext(Record& _record) {
    // cout << "duplicate removal get next" << endl;
    
    // use whatever set-like data structure you like
    // Map.h, std::set, std::unordered_set
    // you may add the data structure as a class member in RelOp.h

    // in a loop:
    // 1. get record from parent
    // 2a. if record's value not already in set
    //     i. insert value into set
    //     ii. return true

    while (producer->GetNext(_record)) {
        string bits(_record.GetBits(), _record.GetSize());
        if (distinct.find(bits) == distinct.end()) {
            // _record.print(cout, schema);
            // cout << " ..." << endl;
            distinct.insert(bits);
            // cout << " ... added to distinct" << endl;
            return true;
        }
        // _record.print(cout, schema);
        // cout << " already in distinct" << endl;
    }

    return false;
}

ostream& DuplicateRemoval::print(ostream& _os) {
    _os << "DISTINCT\n└────>";
    producer->print(_os);
    return _os;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
    RelationalOp* _producer) :
    schemaIn(_schemaIn), schemaOut(_schemaOut), compute(_compute),
    producer(_producer) {
    done = false;
}

Sum::~Sum() {
    delete producer;
}

bool Sum::GetNext(Record& _record) {
    if (done) return false;
    int intSum = 0;
    double doubleSum = 0;
    while (producer->GetNext(_record)) {
        // cout << "received ";
        // _record.print(cout, schemaIn);

        int intRes = 0;
        double doubleRes = 0;
        Type t = compute.Apply(_record, intRes, doubleRes);
        doubleSum += doubleRes;
        // cout << " SUM = " << doubleSum << endl;
    }

    char* res;
    int numBytes = 2 * sizeof(int) + sizeof(double);
    res = new char[numBytes];
    ((int *) res)[0] = numBytes;
    ((int *) res)[1] = 2 * sizeof(int);
    *((double *) &(res[2 * sizeof(int)])) = doubleSum;
    _record.CopyBits(res, numBytes);
    // _record.print(cout, schemaOut); cout << endl;
    done = true;
    return true;
}

ostream& Sum::print(ostream& _os) {
    _os << "SUM\n└────>";
    producer->print(_os);
     return _os;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
    Function& _compute, RelationalOp* _producer) :
    schemaIn(_schemaIn), schemaOut(_schemaOut), groupingAtts(_groupingAtts),
    compute(_compute), producer(_producer) {

}

GroupBy::~GroupBy() {
    delete producer;
}

bool GroupBy::GetNext(Record& _record) {
    // cout << "GroupBy get next" << endl;
    
    if (rdy == false) {
        // cout << "before calling producer get next " << endl;
        while (producer->GetNext(_record)) {
            // cout << "inside" <<endl;
            // cout << endl;
            // cout << endl;
            // cout << "_record.print(cout, schemaIn)" <<endl;
            // _record.print(cout, schemaIn); 
            // cout << endl;
            // cout << "_record.print(cout, schemaIn)" <<endl;
            // cout << endl;
            // cout << endl;

            Record copy;
            copy = _record;
            // cout << groupingAtts.whichAtts <<endl;
            // cout << groupingAtts.numAtts <<endl;
            // cout << schemaIn.GetNumAtts() <<endl;
            copy.Project(groupingAtts.whichAtts, groupingAtts.numAtts, schemaIn.GetNumAtts());
            // cout << "after" <<endl;
            string bits(copy.GetBits(), copy.GetSize());
            if (groupSums.find(bits) == groupSums.end()) {
                groupSums[bits] = new char[2 * sizeof(int) + sizeof(double)];
                ((int *) groupSums[bits])[0] = 2 * sizeof(int) + sizeof(double);
                ((int *) groupSums[bits])[1] = 2 * sizeof(int);
                *((double *) &(groupSums[bits][2 * sizeof(int)])) = 0.;
            }
            int intRes = 0;
            double doubleRes = 0;
            Type t = compute.Apply(_record, intRes, doubleRes);
            *((double *) &(groupSums[bits][2 * sizeof(int)])) += doubleRes;
            // cout << "Running sum is a float (" << *((double *) &(groupSums[bits][2 * sizeof(int)])) << ")" << endl;
        }
        it = groupSums.begin();
        rdy = true;
    }

    if (it == groupSums.end()) return false;
    
    Record agg;
    Record groupAtts;
    string bits = it->first;
    groupAtts.CopyBits(&bits[0], it->first.size());
    agg.CopyBits(it->second, 2 * sizeof(int) + sizeof(double));
    _record.AppendRecords(agg, groupAtts, 1, groupingAtts.numAtts);

    ++it;
    return true;
}

ostream& GroupBy::print(ostream& _os) {
    _os << "GROUP BY " << groupingAtts << "\n└────>";
    producer->print(_os);
    return _os;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) :
    schema(_schema), outFile(_outFile), producer(_producer) {
    outStream.open(outFile, ios::trunc);
    // outStream << "";
}

WriteOut::~WriteOut() {
    outStream.close();
    delete producer;
}

bool WriteOut::GetNext(Record& _record) {
    // cout << "write out get nect" << endl;
    // write to outStream
    if (producer->GetNext(_record)) {
        _record.print(outStream, schema); outStream << endl;
        return true;
    }
    outStream << endl; // flush stream buffer
    return false;
}

ostream& WriteOut::print(ostream& _os) {
    _os << "OUTPUT (~" << schema.GetNoTuples() << " tuples, "<< schema.GetNumAtts() << " atts)\n└────>";
    producer->print(_os);
    return _os;
}

void QueryExecutionTree::ExecuteQuery() {
    Record r;
    while (root->GetNext(r)) {
        continue;
    }
}

ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
    _os << "QUERY EXECUTION TREE\n";
    _op.root->print(_os);
    return _os;
}

// QueryExecutionTree::~QueryExecutionTree() {
//  delete root;
// }

// javi branch
//     