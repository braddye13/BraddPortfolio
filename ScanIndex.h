#ifndef _SCANINDEX_H
#define _SCANINDEX_H

#include "RelOp.h"
#include "BTreeIndex.h"
#include <vector>
#include <string>

class ScanIndex : public RelationalOp {
private:
    Catalog* catalog;
    std::string tableName;
    std::string attributeName;
    int searchKey;
    bool isRangeQuery;
    int rangeStart;
    int rangeEnd;
    
    BTreeIndex* index;
    std::vector<std::pair<int, int>> matchingRecords; // (pageNum, recordOffset)
    size_t currentRecord;
    
    void LoadIndex();
    void FindMatchingRecords();

public:
    ScanIndex(Catalog& _catalog, std::string _tableName, std::string _attributeName);
    virtual ~ScanIndex();
    
    void SetSearchKey(int key);
    void SetRange(int start, int end);
    
    void Run();
    bool GetNext(Record& record) override;
    std::ostream& print(std::ostream& _os) override { return _os << "ScanIndex"; }
};

#endif 