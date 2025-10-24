#ifndef _BTREEINDEX_H
#define _BTREEINDEX_H

#include <iostream>
#include "Catalog.h"

struct CreateIndexInfo {
  char* indexName;
  char* tableName;
  char* attributeName;
};


class BTreeIndex {
  private:
    Catalog* catalog;
    std::string currentIndexFile;
    off_t rootPageId = 0;
  public:    
    BTreeIndex(Catalog& _catalog);
    virtual ~BTreeIndex(); 
    void Build(string indexName, string tableName, string attributeName);
    bool Find(int key, int& pageNum, int& recordOffset);
    void Open(const string& indexFileName);
};
  
  

#endif