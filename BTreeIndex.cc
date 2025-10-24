#include "BTreeIndex.h"
#include "IndexPage.h"
#include "DBFile.h"
#include "Record.h"
#include "Schema.h"
#include "Catalog.h"


#include <iostream>
#include <fstream>
#include <cstring>
#include <map> // temp in-memory index
#include <vector>
#include <algorithm> // sort


using namespace std;

BTreeIndex::BTreeIndex(Catalog& _catalog) : catalog(&_catalog) {
}

BTreeIndex::~BTreeIndex() {
}

void BTreeIndex::Build(string indexName, string tableName, string attributeName) {
  struct BTreeEntry {
      int key;  // INT attribute value
      int pageNum;
      int recordOffset;
  };


  vector<BTreeEntry> entries;

  const int K = 100;
  vector<IndexPage> leafPages;


  // Step 1: Open DBFile
  DBFile dbfile;
  SString path, tblname(tableName);

  if (!catalog->GetDataFile(tblname, path)) {
      cerr << "Error: couldn't get path for table " << tableName << endl;
      return;
  }

  if (dbfile.Open(const_cast<char*>(string(path).c_str())) < 0) {
      cerr << "Error: couldn't open DBFile for " << tableName << endl;
      return;
  }
  dbfile.MoveFirst();

  // Step 2: Get Schema
  Schema schema;
  if (!catalog->GetSchema(tblname, schema)) {
      cerr << "Error: couldn't get schema for " << tableName << endl;
      return;
  }

  // Step 3: Find attribute offset
  SString attrName(attributeName);
  int attIndex = schema.Index(attrName);
  if (attIndex < 0) {
      cerr << "Error: attribute " << attributeName << " not found\n";
      return;
  }

  Type attrType = schema.FindType(attrName);
  if (attrType != Integer) {
      cerr << "Error: only Integer attributes supported for indexing\n";
      return;
  }

  // Step 4: Scan heap file, extract INT keys, build entries
  Record rec;
  while (dbfile.GetNext(rec) > 0) {
    char* bits = rec.GetBits();
    int offset = ((int*)bits)[attIndex + 1];
    int val = *((int*)(bits + offset));

    int pageNum = dbfile.GetCurrentPage();
    int recordOffset = dbfile.GetCurrentRecordOffset();

    entries.push_back({val, pageNum, recordOffset});

  }

  cout << "Collected " << entries.size() << " entries, sorting...\n";

  // Step 5: Sort by key
  sort(entries.begin(), entries.end(), [](const BTreeEntry& a, const BTreeEntry& b) {
      return a.key < b.key;
  });

  cout << "Entries sorted. Ready to construct leaf pages.\n";

  // Step 6: Build leaf nodes from sorted entries and start bottom-up tree construction
  for (size_t i = 0; i < entries.size(); i += K) {
    IndexPage page;
    page.pageType = PageType::LEAF;
    page.pageId = leafPages.size();  // consecutive ID
    page.numKeys = std::min(static_cast<int>(entries.size() - i), K);

    for (int j = 0; j < page.numKeys; ++j) {
        page.keys.push_back(entries[i + j].key);
        page.pointers.push_back(entries[i + j].pageNum);        // page number
        page.recordOffsets.push_back(entries[i + j].recordOffset); // offset in that page
    }

    // Add a dummy pointer at the end for B+-tree consistency (n keys → n+1 pointers)
    page.lastPointer = -1;

    leafPages.push_back(page);
  }
  cout << "Built " << leafPages.size() << " leaf pages.\n";

  File indexFile;
  currentIndexFile = indexName + ".idx";
  catalog->RegisterIndex(tableName, attributeName, indexName + ".idx");
  if (indexFile.Open(0, const_cast<char*>((currentIndexFile).c_str())) < 0) {
      cerr << "Error: could not open index file for writing\n";
      return;
  }

  // Step 7: Write leaf pages to disk
  for (auto& leaf : leafPages) {
    indexFile.AddPage(leaf, leaf.pageId);
  }

  cout << "Leaf pages written to disk.\n";

  // Step 8: Build first level of internal pages from leaf pages
  vector<IndexPage> internalPages;
  int nextPageId = leafPages.size();  // continue page IDs after leaves

  for (size_t i = 0; i < leafPages.size(); i += K) {
    IndexPage page;
    page.pageType = PageType::INTERNAL;
    page.pageId = nextPageId++;
    
    int groupSize = std::min(static_cast<int>(leafPages.size() - i), K);
    page.numKeys = groupSize - 1;

    for (int j = 0; j < groupSize; ++j) {
      page.pointers.push_back(leafPages[i + j].pageId);
      if (j > 0) {
        page.keys.push_back(leafPages[i + j].keys.front());
      }
    }

    // lastPointer for (n+1)-th pointer
    page.lastPointer = page.pointers.back();

    // write to file
    indexFile.AddPage(page, page.pageId);
    internalPages.push_back(page);
  }

  cout << "Built and wrote " << internalPages.size() << " internal pages.\n";

  // Step 9: Finalize tree by making a root at page 0
  // Finalize root
  IndexPage root;
  if (internalPages.size() == 1) {
      root = internalPages[0];
  } else {
      // Recurse upward until we have a single root
vector<IndexPage> currLevel = internalPages;
while (currLevel.size() > 1) {
    vector<IndexPage> nextLevel;
    for (size_t i = 0; i < currLevel.size(); i += K) {
        IndexPage page;
        page.pageType = PageType::INTERNAL;
        page.pageId = nextPageId++;

        int groupSize = min(static_cast<int>(currLevel.size() - i), K);
        page.numKeys = groupSize - 1;

        for (int j = 0; j < groupSize; ++j) {
            page.pointers.push_back(currLevel[i + j].pageId);
            if (j > 0) {
                page.keys.push_back(currLevel[i + j].keys.front());
            }
        }

        page.lastPointer = page.pointers.back();

        indexFile.AddPage(page, page.pageId);
        nextLevel.push_back(page);
    }
    currLevel = nextLevel;
}

// The only remaining node is the root
root = currLevel[0];
rootPageId = root.pageId;

  }

  root.pageId = nextPageId++;
  indexFile.AddPage(root, root.pageId);
  rootPageId = root.pageId;  // <- store for Find() to use
  

  indexFile.Close();
  cout << "Index build complete.\n";
}

bool BTreeIndex::Find(int key, int& pageNum, int& recordOffset) {
  File indexFile;
  if (indexFile.Open(1, const_cast<char*>((currentIndexFile).c_str())) < 0) {
      cerr << "Error: could not open index file for reading\n";
      return false;
  }

  off_t currentPageId = rootPageId;

  cout << "Index file length: " << indexFile.GetLength() << " pages.\n";
  while (true) {
    cout << "Current page ID: " << currentPageId << endl;
    if (currentPageId >= indexFile.GetLength()) {
      cerr << "ERROR: Trying to access invalid page " << currentPageId
           << ", file only has " << indexFile.GetLength() << " pages.\n";
      return false;
    }
    IndexPage page;      
    if (indexFile.GetPage(page, currentPageId) < 0) {
        cerr << "Error: could not read page " << currentPageId << endl;
        return false;
    }
    cout << "got page" << endl;
    if (page.pageType == PageType::LEAF) {
        cout << "found leaf" << endl;
        // Step 1: Search for key in leaf
        for (int i = 0; i < page.numKeys; ++i) {
            if (page.keys[i] == key) {
                cout << "found key" << endl;
                pageNum = page.pointers[i]; // Found it
                recordOffset = page.recordOffsets[i];
                return true;
            }
        }
        cout << "key not found" << endl;
        return false; // Key not found
    } else {
      
      // Step 2: Traverse internal node
      cout << "found internal" << endl;
      int i = 0;
      while (i < page.numKeys && key >= page.keys[i]) {
          ++i;
      }
      currentPageId = (i < page.numKeys) ? page.pointers[i] : page.lastPointer;
    }
  }
  cout << "done" << endl;
  return false; // fallback
}


void BTreeIndex::Open(const string& indexFileName) {
  currentIndexFile = indexFileName;
  
  // Open the index file and read the rootPageId
  File indexFile;
  if (indexFile.Open(1, const_cast<char*>((currentIndexFile).c_str())) < 0) {
      cerr << "Error: could not open index file for reading" << endl;
      return;
  }
  
  // In the current implementation, the root is always at page ID 16
  // This is a hardcoded value based on the Build method's behavior
  // A better implementation would store the root page ID in a metadata page
  rootPageId = 16;
  
  indexFile.Close();
}

