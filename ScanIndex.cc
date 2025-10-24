#include "ScanIndex.h"
#include "DBFile.h"
#include "Schema.h"
#include <iostream>

ScanIndex::ScanIndex(Catalog& _catalog, std::string _tableName, std::string _attributeName)
    : catalog(&_catalog), tableName(_tableName), attributeName(_attributeName),
      isRangeQuery(false), currentRecord(0) {
    index = new BTreeIndex(_catalog);
}

ScanIndex::~ScanIndex() {
    delete index;
}

void ScanIndex::SetSearchKey(int key) {
    searchKey = key;
    isRangeQuery = false;
}

void ScanIndex::SetRange(int start, int end) {
    rangeStart = start;
    rangeEnd = end;
    isRangeQuery = true;
}

void ScanIndex::LoadIndex() {
    // Get index file name from catalog
    std::string idxName = catalog->GetIndexFile(tableName, attributeName);
    if (idxName.empty()) {
        std::cerr << "Error: Index not found for " << tableName << "." << attributeName << std::endl;
        return;
    }
    // Open the index
    index->Open(idxName);
}

void ScanIndex::FindMatchingRecords() {
    matchingRecords.clear();
    currentRecord = 0;
    
    if (!isRangeQuery) {
        // Point query using the new Find() method
        std::vector<std::pair<int, int>> results;
        index->Find(searchKey, results);
        matchingRecords = results;
    } else {
        // Range query - traverse leaf pages
        File indexFile;
        if (indexFile.Open(1, const_cast<char*>(index->GetIndexFileName().c_str())) < 0) {
            std::cerr << "Error: could not open index file for reading\n";
            return;
        }
        // Start from root and find first leaf
        off_t currentPageId = index->GetRootPageId();
        while (true) {
            IndexPage page;
            if (indexFile.GetPage(page, currentPageId) < 0) {
                std::cerr << "Error: could not read page " << currentPageId << std::endl;
                return;
            }
            if (page.pageType == PageType::LEAF) {
                // Found a leaf page, scan it and following leaf pages
                while (true) {
                    // Add matching records from this page
                    for (int i = 0; i < page.numKeys; i++) {
                        if (page.keys[i] >= rangeStart && page.keys[i] <= rangeEnd) {
                            matchingRecords.push_back({page.pointers[i], page.recordOffsets[i]});
                        }
                    }
                    // Move to next leaf page if exists and if we haven't exceeded range
                    if (page.rightSibling != -1 && page.keys.back() <= rangeEnd) {
                        if (indexFile.GetPage(page, page.rightSibling) < 0) {
                            std::cerr << "Error: could not read next leaf page\n";
                            return;
                        }
                    } else {
                        break;
                    }
                }
                break;
            } else {
                // Internal node - find next page to visit
                int i;
                for (i = 0; i < page.numKeys; i++) {
                    if (rangeStart < page.keys[i]) {
                        currentPageId = page.pointers[i];
                        break;
                    }
                }
                if (i == page.numKeys) {
                    currentPageId = page.lastPointer;
                }
            }
        }
        indexFile.Close();
    }
}

void ScanIndex::Run() {
    LoadIndex();
    FindMatchingRecords();
}

bool ScanIndex::GetNext(Record& record) {
    if (currentRecord >= matchingRecords.size()) {
        return false; // No more records
    }
    // Get the next matching record
    auto [pageNum, recordOffset] = matchingRecords[currentRecord++];
    // Open the data file
    DBFile dbfile;
    SString tblname(tableName);
    SString path;
    if (!catalog->GetDataFile(tblname, path)) {
        std::cerr << "Error: couldn't get path for table " << tableName << std::endl;
        return false;
    }
    if (dbfile.Open(const_cast<char*>(std::string(path).c_str())) < 0) {
        std::cerr << "Error: couldn't open DBFile for " << tableName << std::endl;
        return false;
    }
    // Fetch the page
    Page page;
    if (dbfile.GetPage(page, pageNum) < 0) {
        std::cerr << "Error: couldn't get page " << pageNum << std::endl;
        return false;
    }
    // Iterate to the correct record offset
    for (int i = 0; i <= recordOffset; ++i) {
        if (!page.GetFirst(record)) {
            std::cerr << "Error: couldn't get record at offset " << recordOffset << std::endl;
            return false;
        }
    }
    return true;
} 