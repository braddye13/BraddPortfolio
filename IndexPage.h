#pragma once

#include "Config.h"
#include <vector>
#include <cstdint>

// Constants
const int HEADER_SIZE = 16;
const int KEY_SIZE = 4;       // int
const int PTR_SIZE = 4;       // int (simplified page number only)
const int MAX_KEYS = (PAGE_SIZE - HEADER_SIZE - PTR_SIZE) / (KEY_SIZE + PTR_SIZE); // Conservative calc

enum class PageType {
    INTERNAL,
    LEAF
};

class IndexPage {
public:
    PageType pageType;
    int pageId;
    int lastPointer;
    int numKeys;
    

    std::vector<int> keys;
    std::vector<int> pointers; // length = numKeys + 1
    std::vector<int> recordOffsets; 
    IndexPage();

    void ToBinary(char* dest) const;
    void FromBinary(const char* src);
};
