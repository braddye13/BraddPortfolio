#include "IndexPage.h"
#include <cstring>

IndexPage::IndexPage() : pageType(PageType::LEAF), pageId(-1), lastPointer(-1), numKeys(0) {}

void IndexPage::ToBinary(char* dest) const {
    int* intDest = reinterpret_cast<int*>(dest);
    intDest[0] = static_cast<int>(pageType);
    intDest[1] = pageId;
    intDest[2] = lastPointer;
    intDest[3] = numKeys;

    int idx = 4;
    if (pageType == PageType::LEAF) {
        for (int i = 0; i < numKeys; ++i) {
            intDest[idx++] = keys[i];
            intDest[idx++] = pointers[i];
            intDest[idx++] = recordOffsets[i];
        }
    } else { // INTERNAL
        for (int i = 0; i < numKeys; ++i) {
            intDest[idx++] = keys[i];
            intDest[idx++] = pointers[i];
        }
    }
    intDest[idx] = lastPointer;
}

void IndexPage::FromBinary(const char* src) {
    const int* intSrc = reinterpret_cast<const int*>(src);
    pageType = static_cast<PageType>(intSrc[0]);
    pageId = intSrc[1];
    lastPointer = intSrc[2];
    numKeys = intSrc[3];

    keys.clear();
    pointers.clear();
    recordOffsets.clear(); // ADD THIS

    int idx = 4;
    if (pageType == PageType::LEAF) {
        for (int i = 0; i < numKeys; ++i) {
            keys.push_back(intSrc[idx++]);
            pointers.push_back(intSrc[idx++]);
            recordOffsets.push_back(intSrc[idx++]);
        }
    } else {
        for (int i = 0; i < numKeys; ++i) {
            keys.push_back(intSrc[idx++]);
            pointers.push_back(intSrc[idx++]);
        }
    }
}
