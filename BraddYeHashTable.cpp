#include <iostream>
#include <vector>
#include <list>
#include <algorithm>
#include <iterator>
#include <string> 

class HashTable {
    std::vector<std::list<int> > table;
    int length;  
    int hash(int k) { return k % length; }

public:
    HashTable(int size) : length(size), table(size) {}
    void insert(int k) {
        table[hash(k)].push_front(k);
    }
    void remove(int k) {
        std::list<int>& bucket = table[hash(k)];
        auto it = std::find(bucket.begin(), bucket.end(), k);
        if (it != bucket.end()) {
            bucket.erase(it);
            std::cout << k << ":DELETED;\n";
        } else {
            std::cout << k << ":DELETE_FAILED;\n";
        }
    }
    void search(int k) {
        std::list<int>& bucket = table[hash(k)];
        auto it = std::find(bucket.begin(), bucket.end(), k);
        if (it != bucket.end()) {
            std::cout << k << ":FOUND_AT" << hash(k) << ","
                      << std::distance(bucket.begin(), it) << ";\n";
        } else {
            std::cout << k << ":NOT_FOUND;\n";
        }
    }
    void display() {
        for (int i = 0; i < length; ++i) {
            std::cout << i << ":";
           for (auto it = table[i].begin(); it != table[i].end(); ++it) {
                std::cout << *it << "->";
            }
            std::cout << ";\n";
        }
    }
};
int main() {
   int x;
   std::cin >> x; 
   HashTable newHashTable(x);
   char command;
   while (std::cin >> command) {
       if (command == 'e')
           break;
        if (command == 'o') {
            newHashTable.display();
            continue;
        }
        int k;
        std::cin >> k;
       if (command == 'i') {
           newHashTable.insert(k);
       } else if (command == 's') {
            newHashTable.search(k);
          } else if (command == 'd') {
            newHashTable.remove(k);
        } else {
            std::cerr << "Invalid command";
        }
    }
    return 0;
}