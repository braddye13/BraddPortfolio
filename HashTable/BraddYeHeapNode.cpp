#include <iostream>
#include <queue>
#include <vector>
#include <string>
#include <unordered_map>
using namespace std;

struct Min_Heap_Node {
    char data;              
    unsigned freq;           
    Min_Heap_Node *left, *right;
    Min_Heap_Node(char data, unsigned freq) {
        left = right = nullptr;
        this->data = data;
        this->freq = freq;
    }
};
struct compare {
    bool operator()(Min_Heap_Node* l, Min_Heap_Node* r) {
        return (l->freq > r->freq);
    }
};
void printCodes(Min_Heap_Node* root, string str, unordered_map<char, string>& huffmanCode) {
    if (!root)
        return;
    if (root->data != '$') 
        huffmanCode[root->data] = str;
    printCodes(root->left, str + "0", huffmanCode);
    printCodes(root->right, str + "1", huffmanCode);
}
void HuffmanCodes(char data[], int freq[], int size) {
    Min_Heap_Node *left, *right, *top;
    priority_queue<Min_Heap_Node*, vector<Min_Heap_Node*>, compare> minHeap;
    for (int i = 0; i < size; ++i)
        minHeap.push(new Min_Heap_Node(data[i], freq[i]));
    while (minHeap.size() != 1) {
        left = minHeap.top();
        minHeap.pop();
        right = minHeap.top();
        minHeap.pop();
        top = new Min_Heap_Node('$', left->freq + right->freq);
        top->left = left;
        top->right = right;
        minHeap.push(top);
    }
    unordered_map<char, string> huffmanCode;
    printCodes(minHeap.top(), "", huffmanCode);
    // Print in the correct order
    for (int i = 0; i < size; i++) {
        cout << data[i] << ": " << huffmanCode[data[i]] << "\n";
    }
}
int main() {
    char arr[] = { 'A', 'B', 'C', 'D', 'E', 'F'};
    int freq[6];
    int size = sizeof(arr) / sizeof(arr[0]);

    // cout << "Enter frequencies for characters A, B, C, D, E, F:\n";
    for (int i = 0; i < size; i++)
        cin >> freq[i];

    HuffmanCodes(arr, freq, size);
    return 0;
}
