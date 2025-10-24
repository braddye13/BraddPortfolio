#include <iostream>
#include <queue>
#include <vector>
#include <string>
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

void printCode(Min_Heap_Node* root, string str) {
    if (!root)
        return;
    if (root->data != '$') 
        cout << root->data << ":" << str << "\n";
    printCode(root->left, str + "0");
    printCode(root->right, str + "1");
}
void deleteTree(Min_Heap_Node* root) { // this segment manages the memory and deletes when needed. 
    if (!root) return;
    deleteTree(root->left);
    deleteTree(root->right);
    delete root;
}
void HuffmanCode(char data[], int freq[], int size) {
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
    printCode(minHeap.top(), "");
}
int main() {
    char arr[] = { 'A', 'B', 'C', 'D', 'E', 'F'};
    int freq[6];
    int size = sizeof(arr) / sizeof(arr[0]);

    // cout << "Enter frequencies for characters A, B, C, D, E, F:\n";
    for (int i = 0; i < size; i++)
        cin >> freq[i];

    HuffmanCode(arr, freq, size);
    return 0;
}
