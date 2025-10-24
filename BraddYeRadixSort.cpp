#include <iostream>
#include <vector>
#include <algorithm>
#include <queue>

using namespace std;
void radixSort(vector<vector<int>>& arr) {
     int numbers = 10;  
     int base = 4;    
    for (int digit = numbers - 1; digit >= 0; digit--) {
        vector<queue<vector<int>>> bucket(base);
        for (const auto& vec : arr) {
            int num = vec[digit];  
            bucket[num].push(vec);  
        }
        arr.clear();
        for (int i = 0; i < base; i++) {
            while (!bucket[i].empty()) {
                arr.push_back(bucket[i].front());
                bucket[i].pop();
            }
        }
    }
}
int main() {
    int n;
    cin >> n; 
    vector<vector<int>> vectors(n, vector<int>(10));  
    // Read input vectors
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < 10; j++) {
            cin >> vectors[i][j]; 
        }
    }
    radixSort(vectors);
    for (const auto& vec : vectors) {
        for (int i = 0; i < 10; i++) {
            cout << vec[i];
            if (i < 9) cout << ";";  
        }
        cout << ";" << endl; 
    }

    return 0;
}
