#include <iostream>
#include <vector>
#include <limits>
#include <string>
using namespace std;

void printOptimalParens(vector<vector<int>>& split, int i, int j) {
    if (i == j) {
        cout << "A" << i; 
    } else {
        cout << "(";
        printOptimalParens(split, i, split[i][j]);
        printOptimalParens(split, split[i][j] + 1, j);
        cout << ")";
    }
}

int main() {
    int n;
    cin >> n; 
    vector<int> length(n + 1); 

    for (int i = 0; i <= n; i++) {
        cin >> length[i];
    }
    vector<vector<int>> table(n, vector<int>(n, numeric_limits<int>::max()));
    vector<vector<int>> split(n, vector<int>(n, 0));
    for (int i = 0; i < n; i++) {
        table[i][i] = 0;
    }
    for (int len = 2; len <= n; len++) {
        for (int i = 0; i <= n - len; i++) {
            int j = i + len - 1;
            for (int k = i; k < j; k++) {
                int cost = length[i] * length[k + 1] * length[j + 1];
                int q = table[i][k] + table[k + 1][j] + cost;
                if (q < table[i][j]) {
                    table[i][j] = q;
                    split[i][j] = k;
                }
            }
        }
    }
    cout << table[0][n - 1] << endl;
    printOptimalParens(split, 0, n - 1);
    cout << endl;

    return 0;
}

