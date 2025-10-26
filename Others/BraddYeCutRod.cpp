#include <iostream>
#include <vector>
#include <limits.h>
using namespace std;
int CutRod(int p[], int size, vector<int>& cuts) {
    vector<int> t(size + 1, 0);  // Array to store the maximum revenue for each rod length
    vector<int> Cut(size + 1, 0); // Array to track the best cuts for each length
    for (int x = 1; x <= size; x++) {
        int profit = INT_MIN;
        for (int y = 0; y < x; y++) {
            if (profit < p[y] + t[x - y - 1]) {
                profit = p[y] + t[x - y - 1];
                Cut[x] = y + 1; // Store the piece length for maximum revenue
            }
        }
        t[x] = profit;
    }
    // Generate the cuts from the bestCut array
    int n = size;
    while (n > 0) {
        cuts.push_back(Cut[n]);
        n -= Cut[n];
    }
    return t[size];
}
int main() {
    int a;
    cin >> a;
    int p[a];
    for (int i = 0; i < a; i++) {
        cin >> p[i];
    }
    vector<int> cuts; 
    int profit = CutRod(p, a, cuts);
    // Output the maximum revenue
    cout << profit << endl;
    // Output the cuts that achieve the maximum revenue
    for (int i = 0; i < cuts.size(); i++) {
        cout << cuts[i];
        if (i < cuts.size() - 1) {
            cout << " ";
        }
    }
    cout << " -1" << endl;
    return 0;
}