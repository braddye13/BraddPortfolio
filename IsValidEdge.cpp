#include <iostream>
#include <vector>
#include <climits>
#define MAX 1000
using namespace std;

int isValidEdge(int u, int v, vector<int>& visited) {
    if (u == v) return 0;
    if (visited[u] == 0 && visited[v] == 0) return 0;
    if (visited[u] == 1 && visited[v] == 1) return 0;
    return 1;
}
void prims(int g[MAX][MAX], int v) {
    vector<int> visited(v, 0);
    vector<int> parent(v, -1); 
    visited[0] = 1;
    int edge_count = 0, mincost = 0;
    while (edge_count < v - 1) {
        int min = INT_MAX, a = -1, b = -1;
        for (int i = 0; i < v; i++) {
            for (int j = 0; j < v; j++) {
                if (g[i][j] < min && isValidEdge(i, j, visited)) {
                    min = g[i][j];
                    a = i;
                    b = j;
                }
            }
        }
        if (a != -1 && b != -1) {
            printf("Edge %d:(%d, %d) cost: %d\n", edge_count++, a, b, min);
            mincost += min;
            visited[b] = 1;
            parent[b] = a; 
        }
    }
    printf("\nMinimum cost = %d\n", mincost);  
    for (int i = 0; i < v; i++) {
        if (i == 0)
            cout << "Root: NIL" << endl; 
        else
            cout << i << " <- " << parent[i] << endl;
    }
}
int main() {
    int v, e;
    cin >> v >> e;
    int g[MAX][MAX];
    for (int i = 0; i < v; i++) {
        for (int j = 0; j < v; j++) {
            g[i][j] = INT_MAX;
        }
    }
    int ip1, ip2, ip3;
    for (int m = 0; m < e; m++) {
        cin >> ip1 >> ip2 >> ip3;
        g[ip1][ip2] = ip3;
        g[ip2][ip1] = ip3;
    }
    prims(g, v);
    return 0;
}
