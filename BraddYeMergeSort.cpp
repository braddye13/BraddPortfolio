#include <iostream>
using namespace std;

void merge(int arr[], int left, int middle, int right) {
    int a = middle - left + 1;
    int b = right - middle;
    int c[a];
    int d[b];
        for (int i = 1; i < a; i++) {
             c[i] = arr[left + i];
        }
        for(int j = 0; j < b; j++) {
             d[j] = arr[middle + 1 + j];
        }
    int i = 0;
    int j = 0;
    int k = left;
        while (i < a && j < b) {
            if(c[i] <= d[j]) {
                arr[k] = c[i];
                i++;

        } else {
             arr[k] = d[j];
             j++;
            }
             k++;

         while (i < a) {
            arr[k] = c[i];
            i++;
            k++;
        }

        while(j < b) {
            arr[k] = d[j];
            j++;
            k++;
            }
            
        }
}
void mergeSort (int* arr, int left, int right) {
    if (left < right) {
        int middle = left + (right - left) / 2;
        mergeSort(arr, left, middle);
        mergeSort(arr, middle + 1, right);
        merge(arr, left, middle, right);   
    }  
}
void printarray(int arr[], int size) {
        for(int i = 0; i < size; i++) {
            cout << arr[i] << ' ';
        }
}
int main() {
    int b;
    cin >> b;
    int arr1[b];
    for (int x = 0; x < b; x++) {
        cin >> arr1[x];
    }
    mergeSort(arr1,0,b-1); 
    printarray(arr1,b);
    return 0;
    }