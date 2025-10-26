#include <iostream>
#include <cstdlib>
#include <time.h>
using namespace std;

void swap (int* x, int* y) { // swaps the value of x and y 
    int z = *x;
    *x = *y;
    *y= z;
}

int partition(int arr[],int x,int y) // Partitions the array around a pivot element that is chosen randomly.

{
    int pivotIndex = rand() % (y - x + 1) + x; // random location
    swap(&arr[pivotIndex], &arr[y]);
    int pivot = arr[y];
    int i = x - 1;

        for (int j = x; j < y; j++) {
            if (arr[j] < pivot) {
            i++;
        swap(&arr[i], &arr[j]); // swap elements I and J 
    }
}
        swap (&arr[i + 1],&arr[y]);
        return (i + 1);
}

void quickSort(int arr[], int x, int y) // Quick sort function allows the array to be sorted recurrsively. 
{
    if (x < y)
{
    int pivot = partition(arr, x, y);
    quickSort(arr, x, pivot - 1);
    quickSort(arr, pivot + 1, y);
    }
}
int main(){
    srand(time(0)); // srand call initializes a seed for random number generation 
    int arr[100]; // Takes an integer x from the user, representing the number of elements. Reads input into arr[a]
    int x;
    cout << "number of elements: ";
    cin >> x;
        for (int a = 0; a < x; a++)
            cin>>arr[a];
            quickSort(arr, 0, x - 1); // prints sorted elements
                for (int a = 0 ; a < x; a++)
                cout << arr[a] << " ; ";
return 0;
}