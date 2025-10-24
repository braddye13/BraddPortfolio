#include <iostream>
#include <cstdlib>
#include <time.h>
using namespace std;

void swap (int* x, int* y) {
    int z = *x;
    *x = *y;
    *y= z;
}
int partition(int arr[],int x,int y)
{
    srand(time(0));
    int pivot = rand()%(y-x);
    pivot += x;
    swap(arr[x],arr[pivot]);
    pivot = y;
    int temp = arr[x];
    int i = x-1;
        for (int j = x; j < y; j++) {
            if (arr[j] < temp) {
            i++;
        swap (arr[i],arr[j]);
    }
}
swap (arr[x],arr[i]);
return (i);
}
void quickSort(int arr[], int x, int y)
{
    if (x < y)
{
    int pivot = partition(arr, x, y);
    quickSort(arr, x, pivot - 1);
    quickSort(arr, pivot + 1, y);
    }
}
int main(){
    int arr[100];
    int x;
    cin >> x;
    int a;
        for (a=0; a < x; a++)
            cin>>arr[x];
            quickSort(arr, 0, x-1);
                for (a=0; a < x; a++)
                cout<<arr[x]<<";";
return 0;
}