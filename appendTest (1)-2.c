#include <stdio.h>
#include <string.h>

/*
 * Return the result of appending the characters in s2 to s1.
 * Assumption: enough space has been allocated for s1 to store the extra
 * characters.
 */

/* 
 * DO NOT change the definition of the append function when it comes to 
 * adding/removing/modifying the function parameters, or changing its return 
 * type. You may, however, modify the body of the function if you wish.
 */
char* append(char s1[], int s1_size, char s2[]) {
    int s1len = strlen(s1);
    int s2len = strlen(s2);
    int k;
    for (k = 0; k < s2len; k++) {
        if(s1len + k <s1_size - 1) {
        s1[k + s1len] = s2[k];
    } else {
        break;
}
        s1[k + s1len] = '\0';
        return s1;
    }
}
int main() {
    char str1[20]; 
    char str2[20];
    while (1) {
        printf("str1 = ");
        scanf("%[^\n]%*c", str1); 
         if (!gets(str1,sizeof(str1),stdin)) {
         return 0;
         }
         str1[strcspn(str1, '\n')] = '\0';
        printf("str2 = ");
        scanf("%[^\n]%*c", str2);  
        if (!gets(str2)) return 0;

        printf ("The result of appending str2 to str1 is %s.\n", append(str1, str2));
        fflush(stdin);
    }
    return 0;
}
