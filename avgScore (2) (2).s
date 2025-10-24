.data 

orig: .space 100	# In terms of bytes (25 elements * 4 bytes each)
sorted: .space 100

str0: .asciiz "Enter the number of assignments (between 1 and 25): "
str1: .asciiz "Enter score: "
str2: .asciiz "Original scores: "
str3: .asciiz "Sorted scores (in descending order): "
str4: .asciiz "Enter the number of (lowest) scores to drop: "
str5: .asciiz "Average (rounded down) with dropped scores removed: "


.text 

# This is the main program.
# It first asks user to enter the number of assignments.
# It then asks user to input the scores, one at a time.
# It then calls selSort to perform selection sort.
# It then calls printArray twice to print out contents of the original and sorted scores.
# It then asks user to enter the number of (lowest) scores to drop.
# It then calls calcSum on the sorted array with the adjusted length (to account for dropped scores).
# It then prints out average score with the specified number of (lowest) scores dropped from the calculation.
main: 
	addi $sp, $sp -4
	sw $ra, 0($sp)
	li $v0, 4 
	la $a0, str0 
	syscall 
	li $v0, 5	# Read the number of scores from user
	syscall
	move $s0, $v0	# $s0 = numScores
	move $t0, $0
	la $s1, orig	# $s1 = orig
	la $s2, sorted	# $s2 = sorted
loop_in:
	li $v0, 4 
	la $a0, str1 
	syscall 
	sll $t1, $t0, 2
	add $t1, $t1, $s1
	li $v0, 5	# Read elements from user
	syscall
	sw $v0, 0($t1)
	addi $t0, $t0, 1
	bne $t0, $s0, loop_in
	
	move $a0, $s0
	jal selSort	# Call selSort to perform selection sort in original array
	
	li $v0, 4 
	la $a0, str2 
	syscall
	move $a0, $s1	# More efficient than la $a0, orig
	move $a1, $s0
	jal printArray	# Print original scores
	li $v0, 4 
	la $a0, str3 
	syscall 
	move $a0, $s2	# More efficient than la $a0, sorted
	jal printArray	# Print sorted scores
	
	li $v0, 4 
	la $a0, str4 
	syscall 
	li $v0, 5	# Read the number of (lowest) scores to drop
	syscall
	move $a1, $v0
	sub $a1, $s0, $a1	# numScores - drop
	move $a0, $s2
	jal calcSum	# Call calcSum to RECURSIVELY compute the sum of scores that are not dropped
	
	# Your code here to compute average and print it
mfhi $t0 #Remainder
mflo $t1 #Answer
move $a0, $t1
bgt $t0, 4, roundup
cont:
move $t1, $v0
move $a0, $v0
lw $t0, ($t1) #load array ele into t0
li $v0, 1 
move $a0, $t1 	
	lw $ra, 0($sp)
	addi $sp, $sp 4
	li $v0, 10 
	syscall
	
	
# printList takes in an array and its size as arguments. 
# It prints all the elements in one line with a newline at the end.
printArray:
	# Your implementation of printList here	
move $t0, $a1        # Set counter to array size
la $t1, ($a0)        # Base address of array

printLoop:
beqz $t0, printEnd   # If counter == 0, exit loop
lw $t2, 0($t1)       # Load array element
li $v0, 1
move $a0, $t2        # Print integer
syscall

li $v0, 11
li $a0, ' '          # Print space
syscall

addi $t1, $t1, 4     # Move to next element
addi $t0, $t0, -1    # Decrement counter
j printLoop

printEnd:
li $v0, 11
li $a0, '\n'         # Print newline
syscall
jr $ra	
# selSort takes in the number of scores as argument. 
# It performs SELECTION sort in descending order and populates the sorted array
selSort:
	sw $ra, 4($sp)
sw $s2, 8($sp)
#move $t1, $0
la $t0, ($s1) #OG array
la $t5, ($s2) #COPY
li $t1, 0 #counter
li $t3, 0

loop_start:
lw $t3, ($t0) #load ele of OG array into t3
sw $t3, ($s2) #save t3(OG array ele) into $s2(SORTED array)
addi $t0, $t0, 4 #move index of OG array by four
addi $s2, $s2, 4 #move index of SORTED array by four
addi $t1, $t1, 1 #increase counter by 1
blt $t1, $a0, loop_start #if counter($t1) < size break
Resetting: #Need to reset the SORTED array so that when we print it later it starts at the beginning
add $s2, $t5, $0 #setting SORTED array to the beginnning.
beq $t1, $zero, Resetting

#Setting up for loops
li $t0, 0 # I index
addi $t2, $a0, -1 # size - 1
ILoop: bgt $t0, $t2, End
add $t3, $zero, $t0 # maxindex
addi $t1, $t0, 1 # J index = i + 1
JLoop: bgt $t1, $a0, IEnd

#setting up for greater than compare
sll $t4, $t3, 2
add $s2, $s2, $t4
lw $t5, ($s2) # t5 = SORTED[maxindex]
sub $s2, $s2, $t4 #resetting pos.
sll $t4, $t1, 2 # J * 2 = 4bits
add $s2, $s2, $t4 # SORTED[0] + j
lw $t6, ($s2) # t6 = SORTED[j]
sub $s2, $s2, $t4 #resetting pos.
bgt $t6, $t5, NewMax
addi $t1, $t1, 1
j JLoop

IEnd: # Swapping
sll $t5, $t3, 2 # shifting to get to maxindex location
add $s2, $s2, $t5 # at SORTED[maxindex]
lw $t6, 0($s2) # temp
sub $s2, $s2, $t5 # resetting array back to SORTED[0]
sll $t5, $t0, 2 # shifting to get to i location
add $s2, $s2, $t5 # at SORTED[i]
lw $t7, 0($s2) # t7 = SORTED[i]
sub $s2, $s2, $t5 # resetting array back to SORTED[0]
sll $t5, $t3, 2 #maxindex location
add $s2, $s2, $t5 # SORTED[maxindex]
sw $t7, 0($s2) # SORTED[maxindex] = t7(SORTED[i])
sub $s2, $s2, $t5 # resetting array back to SORTED[0]
sll $t5, $t0, 2 #shifting to SORTED[i]
add $s2, $s2, $t5 # at SORTED[i]
sw $t6, 0($s2) # SORTED[i] = t6(temp)
sub $s2, $s2, $t5 #resetting array back to SORTED[0]
addi $t0, $t0, 1 #increase i
j ILoop

End:
lw $s2, 8($sp)
lw $ra, 4($sp)
addi $sp, $sp 8
jr $ra
NewMax:
add $t3, $t1, $zero # maxindex = j
#addi $t0, $t0, 1
addi $t1, $t1, 1
j JLoop
jr $ra

# calcSum takes in an array and its size as arguments.
# It RECURSIVELY computes and returns the sum of elements in the array.
# Note: you MUST NOT use iterative approach in this function.
calcSum:
    beq $a1, $zero, calcSum_base
    addi $sp, $sp, -8       # Allocate stack space
    sw $ra, 4($sp)          # Save return address
    sw $a1, 0($sp)          # Save size
    
    
    lw $t0, 0($a0)          
    addi $a0, $a0, 4        
    addi $a1, $a1, -1       
    
    jal calcSum             
    add $v0, $v0, $t0       
    lw $a1, 0($sp)
    lw $ra, 4($sp)
    addi $sp, $sp, 8
    jr $ra
calcSum_base:
    li $v0, 0              
    jr $ra
	jr $ra
	
