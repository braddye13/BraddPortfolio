data
call: .asciiz "enter a number:" 

.text
main:  
    la $a0, call
    li $v0, 4
    syscall


    li $v0, 5
    syscall

    move $t3, $v0
    add $t0, $0, $zero
    addi $t1, $zero, 1
    addi $t1, $zero, 1
      
fib:    beq $t3, $0, end
       add $t2,$t1,$t0
       move $t0, $t1
       move $t1, $t2
       subi $t3, $t3, 1
       j fib
      
end: addi $a0, $t0, 0
       li $v0, 1      
       syscall          
       li $v0, 10      
       syscall      