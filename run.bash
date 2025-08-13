echo -n "Enter testcase number: "
read t

gcc scheduler.c -o scheduler.out -Wall -Wextra

xterm -hold -e "./validation.out $t" &
sleep 0.1
xterm -hold -e "time ./scheduler.out $t" &
wait