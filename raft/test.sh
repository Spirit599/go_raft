#!/bin/bash
cnt=0
fatal=0
num=1000

for i in `seq $num`
do
    
    go test -run 2B > tmp
    echo `tail -n 2 tmp`
    echo `tail -n 2 tmp` > res
    ok=`grep -rn ok res`
    interrupt=`grep -rn interrupt res`

    if [[ ! -z "$ok" ]];then
        ((cnt++))
    else
        if [[ -z "$interrupt" ]];then
        ((fatal++))
        mv tmp "./fatal/fatal_log$fatal"
        fi
    fi
    echo "$cnt success in $i"
    # sleep 1
done

echo $cnt " success in " $num
