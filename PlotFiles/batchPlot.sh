#!/usr/bin/sh

for a in *.agr;
do
    b=`echo $a | sed -e s/.agr/.png/`
    xmgrace -nxy $a -hdevice PNG -hardcopy -printfile $b
done
