#!/usr/bin/sh

for a in *.ps;
do
    b=`echo $a | sed -e s/.ps/.png/`
    convert -density 300 -geometry 100% $a $b
done
