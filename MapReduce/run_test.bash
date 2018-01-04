#!/usr/bin/bash




for f in filelist_*.txt; do
   qty=`cat $f | while read file;do   du "$file"; done | awk '{i+=$1} END {print i}'`
   t=`time java -Xmx4g -cp ./out/production/MapReduce/ org.isep.Main $f > out.txt`
	echo "$qty $t"
done


#for f in filelist_*.txt; do
 #  qty=`cat $f | while read file;do   du "$file"; done | awk '{i+=$1} END {print i}'`
  # t=`time java -Xmx4g -cp ./out/production/MapReduce/ org.isep.Sequential $f > out.txt`
  #      echo "$qty $t"
#done
