#!/bin/bash 

num_threads="$1"
memory_limit="$2"
k_num="$3"
file_name="vault.memo"

rm -f "$file_name"

free >/dev/null && sync >/dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free >/dev/null
sleep 3

./vault -t $num_threads -o $num_threads -i $num_threads -f $file_name -m $memory_limit -k $k_num

echo $(du -hs $file_name)
