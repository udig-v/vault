#!/bin/bash

num_threads=2
#torus
max_num_threads=16 #6 runs
#pi
#max_num_threads=4 #3 runs
memory=$((1024 * 1024 * 16)) #32MB
max_memory=$((1024 * 1024 * 512))  # 1GB #6 runs
k=25
echo "threads,memory,hash_time,sort_time,sync_time">> "vault-k${k}.txt"

for (( t=num_threads; t<=$max_num_threads; t=$((t * 2)) ))
do
    for (( m=memory; m<=$max_memory; m=$((m * 2)) ))
    do
        # Remove the output file
        #rm ../../output/output.bin
        rm vault.memo
        echo "$k $t $m"
  
        # Run the cargo command with the current value of k and pipe the output to a file
        echo -n "$t,$m," >> "vault-k${k}.txt"
        #cargo run --release -- -k "$k" -t "$t" -m "$m" >> "vault-76-k${k}.txt"
		./vault -w true -t "$t" -o "$t" -i "$t" -m "$m" -k "$k" -f vault.memo -x true -z true >> "vault-k${k}.txt"
    done
done
