#!/bin/bash

num_threads=2
#torus
max_num_threads=32 #6 runs
#pi
#max_num_threads=4 #3 runs
memory=1        #1MB
max_memory=1024 # 1GB #11 runs
k=25
echo "threads,memory,hash_time,sort_time" >"vault_csv/vault_k${k}.csv"

for ((t = num_threads; t <= $max_num_threads; t = t * 2)); do
    for ((m = memory; m <= $max_memory; m = m * 2)); do
        echo "$k $t $m"
        
        for n in {1..5}; do
            # Remove the output file
            rm vault.memo
            rm vault.memo.config

            free >/dev/null && sync >/dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free >/dev/null
            sudo sync
            sleep 1

            # Run the cargo command with the current value of k and pipe the output to a file
            # echo -n "$t,$m," >>"vault_csv/vault-k${k}.csv"
            output=$(./../vault -t $t -o $t -i $t -f "vault.memo" -m $m -k $k)

            # Append the output to the CSV file
            if [ -z "$output" ]; then
                echo "$t,$m," >>"vault_csv/vault_k${k}.csv"
            else
                echo "$t,$m,$output" >>"vault_csv/vault_k${k}.csv"
            fi
        done
    done
done
