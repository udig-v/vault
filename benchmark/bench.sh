#!/bin/bash 

k="$1"
threads="$2"
memory="$3"
file_name="vault.memo"

echo "Gen&Flush,Sort" >"vault_csv/vault_torus_$k"_"$threads"t"_$memory"m".csv"

# for n in {1..5}; do  
# Remove the output file
rm -f "$file_name"
rm -f "$file_name.config"

# Clean cache
free >/dev/null && sync >/dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free >/dev/null
sudo sync
sleep 1

# Run the program
./../vault -t $threads -o $threads -i $threads -f $file_name -m $memory -k $k >>"vault_csv/vault_torus_$k"_"$threads"t"_$memory"m".csv"
# done

# echo $(du -hs $file_name)
