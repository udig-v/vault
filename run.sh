#!/bin/bash

RAM_SIZE=1 ; echo ${RAM_SIZE} ; rm vault.memo ; ./vault -f vault.memo -m ${RAM_SIZE} -s 64 -t 16 >> vault-${RAM_SIZE}-64-16.txt
RAM_SIZE=2 ; echo ${RAM_SIZE} ; rm vault.memo ; ./vault -f vault.memo -m ${RAM_SIZE} -s 64 -t 16 >> vault-${RAM_SIZE}-64-16.txt
RAM_SIZE=4 ; echo ${RAM_SIZE} ; rm vault.memo ; ./vault -f vault.memo -m ${RAM_SIZE} -s 64 -t 16 >> vault-${RAM_SIZE}-64-16.txt
RAM_SIZE=8 ; echo ${RAM_SIZE} ; rm vault.memo ; ./vault -f vault.memo -m ${RAM_SIZE} -s 64 -t 16 >> vault-${RAM_SIZE}-64-16.txt
RAM_SIZE=16 ; echo ${RAM_SIZE} ; rm vault.memo ; ./vault -f vault.memo -m ${RAM_SIZE} -s 64 -t 16 >> vault-${RAM_SIZE}-64-16.txt
RAM_SIZE=24 ; echo ${RAM_SIZE} ; rm vault.memo ; ./vault -f vault.memo -m ${RAM_SIZE} -s 64 -t 16 >> vault-${RAM_SIZE}-64-16.txt

