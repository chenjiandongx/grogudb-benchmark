#!/bin/bash

ops=(PutUnique PutDuplicate PutIf Has Del Range)
storage=(grogudb leveldb badger)

for op in ${ops[*]}; do
  for stor in ${storage[*]}; do
    go run . -storage $stor -operation $op -iter 100000 -buckets 100
  done
done

for stor in ${storage[*]}; do
  go run . -storage $stor -operation Get -iter 10000 -buckets 100
done
