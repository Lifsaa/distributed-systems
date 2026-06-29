#!/bin/bash
# splits a file into N line-aligned chunks named split-0 ... split-N-1
# usage: ./split.sh data.txt 9

FILE=$1
N=$2
LINES=$(wc -l < "$FILE")
CHUNK=$(( (LINES + N - 1) / N ))

for ((i=0; i<N; i++)); do
    START=$(( i * CHUNK + 1 ))
    END=$(( START + CHUNK - 1 ))
    sed -n "${START},${END}p" "$FILE" > "split-${i}"
done

echo "created $N splits from $FILE"
