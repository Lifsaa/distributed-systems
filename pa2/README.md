# PA2 - MapReduce
CPE-469 Distributed Systems

## What it does
Counts word occurrences in a text file using MapReduce. A coordinator
splits the file and hands out tasks to parallel workers over RPC.

## How to run

```bash
go run . -input=data.txt -M=9 -R=3 -W=3
```

- `-input` — input text file
- `-M` — number of map splits
- `-R` — number of output files
- `-W` — number of workers

## Output
Produces `mr-out-0` to `mr-out-R-1`, each with sorted word counts:
```
fox 2
the 3
```

## Cleanup
```bash
make clean
```
