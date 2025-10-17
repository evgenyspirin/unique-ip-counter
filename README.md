#  Unique IPv4 Counter

### Introduction.
Dear reviewers team,

To implement the [`task`](https://github.com/Ecwid/new-job/blob/master/IP-Addr-Counter-GO.md),  I tried to pay maximum attention to memory economy and process time.   
The main and most important aspect, as in any algorithm, is to choose a correct data structure, so I chose a "bitset"    
since it is the most efficient approach in GO for storing all possible IPv4 addresses within maximum 512MB.  
"**16-sharded bitset(lazy)**".  
- "**/16**" means: we take the upper 16 bits of the address (the first two octets a.b) as the range key.
- "**sharded**": we divide the entire IPv4 space into 65,536 shards based on these 16 bits.  
Each shard covers 2^16 addresses (the last two octets c.d) and is stored in a local bitset of 65,536 bits = 8 KB.
- "**lazy**" - dynamic allocate new memory on demand.

Why not "**flat bitset**":
- First, faster start because we don't spend time allocating 512 MB like in the case of a flat.
- Memory is allocated dynamically instead of consuming 512 MB upfront.
- Scalability. The load is distributed across different buckets.  
In the case of a flat bitset, all threads concurrently use the same array, which causes lock latency and therefore increased contention.  

I also added resource log monitoring so that memory allocation, the number of parallel processes, etc. are clearly visible.  

**My result of processed file(ip_addresses) with 120GB**:
- Hardware: MacBook Pro M1 / Cores 10 (8 performance and 2 efficiency) / 16GB Ram  
- total time: 37.826557 sec / maxAllocation(lazy) = 534.71MB

---

## Overview

The application has a structure based on Go convention:
- https://github.com/golang-standards/project-layout

---

## Tests

Covered the most important core logic.

- Pattern: **TableDrivenTests**
- Library: [`testify`](https://github.com/stretchr/testify)

Run from the root project directory to see code coverage:

```bash
$ go test ./... -coverprofile=coverage.out
$ go tool cover -html=coverage.out
```

---

## Application Initialization Steps

1. Create application
2. Get configuration
3. Init app(logs, pars args etc.)
4. Run "Worker pool"
5. On `SIGURG` signal or context cancel, gracefully shut down the application

---

## Using

The application accepts **two command-line arguments**:

| Flag               | Type    | Required | Description                                                                        |
|--------------------|---------|:--------:|------------------------------------------------------------------------------------|
| `-f=path/to/file`  | string  |   YES    | Path to the input file with data.                                                  |
| `-th=8`            | int     |    NO    | Number of goroutines **and** shards used for parallel processing(default=NumCPU()) |

### Examples

```bash
# build
go build -o ./bin/unique-ip-counter ./cmd/uip_counter

# run (basic)
./bin/unique-ip-counter -f=/path/to/file -th=8
