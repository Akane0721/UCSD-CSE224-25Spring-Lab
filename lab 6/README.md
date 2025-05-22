[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/vemBksKM)
# GlobeSort: Distributed Sort

## Building

```bash
go build -o bin/globesort cmd/globesort/main.go
```

## Usage

```bash
bin/globesort <nodeID> <inputFilePath> <outputFilePath> <configFilePath>
```

Example:

```bash
bin/globesort 0 input.dat sorted.dat config.yaml
```

Multi-node Example:

```bash
nohup bin/globesort 0 input_0.dat sorted_0.dat config.yaml &
nohup bin/globesort 1 input_1.dat sorted_1.dat config.yaml &
nohup bin/globesort 2 input_2.dat sorted_2.dat config.yaml &
nohup bin/globesort 3 input_3.dat sorted_3.dat config.yaml &
wait  # block until all nohup processes are finished
```

nohup will redirect all output to `nohup.out` file.

```bash
cat nohup.out
# or, to see output as it is generated
tail -f nohup.out
```

## Testing

You can use the same tools in Lab 1 to test your sort:

```bash
cp utils/<os-arch>/bin . -r  # Replace <os-arch> with your OS and architecture

bin/gensort "1 mb" 1mb.dat
bin/showsort 1mb.dat | sort > 1mb_sorted.txt
```

For multi-node results, you can concatenate the sorted files on each node to a single file:

```bash
# start by generating your input files
bin/gensort "1 mb" input_0.dat
bin/gensort "1 mb" input_1.dat
bin/gensort "1 mb" input_2.dat
bin/gensort "1 mb" input_3.dat

# sort the input files
nohup bin/globesort 0 input_0.dat sorted_0.dat config.yaml &
nohup bin/globesort 1 input_1.dat sorted_1.dat config.yaml &
nohup bin/globesort 2 input_2.dat sorted_2.dat config.yaml &
nohup bin/globesort 3 input_3.dat sorted_3.dat config.yaml &
wait  # block until all nohup processes are finished

# generate reference text file
cat input_*.dat > input_all.dat
bin/showsort input_all.dat | sort > ref.txt

# show your multinode results
cat sorted_*.dat > sorted_all.dat
bin/showsort sorted_all.dat | sort > sorted_all.txt

# check if they are different
diff sorted_all.txt ref.txt
```
