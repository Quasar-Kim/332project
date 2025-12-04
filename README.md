# Initializing and updating project
```cluster_scripts/cluster.sh``` allows to initialize worker machines and update code. "Initializing" refers to cloning the repository and copying valsort to the worker, and "updating" refers to pulling, compiling, and assembling code.
Usage:
```bash
cluster_scripts/cluster.sh update <# workers>
cluster_scripts/cluster.sh update_specific <idx of worker>
cluster_scripts/cluster.sh init_specific <idx of workers>
```
Examples:
```bash
cluster_scripts/cluster.sh update 3           # will update master, vm01, vm02, and vm03
cluster_scripts/cluster.sh update_specific 4  # will update vm04
cluster_scripts/cluster.sh init_specific 5    # will initialize and update vm05
```

# Deployment scripts
These scripts allow for manual (or semi-manual) testing. Please ensure the needed machines have been initialized and updated prior to deployment. Available commands:
1) Start master (from master): ```cluster_scripts/master.sh <num_workers> [-p <port>]```
2) Start multiple worker machines with identical arguments (from master): ```cluster_scripts/n-workers.sh <num_workers> <IP>:<m_port> -I <inputs...> -O <output> [-p <w_port>] [-r <r_port>]```
3) Start worker (from worker machine): ```cluster_scripts/worker.sh <IP>:<m_port> -I <inputs...> -O <output> [-p <w_port>] [-r <r_port>]```

Run ```cluster_scripts/<script name>.sh``` for udage details and examples.

# Test scripts
These scripts provide a straightforward way to test the project on the real datasets (```/dataset/small, /dataset/big, /dataset/large```), as well as other configurations (edge cases). Before testing, ensure the needed machines have been initialized and updated.

## Real data
```cluster_scripts/real-data/test.sh``` allows to test the program against datasets in ```/dataset``` by running master and workers, then validating the results. Validation is done by creating ```partition.i.sum``` for each partition, copying it to master, concatenating them in order, and validating the result with ```valsort```. Additionally, the validation outputs the number of records that is expected (based on the number of records in the input), whcich can be compared to the number of records output by ```valsort```.
Requires the dataset name ("small", "big", "large") and the number of worker machines. Optionally, ports for master, the 1st worker thread, and the replicator can be specified (defaults are 5000, 6001, and 7000 respectively; note that to specify a port, all prior optional ports also need to be specified).
Usage:
```bash
cluster_scripts/real-data/test.sh <dataset> master <# of workers> <opt: port for master>
cluster_scripts/real-data/test.sh <dataset> workers <# of workers> <opt: port for master> <opt: port for 1st worker> <opt: port for replicator>
cluster_scripts/real-data/test.sh <dataset> validate <# of workers>
```
Examples:
```bash
cluster_scripts/real-data/test.sh small master 2 5050               # starts master for 2 worker machines on port 5050
cluster_scripts/real-data/test.sh small workers 2 5050 6161 7070    # starts vm01, vm02 for master on port 5050; 1st worker thread takes uses 6161, replicator uses port 7070
cluster_scripts/real-data/test.sh small validate 2                  # creates overall summary of partitions on vm01 and vm02, validates it, and shows the expected total number of records
```
Results of some correctness tests are stored in ```real-data/results.md```.

## Edge cases
The following scripts generate certain data based on the edge case and given parameters, then they output commands that will run the tests (from within the ```cluster_scripts/edge-cases``` directory):
```
cluster_scripts/edge-cases/empty.sh         <-- what if some or all input directories/files are empty?
cluster_scripts/edge-cases/size-count.sh    <-- what if input consists of many very small files? what about few large files?
cluster_scripts/edge-cases/binary.sh        <-- what if records are binary, not ascii?
cluster_scripts/edge-cases/duplicates.sh    <-- what if input sets on machines are identical? what if input files are identical?
cluster_scripts/edge-cases/skew.sh          <-- what if input data is skewed?
```
Run ```cluster_scripts/edge-cases/<needed case>``` for usage specifics. After generating the data, use ```cluster_scripts/edge-cases/test.sh``` to run the test (follow the instructions in the output of the data-generating script).

Example output (from ```cluster_scripts/edge-cases```, i.e., ```/home/red/332project/cluster_scripts/edge-cases```):
```bash
$ ./empty.sh dir 1 1
Execute the test with the following commands.

Master (optionally add port at the end):
    ./test.sh master 1

Workers (optionally add ports for master, 1st worker, and replicator at the end):
    ./test.sh workers 1 "/home/red/edge_case_input/empty/dir/1-machines/1-dir/dir0 /home/red/edge_case_input/empty/dir/1-machines/1-dir/dir1" "/home/red/edge_case_output/empty/dir/1-machines/1-dir"

Validation:
    ./test.sh validate 1 "/home/red/edge_case_output/empty/dir/1-machines/1-dir" "/home/red/edge_case_summaries/empty/dir/1-machines/1-dir" 0
```