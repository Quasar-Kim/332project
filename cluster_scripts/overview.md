Structure:
```
+-cluster.sh
+-real_data
    +-test.sh
    +-results.mb
+-edge_cases
    +-
    +-
    +-
```

# Deployment scripts
TODO

# Test scripts
## Real data
```real-data/test.sh``` allows to test the program against datasets in ```/dataset``` by running master and workers, then validating the results.
Requires the dataset name ("small", "big", "large") and the number of worker machines. Optionally, ports for master, the 1st worker, and the replicator can be specified (defaults are 5000, 6001, and 7000 respectively).
Usage:
```bash
real-data/test.sh <dataset> master <# of workers> <port for master>
real-data/test.sh <dataset> workers <# of workers> <port for master> <port for 1st worker> <port for replicator>
real-data/test.sh <dataset> validate <# of workers>
```
Examples:
```bash
real-data/test.sh small master 2 5050
real-data/test.sh small workers 2 5050 6161 7070
real-data/test.sh small validate 2
```
Results of correctness tests are stored in ```real-data/results.md```.

## Edge cases
TODO

# Initialization and code updates
```cluster.sh``` allows to initialize worker machines and update code. "Initializing" refers to cloning the repository and copying valsort to the worker, and "updating" refers to pulling, compiling, and assembling code.
Usage:
```bash
./cluster.sh update <# workers>
./cluster.sh update_specific <idx of worker>
./cluster.sh init_specific <idx of workers>
```
Examples:
```bash
./cluster.sh update 3           # will update master, vm01, vm02, and vm03
./cluster.sh update_specific 4  # will update vm04
./cluster.sh init_specific 5    # will initialize and update vm05
```