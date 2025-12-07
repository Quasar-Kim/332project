# Redsort

Redsort is an implementation of fault-tolerant distributed sorting program for 2025 CSED332 fall/winter semester.

## Usage

Download `master.jar` and `worker.jar` from [releases](https://github.com/Quasar-Kim/332project/releases) or [build them yourself](#building).

Run `master.jar` with number of worker machines on master machine. Following command will start `master` with two worker machines and wait for all workers to register. By default server starts at port 5000.

```
java -jar master.jar 2
```

Run `worker.jar` on worker machines with input directories, output directory, and network address of master machine. Following command will start `worker` input directories `/data1/input` and `/data2/input`, output directory set to `/home/red/data`, and will try registration to master machine at `141.223.91.80:5000`. By default remote replicator service starts at port 6000, 4 workers at port from 6001 to 6004, and local replicator server at port 7000.

```
java -jar worker.jar 141.223.91.80:5000 -I /data1/input /data2/input -O /home/red/data
```

Spawn order does not matter. Once a scheduler and all workers are ready sorting will start automatically on all records found in all input directories. Sorted results are written to output directories with name `partition.<n>`.

## Building

Run `assembly` job to build .jar files:

```
sbt assembly
```

This will build `master.jar` under `master/target/scala-2.XX` and `worker.jar` under `worker/target/scala-2.XX` respectively.

## Hacking

This project consists of three subprojects.

- `redsort.jobs`: fault-tolerant job runner infrastructure library. Provides `Scheduler` that dispatches jobs to workers, and `Worker` that runs registered jobs when requested by `Scheduler`. Lives in `jobs` directory.
- `redsort.master`: master binary that orchestrates workers to perform distributed sorting. Implemented on top of `Scheduler` providied by `redsort.jobs`. Lives in `master` directory.
- `redsort.worker`: worker binary that does the actual distributed sorting. Implemented on top of `Worker` provided by `redsort.jobs`. Lives in `worker` directory.

Some useful resources:

- See [TESTING.md](./TESTING.md) for usage of automated testing script on actual cluster environment.
- See `doc/` directories for design documents.
- See `cluster_scripts/` directories for test and benchmark results on actual cluster.