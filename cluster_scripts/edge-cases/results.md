# Empty
## Directory
Empty input directory: directory exists but contains no files.
*Wanted behaviour*: simply producing no output; succeeding rather than crashing.
**Fail**: 1 machine, 1 (or 3) empty directory (```./empty.sh dir 1 1``` and ```./empty.sh dir 1 3```). Results in ```StatusRuntimeException``` and the output directory on worker contains the temporary directory ```redsort-working``` (that directory is empty, and nothing else is in the output directory). From master:
```
10.1.25.21:5050
2.2.2.101
io.grpc.StatusRuntimeException: INTERNAL: input size must be 1
        at io.grpc.Status.asRuntimeException(Status.java:533)
        at fs2.grpc.client.internal.Fs2UnaryCallHandler$$anon$1.$anonfun$onClose$4(Fs2UnaryCallHandler.scala:111)
        at in @ fs2.grpc.client.internal.Fs2UnaryCallHandler$ReceiveState$.init(Fs2UnaryCallHandler.scala:44)
        at map @ fs2.grpc.client.internal.Fs2UnaryCallHandler$.$anonfun$unary$1(Fs2UnaryCallHandler.scala:126)
        at async @ fs2.grpc.client.internal.Fs2UnaryCallHandler$.unary(Fs2UnaryCallHandler.scala:125)
        at map @ fs2.grpc.client.Fs2ClientCall.unaryToUnaryCall(Fs2ClientCall.scala:64)
        at delay @ fs2.grpc.client.Fs2ClientCall$PartiallyAppliedClientCall$.apply$extension(Fs2ClientCall.scala:132)
        at flatMap @ redsort.jobs.messages.WorkerFs2Grpc$$anon$1.$anonfun$runJob$1(WorkerFs2Grpc.scala:26)
        at flatMap @ redsort.jobs.messages.WorkerFs2Grpc$$anon$1.runJob(WorkerFs2Grpc.scala:25)
        at handleErrorWith @ redsort.jobs.scheduler.WorkerRpcClientFiber$.runJobWithRetry(WorkerRpcClientFiber.scala:100)
        at map @ redsort.jobs.RPChelper$.$anonfun$handleRpcErrorWithRetry$3(RPChelper.scala:20)
        at flatMap @ redsort.jobs.RPChelper$.$anonfun$handleRpcErrorWithRetry$2(RPChelper.scala:19)
```

## File
Empty input file: file exists but contains no records.
*Wanted behaviour*: producing no partitions (or empty one(s)?)
**Success (?)**: 1 machine, 1 directory with 1 empty file. Results in no partition file (but also no crash).

## Mix
Mix of empty/non-empty direcotries and files.
*Wanted behaviour*: ignore empty directories/files and correctly sort the data from the non-empty files.
**Successs**: 1 machine with the following input directories:
```
+-dir0
  +-file0     <-- empty
  +-file1     <-- 1000 records
+-dir1        <-- empty
```
Results in a single partition file with 1000 records (corectly sorted).
(tried giving both the empty and non-empty directory first -- result is the same)

# File size and number of files
TODO
    - Tiny input file: very small number of records (e.g., 10)
    - Data heavily split into small files: e.g., instead of 2 files with 320_000 records each, it is 20000 files with 32 records each
    - Giant input file: all input data is in 1 file
    - Multiple giant input files: few files, but all are large

# Partition ranges
TODO
    - Partition from the start: the input boundaries on different workers are disjoint from the start, e.g., ```vm01``` has the largest key ```K```, and the smallest key in the input on ```vm02``` is larger than ```K```
    - Records on partition boundary: dealing with keys that are exactly on the cusp of the partition ranges

# Skeweness and distribution
TODO
    - Skewed towards the start: many "AAA..."
    - Skewed towads the end: many "ZZZ..."
    - Uneven distribution: large percentage share a prefix

# Duplicates
TODO
    - All identical keys: all the same in some or all files
    - All identical files: identical input sets on each machine

TODO ascii vs binary input