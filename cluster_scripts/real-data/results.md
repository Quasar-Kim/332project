# Correctness & Performance
The results below include the correctness evaluation, the time for each phase, and the total time.

For no-fault tests, the program was started, and no machines were interrupted during execution.

Fault was imitated as follows:
- master is started (in verbose manner to investigate faults at specific stages)
- (N-1) workers are started with the chosen dataset
-  the N-th worker is started
- after a certain period of time (this time is adjust to stopduring a specific stage), the worker is stopped
- after another period of time (10 seconds in the tests below), that same worker is restarted

Afterward, the results were validated with ```valsort```, and the numbers of records in the input and output were compared to ensure correctness.
## Small dataset
| Machines | Data size | Fault stage | Sampling | Sorting | Partitioning | Merging | Total time | Valsort | Record count |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 64 MB | no fault | 1.479 s | 4.272 s | 0.207 s | 8.488 s | 14.446 s | SUCCESS | match |
| 2 | 128 MB | no fault | 1.601 s | 4.958 s | 0.781 s | 15.558 s | 22.898 s | SUCCESS | match |
| 3 | 192 MB | no fault | 2.632 s | 5.039 s | 0.868 s | 13.921 s | 22.460 s | SUCCESS | match |
| 3 | 192 MB | sampling | **15.635** s | 5.118 s | 0.713 s | 12.338 s | 33.804 s | SUCCESS | match |
| 3 | 192 MB | sorting | 2.632 s | **21.934** s | 0.940 s | 14.249 s | 39.755 s | SUCCESS | match |
| 3 | 192 MB | merging | 2.649 s | 5.127 s | 0.763 s | **14.907** s | 23.446 s | SUCCESS | match |
| 4 | 256 MB | no fault | 0.718 s | 5.515 s | 0.915 s | 15.390 s | 22.538 s | SUCCESS | match |
| 5 | 320 MB | no fault | 2.666 s | 5.556 s | 1.278 s | 21.038 s | 30.538 s | SUCCESS | match |
| 5 | 320 MB | sampling | **16.703** s | 5.831 s | 1.030 s | 16.727 s | 40.291 s | SUCCESS | match |
| 5 | 320 MB | sorting | 2.724 s | **16.167** s | 1.311 s | 17.085 s | 37.287 s | SUCCESS | match |
| 5 | 320 MB | partitioning | 1.677 s | 5.752 s | **9.200** s | 16.973 s | 33.602 s | SUCCESS | match |
| 5 | 320 MB | merging | 2.724 s | 5.432 s | 1.158 s | **18.550** s | 27.858 s | SUCCESS | match |

# Before
Before the addition of fault-tolerance and optimization of partitioning, the follwing configurations were tested.
| Dataset | Machines | Records | Data size |
|---|---|---|---|
| Large | 2 | 64_000_000 | 6.4 GB |
| Big | 2 |6_400_000 | 640 MB |
| Big | 1 | 3_200_000 | 320 MB |
| Small | 13 | 8_320_000 | 832 MB |
| Small | 12 | 7_680_000 | 768 MB |
| Small | 11 | 7_040_000 | 704 MB |
| Small | 10 | 6_400_000 | 640 MB |
| Small | 9 | 5_760_000 | 576 MB |
| Small | 8 | 5_120_000 | 512 MB |
| Small | 7 | 4_480_000 | 448 MB |
| Small | 6 | 3_840_000 | 384 MB |
| Small | 5 | 3_200_000 | 320 MB |
| Small | 4 | 2_560_000 | 256 MB |
| Small | 3 | 1_920_000 | 192 MB |
| Small | 2 | 1_280_000 | 128 MB |
| Small | 1 | 640_000 | 64 MB |