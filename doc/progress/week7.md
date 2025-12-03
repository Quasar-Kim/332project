# Week 7 Progress Report

- Week 7: 11/24 - 11/30

## Progress in this week

- Implemented handlers of each job and corresponding tests
- Implemented `master` and `worker` binaries
- Achieved milestone 2: Being able to sort small data on a local machine, without fault
- Achieved milestone 3: Sorting large data on actual cluster without fault (details below)
- Tested our implementation on local machine and fix bugs (WIP)

---

* In the actual cluster, sorting succeeded under the following conditions in 6 minutes 15 seconds:
  - Number of workers machine: 4
  - Number of threads per worker: 4
  - Number of input directories: 2
  - Number of files per input directory: 2
    - Each machine has 4 files, so total 16 files
  - Size of each file: 128 MB
    - Total input data size: 2 GB
* Sampling and sorting phases work effectively, but the paritioning and merging phases need optimization.
  
| Phase         | Time spent (seconds)  |
|:-------------:|----------------------:|
| Sampling      | 1.557                 |
| Sorting       | 20.965                |
| Partitioning  | 156.946               |
| Merging       | 172.115               |
| **Total**     | **351.583**           |

## Goal of the next week

- Start working on milestone 4: fault tolerance
- Make sure our program runs correctly on the local or actual cluster
- Optimize the performance of partition and merge job handlers

## Goal of the next week for each individual member

- Junseong:
  * Upgrade program to be fault tolerant
- Jaehwan:
  * Test under various conditions and fix bugs
- Dana:
  * Implement an automating script to test our program on the actual cluster
