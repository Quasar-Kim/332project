# Empty
## Directory
Empty input directory: directory exists but contains no files.

*Wanted behaviour*: simply producing no output; succeeding rather than crashing.

**Success**: 1 machine, 1 empty directory (```./empty.sh dir 1 1```). Does not crash, and the output directory remains empty.

## File
Empty input file: file exists but contains no records.

*Wanted behaviour*: producing no partitions.

**Success**: 1 machine, 1 directory with 1 empty file. Results in no partition file (but also no crash).

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

# File size and file count
## Tiny input file
There is a very small number of records (e.g., 10).

**Success**. Tried:
- 1 machine, 1 directory, 1 file with 10 records (```./size-count.sh 1 1 1 10```)
- 2 machines, 3 directories, 4 files with 5 records, i.e., 120 records split between 24 files (```./size-count.sh 2 3 4 5```)

## Data heavily split into small files
For example, instead of 2 files with 320_000 records each, it is 20000 files with 32 records each.

**Success**. Tried:
- 3 machines, 1 directory, 1000 files with 10 records, i.e., 30_000 records (3 MB) split between 3000 files.

Results in 3 partition files (9000, 9000, and 12000 records); all are sorted correctly, and no records are lost.

## Giant input file
All input data is in 1 file.
- 1 machine, 1 file with 2_560_000 records (256 MB)

Results in 2 partition files (1_280_000 records each) sorted correctly.

## Multiple giant input files
A few files, but all are large.

# Skewed
The genereted records are skewed, i.e., they are non-uniformly distributed.

**Success**. Tried:
- 2 machines, 1 directory, 2 files with 100 ascii records (400 records overall)
- 5 machines, 2 directories, 4 files with 10_000 binary records each (400_000 records overall, 390_000 duplicate keys)

# Duplicates
## Identical files
All input files are identical.

**Success**: 3 machines, 3 directories, 3 files with 300 records, i.e., 9 identical files overall (```./duplicates.sh files 3 3 3 300```)

## Identical file sets
The file sets on each machine are the same.

**Success**: 2 machines, 2 directories, 2 files with 2000 records, where the input directory pairs on ```vm01``` and ```vm02``` are identical (```./duplicates.sh sets 2 2 2 2000```).

## Mix
Some files and file sets are identical, some are not.

**Success**: the input data is as follows (with ```./duplicates.sh mix 4 2 2 3 2 1000```):
```
vm01:
+-dir0
  +-file0
  +-file1   <-- copy of file0
  +-file2   <-- copy of file0
  +-file3
  +-file4
+-dir1
  +-file0   <-- copy of ../dir0/file0
  +-file1   <-- copy of ../dir0/file0
  +-file2   <-- copy of ../dir0/file0
  +-file3
  +-file4
```
This set is copied to ```vm02```, while ```vm03``` and ```vm04``` have sets that are indepentdent of this one (each also with 2 directories with 5 files, each with 1000 records).

# Binary records
The data is binary instead of ASCII.

**Success**: 4 machines, 5 directories, 6 files per directory, 7000 *binary* records per file (840_000 records in total).