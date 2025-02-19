# MapReduce-Simulator

This project is an implementation of the MapReduce framework using multi-process programming on a single machine. The goal is to simulate distributed data processing by splitting input files into multiple parts, processing them in parallel using worker processes, and then aggregating the results in a reduce phase.

Features:
- Letter Counter Task: Counts occurrences of each letter (A-Z) in the input file.
- Multi-Processing: Uses fork() to create multiple worker processes.
- Intermediate Storage: Stores results in intermediate files (mr-<id>.itm) before the reduce phase.
- Final Output: Outputs results in mr.rst.
- Word Finder Task: Searches for a specific word in the file and outputs all lines containing the word.

Compile the project:
make

Letter Counter:
./run-mapreduce "counter" sample.txt 4
This will count the occurrences of letters (A-Z) in sample.txt using 4 worker processes.

Word Finder (Bonus Task):
./run-mapreduce "finder" sample.txt 4 "word_to_find"
This will search for word_to_find in sample.txt using 4 worker processes.

─ main.c: Driver program
─ mapreduce.c: Core logic for map and reduce<br />
─ user_functions.c: Custom map/reduce functions<br />
─ mapreduce.h: Header file for mapreduce functions<br />
─ user_functions.h: Header file for user-defined functions<br />
─ input-moon10.txt: Sample input files for testing<br />

Map Phase:
The input file is split into N parts based on the number of processes.
Each worker process reads its assigned split and applies a map function.
Reduce Phase:
The first worker forks the reduce process.
The reducer merges the results from all intermediate files and outputs the final counts.
