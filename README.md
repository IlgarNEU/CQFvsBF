This project compares the performance of Counting Quotient Filter with Bloom Filter in Summary Cache Protocol 
by creating a distributed cache simulator and implementing each data structure in the simulator.

To compile, inside src folder, run "make clean" and then "make";

To run regular bloom filter tests: PROCESS_BINARY=./process_bloom ./manager_bloom 4 500000
4 is the number of processes, you can change up to the number your machine can handle. 
we tested with up to 24 processes; 500000 is the number of keys per process; You can increase with powerful machines.

To run counting bloom filter tests: PROCESS_BINARY=./process_counting_bloom ./manager_counting_bloom 4 500000

To run counting quotient filter tests: PROCESS_BINARY=./process_cqf ./manager_cqf 4 500000

IMPORTANT NOTE: There is a "wait" for data structure construction and broadcasting, so depending on the machine's state, you may want to change them: 
Manager_bloom.c: Lines 18 and 19; 
Manager_cqf.c: Line 21; 
Manager_counting_bloom: Lines 16 and 17;
We have overprovisioned to 2 minute wait times because of our hardware limitations.

We used https://github.com/barrust/counting_bloom, https://github.com/barrust/bloom as bloom filter implementations 
and https://github.com/splatlab/cqf/tree/master as counting quotient filter implementation.
