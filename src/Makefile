# Compiler and flags
CC = gcc
CFLAGS = -O3 -I. -I./cqf/include -w
LDFLAGS = -lm -lssl -lcrypto -lpthread 

# Directories
BLOOM_DIR = ./
CQF_DIR = ../cqf
COUNTING_BLOOM_DIR = ./

# Sources
BLOOM_SRC = $(BLOOM_DIR)/bloom.c
CQF_OBJS = $(CQF_DIR)/obj/gqf.o $(CQF_DIR)/obj/gqf_file.o $(CQF_DIR)/obj/hashutil.o $(CQF_DIR)/obj/partitioned_counter.o
COUNTING_BLOOM_SRC = $(COUNTING_BLOOM_DIR)/counting_bloom.c

# Object files
OBJ_IPC = IPC.o
OBJ_BLOOM = bloom.o
OBJ_COUNTING_BLOOM = counting_bloom.o
OBJ_PROCESS_BLOOM = Process.o
OBJ_PROCESS_CQF = Process_cqf.o
OBJ_PROCESS_COUNTING_BLOOM = Process_counting_bloom.o
OBJ_MANAGER = Manager_bloom.o
OBJ_MANAGER_CQF = Manager_cqf.o
OBJ_MANAGER_COUNTING_BLOOM = Manager_counting_bloom.o


# Executables
TARGETS = manager_bloom manager_cqf manager_counting_bloom process_bloom process_cqf process_counting_bloom
#TARGETS = manager_cqf process_cqf


# Default target
all: $(TARGETS)

# Build rules
manager_bloom: $(OBJ_MANAGER) $(OBJ_IPC)
	$(CC) $(CFLAGS) -o $@ $(OBJ_MANAGER) $(OBJ_IPC) $(LDFLAGS)

manager_counting_bloom: $(OBJ_MANAGER_COUNTING_BLOOM) $(OBJ_IPC)
	$(CC) $(CFLAGS) -o $@ $(OBJ_MANAGER_COUNTING_BLOOM) $(OBJ_IPC) $(LDFLAGS)

manager_cqf: $(OBJ_MANAGER_CQF) $(OBJ_IPC)
	$(CC) $(CFLAGS) -o $@ $(OBJ_MANAGER_CQF) $(OBJ_IPC) $(CQF_OBJS) $(LDFLAGS)

process_bloom: $(OBJ_PROCESS_BLOOM) $(OBJ_IPC) $(OBJ_BLOOM)
	$(CC) $(CFLAGS) -o $@ $(OBJ_PROCESS_BLOOM) $(OBJ_IPC) $(OBJ_BLOOM) $(LDFLAGS)

process_counting_bloom: $(OBJ_PROCESS_COUNTING_BLOOM) $(OBJ_IPC) $(OBJ_COUNTING_BLOOM)
	$(CC) $(CFLAGS) -o $@ $(OBJ_PROCESS_COUNTING_BLOOM) $(OBJ_IPC) $(OBJ_COUNTING_BLOOM) $(LDFLAGS)

process_cqf: $(OBJ_PROCESS_CQF) $(OBJ_IPC) $(CQF_OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJ_PROCESS_CQF) $(OBJ_IPC) $(CQF_OBJS) $(LDFLAGS)

# Object compilation
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

bloom.o: $(BLOOM_SRC)
	$(CC) $(CFLAGS) -I$(BLOOM_DIR) -c $(BLOOM_SRC) -o bloom.o

counting_bloom.o: $(COUNTING_BLOOM_SRC)
	$(CC) $(CFLAGS) -I$(COUNTING_BLOOM_DIR) -c $(COUNTING_BLOOM_SRC) -o counting_bloom.o

# Ensure CQF library is built
$(CQF_OBJS):
	cd $(CQF_DIR) && $(MAKE)

# Clean
clean:
	rm -f *.o $(TARGETS)
	rm -rf /tmp/distributed_cache_sockets
	rm -f /tmp/bloom_process_*.dat
	rm -f /tmp/cqf_process_*.cqf

.PHONY: all clean
