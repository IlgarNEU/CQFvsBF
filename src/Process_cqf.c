#define _POSIX_C_SOURCE 199309L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <search.h>
#include <time.h>
#include "IPC.h"
#include "../cqf/include/gqf.h"
#include "../cqf/include/gqf_int.h"
#include "../cqf/include/gqf_file.h"

#define BUF_SIZE 256
#define DT_MSG_SIZE 262144 
#define CQF_FILE_DIR "/tmp"

int process_id; 
int num_processes; 


int *own_keys = NULL;
int num_own_keys = 0;
int own_keys_capacity = 0;

typedef struct{
    int key;
    int owner_process_id;
} KeyOwnerPair;    

KeyOwnerPair *all_keys = NULL;

int num_all_keys = 0;
int all_keys_capacity = 0;

int keys_finalized = 0;

QF global_cqf;
int cqf_initialized = 0;

int comm_fd = -1;

struct {
    double total_own_lookup_ms;
    double total_all_cqf_checks_ms;
    double total_single_cqf_check_ms;
    double total_cqf_update_ms;
    int num_own_lookups;
    int num_query_rounds;
    int num_individual_cqf_checks;
    int num_cqf_updates;
} cqf_stats = {0, 0, 0, 0, 0, 0, 0, 0};

void signal_handler(int signum);
int check_own_keys(int key);
void assign_own_keys_from_message(const char *msg);
void assign_all_keys_from_message(const char *msg);
void finalize_keys();
void create_cqf();
void handle_query_from_manager(const char *msg);
void handle_query_from_process(const char *msg);
void handle_response_from_process(const char *msg);
void handle_delete_keys(const char *msg);
void handle_insert_keys(const char *msg);

uint64_t hash_key(int key){
    uint64_t x = (uint64_t)key;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

void signal_handler(int signum){
    if(cqf_stats.num_own_lookups >0 || cqf_stats.num_query_rounds > 0){
        char stats_file[256];
        snprintf(stats_file, sizeof(stats_file), "/tmp/process_%d_stats.txt", process_id);
        
        FILE *fp = fopen(stats_file, "w");
        if(fp){
            fprintf(fp, "PROCESS %d CQF TIMING\n", process_id);
            fprintf(fp, "Own Hash Table Lookups:\n");
            fprintf(fp, "Count: %d\n", cqf_stats.num_own_lookups);
            fprintf(fp, "Avg time: %.6f ms (%.2f μs)\n", cqf_stats.total_own_lookup_ms / cqf_stats.num_own_lookups, (cqf_stats.total_own_lookup_ms / cqf_stats.num_own_lookups) * 1000 );
            fprintf(fp, "\n");
            fprintf(fp, "CQF Peer Checks:\n");
            fprintf(fp, "Query rounds: %d\n", cqf_stats.num_query_rounds);
            fprintf(fp, "Total individual CQF lookups: %d\n", cqf_stats.num_individual_cqf_checks);
            fprintf(fp, "\n");
            fprintf(fp, "Single CQF Lookup Performance:\n");
            fprintf(fp, "Avg time per CQF lookup: %.6f ms (%.2f μs)\n", cqf_stats.total_single_cqf_check_ms / cqf_stats.num_individual_cqf_checks, (cqf_stats.total_single_cqf_check_ms / cqf_stats.num_individual_cqf_checks) * 1000 );
            fprintf(fp, "\n");
            fprintf(fp, "All Peers Check Performance:\n");
            fprintf(fp, "Avg time to check all peers: %.6f ms (%.2f μs)\n",cqf_stats.total_all_cqf_checks_ms / cqf_stats.num_query_rounds, (cqf_stats.total_all_cqf_checks_ms / cqf_stats.num_query_rounds) * 1000);
            fprintf(fp, "\n");
            fprintf(fp, "CQF_update_details:\n");
            fprintf(fp, "Total time to update: %.6f ms (%.2f μs)\n",cqf_stats.total_cqf_update_ms, cqf_stats.total_cqf_update_ms * 1000);
            fprintf(fp, "Total updates: %d\n", cqf_stats.num_cqf_updates);
            
            fclose(fp);
            printf("Process %d Stats written to %s\n", process_id, stats_file);
        }
    }

    if(cqf_initialized){
        qf_deletefile(&global_cqf);
    }

    if(own_keys != NULL){
        free(own_keys);
    }

    if(all_keys != NULL){
        free(all_keys);
    }

    hdestroy();

    if(comm_fd >= 0){
        close_communication(process_id, comm_fd);
    }

    exit(0);
}

//Checking own hash table
int check_own_keys(int key){
    if(!keys_finalized) return 0;

    char key_str[32];
    snprintf(key_str, sizeof(key_str), "%d", key);

    ENTRY e, *ep;
    e.key = key_str;
    e.data = NULL;

    ep = hsearch(e, FIND);
    return (ep != NULL);
}

//Receive own keys and add to array before hashing
void assign_own_keys_from_message(const char *msg){
    const char *ptr = msg + 10; // "OWN_KEYS:"
    char *copy = strdup(ptr);
    char *tok = strtok(copy, ",");

    while (tok != NULL){
        if(num_own_keys >= own_keys_capacity){
            int new_capacity = own_keys_capacity == 0 ? 100000 : own_keys_capacity * 2;
            int *new_keys = realloc(own_keys, new_capacity * sizeof(int));
            if(new_keys == NULL){
                fprintf(stderr, "[ERROR HAPPENED] : process %d failed to allocate own_keys\n", process_id);
                free(copy);
                exit(1);
            }
            own_keys = new_keys;
            own_keys_capacity = new_capacity;
        }
        own_keys[num_own_keys++] = atoi(tok);
        tok = strtok(NULL, ",");
    }
    free(copy);


}

//While receving update keys from Manager, update the CQF on fly (insert)
void update_all_keys_from_message(const char *msg){
    const char *ptr = msg + 16; // "ALL_UPDATE_KEYS:"
    int owner_id = atoi(ptr);
    const char *colon = strchr(ptr, ':');
    const char *key_str = colon + 1;
    char *copy = strdup(key_str);
    char *tok = strtok(copy, ",");
    int inserts = 0;
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    while(tok != NULL){
        int key = atoi(tok);
        uint64_t hash = hash_key(key);
        uint64_t cqf_key = hash % global_cqf.metadata->range;
        int ret = qf_insert(&global_cqf, cqf_key, owner_id, 1, QF_NO_LOCK);
        if(ret >= 0){
            inserts++;
        }
        tok = strtok(NULL, ",");
    }
    free(copy);
    clock_gettime(CLOCK_MONOTONIC, &end);
    double elapsed_ms = (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_nsec - start.tv_nsec) / 1000000.0;
    cqf_stats.num_cqf_updates += inserts;
    cqf_stats.total_cqf_update_ms += elapsed_ms;
    //printf("Process %d inserted %d keys in %.3f ms\n", process_id, inserts, elapsed_ms);


}

//Receive all keys and add to array before hashing
void assign_all_keys_from_message(const char *msg){
    const char *ptr = msg+9; //"ALL_KEYS:"
    int owner_id = atoi(ptr);

    const char *colon = strchr(ptr, ':');
    const char *key_str = colon + 1;
    char *copy = strdup(key_str);
    char *tok = strtok(copy, ",");

    while(tok != NULL){
        if(num_all_keys >= all_keys_capacity){
            int new_capacity = all_keys_capacity == 0 ? 200000 : all_keys_capacity * 2;
            KeyOwnerPair *new_array = realloc(all_keys, new_capacity * sizeof(KeyOwnerPair));
            if(new_array == NULL){
                fprintf(stderr, "[ERROR HAPPENED] : process %d failed to allocate all_keys\n", process_id);
                free(copy);
                exit(1);
            }
            all_keys = new_array;
            all_keys_capacity = new_capacity;
        }
        all_keys[num_all_keys].key = atoi(tok);
        all_keys[num_all_keys].owner_process_id = owner_id;
        num_all_keys++;
        tok = strtok(NULL, ",");
    }
    free(copy);

}

//Once received all keys, create hash table
void finalize_keys(){
    if(keys_finalized) return;


    if(hcreate(num_own_keys * 2) == 0){
        fprintf(stderr, "[ERROR HAPPENED] : Process %d failed to create hash table\n", process_id);
        exit(1);
    }
    for(int i = 0; i < num_own_keys; i++){
        char *key_str = malloc(32);
        snprintf(key_str, 32, "%d", own_keys[i]);

        ENTRY e;
        e.key = key_str;
        e.data = (void*)(long)1;

        if(hsearch(e, ENTER) == NULL){
            fprintf(stderr, "[ERROR HAPPENED] : Process %d failed to insert into hash table\n", process_id);
        }
    }

    keys_finalized = 1;
    create_cqf();
}

//Once received all keys, create cqf
void create_cqf(){
    if(cqf_initialized){
        qf_deletefile(&global_cqf);
    }

    uint64_t qbits = 0;
    uint64_t temp_qbits = num_all_keys - 1;
    while(temp_qbits > 0){
        qbits++;
        temp_qbits >>= 1;
    } 

    uint64_t rbits = 7;
    uint64_t nhashbits = qbits + rbits;
    uint64_t nslots = (1ULL << qbits);

    //Below is the calculation of log for value bits
    uint64_t value_bits = 0;
    int temp = num_processes - 1;
    while(temp > 0){
        value_bits++;
        temp >>= 1;
    }

    char init_file[512];
    snprintf(init_file, sizeof(init_file), "/tmp/cqf_p%d.cqf", process_id);

    if(!qf_initfile(&global_cqf, nslots, nhashbits, value_bits, QF_HASH_INVERTIBLE, 0, init_file)){
        fprintf(stderr, "Process %d can't allocate CQF\n");
        exit(1);
    }

    qf_set_auto_resize(&global_cqf, true); //WE CAN TRY THIS BRINIGNG BACK TO TRUE AS "TEST.C" FILE OF CQF, I HAD TO CHANGE FOR DEBUGGING

    //WE CAN COMMENT THIS OUT, IT WAS FOR DEBUGGING
    int successful_inserts = 0;
    int failed_inserts = 0;
    int duplicate_skips = 0;

    for(int i = 0; i < num_all_keys; i++){
        uint64_t hash = hash_key(all_keys[i].key);
        uint64_t cqf_key = hash % global_cqf.metadata->range;
        int owner_id = all_keys[i].owner_process_id;
        //WE CAN COMMENT THIS OUT AS WELL, FOR DEBUGGING THE RANDOM KEY ISSUES
        uint64_t existing_count = qf_count_key_value(&global_cqf, cqf_key, owner_id, 0);
        if(existing_count > 0){
            duplicate_skips++;
        } else{
            int ret = qf_insert(&global_cqf, cqf_key, owner_id, 1, QF_NO_LOCK);
            if(ret >= 0){
                successful_inserts++;
            }else{
                failed_inserts++;
            }
        }
    }
    
    uint64_t metadata_size = sizeof(qfmetadata);
    uint64_t data_size = global_cqf.metadata->total_size_in_bytes;
    uint64_t total_size = metadata_size + data_size;
    
    printf("[Process %d] CQF memory size: %llu bytes (%.2f KB, %.2f MB)\n",
           process_id, 
           (unsigned long long)total_size,
           total_size / 1024.0,
           total_size / (1024.0 * 1024.0));
    
    printf("[Process %d] CQF details: nslots=%llu, occupied=%llu, load=%.1f%%\n",
           process_id,
           (unsigned long long)nslots,  
           (unsigned long long)global_cqf.metadata->noccupied_slots,
           (global_cqf.metadata->noccupied_slots * 100.0) / nslots);  

    fflush(stdout);
    cqf_initialized = 1;
    printf("Success - %d, duplicate - %d, failed - %d\n", successful_inserts, duplicate_skips, failed_inserts);
    free(all_keys);
    all_keys = NULL;
    num_all_keys = 0;
    all_keys_capacity = 0;
}

//Delete keys on fly
void handle_delete_keys(const char *msg){
    const char *ptr = msg + 12; //DELETE_KEYS:
    int owner_id = atoi(ptr);
    const char *colon = strchr(ptr, ':');
    const char *key_str = colon + 1;
    char *copy = strdup(key_str);
    char *tok = strtok(copy, ",");
    int deletes = 0;
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    
    while(tok != NULL){
        int key = atoi(tok);
        if(cqf_initialized){
            uint64_t hash = hash_key(key);
            uint64_t cqf_key = hash % global_cqf.metadata->range;
            int ret = qf_delete_key_value(&global_cqf, cqf_key, owner_id, QF_NO_LOCK);
            deletes++;
            (void)ret;
        }
        tok = strtok(NULL, ",");
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    double elapsed_ms = (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_nsec - start.tv_nsec) / 1000000.0;
    cqf_stats.total_cqf_update_ms += elapsed_ms;
    
    free(copy);
    cqf_stats.num_cqf_updates += deletes;
    //printf("Process %d deleted %d keys in %.3f ms\n", process_id, deletes, elapsed_ms);
}

//User query is below, it will come from manager (manager.c simulates users)
void handle_query_from_manager(const char *msg){
    int key = atoi(msg+6);


    struct timespec own_start, own_end;
    clock_gettime(CLOCK_MONOTONIC, &own_start);
    int found_locally = check_own_keys(key);
    clock_gettime(CLOCK_MONOTONIC, &own_end);
    double own_lookup_ms = (own_end.tv_sec - own_start.tv_sec) * 1000.0 + (own_end.tv_nsec - own_start.tv_nsec) / 1000000.0;
    cqf_stats.total_own_lookup_ms += own_lookup_ms;
    cqf_stats.num_own_lookups++;

    if(found_locally){
        char response[BUF_SIZE];
        snprintf(response, sizeof(response), "FOUND:%d:PROCESS_%d", key, process_id);
        send_msg(process_id, num_processes, response);
        return;
    }

    if(!cqf_initialized){
        char response[BUF_SIZE];
        snprintf(response, sizeof(response), "NOTFOUND:%d:CQF_NOT_READY", key);
        send_msg(process_id, num_processes, response);
        return;
    }
    struct timespec all_cqf_start, all_cqf_end;
    clock_gettime(CLOCK_MONOTONIC, &all_cqf_start);

    int queries_sent = 0;
    uint64_t hash = hash_key(key);
    uint64_t cqf_key = hash % global_cqf.metadata->range;

    for(int p = 0; p < num_processes; p++){
        if(p == process_id) continue;

        struct timespec single_start, single_end;
        clock_gettime(CLOCK_MONOTONIC, &single_start);

        uint64_t count = qf_count_key_value(&global_cqf, cqf_key, p, 0);

        clock_gettime(CLOCK_MONOTONIC, &single_end);
        double single_check_ms = (single_end.tv_sec - single_start.tv_sec) * 1000.0 + (single_end.tv_nsec - single_start.tv_nsec) / 1000000.0;
        cqf_stats.total_single_cqf_check_ms += single_check_ms;
        cqf_stats.num_individual_cqf_checks++;

        if(count > 0){
            char buf[BUF_SIZE];
            snprintf(buf, sizeof(buf), "PQUERY:%d:FROM_%d", key, process_id);
            send_msg(process_id, p, buf);
            queries_sent++;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &all_cqf_end);
    double all_cqf_ms = (all_cqf_end.tv_sec - all_cqf_start.tv_sec) * 1000.0 + (all_cqf_end.tv_nsec - all_cqf_start.tv_nsec) / 1000000.0;
    cqf_stats.total_all_cqf_checks_ms += all_cqf_ms;
    cqf_stats.num_query_rounds++;

    if(queries_sent == 0){
        char response[BUF_SIZE];
        snprintf(response, sizeof(response), "NOTFOUND:%d:CHECKED_BY_PROCESS_%d", key, process_id);
        send_msg(process_id, num_processes, response);
    }
}

//This is for handling the "redirected" query from a peer cache
void handle_query_from_process(const char *msg){
    if(strncmp(msg, "PQUERY:", 7) != 0) return;

    int key = atoi(msg+7);

    const char *from_marker = strstr(msg, ":FROM_");
    int sender_process = -1;

    if(from_marker != NULL){
        sender_process = atoi(from_marker + 6);
    }

    if(check_own_keys(key)){
        if(sender_process >= 0){
            char response[BUF_SIZE];
            snprintf(response, sizeof(response), "PFOUND:%d:IN_PROCESS_%d", key, process_id);
            send_msg(process_id, sender_process, response);
        }

    } else {
        if(sender_process >= 0){
            char response[BUF_SIZE];
            snprintf(response, sizeof(response), "PNOTFOUND:%d:IN_PROCESS_%d", key, process_id);
            send_msg(process_id, sender_process, response);
        }
    }
}

//To see if peer found or not the peer redirected key locally
//We can later use this function to redirect the "not found" key to the web server
void handle_response_from_process(const char *msg){
    if(strncmp(msg, "PFOUND:", 7) == 0){
        int key = atoi(msg + 7);
        const char *process_marker = strstr(msg, ":IN_PROCESS_");
        int found_in_process = -1;
        if(process_marker != NULL){
            found_in_process = atoi(process_marker + 12);
        }

        char response[BUF_SIZE];

        snprintf(response, sizeof(response), "FOUND:%d:PROCESS_%d", key, found_in_process);
        send_msg(process_id, num_processes, response);
    }
}


int main(int argc, char *argv[]){
    process_id = atoi(argv[1]);
    num_processes = atoi(argv[2]);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    comm_fd = initiate_communication(process_id);
    printf("SUCCESS: Process %d started\n", process_id);

    char *buf = malloc(DT_MSG_SIZE);

    while(1){
        int messages_processed = 0;

        while(1){
            int n = receive_msg(comm_fd, buf, DT_MSG_SIZE);
            if(n <= 0) break;

            messages_processed++;

            if(strncmp(buf, "OWN_KEYS:", 9) == 0){
                assign_own_keys_from_message(buf);
            } else if(strncmp(buf, "ALL_KEYS:", 9) == 0){
                assign_all_keys_from_message(buf);
            } else if(strncmp(buf, "KEYS_DONE", 9) == 0){
                finalize_keys();
            } else if(strncmp(buf, "QUERY:", 6) == 0){
                handle_query_from_manager(buf);
            } else if(strncmp(buf, "PQUERY:", 7) == 0){
                handle_query_from_process(buf);
            } else if(strncmp(buf, "PFOUND:", 7) == 0 || strncmp(buf, "PNOTFOUND:", 10) == 0){
                handle_response_from_process(buf);
            } else if(strncmp(buf, "DELETE_KEYS:", 12) == 0){
                handle_delete_keys(buf);
            } else if(strncmp(buf, "ALL_UPDATE_KEYS:", 16) == 0){
                update_all_keys_from_message(buf);
            }
        }

        if(messages_processed == 0){
            usleep(1000);
        }
    }

    free(buf);
    signal_handler(0);
    return 0;
}
