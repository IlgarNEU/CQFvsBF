#define _POSIX_C_SOURCE 199309L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include "IPC.h"
#include "bloom.h"
#include <search.h>
#include <time.h>



#define BUF_SIZE 256          
#define BLOOM_MSG_SIZE 262144 
#define FALSE_POSITIVE_RATE 0.01
#define BLOOM_FILE_DIR "/tmp"

int process_id; 

int num_processes; 

int *keys = NULL; 
int num_keys = 0;
int keys_capacity = 0;

int keys_finalized = 0;
int deletes_finalized = 0;
int inserts_finalized = 0;

BloomFilter own_bloom;
BloomFilter *peer_bloom_filters = NULL;

int bloom_initialized = 0;
int *peer_bloom_received = NULL;
int bloom_broadcasted = 0;

int comm_fd = -1;

struct {
    double total_own_lookup_ms;
    double total_all_peer_bloom_checks_ms;
    double total_single_bloom_check_ms;
    double total_update_time;
    int num_own_lookups;
    int num_query_rounds;
    int num_individual_bloom_checks;
    int num_updates;
} bloom_stats = {0, 0, 0, 0, 0, 0, 0, 0};

void signal_handler(int signum);
int check_own_keys(int key);
void assign_keys_from_message(const char *msg);
void create_own_bloom_filter();
void broadcast_bloom_filter();
void update_peer_bloom_filter_from_file(int peer_id, const char *bloom_data);
void handle_query_from_manager(const char *msg);
void handle_bloom_message(const char *msg);
void handle_query_from_process(const char *msg);
void remove_keys_from_message(const char *msg);
void insert_keys_from_message(const char *msg);
void rebuild_hash_and_bloom_and_broadcast();

//This is used to remove the "delete keys" from the array before creating the hash table and bloom filters
void remove_keys_from_message(const char *msg){
    int deleted_count = 0;
    const char *ptr = msg+strlen("DELETE_KEYS:");
    char *copy = strdup(ptr);
    char *tok = strtok(copy, ",");
    if(tok == NULL){
        free(copy);
        return;
    }
    while(tok != NULL){
        int del_key = atoi(tok);
        int write_idx = 0;
        for(int i = 0; i < num_keys; i++){
            if(keys[i] == del_key){
                deleted_count++;
                continue;
            }else{
                keys[write_idx++] = keys[i];
            }
        }
        tok = strtok(NULL, ",");
    }
    free(copy);

    const char *ptr2 = msg+strlen("DELETE_KEYS:");
    char *copy2 = strdup(ptr2);
    char *tok2 = strtok(copy2, ",");

    //the rest is to delete the keys that could be duplicate
    int del_capacity = 1024;
    int del_count = 0;
    int *del_list = malloc(del_capacity * sizeof(int));
    while(tok2 != NULL){
        if(del_count >= del_capacity){
            del_capacity *= 2;
            del_list = realloc(del_list, del_capacity * sizeof(int));
        }
        del_list[del_count++] = atoi(tok2);
        tok2 = strtok(NULL, ",");
    }
    free(copy2);

    int write = 0;
    for(int i = 0; i < num_keys; i++){
        int keep = 1;
        for(int j = 0; j < del_count; j++){
            if(keys[i] == del_list[j]){
                deleted_count++;
                keep = 0;
                break;
            }
        }
        if(keep){
            keys[write++] = keys[i];
        }
    }
    num_keys = write;
    free(del_list);
    

    //so the deleted count can be a little larger than the normal delete count because of duplicates
    //we can later change the manager to send non-duplicate keys, but for now this doesn't hurt
    bloom_stats.num_updates += deleted_count;
}

//This is to insert (update) new keys for measuring time to recreate bloom filters
void insert_keys_from_message(const char *msg){
    const char *ptr = msg + strlen("UPDATE_KEYS:");
    char *copy = strdup(ptr);
    char *tok = strtok(copy, ",");
    int inserted_count = 0;
    while(tok != NULL){
        if(num_keys >= keys_capacity){
            int new_capacity;
            if(keys_capacity == 0){
                new_capacity = 100000;
            } else{
                new_capacity = keys_capacity * 2;
            }
            int *new_keys = realloc(keys, new_capacity * sizeof(int));
            keys = new_keys;
            keys_capacity = new_capacity;
        }
        keys[num_keys++] = atoi(tok);
        inserted_count++;
        tok = strtok(NULL, ",");
    }
    free(copy);
    
    bloom_stats.num_updates += inserted_count;
}

//Once we receive the command to reconstruct the bloom
void finalize_deletes(){
    if(deletes_finalized){
        return;
    }
    printf("Received all keys, now hashing and broadcasting\n");
    rebuild_hash_and_bloom_and_broadcast();
}

//Once we receive the command that insert keys are completed, so start reconstructing the bloom
void finalize_inserts(){
    if(inserts_finalized){
        return;
    }
    rebuild_hash_and_bloom_and_broadcast();
}


//To reconstruct the bloom after receiving deletes and inserts
//We do not rebuild hash table, because it does not play a role
void rebuild_hash_and_bloom_and_broadcast(){
    printf("Building bloom and broadcasting\n");
    
    keys_finalized = 1;
    struct timespec bloom_update_start, bloom_update_end;
    clock_gettime(CLOCK_MONOTONIC, &bloom_update_start);
    create_own_bloom_filter();
    //bloom_broadcasted = 0;
    //broadcast_bloom_filter();
    clock_gettime(CLOCK_MONOTONIC, &bloom_update_end);
    double bloom_update_timing_ms = (bloom_update_end.tv_sec - bloom_update_start.tv_sec) * 1000.0 + (bloom_update_end.tv_nsec - bloom_update_start.tv_nsec) / 1000000.0;
    bloom_stats.total_update_time += bloom_update_timing_ms;
}


void signal_handler(int signum){
    if(bloom_stats.num_own_lookups > 0 || bloom_stats.num_query_rounds > 0){
        char stats_file[256];
        snprintf(stats_file, sizeof(stats_file), "/tmp/process_%d_bloom_stats.txt", process_id);
        
        FILE *fp = fopen(stats_file, "w");
        if(fp){
            fprintf(fp, "PROCESS %d BLOOM FILTER TIMING\n", process_id);
            fprintf(fp, "Own Hash Table Lookups:\n");
            fprintf(fp, "Count: %d\n", bloom_stats.num_own_lookups);
            fprintf(fp, "Total time: %.6f ms\n", bloom_stats.total_own_lookup_ms);
            fprintf(fp, "Avg time: %.6f ms (%.2f μs)\n", bloom_stats.total_own_lookup_ms / bloom_stats.num_own_lookups, (bloom_stats.total_own_lookup_ms / bloom_stats.num_own_lookups) * 1000);
            fprintf(fp, "\n");
            fprintf(fp, "Peer Bloom Filter Checks:\n");
            fprintf(fp, "Query rounds: %d\n", bloom_stats.num_query_rounds);
            fprintf(fp, "Total individual Bloom checks: %d\n", bloom_stats.num_individual_bloom_checks);
            fprintf(fp, "\n");
            fprintf(fp, "Single Bloom Filter Lookup Performance:\n");
            fprintf(fp, "Total time (all individual checks): %.6f ms\n", bloom_stats.total_single_bloom_check_ms);
            fprintf(fp, "Avg time per Bloom check: %.6f ms (%.2f μs)\n", (bloom_stats.total_single_bloom_check_ms / bloom_stats.num_individual_bloom_checks), (bloom_stats.total_single_bloom_check_ms / bloom_stats.num_individual_bloom_checks) * 1000);
            fprintf(fp, "\n");
            fprintf(fp, "All Peers Check Performance:\n");
            fprintf(fp, "Total time (all query rounds): %.6f ms\n", bloom_stats.total_all_peer_bloom_checks_ms);
            fprintf(fp, "Avg time to check all peers: %.6f ms (%.2f μs)\n", (bloom_stats.total_all_peer_bloom_checks_ms / bloom_stats.num_query_rounds), (bloom_stats.total_all_peer_bloom_checks_ms / bloom_stats.num_query_rounds) * 1000);
            fprintf(fp, "Total time to recreate and broadcast bloom after deletes first and then inserts again : %.6f ms\n", bloom_stats.total_update_time);
            fprintf(fp, "Total updates: %d\n", bloom_stats.num_updates);
            fclose(fp);
            printf("Process %d Stats written to %s\n", process_id, stats_file);
        }
    }
    if(bloom_initialized){
        bloom_filter_destroy(&own_bloom);
    }
    if(peer_bloom_filters != NULL){
        for(int i = 0; i < num_processes; i++){
            if(peer_bloom_received && peer_bloom_received[i]){
                bloom_filter_destroy(&peer_bloom_filters[i]);
            }
        }
        free(peer_bloom_filters);
    }

    if(peer_bloom_received != NULL){
        free(peer_bloom_received);
    }
    if(keys != NULL){
        free(keys);
    }

    hdestroy();
    if(comm_fd >= 0){
        close_communication(process_id, comm_fd);
    }

    char filepath[256];
    snprintf(filepath, sizeof(filepath), "%s/bloom_process_%d.dat", BLOOM_FILE_DIR, process_id);
    unlink(filepath);
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

//Receive keys and add to array before hashing
void assign_keys_from_message(const char *msg){
    const char *ptr = msg+5;
    char *copy = strdup(ptr);
    char *tok = strtok(copy, ",");
    while(tok != NULL){
        if(num_keys >= keys_capacity){
            int new_capacity = keys_capacity == 0 ? 100000 : keys_capacity * 2;
            int *new_keys = realloc(keys, new_capacity * sizeof(int));
            if(new_keys == NULL){
                fprintf(stderr, "ERROR HAPPENED: process %d failed to allocate memory for keys \n", process_id);
                free(copy);
                exit(1);
            }
            keys = new_keys;
            keys_capacity = new_capacity;
        }
        keys[num_keys++] = atoi(tok);
        tok = strtok(NULL, ",");
    }
    free(copy);
}

//Once received all keys, hash and create bloom
void finalize_keys(){
    if(keys_finalized) return;
    if(hcreate(num_keys * 2) == 0){
        fprintf(stderr, "[ERROR HAPPENED] Process %d failed to create hash table \n", process_id);
        exit(1);
    }

    for(int i = 0; i < num_keys; i++){
        char *key_str = malloc(32);
        snprintf(key_str, 32, "%d", keys[i]);

        ENTRY e;
        e.key = key_str;
        e.data = (void*)(long)1;
        if(hsearch(e, ENTER) == NULL){
            fprintf(stderr, "Process %d failed to insert key %d\n", process_id, keys[i]);
        }
    }

    keys_finalized = 1;
    create_own_bloom_filter();
}

//Create own bloom filter after receiving all keys
void create_own_bloom_filter(){
    if(bloom_initialized){
        bloom_filter_destroy(&own_bloom);
    }
    bloom_filter_init(&own_bloom, num_keys > 0 ? num_keys:10, FALSE_POSITIVE_RATE);
    for(int i = 0; i < num_keys; i++){
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "%d", keys[i]);
        bloom_filter_add_string(&own_bloom, key_str);
    }
    bloom_initialized = 1;
    //bloom_filter_stats(&own_bloom);
}

//Once blooms are ready, broadcast to peers
void broadcast_bloom_filter(){
    if(bloom_broadcasted) return;
    char filepath[256];
    snprintf(filepath, sizeof(filepath), "%s/bloom_process_%d.dat", BLOOM_FILE_DIR, process_id);
    int result = bloom_filter_export(&own_bloom, filepath);
    if(result != BLOOM_SUCCESS){
        fprintf(stderr, "ERROR HAPPENED: process %d failed to export bloom filter", process_id);
        return;
    }

    FILE *fp = fopen(filepath, "rb");
    if(fp){
        fseek(fp, 0, SEEK_END);
        long size = ftell(fp);
        fclose(fp);
        printf("Process %d Bloom Filter size: %ld bytes (%.2f KB, %.2f MB)\n", 
               process_id, size, size/1024.0, size/(1024.0*1024.0));
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "BLOOM_FILE:%d:%s", process_id, filepath);

    for (int p = 0; p < num_processes; p++){
        if(p == process_id) continue;
        send_msg(process_id, p, msg);
    }
    bloom_broadcasted = 1;
}

//receive blooms from peers
void handle_bloom_message(const char *msg){
    if(strncmp(msg, "BLOOM_FILE:", 11) != 0){
        fprintf(stderr, "Process %d invalid bloom message \n", process_id);
        return;
    }

    int peer_id = atoi(msg+11);
    const char *colon = strchr(msg+11, ':');
    if(colon == NULL) return;

    const char *filepath = colon + 1;
    update_peer_bloom_filter_from_file(peer_id, filepath);
}

//once received blooms from peers, recreate it from the file for peers
void update_peer_bloom_filter_from_file(int peer_id, const char *filepath){
    if(peer_bloom_filters == NULL){
        peer_bloom_filters = calloc(num_processes, sizeof(BloomFilter));
        peer_bloom_received = calloc(num_processes, sizeof(int));
    }
    if(peer_bloom_received[peer_id]){
        bloom_filter_destroy(&peer_bloom_filters[peer_id]);
    }
    int result = bloom_filter_import(&peer_bloom_filters[peer_id], (char*)filepath);
    if(result == BLOOM_SUCCESS){
        peer_bloom_received[peer_id] = 1;
    } else {
        fprintf(stderr, "[ERROR HAPPENED] : Process %d failed to import bloom filter from %d\n", process_id, peer_id);
    }
}

//User query is below, it will come from manager (manager.c simulates users)
void handle_query_from_manager(const char *msg){
    int key = atoi(msg + 6);

    struct timespec own_start, own_end;
    clock_gettime(CLOCK_MONOTONIC, &own_start);
    int found_locally = check_own_keys(key);
    clock_gettime(CLOCK_MONOTONIC, &own_end);
    double own_lookup_ms = (own_end.tv_sec - own_start.tv_sec) * 1000.0 + (own_end.tv_nsec - own_start.tv_nsec) / 1000000.0;

    bloom_stats.total_own_lookup_ms += own_lookup_ms;
    bloom_stats.num_own_lookups++;

    if(found_locally){
        char response[BUF_SIZE];
        snprintf(response, sizeof(response), "FOUND:%d:PROCESS_%d", key, process_id);
        send_msg(process_id, num_processes, response);
        return;
    }
    char key_str[32];
    snprintf(key_str, sizeof(key_str), "%d", key);

    struct timespec all_peers_start, all_peers_end;
    clock_gettime(CLOCK_MONOTONIC, &all_peers_start);

    int queries_sent = 0;
    for (int p = 0; p < num_processes; p++){
        if(p == process_id) continue;
        if(peer_bloom_received != NULL && peer_bloom_received[p]){
            struct timespec single_start, single_end;
            clock_gettime(CLOCK_MONOTONIC, &single_start);

            int check_result = bloom_filter_check_string(&peer_bloom_filters[p], key_str);
            clock_gettime(CLOCK_MONOTONIC, &single_end);
            double single_check_ms = (single_end.tv_sec - single_start.tv_sec) * 1000.0 + (single_end.tv_nsec - single_start.tv_nsec) / 1000000.0;
            bloom_stats.total_single_bloom_check_ms += single_check_ms;
            bloom_stats.num_individual_bloom_checks++;
            
            if(check_result != BLOOM_FAILURE){
                char buf[BUF_SIZE];
                snprintf(buf, sizeof(buf), "PQUERY:%d:FROM_%d", key, process_id);
                send_msg(process_id, p, buf);
                queries_sent++;
            }
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &all_peers_end);
    double all_peers_ms = (all_peers_end.tv_sec - all_peers_start.tv_sec) * 1000.0 + (all_peers_end.tv_nsec - all_peers_start.tv_nsec) / 1000000.0;
    bloom_stats.total_all_peer_bloom_checks_ms += all_peers_ms;
    bloom_stats.num_query_rounds++;
    
    if(queries_sent == 0){
        char response[BUF_SIZE];
        snprintf(response, sizeof(response), "NOTFOUND:%d:CHECKED_BY_PROCESS_%d", key, process_id);
        send_msg(process_id, num_processes, response);
    }
}

//This is for handling the "redirected" query from a peer cache
void handle_query_from_process(const char *msg){
    if(strncmp(msg, "PQUERY:", 7) != 0){
        return;
    }
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
    } else{
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
    } else if (strncmp(msg, "PNOTFOUND:", 10) == 0){
        int key = atoi(msg + 10);
        const char *process_marker = strstr(msg, ":IN_PROCESS_");
        //int checked_process = -1;
        //if(process_marker != NULL){
          //  checked_process = atoi(process_marker + 12);
        //}
    }
}





int main(int argc, char *argv[]){
    process_id = atoi(argv[1]);
    num_processes = atoi(argv[2]);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    comm_fd = initiate_communication(process_id);

    char *buf = malloc(BLOOM_MSG_SIZE);

    if(buf == NULL){
        fprintf(stderr, "Process %d failed to allocate receive buffer\n", process_id);
        return 1;
    }

    while(1){
        if(bloom_initialized && !bloom_broadcasted){
            broadcast_bloom_filter();
        }

        int messages_processed = 0;
        while(1){
            int n = receive_msg(comm_fd, buf, BLOOM_MSG_SIZE);
            if(n <= 0) break;
            messages_processed++;
            if (strncmp(buf, "KEYS:", 5) == 0) {
                assign_keys_from_message(buf);
            } else if(strncmp(buf, "KEYS_DONE", 9) == 0){
                finalize_keys();
            } else if (strncmp(buf, "QUERY:", 6) == 0) {
                handle_query_from_manager(buf);
            } else if (strncmp(buf, "BLOOM_FILE:", 11) == 0) {
                handle_bloom_message(buf);
            } else if (strncmp(buf, "PQUERY:", 7) == 0) {
                handle_query_from_process(buf);
            } else if (strncmp(buf, "PFOUND:", 7) == 0 || strncmp(buf, "PNOTFOUND:", 10) == 0) {
                handle_response_from_process(buf);
            } else if(strncmp(buf, "DELETE_KEYS:", 12) == 0){
                remove_keys_from_message(buf);
            } else if(strncmp(buf, "UPDATE_KEYS:", 12) == 0){
                insert_keys_from_message(buf);
            }else if(strncmp(buf, "UPDATES_DONE", 12) == 0){
                finalize_inserts();
            } else if(strncmp(buf, "DELETE_KEYS_DONE", 16) == 0){
                finalize_deletes();
            }
            else {
                fprintf(stderr, "[Process %d] Unknown message: %s\n", process_id, buf);
            }
        }
        if (messages_processed == 0) {
            usleep(1000);
        }
    }
    free(buf);
    signal_handler(0);
    
    return 0;
}





