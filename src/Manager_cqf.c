#define POSIX_C_SOURCE 199309L
#define _DEFAULT_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>
#include "IPC.h"

int num_local_query = 0;
int num_remote_query = 0;
int num_nonexisting_query = 0;

//We are sending a large number of keys (although in chunks, so define the max message length and the number of keys per chunk)
#define MAX_MSG_LEN 65536 
#define MAX_KEYS_PER_CHUNK 7000 

//We need some time for exchanging bloom filters (or other data structures)
#define DT_EXCHANGE_TIME 120 //MAY NEED TO ADAPT BASED ON THE COUNT OF KEYS, SIZE

//These are for "random" querying, the percentage of local, remote and non existing keys
#define PCT_LOCAL 30
#define PCT_REMOTE 40
#define PCT_MISS 30

//We define the number of keys that we delete and insert as "updates" to test the configuration time of the data structures
int num_delete_per_process;
int num_insert_per_process;
int num_all_deletes;
int num_all_inserts;


int num_processes; 
int keys_per_process; 


pid_t *process_pids;
int manager_fd;


int *all_keys;
int total_keys;


int **process_keys;
int *process_key_counts;


typedef struct{
    int key;
    int answered;
} QueryTracker;

QueryTracker *query_trackers = NULL;
int num_queries_total = 0;


int *all_update_keys;
int **process_update_keys;
int *process_update_key_counts;
int updates_per_process;

//WHEN RUNNING, WE NEED TO DEFINE PROCESS_BLOOM or PROCESS_CQF as Binaries
void create_processes(){
    process_pids = malloc(num_processes * sizeof(pid_t));

    const char *process_binary = getenv("PROCESS_BINARY");
    if(process_binary == NULL){
        process_binary = "./process_cqf";
    }

    for(int i = 0; i < num_processes; i++){
        pid_t pid = fork();

        if(pid == 0){
            char process_id_str[16];
            char num_proc_str[16];

            snprintf(process_id_str, sizeof(process_id_str), "%d", i);
            snprintf(num_proc_str, sizeof(num_proc_str), "%d", num_processes);

            execl(process_binary, "process", process_id_str, num_proc_str, NULL);
            perror("ERROR HAPPENED: execl failed");
            exit(1);
        } else if(pid >0){
            process_pids[i] = pid;
            //printf("Process %d is created with PID %d\n", i, pid);
        } else{
            perror("ERROR HAPPENED : fork failed");
            exit(1);
        }
    }
    sleep(5);
}

//This function creates radom keys for all processes, stores in an array, and for each process, we store the corresponding keys in corresponding indexes
void create_random_keys(){
    total_keys = num_processes * keys_per_process;
    all_keys = malloc(total_keys * sizeof(int));

    if(all_keys == NULL){
        fprintf(stderr, "[ERROR HAPPENED] Manager failed to allocate memory for %d keys\n", total_keys);
        exit(1);
    }

    process_keys = malloc(num_processes * sizeof(int*));
    process_key_counts = calloc(num_processes, sizeof(int));

    for(int p = 0; p < num_processes; p++){
        process_keys[p] = malloc(keys_per_process * sizeof(int));
    }

    printf("Manager creating %d random keys\n", total_keys);

    srand(time(NULL));

    for(int i = 0; i < total_keys; i++){
        all_keys[i] = rand() % 100000000; 
        int process_id = i / keys_per_process;
        int key_index = i % keys_per_process;
        process_keys[process_id][key_index] = all_keys[i];
        process_key_counts[process_id]++;

        
    }
   
}

//We send the keys in chunks to processes; 
//The message starts with "OWN_KEYS:" and "ALL_KEYS:", so we can define the message type in the processes;
//After sending a process its local keys, we send the keys for the remaining processes
//We could do this from process to process, but it adds complexity and doesn't make any difference for the project scope
//We can change this if needed later
//Once we send all keys to a process, we send "KEYS_DONE" to let the process that it can create hash table, bloom filter, etc;
void assign_keys_to_all_processes(){
    printf("\nManager distributing all keys to all processes\n");
    for(int p = 0 ; p < num_processes; p++){
        int keys_sent = 0;

        while(keys_sent < keys_per_process){
            char msg[MAX_MSG_LEN];
            int msg_pos = sprintf(msg, "OWN_KEYS:");
            int keys_in_chunk = 0;

            while(keys_sent < keys_per_process && keys_in_chunk < MAX_KEYS_PER_CHUNK){
                char key_str[20];
                int key_str_len;

                if(keys_in_chunk == 0){
                    key_str_len = snprintf(key_str, sizeof(key_str), "%d", 
                                          process_keys[p][keys_sent]);
                } else {
                    key_str_len = snprintf(key_str, sizeof(key_str), ",%d", 
                                          process_keys[p][keys_sent]);
                }

                if(msg_pos + key_str_len >= MAX_MSG_LEN - 1) break;

                strcpy(msg + msg_pos, key_str);
                msg_pos += key_str_len;
                keys_in_chunk++;
                keys_sent++;
            }
            send_msg(num_processes, p, msg);
            usleep(100);
        }
    }

    printf("Sharing keys between processes\n");

    for(int owner_process = 0; owner_process < num_processes; owner_process++){
        

        for (int receiver = 0; receiver < num_processes; receiver++){
            int keys_sent = 0;

            while(keys_sent < keys_per_process){
                char msg[MAX_MSG_LEN];
                int msg_pos = sprintf(msg, "ALL_KEYS:%d:", owner_process);
                int keys_in_chunk = 0;

                while(keys_sent < keys_per_process && keys_in_chunk < MAX_KEYS_PER_CHUNK){
                    char key_str[20];
                    int key_str_len;

                    if(keys_in_chunk == 0){
                        key_str_len = snprintf(key_str, sizeof(key_str), "%d", 
                                              process_keys[owner_process][keys_sent]);
                    } else {
                        key_str_len = snprintf(key_str, sizeof(key_str), ",%d", 
                                              process_keys[owner_process][keys_sent]);
                    }

                    if(msg_pos + key_str_len >= MAX_MSG_LEN - 1) break;

                    strcpy(msg + msg_pos, key_str);
                    msg_pos += key_str_len;
                    keys_in_chunk++;
                    keys_sent++;
                }
                send_msg(num_processes, receiver, msg);
                usleep(100);
            }
        }
    }
    sleep(DT_EXCHANGE_TIME);
    for(int p = 0; p < num_processes; p++){
        send_msg(num_processes, p, "KEYS_DONE");
    }
    
    sleep(DT_EXCHANGE_TIME);
}



//Once the process looks for a key in its own hash table, peers' bloom/cq/cb filters, and queries peer processes,
//it sends a response to the manager/user
//The format is either "FOUND:" or "NOTFOUND:
//Not using the "NOTFOUND" any more, it was for error detection. Since we added random queries (for nonexisting keys), we comment this out
void handle_process_response(const char *msg){
    if(strncmp(msg, "FOUND:", 6) == 0){
        int key = atoi(msg + 6);
        const char *process_marker = strstr(msg, ":PROCESS_");
        //int found_in_process = -1;
        //if(process_marker != NULL){
          //  found_in_process = atoi(process_marker+9);
        //}
        
        for(int i = 0; i < num_queries_total; i++){
            if(query_trackers[i].key == key && !query_trackers[i].answered){
                query_trackers[i].answered = 1;
                break;
            }
        }
        
    } /*else if(strncmp(msg, "NOTFOUND:", 9) == 0){
        int key = atoi(msg + 9);
        const char *process_marker = strstr(msg, ":CHECKED_BY_PROCESS_");
        int checked_process = -1;
        //We can bring this back for debugging, but it works for now, no need

        //if(process_marker != NULL){
            //printf("Manager received not found signal for Key %d Checked by process %d\n", key, checked_process);
        //}
        for (int i = 0; i < num_queries_total; i++) {
            if (query_trackers[i].key == key && !query_trackers[i].answered) {
                query_trackers[i].answered = 1;
                //We can comment out, but it is better to keep for error detection
                //printf("KEY %d NOT FOUND (ERROR - should exist!)\n", key);
                break;
            }
        }
    } */
}

//This function creates a list of keys for updates (new insertions)
//To make sure that each process receives approximately same number of insertions, we use the same algorithm as initial insertions
void create_update_random_keys(){
    all_update_keys = malloc(num_all_inserts * sizeof(int));
    process_update_keys = malloc(num_processes * sizeof(int*));
    process_update_key_counts = calloc(num_processes, sizeof(int));
    updates_per_process = num_insert_per_process;
    for(int p = 0; p < num_processes; p++){
        process_update_keys[p] = malloc(updates_per_process * sizeof(int));
    }
    for(int i = 0; i < num_all_inserts; i++){
        all_update_keys[i] = rand() % 100000000;
        int process_id = i / updates_per_process;
        int key_index = i % updates_per_process;
        process_update_keys[process_id][key_index] = all_update_keys[i];
        process_update_key_counts[process_id]++;
    }
}

//This function is for sending the new insertions in chunks
//We use the same algorithm as we used in the initial insertions/
//In the new insertions, we use "ALL_UPDATE_KEYS:" message
void assign_update_to_all_processes(){
    updates_per_process = num_insert_per_process;

    for(int owner_process = 0; owner_process < num_processes; owner_process++){
        for(int receiver = 0; receiver < num_processes; receiver++){
            int keys_sent = 0;
            while(keys_sent < updates_per_process){
                char msg[MAX_MSG_LEN];
                int msg_pos = sprintf(msg, "ALL_UPDATE_KEYS:%d:", owner_process);
                int keys_in_chunk = 0;

                while(keys_sent < updates_per_process && keys_in_chunk < MAX_KEYS_PER_CHUNK){
                    char key_str[20];
                    int key_str_len;

                    if(keys_in_chunk == 0){
                        key_str_len = snprintf(key_str, sizeof(key_str), "%d", 
                                              process_update_keys[owner_process][keys_sent]);
                    } else {
                        key_str_len = snprintf(key_str, sizeof(key_str), ",%d", 
                                              process_update_keys[owner_process][keys_sent]);
                    }

                    if(msg_pos + key_str_len >= MAX_MSG_LEN - 1) break;

                    strcpy(msg + msg_pos, key_str);
                    msg_pos += key_str_len;
                    keys_in_chunk++;
                    keys_sent++;
                }
                send_msg(num_processes, receiver, msg);
                usleep(100);
            }
        }
    }
    sleep(DT_EXCHANGE_TIME);
    
}


//This is for sending deletes in chunks
//The message format is "DELETE_KEYS:"
void send_deletes(){
    updates_per_process = num_delete_per_process;
    for(int p = 0; p < num_processes; p++){
        int *delete_indices = malloc(updates_per_process * sizeof(int));
        int *used = calloc(total_keys, sizeof(int));
        int picked = 0;
        while(picked < updates_per_process){
            int idx = rand() % keys_per_process;
            if(!used[idx]){
                used[idx] = 1;
                delete_indices[picked++] = idx;
            }
        }

        
        int di = 0;
        int cr_idx = 0;
        while(di < updates_per_process){
            char msg[MAX_MSG_LEN];
            int msg_pos = sprintf(msg, "DELETE_KEYS:%d:", p);
            int keys_in_chunk = 0;
            while(di < updates_per_process && keys_in_chunk < MAX_KEYS_PER_CHUNK){
                char key_str[32];
                int key_str_len;
                cr_idx = delete_indices[di] + p * keys_per_process;
                if(keys_in_chunk == 0){
                    key_str_len = snprintf(key_str, sizeof(key_str), "%d", all_keys[cr_idx]);
                } else {
                    key_str_len = snprintf(key_str, sizeof(key_str), ",%d", all_keys[cr_idx]);
                } 
                if(msg_pos + key_str_len >= MAX_MSG_LEN - 1) break;
                strcpy(msg + msg_pos, key_str);
                msg_pos += key_str_len;
                keys_in_chunk++;
                di++;
            }

            for(int k = 0; k < num_processes; k++){
                send_msg(num_processes, k, msg);
                usleep(100);
            }
        }
        free(delete_indices);
        free(used);
    }
}

//This is for random querying (actually not fully random), we divide it to 30/40/30 percentage
//wE HAVE defined the MACROS above, the user can change it 
//30 - Local keys
//40 - Remote keys (in peers)
//30 - that do not exist in any cache
void do_random_queries(int num_queries){
    char response_buf[MAX_MSG_LEN];
    //int num_queries = 100000; //NUMBER OF QUERIES, WE CAN CHANGE FOR TESTS
    num_queries_total = num_queries;
    
    query_trackers = calloc(num_queries, sizeof(QueryTracker));

    struct timespec *query_start_times = calloc(num_queries, sizeof(struct timespec));
    struct timespec *query_end_times = calloc(num_queries, sizeof(struct timespec));

    int responses_collected = 0;

    for(int i = 0; i < num_queries; i++){
        int r = rand() % 100;
        int key_index;
        int actual_process;
        int query_key;
        if(r < (PCT_LOCAL + PCT_REMOTE)){
            key_index = rand() % total_keys;
            actual_process = key_index / keys_per_process;
            query_key = all_keys[key_index];


        } else{
            key_index = rand() % total_keys + total_keys;
        }
        char query_msg[MAX_MSG_LEN];
        int target_process;

        if(r < PCT_LOCAL){
            num_local_query++;
            target_process = actual_process;
        } else if(r < PCT_LOCAL + PCT_REMOTE){
            num_remote_query++;
            do{
                target_process = rand() % num_processes;
            } while(target_process == actual_process);
        } else{
            num_nonexisting_query++;
            target_process = rand() % num_processes;
        }
        

        query_trackers[i].key = query_key;
        query_trackers[i].answered = 0;

        clock_gettime(CLOCK_MONOTONIC, &query_start_times[i]);

        snprintf(query_msg, sizeof(query_msg), "QUERY:%d", query_key);
        send_msg(num_processes, target_process, query_msg);

        if(i%5 == 0){
            while(1){
                int n = receive_msg(manager_fd, response_buf, sizeof(response_buf));
                if(n <= 0) break;

                int response_key = -1;
                if(strncmp(response_buf, "FOUND:", 6) == 0){
                    response_key = atoi(response_buf + 6);
                } /*else if(strncmp(response_buf, "NOTFOUND:", 9) == 0){
                    response_key = atoi(response_buf+9);
                }*/

                for(int k = 0; k <= i; k++){
                    if(query_trackers[k].key == response_key && !query_trackers[k].answered){
                        clock_gettime(CLOCK_MONOTONIC, &query_end_times[k]);
                        query_trackers[k].answered = 1;
                        responses_collected++;
                        break;
                    }
                }
                handle_process_response(response_buf);
            }
        }

        usleep(100);
        
        
    }

   

    int max_wait_iterations = 10000; 
    int iterations = 0;

    while(responses_collected < num_queries && iterations < max_wait_iterations){
        int n = receive_msg(manager_fd, response_buf, sizeof(response_buf));
        if(n > 0){
            int response_key = -1;
            if(strncmp(response_buf, "FOUND:", 6) == 0){
                response_key = atoi(response_buf + 6);
            } /*else if(strncmp(response_buf, "NOTFOUND:", 9) == 0){
                response_key = atoi(response_buf + 9);
            }*/
            
            for (int k = 0; k < num_queries; k++) {
                if (query_trackers[k].key == response_key && !query_trackers[k].answered) {
                    clock_gettime(CLOCK_MONOTONIC, &query_end_times[k]);
                    query_trackers[k].answered = 1;
                    responses_collected++;
                    
                    /*if(responses_collected % 1000 == 0){
                        printf("%d/%d responses collected\n", responses_collected, num_queries);
                        fflush(stdout);
                    }*/
                    break;
                }
            }
            handle_process_response(response_buf);
        }
        usleep(100);
        iterations++;
    }

    

    double total_query_time_ms = 0;
    double min_time = 999999.0;
    double max_time = 0.0;
    int queries_with_timing = 0;

    for(int i = 0; i < num_queries; i++){
        if(query_trackers[i].answered){
            if(query_end_times[i].tv_sec >= query_start_times[i].tv_sec){
                double elapsed_ms = (query_end_times[i].tv_sec - query_start_times[i].tv_sec) * 1000.0 + (query_end_times[i].tv_nsec - query_start_times[i].tv_nsec) / 1000000.0;

                if(elapsed_ms >= 0){
                    total_query_time_ms += elapsed_ms;
                    queries_with_timing++;

                    if(elapsed_ms < min_time) min_time = elapsed_ms;
                    if(elapsed_ms > max_time) max_time = elapsed_ms;
                }
            }
        }
    }

    int found_count = queries_with_timing;
    

    printf("\nRESULTS:\n");
    printf("Queries sent: %d\n", num_queries);
    printf("Queries answered: %d\n", found_count);
    printf("Avg time: %.2f ms\n", queries_with_timing > 0 ? total_query_time_ms / queries_with_timing : 0);
    printf("Min time: %.2f ms\n", min_time);
    printf("Max time: %.2f ms\n", max_time);
    free(query_trackers);
    free(query_start_times);
    free(query_end_times);
}


//This querying function is used for specific queries, 
//In other words, we do not send queries to a process for a key that exists in local
//So that each process needs to check the blooms/cqfs/cbfs and query peer processes
//ALso we make sure that the key exists in at least one cache
void do_specific_queries(int num_queries){
    char response_buf[MAX_MSG_LEN];
    //int num_queries = 100000; //NUMBER OF QUERIES, WE CAN CHANGE FOR TESTS
    num_queries_total = num_queries;
     
    query_trackers = calloc(num_queries, sizeof(QueryTracker));

    struct timespec *query_start_times = calloc(num_queries, sizeof(struct timespec));
    struct timespec *query_end_times = calloc(num_queries, sizeof(struct timespec));

    int responses_collected = 0;

    for(int i = 0; i < num_queries; i++){
        char query_msg[MAX_MSG_LEN];
        int key_index = rand() % total_keys;
        int query_key = all_keys[key_index];
        int actual_process = key_index / keys_per_process;
        int target_process;

        //We do not send the query to the cache that owns (so that we can query other peer processes)
        do{
            target_process = rand() % num_processes;
        } while(target_process == actual_process);

        query_trackers[i].key = query_key;
        query_trackers[i].answered = 0;

        clock_gettime(CLOCK_MONOTONIC, &query_start_times[i]);

        snprintf(query_msg, sizeof(query_msg), "QUERY:%d", query_key);
        send_msg(num_processes, target_process, query_msg);

        if(i%5 == 0){
            while(1){
                int n = receive_msg(manager_fd, response_buf, sizeof(response_buf));
                if(n <= 0) break;

                int response_key = -1;
                if(strncmp(response_buf, "FOUND:", 6) == 0){
                    response_key = atoi(response_buf + 6);
                } else if(strncmp(response_buf, "NOTFOUND:", 9) == 0){
                    response_key = atoi(response_buf+9);
                }

                for(int k = 0; k <= i; k++){
                    if(query_trackers[k].key == response_key && !query_trackers[k].answered){
                        clock_gettime(CLOCK_MONOTONIC, &query_end_times[k]);
                        query_trackers[k].answered = 1;
                        responses_collected++;
                        break;
                    }
                }
                handle_process_response(response_buf);
            }
        }

        usleep(100);
        
        
    }

    

    int max_wait_iterations = 10000; 
    int iterations = 0;

    while(responses_collected < num_queries && iterations < max_wait_iterations){
        int n = receive_msg(manager_fd, response_buf, sizeof(response_buf));
        if(n > 0){
            int response_key = -1;
            if(strncmp(response_buf, "FOUND:", 6) == 0){
                response_key = atoi(response_buf + 6);
            } else if(strncmp(response_buf, "NOTFOUND:", 9) == 0){
                response_key = atoi(response_buf + 9);
            }
            
            for (int k = 0; k < num_queries; k++) {
                if (query_trackers[k].key == response_key && !query_trackers[k].answered) {
                    clock_gettime(CLOCK_MONOTONIC, &query_end_times[k]);
                    query_trackers[k].answered = 1;
                    responses_collected++;
                    
                    break;
                }
            }
            handle_process_response(response_buf);
        }
        usleep(100);
        iterations++;
    }

    

    double total_query_time_ms = 0;
    double min_time = 999999.0;
    double max_time = 0.0;
    int queries_with_timing = 0;

    for(int i = 0; i < num_queries; i++){
        if(query_trackers[i].answered){
            if(query_end_times[i].tv_sec >= query_start_times[i].tv_sec){
                double elapsed_ms = (query_end_times[i].tv_sec - query_start_times[i].tv_sec) * 1000.0 + (query_end_times[i].tv_nsec - query_start_times[i].tv_nsec) / 1000000.0;

                if(elapsed_ms >= 0){
                    total_query_time_ms += elapsed_ms;
                    queries_with_timing++;

                    if(elapsed_ms < min_time) min_time = elapsed_ms;
                    if(elapsed_ms > max_time) max_time = elapsed_ms;
                }
            }
        }
    }

    int found_count = queries_with_timing;
    

    printf("\nRESULTS:\n");
    printf("Queries sent: %d\n", num_queries);
    printf("Queries answered: %d\n", found_count);
    printf("Avg time: %.2f ms\n", queries_with_timing > 0 ? total_query_time_ms / queries_with_timing : 0);
    printf("Min time: %.2f ms\n", min_time);
    printf("Max time: %.2f ms\n", max_time);
    free(query_trackers);
    free(query_start_times);
    free(query_end_times);

}



int main(int argc, char *argv[]){
    if(argc < 3){
        fprintf(stderr, "Usage: %s <num_processes> <keys_per_process>\n", argv[0]);
        exit(1);
    }

    num_processes = atoi(argv[1]);
    keys_per_process = atoi(argv[2]);
    manager_fd = initiate_communication(num_processes);

    create_processes();
    create_random_keys();
    assign_keys_to_all_processes();
    int num_queries = 100000;
    
    //do_specific_queries(num_queries);
    do_random_queries(num_queries);
    printf("Local queries:%d\n", num_local_query);
    printf("Remote queries:%d\n", num_remote_query);
    printf("Nonexisting queries:%d\n", num_nonexisting_query);

    num_delete_per_process = 20000;
    num_insert_per_process = 20000;
    num_all_deletes = num_delete_per_process*num_processes;
    num_all_inserts = num_insert_per_process*num_processes;
    send_deletes();
    sleep(30);
    create_update_random_keys();
    assign_update_to_all_processes();
    

    
    for(int i = 0; i < num_processes; i++){
        kill(process_pids[i], SIGTERM);
    }
    for(int i = 0; i < num_processes; i++){
        waitpid(process_pids[i], NULL, 0);
    }
    close_communication(num_processes, manager_fd);
    free(all_keys);
    free(process_pids);
    
    for (int p = 0; p < num_processes; p++) {
        free(process_keys[p]);
    }
    free(process_keys);
    free(process_key_counts);
    free(all_update_keys);
    for(int p = 0; p < num_processes; p++){
        free(process_update_keys[p]);
    }
    free(process_update_keys);
    free(process_update_key_counts);
    return 0;
}


