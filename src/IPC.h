#ifndef IPC_H

#define IPC_H
#include <stddef.h>

int initiate_communication(int process_id);
int send_msg(int sender_id, int receiver_id, const char *msg);
int receive_msg(int fd, char *buf, size_t buf_size);
void close_communication(int process_id, int fd);
void cleanup_ipc();

#endif