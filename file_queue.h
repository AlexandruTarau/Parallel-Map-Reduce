#include <pthread.h>
#include <stdlib.h>

// File structure
typedef struct {
    int id;
    int size;
    char *text;
} File;

// Queue structure
typedef struct {
    File *files;
    int file_nr;
    int next_file;
    pthread_mutex_t mutex;
} File_Queue;

// Queue functions
File_Queue *init_queue(File *files, int file_nr);
void destroy_queue(File_Queue *queue);
File *get_and_pop_file(File_Queue *queue);