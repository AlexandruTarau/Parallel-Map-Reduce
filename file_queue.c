#include "file_queue.h"

File_Queue *init_queue(File *files, int file_nr) {
    File_Queue *queue = (File_Queue *)malloc(sizeof(File_Queue));
    queue->files = files;
    queue->file_nr = file_nr;
    queue->next_file = 0;
    pthread_mutex_init(&queue->mutex, NULL);

    return queue;
}

void destroy_queue(File_Queue *queue) {
    free(queue->files);
    pthread_mutex_destroy(&queue->mutex);
    free(queue);
}

File *get_and_pop_file(File_Queue *queue) {
    File *file = NULL;

    pthread_mutex_lock(&queue->mutex);

    if (queue->next_file < queue->file_nr) {
        file = &queue->files[queue->next_file++];
    }

    pthread_mutex_unlock(&queue->mutex);

    return file;
}