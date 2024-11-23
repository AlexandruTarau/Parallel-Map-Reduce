#include <pthread.h>
#include <stdlib.h>
#include <string.h>

// Map structure
typedef struct MapElem {
    char *word;
    int file_id;
    struct MapElem* next;
    pthread_mutex_t mutex;
} *Map, MapElem;

// Map functions
void add_word(Map *map, char *word, int file_id);
void destroy_map(Map map);