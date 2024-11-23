#include "file_map.h"

void add_word(Map *map, char *word, int file_id) {
    Map new_elem = (Map)malloc(sizeof(MapElem));
    new_elem->word = (char *)malloc(strlen(word) + 1);
    strcpy(new_elem->word, word);
    new_elem->file_id = file_id;
    new_elem->next = NULL;
    pthread_mutex_init(&new_elem->mutex, NULL);

    if (*map == NULL) {
        *map = new_elem;
        return;
    }

    Map aux = *map;
    while (aux->next != NULL) {
        aux = aux->next;
    }
    aux->next = new_elem;
}

void destroy_map(Map map) {
    Map aux;

    while (map != NULL) {
        aux = map;
        map = map->next;
        free(aux->word);
        pthread_mutex_destroy(&aux->mutex);
        free(aux);
    }
}