#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <queue>
#include <pthread.h>
#include <cstring>
#include <cstdlib>
#include <algorithm>

// File structure
typedef struct {
    int id;
    int size;
    char *text;
} File;

struct ThreadArgs {
    long id;
    std::queue<File> *file_queue;
    std::vector<std::map<std::pair<std::string, int>, bool>> *partial_maps;
    std::vector<std::pair<std::string, int>> *words;
    pthread_mutex_t *queue_mutex;
    pthread_mutex_t *words_mutex;
    pthread_barrier_t *mappers_barrier;
    pthread_barrier_t *reducers_barrier;
    std::vector<pthread_mutex_t> *map_mutexes;
    int array_size;
    int num_threads;
};

// REMOVE GLOBAL VARIABLES LATER!!!!!!!!!
pthread_barrier_t mappers_barrier;
pthread_barrier_t reducers_barrier;
std::vector<std::map<std::pair<std::string, int>, bool>> partial_maps;
std::vector<std::pair<std::string, int>> words;
std::vector<std::pair<std::string, std::vector<int>>> complete_map;
std::queue<File> file_queue;
std::vector<pthread_mutex_t> map_mutexes;
pthread_mutex_t complete_map_mutex;
pthread_mutex_t queue_mutex;
pthread_mutex_t words_mutex;
int array_size;
int num_threads;
FILE *output_files[26];
std::vector<pthread_mutex_t> files_mutexes(26);

// Function that returns the minimum of 2 numbers
double min(double x, double y) {
    return x < y ? x : y;
}

// Get the size of a file
int get_file_size(FILE *file) {
    int size = 0;

    fseek(file, 0, SEEK_END);
    size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    return size;
}

// Function that removes special characters from a word and converts it to lowercase
void remove_special_chars(char *word) {
    int i = 0, j = 0;

    while (word[i]) {
        if (isalpha(word[i])) {
            word[j] = tolower(word[i]);
            j++;
        }
        i++;
    }
    word[j] = '\0';
}

// Compare 2 files for qsort
int compare_files(const void *a, const void *b) {
    File *file1 = (File *)a;
    File *file2 = (File *)b;

    return file2->size - file1->size;
}

// Compare 2 elements of complete_map for qsort
bool compare_complete_map(const std::pair<std::string, std::vector<int>>& a, const std::pair<std::string, std::vector<int>>& b) {
    if (a.second.size() != b.second.size()) {
        return a.second.size() > b.second.size(); // Sort by vector size in descending order
    }
    return a.first < b.first; // Sort by string in ascending order
}

void *map_func(void *arg) {
    long id = *(long *)arg;
    File file;

    while (1) {
        pthread_mutex_lock(&queue_mutex);
        if (file_queue.empty()) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        file = file_queue.front();
        file_queue.pop();
        pthread_mutex_unlock(&queue_mutex);

        char *aux = (char *)malloc(file.size * sizeof(char));
        strcpy(aux, file.text);

        char *save_ptr;
        char *word = strtok_r(aux, " \n\t,", &save_ptr);
        while (word != NULL) {
            bool found = false;
            remove_special_chars(word);

            // Add the word to the partial map
            pthread_mutex_lock(&map_mutexes[id]);
            std::pair<std::string, int> key = std::make_pair(word, file.id);
            if (partial_maps[id].find(key) != partial_maps[id].end()) {
                found = true;
            }
            partial_maps[id][key] = true;
            pthread_mutex_unlock(&map_mutexes[id]);

            // Add the word to the words vector
            if (!found) {
                pthread_mutex_lock(&words_mutex);
                words.push_back(key);
                pthread_mutex_unlock(&words_mutex);
            }

            word = strtok_r(NULL, " \n\t,", &save_ptr);
        }
        free(aux);
    }
    
    pthread_barrier_wait(&mappers_barrier);

    return NULL;
}

void *reduce_func(void *arg) {
    long id = *(long *)arg;
    int start = id * (double)array_size / num_threads;
    int end = min((id + 1) * (double)array_size / num_threads, array_size);

    for (int i = start; i < end; i++) {
        std::pair<std::string, int> word = words[i];
        pthread_mutex_lock(&complete_map_mutex);

        // If the word is in the map, add the file id to the vector
        auto it = complete_map.begin();
        for (; it != complete_map.end(); it++) {
            if (it->first == word.first) {
                it->second.push_back(word.second);
                break;
            }
        }

        // If the word is not in the map, add it
        if (it == complete_map.end()) {
            std::vector<int> v;
            v.push_back(word.second);
            complete_map.push_back(std::make_pair(word.first, v));
        }

        pthread_mutex_unlock(&complete_map_mutex);
    }

    pthread_barrier_wait(&reducers_barrier);

    // Temporary
    if (id == 0)
        std::sort(complete_map.begin(), complete_map.end(), compare_complete_map);

    pthread_barrier_wait(&reducers_barrier);

    if (id == 0) {
        // Print the map
        printf("Complete map:\n");
        for (long unsigned int i = 0; i < complete_map.size(); i++) {
            printf("%s: [", complete_map[i].first.c_str());
            for (long unsigned int j = 0; j < complete_map[i].second.size(); j++) {
                printf("%d", complete_map[i].second[j]);
                if (j != complete_map[i].second.size() - 1) {
                    printf(" ");
                }
            }
            printf("]\n");
        }
    }

    start = id * 26 / num_threads;
    end = min((id + 1) * 26 / num_threads, 26);

    for (int i = start; i < end; i++) {
        int letter = complete_map[i].first[0] - 'a';

        pthread_mutex_lock(&files_mutexes[letter]);
        fprintf(output_files[letter], "%s: [", complete_map[i].first.c_str());
        for (long unsigned int j = 0; j < complete_map[i].second.size(); j++) {
            fprintf(output_files[letter], "%d", complete_map[i].second[j]);
            if (j != complete_map[i].second.size() - 1) {
                fprintf(output_files[letter], " ");
            }
        }
        fprintf(output_files[letter], "]\n");
        pthread_mutex_unlock(&files_mutexes[letter]);
    }

    return NULL;
}

int main(int argc, char *argv[])
{
    // Check if the number of arguments is correct
    if (argc != 4) {
        printf("Usage: ./tema1 <mappers_number> <reducers_number> <input_file>\n");
        exit(-1);
    }

    int mappers_nr = atoi(argv[1]);
    int reducers_nr = atoi(argv[2]);
    pthread_t mappers[mappers_nr];
    pthread_t reducers[reducers_nr];
    
    int r;
    long id;
    void *status;
    long ids[mappers_nr + reducers_nr];
    int n;  // Number of files
    FILE *input_file = fopen(argv[3], "r");
    FILE **files;

    partial_maps.resize(mappers_nr);

    // Initialize the output files
    for (int i = 0; i < 26; i++) {
        char file_name[6];
        file_name[0] = 'a' + i;
        file_name[1] = '.';
        file_name[2] = 't';
        file_name[3] = 'x';
        file_name[4] = 't';
        file_name[5] = '\0';
        output_files[i] = fopen(file_name, "w+");

        // Check if the file was opened successfully
        if (output_files[i] == NULL) {
            printf("ERROR: couldn't open file %s", file_name);
            exit(-1);
        }
    }

    // Initialize the mutexes
    map_mutexes.resize(mappers_nr);
    for (int i = 0; i < mappers_nr; i++) {
        pthread_mutex_init(&map_mutexes[i], NULL);
    }
    pthread_mutex_init(&queue_mutex, NULL);
    pthread_mutex_init(&words_mutex, NULL);
    pthread_mutex_init(&complete_map_mutex, NULL);

    for (int i = 0; i < 26; i++) {
        pthread_mutex_init(&files_mutexes[i], NULL);
    }

    // Initialize the mappers and reducers barriers
    pthread_barrier_init(&mappers_barrier, NULL, mappers_nr + 1);
    pthread_barrier_init(&reducers_barrier, NULL, reducers_nr);

    // Check if the input file was opened successfully
    if (input_file == NULL) {
        printf("ERROR: couldn't open file %s\n", argv[3]);
        exit(-1);
    }

    // Read the files
    fscanf(input_file, "%d", &n);
    files = (FILE **)malloc(n * sizeof(FILE *));

    for (int i = 0; i < n; i++) {
        char file_name[100];
        fscanf(input_file, "%s", file_name);
        files[i] = fopen(file_name, "r");

        // Check if the file was opened successfully
        if (files[i] == NULL) {
            printf("ERROR: couldn't open file %s", file_name);
            exit(-1);
        }
    }

    // Sort the files by size in descending order
    File *files_struct = (File *)malloc(n * sizeof(File));

    for (int i = 0; i < n; i++) {
        files_struct[i].id = i;
        files_struct[i].size = get_file_size(files[i]);
        files_struct[i].text = (char *)malloc(files_struct[i].size * sizeof(char));
        fread(files_struct[i].text, sizeof(char), files_struct[i].size, files[i]);
    }

    qsort(files_struct, n, sizeof(File), compare_files);

    // Create the queue
    for (int i = 0; i < n; i++) {
        file_queue.push(files_struct[i]);
    }

    // Create mappers and reducers
    for (id = 0; id < mappers_nr + reducers_nr; id++) {
        ids[id] = id;

        if (id < mappers_nr) {
            partial_maps[ids[id]] = std::map<std::pair<std::string, int>, bool>();
            r = pthread_create(&mappers[id], NULL, map_func, &ids[id]);
        } else {
            if (id == mappers_nr) {  // Wait for the mappers to finish
                pthread_barrier_wait(&mappers_barrier);
                array_size = words.size();
                num_threads = reducers_nr;
            }

            int reducer_id = id - mappers_nr;
            ids[id] = reducer_id;
            r = pthread_create(&reducers[reducer_id], NULL, reduce_func, &ids[id]);
        }
        if (r) {
            printf("ERROR: couldn't create thread %ld", ids[id]);
            exit(-1);
        }
    }

    // Join mappers and reducers
    for (id = 0; id < mappers_nr; id++) {
        r = pthread_join(mappers[id], &status);

        if (r) {
            printf("ERROR: couldn't join thread %ld", id);
            exit(-1);
        }
    }

    for (id = 0; id < reducers_nr; id++) {
        r = pthread_join(reducers[id], &status);

        if (r) {
            printf("ERROR: couldn't join thread %ld", id);
            exit(-1);
        }
    }

    // Destroy the mutexes
    for (int i = 0; i < mappers_nr; i++) {
        pthread_mutex_destroy(&map_mutexes[i]);
    }
    pthread_mutex_destroy(&queue_mutex);
    pthread_mutex_destroy(&words_mutex);
    pthread_mutex_destroy(&complete_map_mutex);

    for (int i = 0; i < 26; i++) {
        pthread_mutex_destroy(&files_mutexes[i]);
    }

    // Destroy the barriers
    pthread_barrier_destroy(&mappers_barrier);
    pthread_barrier_destroy(&reducers_barrier);

    // Close the files
    for (int i = 0; i < 26; i++) {
        fclose(output_files[i]);
    }

    fclose(input_file);

    return 0;
}