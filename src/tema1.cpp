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
    // std::vector<std::pair<std::string, std::vector<int>>> *complete_map;
    std::map<std::string, std::vector<int>> *complete_map;
    pthread_mutex_t *queue_mutex;
    pthread_mutex_t *words_mutex;
    pthread_mutex_t *complete_map_mutex;
    pthread_mutex_t *files_mutex;
    pthread_barrier_t *mappers_barrier;
    pthread_barrier_t *reducers_barrier;
    std::vector<pthread_mutex_t> *map_mutexes;
    FILE **output_files;
    int num_threads;
};

void cleanup(FILE **files, File *files_struct, std::queue<File>& file_queue,
             FILE **output_files, FILE *input_file, int n) {

    // Free dynamically allocated memory for file text
    while (!file_queue.empty()) {
        File file = file_queue.front();
        file_queue.pop();
        free(file.text);  // Free dynamically allocated file content
    }

    // Free dynamically allocated memory for file text
    for (int i = 0; i < n; i++) {
        if (files_struct[i].text != NULL) {
            free(files_struct[i].text);  // Free dynamically allocated file content
        }
    }
    free(files_struct);  // Free dynamically allocated file structures

    // Close the output files
    for (int i = 0; i < 26; i++) {
        if (output_files[i] != NULL) {
            fclose(output_files[i]);
        }
    }

    // Close the input files
    for (int i = 0; i < n; i++) {
        if (files[i] != NULL) {
            fclose(files[i]);
        }
    }
    free(files);

    // Close the input file
    if (input_file != NULL) {
        fclose(input_file);
    }
}


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

void *map_func(void *args) {
    ThreadArgs *thread_args = (ThreadArgs *)args;
    long id = thread_args->id;
    std::queue<File> &file_queue = *thread_args->file_queue;
    std::vector<std::map<std::pair<std::string, int>, bool>> &partial_maps = *thread_args->partial_maps;
    std::vector<std::pair<std::string, int>> &words = *thread_args->words;
    pthread_mutex_t &queue_mutex = *thread_args->queue_mutex;
    pthread_mutex_t &words_mutex = *thread_args->words_mutex;
    pthread_barrier_t &mappers_barrier = *thread_args->mappers_barrier;
    std::vector<pthread_mutex_t> &map_mutexes = *thread_args->map_mutexes;
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

        char *aux = (char *)malloc((file.size + 1) * sizeof(char));
        strcpy(aux, file.text);

        char *save_ptr;
        char *word = strtok_r(aux, " \n\t", &save_ptr);
        while (word != NULL) {
            bool found = false;
            remove_special_chars(word);
            if (word[0] == '\0') {
                word = strtok_r(NULL, " \n\t", &save_ptr);
                continue;
            }

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

            word = strtok_r(NULL, " \n\t", &save_ptr);
        }
        free(aux);
    }
    
    pthread_barrier_wait(&mappers_barrier);

    return NULL;
}

void *reduce_func(void *args) {
    ThreadArgs *thread_args = (ThreadArgs *)args;
    long id = thread_args->id;
    std::vector<std::pair<std::string, int>> &words = *thread_args->words;
    pthread_mutex_t &complete_map_mutex = *thread_args->complete_map_mutex;
    //std::vector<std::pair<std::string, std::vector<int>>> &complete_map = *thread_args->complete_map;
    std::map<std::string, std::vector<int>> &complete_map = *thread_args->complete_map;
    pthread_barrier_t &reducers_barrier = *thread_args->reducers_barrier;
    int array_size = words.size();
    int num_threads = thread_args->num_threads;
    FILE **output_files = thread_args->output_files;
    pthread_mutex_t *files_mutexes = thread_args->files_mutex;
    
    int start = id * (double)array_size / num_threads;
    int end = min((id + 1) * (double)array_size / num_threads, array_size);

    for (int i = start; i < end; i++) {
        std::pair<std::string, int> word = words[i];
        pthread_mutex_lock(&complete_map_mutex);

        // auto it = std::find_if(
        //     complete_map.begin(),
        //     complete_map.end(),
        //     [&](const auto &p) {
        //     return p.first == word.first;
        // });

        // If the word is in the map, add the file id to the vector
        if (complete_map.find(word.first) != complete_map.end()) {
            complete_map[word.first].push_back(word.second);
        } else {
            std::vector<int> v;
            v.push_back(word.second);
            complete_map[word.first] = v;
        }
        /*auto it = complete_map.begin();
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
        }*/
        
        pthread_mutex_unlock(&complete_map_mutex);
    }

    pthread_barrier_wait(&reducers_barrier);

    int complete_map_size = complete_map.size();
    start = id * complete_map_size / num_threads;
    end = min((id + 1) * complete_map_size / num_threads, complete_map_size);

    auto it = complete_map.begin();
    std::advance(it, start);
    auto it_end = complete_map.begin();
    std::advance(it_end, end);

    for (; it != it_end; it++) {
        int letter = it->first[0] - 'a';

        pthread_mutex_lock(&files_mutexes[letter]);

        fprintf(output_files[letter], "%s:[", it->first.c_str());
        for (long unsigned int j = 0; j < it->second.size(); j++) {
            fprintf(output_files[letter], "%d", it->second[j]);
            if (j != it->second.size() - 1) {
                fprintf(output_files[letter], " ");
            }
        }
        fprintf(output_files[letter], "]\n");

        pthread_mutex_unlock(&files_mutexes[letter]);
    }

    // for (int i = start; i < end; i++) {
    //     int letter = complete_map[i].first[0] - 'a';

    //     pthread_mutex_lock(&files_mutexes[letter]);

    //     fprintf(output_files[letter], "%s:[", complete_map[i].first.c_str());
    //     for (long unsigned int j = 0; j < complete_map[i].second.size(); j++) {
    //         fprintf(output_files[letter], "%d", complete_map[i].second[j]);
    //         if (j != complete_map[i].second.size() - 1) {
    //             fprintf(output_files[letter], " ");
    //         }
    //     }
    //     fprintf(output_files[letter], "]\n");
    //     pthread_mutex_unlock(&files_mutexes[letter]);
    // }

    pthread_barrier_wait(&reducers_barrier);

    it = complete_map.begin();
    std::advance(it, start);

    for (; it != it_end; it++) {
        int letter = it->first[0] - 'a';
        std::map<std::string, std::vector<int>> file_words;
        char *line = NULL;
        size_t len = 0;

        pthread_mutex_lock(&files_mutexes[letter]);
        fseek(output_files[letter], 0, SEEK_SET);

        std::vector<std::pair<std::string, std::vector<int>>> file_words_vec;

        // Parse file lines directly into file_words_vec
        while (getline(&line, &len, output_files[letter]) != -1) {
            char *save_ptr;
            char *word = strtok_r(line, " \n\t:[]", &save_ptr);
            std::string word_str = word;
            std::vector<int> v;
            while ((word = strtok_r(NULL, " \n\t:[]", &save_ptr)) != NULL) {
                v.push_back(atoi(word));
            }
            std::sort(v.begin(), v.end());
            file_words_vec.push_back({word_str, v});
        }

        // Sort file_words_vec using your custom comparator
        std::sort(file_words_vec.begin(), file_words_vec.end(), compare_complete_map);

        fseek(output_files[letter], 0, SEEK_SET);

        for (long unsigned int j = 0; j < file_words_vec.size(); j++) {
            fprintf(output_files[letter], "%s:[", file_words_vec[j].first.c_str());
            for (long unsigned int k = 0; k < file_words_vec[j].second.size(); k++) {
                fprintf(output_files[letter], "%d", file_words_vec[j].second[k]);
                if (k != file_words_vec[j].second.size() - 1) {
                    fprintf(output_files[letter], " ");
                }
            }
            fprintf(output_files[letter], "]\n");
        }
        pthread_mutex_unlock(&files_mutexes[letter]);
    }
/*
    for (int i = start; i < end; i++) {
        int letter = complete_map[i].first[0] - 'a';
        std::vector<std::pair<std::string, std::vector<int>>> file_words;
        char *line = NULL;
        size_t len = 0;

        pthread_mutex_lock(&files_mutexes[letter]);
        fseek(output_files[letter], 0, SEEK_SET);

        // Parce each line and store it in a vector
        char *save_ptr;
        while (getline(&line, &len, output_files[letter]) != -1) {
            char *word = strtok_r(line, " \n\t:[]", &save_ptr);
            std::string word_str = word;
            std::vector<int> v;
            while ((word = strtok_r(NULL, " \n\t:[]", &save_ptr)) != NULL) {
                v.push_back(atoi(word));
            }
            std::sort(v.begin(), v.end());

            file_words.push_back(std::make_pair(word_str, v));
        }

        std::sort(file_words.begin(), file_words.end(), compare_complete_map);

        fseek(output_files[letter], 0, SEEK_SET);

        for (long unsigned int j = 0; j < file_words.size(); j++) {
            fprintf(output_files[letter], "%s:[", file_words[j].first.c_str());
            for (long unsigned int k = 0; k < file_words[j].second.size(); k++) {
                fprintf(output_files[letter], "%d", file_words[j].second[k]);
                if (k != file_words[j].second.size() - 1) {
                    fprintf(output_files[letter], " ");
                }
            }
            fprintf(output_files[letter], "]\n");
        }
        pthread_mutex_unlock(&files_mutexes[letter]);
    }
*/
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
    int n;  // Number of files
    FILE *input_file = fopen(argv[3], "r");
    FILE **files;

    pthread_barrier_t mappers_barrier;
    pthread_barrier_t reducers_barrier;
    std::vector<std::map<std::pair<std::string, int>, bool>> partial_maps;
    std::vector<std::pair<std::string, int>> words;
    //std::vector<std::pair<std::string, std::vector<int>>> complete_map;
    std::map<std::string, std::vector<int>> complete_map;
    std::queue<File> file_queue;
    std::vector<pthread_mutex_t> map_mutexes;
    pthread_mutex_t complete_map_mutex;
    pthread_mutex_t queue_mutex;
    pthread_mutex_t words_mutex;
    FILE *output_files[26];
    pthread_mutex_t files_mutexes[26];

    std::vector<ThreadArgs> thread_args(mappers_nr + reducers_nr);

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
            cleanup(NULL, NULL, file_queue, output_files, input_file, n);
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
        cleanup(NULL, NULL, file_queue, output_files, input_file, n);
        exit(-1);
    }

    // Read the files
    r = fscanf(input_file, "%d", &n);
    if (r != 1) {
        printf("ERROR: couldn't read the number of files\n");
        cleanup(NULL, NULL, file_queue, output_files, input_file, n);
        exit(-1);
    }
    files = (FILE **)malloc(n * sizeof(FILE *));

    for (int i = 0; i < n; i++) {
        char file_name[200];
        fscanf(input_file, "%s", file_name);
        files[i] = fopen(file_name, "r");

        // Check if the file was opened successfully
        if (files[i] == NULL) {
            printf("ERROR: couldn't open file %s", file_name);
            cleanup(files, NULL, file_queue, output_files, input_file, n);
            exit(-1);
        }
    }

    // Sort the files by size in descending order
    File *files_struct = (File *)malloc(n * sizeof(File));
    if (files_struct == NULL) {
        fprintf(stderr, "Error allocating memory for files_struct\n");
        cleanup(files, NULL, file_queue, output_files, input_file, n);
        exit(1);
    }

    for (int i = 0; i < n; i++) {
        files_struct[i].id = i + 1;
        files_struct[i].size = get_file_size(files[i]);
        files_struct[i].text = (char *)malloc((files_struct[i].size + 1) * sizeof(char));
        if (files_struct[i].text == NULL) {
            fprintf(stderr, "Error allocating memory for files_struct[%d].text\n", i);
            cleanup(files, files_struct, file_queue, output_files, input_file, n);
            exit(1);
        }

        fread(files_struct[i].text, sizeof(char), files_struct[i].size, files[i]);
        files_struct[i].text[files_struct[i].size] = '\0';
    }

    qsort(files_struct, n, sizeof(File), compare_files);

    // Create the queue
    for (int i = 0; i < n; i++) {
        file_queue.push(files_struct[i]);
    }

    // Create mappers and reducers
    for (id = 0; id < mappers_nr + reducers_nr; id++) {
        thread_args[id].file_queue = &file_queue;
        thread_args[id].partial_maps = &partial_maps;
        thread_args[id].words = &words;
        thread_args[id].queue_mutex = &queue_mutex;
        thread_args[id].words_mutex = &words_mutex;
        thread_args[id].mappers_barrier = &mappers_barrier;
        thread_args[id].reducers_barrier = &reducers_barrier;
        thread_args[id].map_mutexes = &map_mutexes;
        thread_args[id].files_mutex = files_mutexes;
        thread_args[id].complete_map_mutex = &complete_map_mutex;
        thread_args[id].complete_map = &complete_map;
        thread_args[id].output_files = output_files;

        if (id < mappers_nr) {
            thread_args[id].id = id;
            thread_args[id].num_threads = mappers_nr;
            partial_maps[id] = std::map<std::pair<std::string, int>, bool>();
            r = pthread_create(&mappers[id], NULL, map_func, &thread_args[id]);
        } else {
            thread_args[id].id = id - mappers_nr;
            thread_args[id].num_threads = reducers_nr;
            if (id == mappers_nr) {  // Wait for the mappers to finish
                pthread_barrier_wait(&mappers_barrier);
            }

            int reducer_id = id - mappers_nr;
            r = pthread_create(&reducers[reducer_id], NULL, reduce_func, &thread_args[id]);
        }
        if (r) {
            printf("ERROR: couldn't create thread %ld", thread_args[id].id);
            cleanup(files, files_struct, file_queue, output_files, input_file, n);
            exit(-1);
        }
    }

    // Join mappers and reducers
    for (id = 0; id < mappers_nr; id++) {
        r = pthread_join(mappers[id], &status);

        if (r) {
            printf("ERROR: couldn't join thread %ld", id);
            cleanup(files, files_struct, file_queue, output_files, input_file, n);
            exit(-1);
        }
    }

    for (id = 0; id < reducers_nr; id++) {
        r = pthread_join(reducers[id], &status);

        if (r) {
            printf("ERROR: couldn't join thread %ld", id);
            cleanup(files, files_struct, file_queue, output_files, input_file, n);
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
    if (pthread_barrier_destroy(&mappers_barrier) != 0) {
        fprintf(stderr, "Error destroying mappers_barrier\n");
    }
    if (pthread_barrier_destroy(&reducers_barrier) != 0) {
        fprintf(stderr, "Error destroying reducers_barrier\n");
    }

    cleanup(files, files_struct, file_queue, output_files, input_file, n);

    return 0;
}