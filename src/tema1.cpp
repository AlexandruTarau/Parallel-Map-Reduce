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
struct File {
    int id;
    int size;
    char *text;
};

// Thread arguments structure
struct ThreadArgs {
    long id;
    int num_threads;
    std::queue<File> *file_queue;
    std::vector<std::map<std::pair<std::string, int>, bool>> *partial_maps;
    std::vector<std::pair<std::string, int>> *words;
    std::vector<std::map<std::string, std::vector<int>>> *complete_map;
    pthread_mutex_t *queue_mutex;
    pthread_mutex_t *words_mutex;
    pthread_mutex_t *letter_mutexes;
    pthread_barrier_t *barrier;
    pthread_barrier_t *reducers_barrier;
    std::vector<pthread_mutex_t> *map_mutexes;
    FILE **output_files;
};

// Clean up function
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

// Mapper function
void *map_func(void *args) {
    ThreadArgs *thread_args = (ThreadArgs *)args;
    long id = thread_args->id;
    std::queue<File> &file_queue = *thread_args->file_queue;
    std::vector<std::map<std::pair<std::string, int>, bool>> &partial_maps = *thread_args->partial_maps;
    std::vector<std::pair<std::string, int>> &words = *thread_args->words;
    pthread_mutex_t &queue_mutex = *thread_args->queue_mutex;
    pthread_mutex_t &words_mutex = *thread_args->words_mutex;
    pthread_barrier_t &barrier = *thread_args->barrier;
    
    File file;
    std::vector<std::pair<std::string, int>> local_words;

    while (1) {
        // Get the next file from the queue
        pthread_mutex_lock(&queue_mutex);
        if (file_queue.empty()) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        file = file_queue.front();
        file_queue.pop();
        pthread_mutex_unlock(&queue_mutex);

        // Parse the file
        char *save_ptr;
        char *word = strtok_r(file.text, " \n\t", &save_ptr);
        while (word != NULL) {
            bool found = false;
            remove_special_chars(word);
            if (word[0] == '\0') {  // Ignore words that contain only special characters
                word = strtok_r(NULL, " \n\t", &save_ptr);
                continue;
            }

            // Add the word to the partial map
            std::pair<std::string, int> key = std::make_pair(word, file.id);
            if (partial_maps[id].find(key) != partial_maps[id].end()) {
                found = true;
            }   
            partial_maps[id][key] = true;

            // Add the word to the local words vector
            if (!found) {
                local_words.push_back(key);
            }

            word = strtok_r(NULL, " \n\t", &save_ptr);
        }
    }

    // Add the local words to the shared words vector to minimize locks number
    pthread_mutex_lock(&words_mutex);
    words.insert(words.end(), local_words.begin(), local_words.end());
    pthread_mutex_unlock(&words_mutex);
    
    pthread_barrier_wait(&barrier);

    return NULL;
}

// Reducer function
void *reduce_func(void *args) {
    ThreadArgs *thread_args = (ThreadArgs *)args;
    long id = thread_args->id;
    pthread_barrier_t &barrier = *thread_args->barrier;

    // Wait the mappers to finish
    pthread_barrier_wait(&barrier);

    std::vector<std::pair<std::string, int>> &words = *thread_args->words;
    std::vector<std::map<std::string, std::vector<int>>> &complete_map = *thread_args->complete_map;
    pthread_barrier_t &reducers_barrier = *thread_args->reducers_barrier;
    FILE **output_files = thread_args->output_files;
    pthread_mutex_t *letter_mutexes = thread_args->letter_mutexes;

    int array_size = words.size();
    int num_threads = thread_args->num_threads;
    
    int start = id * (double)array_size / num_threads;
    int end = std::min((id + 1) * (double)array_size / num_threads, (double) array_size);

    // Add the words to the complete map
    for (int i = start; i < end; i++) {
        std::pair<std::string, int> word = words[i];
        int letter = word.first[0] - 'a';

        pthread_mutex_lock(&letter_mutexes[letter]);

        complete_map[letter][word.first].push_back(word.second);

        pthread_mutex_unlock(&letter_mutexes[letter]);
    }

    // Wait until the complete map is done
    pthread_barrier_wait(&reducers_barrier);
 
    start = id * 26 / num_threads;
    end = std::min((id + 1) * 26.0f / num_threads, 26.0f);

    // Sort and write the words to the output files
    for (int letter = start; letter < end; letter++) {
        std::map<std::string, std::vector<int>>::iterator it = complete_map[letter].begin();
        std::map<std::string, std::vector<int>>::iterator it_end = complete_map[letter].end();

        std::vector<std::pair<std::string, std::vector<int>>> file_words_vec;

        for (; it != it_end; it++) {
            // Sort the file ids
            std::sort(it->second.begin(), it->second.end());

            // Add all words that start with the current letter to a vector to sort them
            file_words_vec.push_back({it->first, it->second});
        }

        // Sort the words starting with the current letter
        std::sort(file_words_vec.begin(), file_words_vec.end(), compare_complete_map);

        // Write the words to the output file
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
    }

    return NULL;
}

int main(int argc, char *argv[])
{
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
    int n = 0;  // Number of files
    FILE *input_file = fopen(argv[3], "r");
    FILE **files;

    if (input_file == NULL) {
        printf("ERROR: couldn't open file %s\n", argv[3]);
        exit(-1);
    }

    pthread_barrier_t barrier;
    pthread_barrier_t reducers_barrier;
    std::vector<std::map<std::pair<std::string, int>, bool>> partial_maps;
    std::vector<std::pair<std::string, int>> words;
    std::vector<std::map<std::string, std::vector<int>>> complete_map(26);
    std::queue<File> file_queue;
    pthread_mutex_t queue_mutex;
    pthread_mutex_t words_mutex;
    FILE *output_files[26];
    pthread_mutex_t letter_mutexes[26];

    std::vector<ThreadArgs> thread_args(mappers_nr + reducers_nr);

    partial_maps.reserve(mappers_nr);

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

        if (output_files[i] == NULL) {
            printf("ERROR: couldn't open file %s", file_name);
            cleanup(NULL, NULL, file_queue, output_files, input_file, n);
            exit(-1);
        }
    }

    // Initialize the mutexes
    pthread_mutex_init(&queue_mutex, NULL);
    pthread_mutex_init(&words_mutex, NULL);

    for (int i = 0; i < 26; i++) {
        pthread_mutex_init(&letter_mutexes[i], NULL);
    }

    // Initialize the barriers
    pthread_barrier_init(&barrier, NULL, mappers_nr + reducers_nr);
    pthread_barrier_init(&reducers_barrier, NULL, reducers_nr);

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

        if (files[i] == NULL) {
            printf("ERROR: couldn't open file %s", file_name);
            cleanup(files, NULL, file_queue, output_files, input_file, n);
            exit(-1);
        }
    }

    // Read the files content
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

    // Sort the files by size in descending order
    qsort(files_struct, n, sizeof(File), compare_files);

    // Put every file in a queue ordered by size
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
        thread_args[id].barrier = &barrier;
        thread_args[id].reducers_barrier = &reducers_barrier;
        thread_args[id].letter_mutexes = letter_mutexes;
        thread_args[id].complete_map = &complete_map;
        thread_args[id].output_files = output_files;

        if (id < mappers_nr) {  // Create mappers
            thread_args[id].id = id;
            thread_args[id].num_threads = mappers_nr;
            partial_maps[id] = std::map<std::pair<std::string, int>, bool>();
            r = pthread_create(&mappers[id], NULL, map_func, &thread_args[id]);
        } else {  // Create reducers
            thread_args[id].id = id - mappers_nr;
            thread_args[id].num_threads = reducers_nr;
            r = pthread_create(&reducers[id - mappers_nr], NULL, reduce_func, &thread_args[id]);
        }
        if (r) {
            printf("ERROR: couldn't create thread %ld", thread_args[id].id);
            cleanup(files, files_struct, file_queue, output_files, input_file, n);
            exit(-1);
        }
    }

    // Join mappers and reducers
    for (id = 0; id < mappers_nr + reducers_nr; id++) {
        if (id < mappers_nr) {
            r = pthread_join(mappers[id], &status);
        } else {
            r = pthread_join(reducers[id - mappers_nr], &status);
        }

        if (r) {
            printf("ERROR: couldn't join thread %ld", id);
            cleanup(files, files_struct, file_queue, output_files, input_file, n);
            exit(-1);
        }
    }

    // Destroy the mutexes
    pthread_mutex_destroy(&queue_mutex);
    pthread_mutex_destroy(&words_mutex);

    for (int i = 0; i < 26; i++) {
        pthread_mutex_destroy(&letter_mutexes[i]);
    }

    // Destroy the barriers
    if (pthread_barrier_destroy(&barrier) != 0) {
        fprintf(stderr, "Error destroying mappers_barrier\n");
    }
    if (pthread_barrier_destroy(&reducers_barrier) != 0) {
        fprintf(stderr, "Error destroying reducers_barrier\n");
    }

    // Free allocated memory
    cleanup(files, files_struct, file_queue, output_files, input_file, n);

    return 0;
}