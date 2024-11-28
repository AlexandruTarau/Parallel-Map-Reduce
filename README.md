# Map-Reduce Inverted Index

## Overview
This project implements a multithreaded **Map-Reduce** approach to build an **inverted index**. An inverted index maps words to the list of files in which they appear, enabling efficient text search and retrieval. The program processes multiple input files concurrently, using mapper and reducer threads to extract and aggregate data.

## Features
- **Map-Reduce Paradigm**: Divides tasks into mappers and reducers for parallel processing.
- **Inverted Index Construction**: Outputs each word with the list of file IDs where it appears.
- **Multithreading**: Utilizes `pthread` for concurrent execution.
- **Efficient Sorting**: Words are sorted by frequency (vector size) and alphabetically.

## Usage
### Command-line Syntax
```bash
make
./program <mappers_number> <reducers_number> <input_file>
```
- `mappers_number`: Number of mapper threads to process the input files.
- `reducers_number`: Number of reducer threads to aggregate and sort the results.
- `input_file`: A file containing the list of text files to be processed. The `input_file` should specify the number of input text files followed by their paths.

## Output
Each reducer writes the results to a separate file (`a.txt`, `b.txt`, ..., `z.txt`) based on the initial letter of the words. Each word is followed by the file IDs where it appears, listed in sorted order.

## How it works
1. **File Reading**: Input files are read into memory and sorted by size for an optimal distribution among mapper threads.
2. **Mapping Phase**: Each mapper extracts words, removes any special characters, and stores <`word`, `file IDs`> pairs in partial maps for thread-safe processing. Once a partial map is completed, all distinct pairs are added to a shared vector, enabling efficient work distribution to the reducer threads.
3. **Reducing Phase**: Reducers merge the partial maps into a complete inverted index. Words are sorted by frequency (vector size) and then alphabetically. Results are written to corresponding output files.
4. **Details**:
- **No Global Variables**: The program avoids the use of global variables. All shared data is contained within the `main` function and is passed to the threads via a custom structure called `ThreadArgs`. This structure holds pointers to the data, ensuring thread-specific access to the necessary resources.

## Synchronization
- **Mutexes**: Protect shared resources such as the file queue and the complete map.
- **Barriers**: Ensure all mappers complete before reducers start, and guarantee the complete map contains all partial maps before reducers begin writing the results.

## Error Handling
- Validates input arguments and files.
- Handles errors related to file I/O, memory allocation, and thread operations.
- Displays error messages and cleans up allocated resources.

## Cleanup
The program ensures:
- All dynamically allocated memory is freed.
- All mutexes and barriers are properly destroyed.
- All input and output files are closed.