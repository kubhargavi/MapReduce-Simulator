#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>

#include "common.h"
#include "usr_functions.h"
#include "mapreduce.h"
#define HASH_SIZE 50000
typedef struct {
    char *line;
    int used;
} HashEntry;

HashEntry hashTable[HASH_SIZE];

void table() {
    for (int i = 0; i < HASH_SIZE; i++) {
        hashTable[i].used = 0;
        hashTable[i].line = NULL;
    }
}


int hash(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; 
    return hash % HASH_SIZE;
}

int seen(const char *line) {
    int index = hash(line);
    for (int i = 0; i < HASH_SIZE; i++) {
        int probe = (index + i) % HASH_SIZE;
        if (!hashTable[probe].used) break;
        if (hashTable[probe].used && strcmp(hashTable[probe].line, line) == 0)
            return 1; 
    }
    return 0; 
}


void marked(const char *line) {
    int index = hash(line);
    for (int i = 0; i < HASH_SIZE; i++) {
        int probe = (index + i) % HASH_SIZE;
        if (!hashTable[probe].used) {
            hashTable[probe].line = strdup(line);
            hashTable[probe].used = 1;
            return;
        }
    }
}

int is_word_boundary(char c) {
    return isspace(c) || ispunct(c) || c == '\0';
}
/* User-defined map function for the "Letter counter" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int letter_counter_map(DATA_SPLIT * split, int fd_out)
{
    if (!split || fd_out < 0){
        return -1; 
    }
    char temp[50000];
    int letter_freq[26] = {0};
    ssize_t x;
    size_t y = 0;
    while ((x = read(split->fd, temp, sizeof(temp))) > 0){
        size_t z = y + x > split->size ? split->size - y : x;
        for (int i = 0; i < z; i++) {
            if (isalpha(temp[i])) {
                char letter = tolower(temp[i]);
                letter_freq[letter - 'a']++;
            }
        }
        y += z;
        if (y >= split->size) break;
    }
    for (int i = 0; i < 26; i++) {
        if (letter_freq[i] > 0) {
            dprintf(fd_out, "%c: %d\n", 'a' + i, letter_freq[i]);
        }
    } 
    return 0;
}

/* User-defined reduce function for the "Letter counter" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int letter_counter_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
    int letter_freq[26] = {0};
    char temp[50000];
    for (int i = 0; i < fd_in_num; i++) {
        FILE* file = fdopen(p_fd_in[i], "r");
        if (!file) continue;
        char letter;
        int count;
        while (fgets(temp, sizeof(temp), file)) {
            if (sscanf(temp, "%c: %d", &letter, &count) == 2) {
                letter_freq[letter - 'a'] += count;
            }
        }
        fclose(file);
    }
    for (int i = 0; i < 26; i++) {
        if (letter_freq[i] > 0) {
            dprintf(fd_out, "%c: %d\n", 'a' + i, letter_freq[i]);
        }
    }
    return 0;
}

// Bonus
/* User-defined map function for the "Word finder" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int word_finder_map(DATA_SPLIT * split, int fd_out)
{
   if (!split || fd_out < 0) {
        perror("Invalid split or file descriptor");
        return -1;
    }
    const char *word_to_find = (char *)get_data();
    if (!word_to_find || word_to_find[0] == '\0') {
        fprintf(stderr, "Cannot find the word\n");
        return -1;
    }
    char buffer[50000];
    char line[50000];
    int line_idx = 0;
    ssize_t bytes_read;
    if (lseek(split->fd, split->sub, SEEK_SET) == -1) {
        perror("Error seeking in input file");
        return -1;
    }
    while ((bytes_read = read(split->fd, buffer, sizeof(buffer) - 1)) > 0) {
        for (int i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n' || buffer[i] == '\0') {
                line[line_idx] = '\0'; 
                if (strstr(line, word_to_find)) {
                    dprintf(fd_out, "%s\n", line);
                }
                line_idx = 0; 
            } else {
                if (line_idx < sizeof(line) - 1) {
                    line[line_idx++] = buffer[i];
                }
            }
        }
    }
    if (line_idx > 0) { 
        line[line_idx] = '\0';
        if (strstr(line, word_to_find)) {
            dprintf(fd_out, "%s\n", line);
        }
    }
    return 0;
    
}

// Bonus
/* User-defined reduce function for the "Word finder" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int word_finder_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
    char line[50000];
    FILE *file;
    table();
    for (int i = 0; i < fd_in_num; i++) {
        file = fdopen(p_fd_in[i], "r");
        if (!file) {
            fprintf(stderr, "Cannot open intermediate file for reading\n");
            continue;
        }
        while (fgets(line, sizeof(line), file)) {
            if (line[strlen(line) - 1] == '\n') {
                line[strlen(line) - 1] = '\0'; 
            }
            if (!seen(line)) {
                dprintf(fd_out, "%s\n", line);
                marked(line);
            }
        }
        fclose(file);
    }
    return 0;
}


