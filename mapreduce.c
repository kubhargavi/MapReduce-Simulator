#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "mapreduce.h"
#include "common.h"

static void *internal_data = NULL;


void *get_data() {
    return internal_data;
}

void splitting_data(MAPREDUCE_SPEC *spec, DATA_SPLIT *splits) {
    struct stat temp;
    internal_data = spec->usr_data; 
    if (stat(spec->input_data_filepath, &temp) != 0) {
        perror("Cannot get the file size.");
        exit(EXIT_FAILURE);
    }
    off_t x = temp.st_size;
    size_t y = x / spec->split_num;
    off_t z = x % spec->split_num;
    int fd = open(spec->input_data_filepath, O_RDONLY);
    if (fd < 0) {
        perror("Cannot open the input file.");
        exit(EXIT_FAILURE);
    }
    off_t a = 0;
    char c;
    for (int i = 0; i < spec->split_num; i++) {
        splits[i].fd = open(spec->input_data_filepath, O_RDONLY);
        if (splits[i].fd < 0) {
            perror("Cannot open the input file for the current split.");
            for (int j = 0; j < i; j++) {
                close(splits[j].fd);
            }
            close(fd);
            exit(EXIT_FAILURE);
        }
        splits[i].sub = a;
        off_t end_sub = a + y + (i == spec->split_num - 1 ? z : 0);
        if (i < spec->split_num - 1) {
            lseek(fd, end_sub, SEEK_SET);
            while (read(fd, &c, 1) == 1 && c != '\n') {
                end_sub++;
            }
        }
        splits[i].size = end_sub - a;
        a = end_sub; 
    }
    close(fd);
}

void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result)
{
    struct timeval start, end;
    if (NULL == spec || NULL == result)
    {
        EXIT_ERROR(ERROR, "NULL pointer!\n");
    }
    gettimeofday(&start, NULL);
    result->map_worker_pid = malloc(spec->split_num * sizeof(int));
    if (result->map_worker_pid == NULL) {
        EXIT_ERROR(ERROR, "Cannot allocate memory for map worker PIDs\n");
    }
    DATA_SPLIT splits[spec->split_num];
    splitting_data(spec, splits);
    for (int i = 0; i < spec->split_num; ++i) {
        pid_t pid = fork();
        if (pid == 0) { 
            if (lseek(splits[i].fd, splits[i].sub, SEEK_SET) == -1) {
                perror("Cannot seek to split sub");
                exit(EXIT_FAILURE);
            }
            char f[256];
            snprintf(f, sizeof(f), "mr-%d.itm", i);
            int fd_out = open(f, O_WRONLY | O_CREAT | O_TRUNC, 0666);
            if (fd_out < 0) {
                perror("Cannot open output file for map function");
                exit(EXIT_FAILURE);
            }
            if (spec->map_func(&splits[i], fd_out) != 0) {
                fprintf(stderr, "Cannot split for the map function %d\n", i);
                close(fd_out);
                exit(EXIT_FAILURE);
            }
            close(splits[i].fd);
            close(fd_out);
            exit(EXIT_SUCCESS);
        } else if (pid > 0) {
            result->map_worker_pid[i] = pid;
        } else {
            EXIT_ERROR(ERROR, "fork() failed\n");
        }
    }
    for (int i = 0; i < spec->split_num; ++i) {
        waitpid(result->map_worker_pid[i], NULL, 0);
        close(splits[i].fd);  
    }
    pid_t pid_reduce = fork();
    if (pid_reduce == 0) { 
        int fd_out = open(result->filepath, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (fd_out < 0) {
            perror("Cannot open output file for reduce");
            exit(EXIT_FAILURE);
        }
        int *fd_in = malloc(spec->split_num * sizeof(int));
        for (int i = 0; i < spec->split_num; i++) {
            char ff[256];
            snprintf(ff, sizeof(ff), "mr-%d.itm", i);
            fd_in[i] = open(ff, O_RDONLY);
            if (fd_in[i] < 0) {
                fprintf(stderr, "Cannot open input file %s for reduce\n", ff);
                exit(EXIT_FAILURE);
            }
        }
        if (spec->reduce_func(fd_in, spec->split_num, fd_out) != 0) {
            fprintf(stderr, "Reduce function failed\n");
        }
        for (int i = 0; i < spec->split_num; i++) {
            close(fd_in[i]);
        }
        close(fd_out);
        free(fd_in);
        exit(EXIT_SUCCESS);
    } else if (pid_reduce < 0) {
        EXIT_ERROR(ERROR, "fork() failed for reduce worker\n");
    }
    result->reduce_worker_pid = pid_reduce;
    waitpid(pid_reduce, NULL, 0);
    gettimeofday(&end, NULL);   
    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
