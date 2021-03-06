#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include "tasks.h"
#include "utils.h"

#define BUFFER_SIZE 10000
#define MAX_FILE_SIZE 10000
#define MAX_KV_REC 999

int main(int argc, char **argv) {

    MPI_Init(&argc, &argv);
    int world_size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Get command-line params
    char *input_files_dir = argv[1];
    int num_files = atoi(argv[2]);
    int num_map_workers = atoi(argv[3]);
    int num_reduce_workers = atoi(argv[4]);
    char *output_file_name = argv[5];
    int map_reduce_task_num = atoi(argv[6]);

    int numtasks, source, rc, count, tag;
    int dest = 1;
    char inmsg, outmsg = 'x';
    MPI_Status Stat;

    // Identify the specific map function to use
    MapTaskOutput *(*map)(char *);
    switch (map_reduce_task_num) {
        case 1:
            map = &map1;
            break;
        case 2:
            map = &map2;
            break;
        case 3:
            map = &map3;
            break;
    }

    // Distinguish between master, map workers and reduce workers
    if (rank == 0) {
        // TODO: Implement master process logic
        printf("Rank (%d): This is the master process\n", rank);
        printf("CONFIG:\n");
        printf("# of files: %d\n", num_files);
        printf("# of map workers: %d\n", num_map_workers);
        printf("# of reduce workers: %d\n", num_reduce_workers);
        char *filename = (char *) malloc(BUFFER_SIZE);
        FILE *input_fptr;
        FILE *output_fptr;

        for (int i = 0; i < num_files; i++) {
            char idx[20];
            sprintf(idx, "%d", i);
            strcpy(filename, input_files_dir);
            strcat(filename, "/");
            strcat(filename, idx);
            strcat(filename, ".txt");
            input_fptr = fopen(filename, "r");
            if (input_fptr == NULL) {
                /* Unable to open file hence exit */
                printf("Unable to open file.\n");
                exit(EXIT_FAILURE);
            }

//            char buffer[BUFFER_SIZE];
//            char *file_contents = (char *) malloc(BUFFER_SIZE);
//            while (fgets(buffer, BUFFER_SIZE, input_fptr) != NULL) {
//                strcat(file_contents, buffer);
//            }
            fseek(input_fptr, 0L, SEEK_END);
            long numbytes = ftell(input_fptr);
            fseek(input_fptr, 0L, SEEK_SET);
            char *file_contents = (char*)calloc(numbytes, sizeof(char));
            if(file_contents == NULL)
                return 1;
            fread(file_contents, sizeof(char), numbytes, input_fptr);
            fclose(input_fptr);

//            printf( "%s\n", file_contents);
//             printf( "%s\n", filename);
//            MapTaskOutput *output = map(file_contents);
//            printf("HAHA1\n");
//            print_kvs(output->kvs,output->len);
            if (dest <= num_map_workers) {
                source = 0;
                tag = 0;
                rc = MPI_Send(file_contents, strlen(file_contents), MPI_CHAR, dest, tag, MPI_COMM_WORLD);
                dest++;
            } else {
                dest = 1;
                source = 0;
                tag = 0;
                rc = MPI_Send(file_contents, strlen(file_contents), MPI_CHAR, dest, tag, MPI_COMM_WORLD);
                dest++;
            }
        }

        //Receive results and combine them
        output_fptr = fopen(output_file_name, "w");
        if (output_fptr == NULL) {
            /* Unable to open file hence exit */
            printf("Unable to open file.\n");
            exit(EXIT_FAILURE);
        }
        KeyValue **kvs_list = (KeyValue **) malloc(BUFFER_SIZE);
        int len[num_reduce_workers];
        for (int i = 0; i < num_reduce_workers; i++) {
            int reduce_worker_idx = i + num_map_workers + 1;
            char *data = (char *) malloc(BUFFER_SIZE);
            int length;
            tag = 0;
            rc = MPI_Recv(&length, sizeof(int), MPI_INT, reduce_worker_idx, tag, MPI_COMM_WORLD, &Stat);
            rc = MPI_Recv(data, length * sizeof(KeyValue), MPI_CHAR, reduce_worker_idx, tag, MPI_COMM_WORLD, &Stat);
            KeyValue *kvs = ((KeyValue *) data);
//            printf("LENGTH: %d\n", length);
//            printf("OUTPUT_VALUE: %s %d\n", kvs[0].key, kvs[0].val);
            kvs_list[i] = kvs;
            len[i] = length;
        }

        for (int i = 0; i < num_reduce_workers; i++) {
            for (int j = 0; j < len[i]; j++) {
             fprintf(output_fptr, "%s %d\n", kvs_list[i][j].key, kvs_list[i][j].val);
            }
        }

        fclose(output_fptr);

    } else if ((rank >= 1) && (rank <= num_map_workers)) {
        // TODO: Implement map worker process logic
        int loop_count = 0;
        while (loop_count * num_map_workers + rank <= num_files) {
            printf("Rank (%d): This is a map worker process\n", rank);
            loop_count ++;
            //Receive the file content and perform map
            char *file_contents = (char *) malloc(BUFFER_SIZE);
            source = 0;
            tag = 0;

            rc = MPI_Recv(file_contents, MAX_FILE_SIZE, MPI_CHAR, source, tag, MPI_COMM_WORLD, &Stat);
            MapTaskOutput *output = map(file_contents);
            printf("HAHA2\n");
            print_kvs(output->kvs,output->len);
            //Partition the output and send it to all reducers
            KeyValue kvs_list[num_reduce_workers][MAX_KV_REC];
            int count[num_reduce_workers];
            for (int i = 0; i < num_reduce_workers; i++)
                count[i] = 0;
            int kvs_list_index;
            for (int i = 0; i < output->len; i++) {
                KeyValue kv = output->kvs[i];
                kvs_list_index = partition(kv.key, num_reduce_workers);
                int insert_loc = count[kvs_list_index];
                kvs_list[kvs_list_index][insert_loc] = kv;
                count[kvs_list_index]++;
            }
            for (int kvs_list_index = 0; kvs_list_index < num_reduce_workers; kvs_list_index++) {
                //  printf("kvs_list_index : %d\n", kvs_list_index );
                dest = kvs_list_index + num_map_workers + 1;
                tag = 0;
                int length = count[kvs_list_index];
                printf("length: %d\n", length);
                printf("dest: %d\n", dest);
                //printf("Rank (%d): loop %d\n", rank, loop_count);
                printf("HAHA\n");
                rc = MPI_Send(&length, sizeof(int), MPI_INT, dest, tag, MPI_COMM_WORLD);
                rc = MPI_Send(kvs_list[kvs_list_index], length * sizeof(KeyValue), MPI_CHAR, dest, tag,
                              MPI_COMM_WORLD);
            }
            free(file_contents);
            free_map_task_output(output);
        }
    } else {
        // TODO: Implement reduce worker process logic
        printf("Rank (%d): This is a reduce worker process\n", rank);
        tag = 0;
        int kv_index = 0;

        //Receive from all map workers
        int kvs_length;
        int file_no_reduce = 0;
        int sender = 1;
        KeyValue **kvs_list = (KeyValue **) malloc(100000 );
        while (file_no_reduce < num_files) {
            printf("Rank (%d): Waiting %d\n", rank, sender);
            rc = MPI_Recv(&kvs_length, sizeof(int), MPI_INT, sender, tag, MPI_COMM_WORLD, &Stat);
            printf("Rank (%d): received %d KV pairs from map worker %d\n", rank, kvs_length, sender);
            char *data = (char *) malloc(BUFFER_SIZE);
            rc = MPI_Recv(data, kvs_length * sizeof(KeyValue), MPI_CHAR, sender, tag, MPI_COMM_WORLD, &Stat);
            KeyValue *kvs = ((KeyValue *) data);
            printf("file_no_reduce: %d\n", file_no_reduce);
            kvs_list[file_no_reduce] = ((KeyValue *) data);
            printf("REDUCED_KVS_TABLE:\n");
            for (int i = 0; i < kvs_length; i++) {
                printf("%s - %d\n", kvs_list[file_no_reduce][i].key, kvs_list[file_no_reduce][i].val);
                kv_index++;
            }
            //free(data);
            //increment
            sender = (sender < num_map_workers) ? sender + 1 : 1;
            file_no_reduce++;
        }
        printf("Rank (%d): received all KV pairs and start reduce.\n", rank);
        //match
        //Init key value pairs
        KeyValue *matches = (KeyValue *) malloc(BUFFER_SIZE);
        for (int k = 0; k < kvs_length; k++) {
//            printf("Here 1? %d\n", k);
//            printf("Hips : %s\n", kvs_list[0][k].key);
            int values[num_files];
           //printf("VALUES: \n");
            for (int i = 0; i < num_files; ++i) {
                values[i] = kvs_list[i][k].val;
               // printf("%d, %d\n", i, values[i]);
            }
            KeyValue output = reduce(kvs_list[0][k].key, values, num_files);
            matches[k] = output;
        }

        printf("OUTPUT TABLE:\n");
        for (int i = 0; i < kvs_length; i++) {
            printf("%s - %d\n", matches[i].key, matches[i].val);
        }

        printf("Rank (%d): finished reduce and send back to master.\n", rank);
        tag = 0;
        dest = 0;
        rc = MPI_Send(&kvs_length, sizeof(int), MPI_INT, dest, tag, MPI_COMM_WORLD);
        rc = MPI_Send(matches, kvs_length * sizeof(KeyValue), MPI_CHAR, dest, tag, MPI_COMM_WORLD);

    }

    //Clean up
    MPI_Finalize();
    return 0;
}