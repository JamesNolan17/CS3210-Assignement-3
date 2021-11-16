#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include "tasks.h"
#include "utils.h"

#define BUFFER_SIZE 1000

int main(int argc, char** argv) {

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
    char inmsg, outmsg='x';
    MPI_Status Stat;

    // Identify the specific map function to use
    MapTaskOutput* (*map) (char*);
    switch(map_reduce_task_num){
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
        char *filename = (char *) malloc(BUFFER_SIZE);
        FILE *input_fptr;

        for (int i = 0; i < num_files; i++) {
            char idx[20];
            sprintf(idx, "%d", i);
            strcpy(filename,input_files_dir);
            strcat(filename, "/" );
            strcat(filename, idx );
            strcat(filename, ".txt" );
            input_fptr = fopen(filename, "r");
            if(input_fptr == NULL)
            {
                /* Unable to open file hence exit */
                printf("Unable to open file.\n");
                exit(EXIT_FAILURE);
            }

            char buffer[BUFFER_SIZE];
            char *file_contents = (char *) malloc(BUFFER_SIZE);
            while(fgets(buffer, BUFFER_SIZE, input_fptr) != NULL)
            {
                strcat(file_contents, buffer);
            }

//             printf( "%s\n", file_contents);
//             printf( "%s\n", filename);
//            MapTaskOutput* output = map1(file_contents);
//            printf("%d\n", output->len);
            if (dest <= num_map_workers){
                source = 0;
                tag = 0;
                rc = MPI_Send(file_contents, strlen(file_contents), MPI_CHAR, dest, tag, MPI_COMM_WORLD);
                dest++;
            }else{
                dest = 1;
                source = 0;
                tag = 0;
                rc = MPI_Send(file_contents, strlen(file_contents), MPI_CHAR, dest, tag, MPI_COMM_WORLD);
                dest++;
            }

        }

    } else if ((rank >= 1) && (rank <= num_map_workers)) {
        // TODO: Implement map worker process logic
        while(1) {
            printf("Rank (%d): This is a map worker process\n", rank);
            char *file_contents = (char *) malloc(BUFFER_SIZE);
            source = 0;
            tag = 0;
            rc = MPI_Recv(file_contents, 10000, MPI_CHAR, source, tag, MPI_COMM_WORLD, &Stat);
            //printf("%s\n", file_contents);
            MapTaskOutput* output = map(file_contents);
            dest =  (rank % num_reduce_workers) + num_map_workers ;
            //printf("%d\n", dest);
            rc = MPI_Send(output, 10000, , dest, tag, MPI_COMM_WORLD);
        }
    } else {
        // TODO: Implement reduce worker process logic
        printf("Rank (%d): This is a reduce worker process\n", rank);
        tag = 0;
        for (int i = 1; i <= num_map_workers; i++) {
            printf("%d\n", i);
            MapTaskOutput* output = (MapTaskOutput* ) malloc(BUFFER_SIZE);
            if(rank == (i % num_reduce_workers) + num_map_workers ){
                tag = 0;
                printf("HAHA\n");
                rc = MPI_Recv(output, 10000, MPI_CHAR, i, tag, MPI_COMM_WORLD, &Stat);
                for (int j = 0; j < output->len; j++) {

                }
            }
        }

    }

    //Clean up
    MPI_Finalize();
    return 0;
}
