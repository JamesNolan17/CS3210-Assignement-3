/***************************************************** 
 * Note: this file is provided for the convenience of
 *       students. Students can choose to edit, delete
 *       or even leave the file as-is. This file will
 *       NOT be replaced during grading.
 ****************************************************/
#include "utils.h"
#include "tasks.h"
/* Helper func to choose a partition based on `key`,*/
int partition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;

    while (c = *key++) {
        hash = ((hash << 5) + hash) + c;
    }

    return hash % num_partitions;
}

void print_kvs(KeyValue *kvs ,int kvs_length){
    printf("KVS TABLE:\n");
    for (int i = 0; i < kvs_length; i++)
        printf("%s - %d\n", kvs[i].key, kvs[i].val);
}
