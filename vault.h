#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include "blake3.h"
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/time.h>
#include <math.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/statvfs.h>
#include <errno.h>
#include <semaphore.h>
#ifdef __linux__
#include <sys/sysinfo.h>
#endif

#ifdef __APPLE__
#include <sys/types.h>
#include <sys/sysctl.h>
#endif

// #include "tbb/parallel_sort.h"

#ifndef O_DIRECT
#define O_DIRECT 040000 /* Direct disk access. */
#endif

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif

// #define NONCE_SIZE 6               // Size of the nonce in bytes
// #define RECORD_SIZE 16

#ifndef NONCE_SIZE
#define NONCE_SIZE 5 // Default buffer size if not provided via compiler flag
#endif

#ifndef RECORD_SIZE
#define RECORD_SIZE 8 // Default buffer size if not provided via compiler flag
#endif

// #define NONCE_SIZE 5               // Size of the nonce in bytes
// #define RECORD_SIZE 8
#define HASH_SIZE (RECORD_SIZE - NONCE_SIZE)
// 8 * 1024 * 1024 / RECORD_SIZE
// int HASHGEN_THREADS_BUFFER = (1024 / RECORD_SIZE);
int HASHGEN_THREADS_BUFFER = (8 * 1024 * 1024 / RECORD_SIZE);
// #define HASHGEN_THREADS_BUFFER (2 * 1024)
// #define HASHGEN_THREADS_BUFFER 2
#define MEGA (1024 * 1024)
// #define MEGA 1024
// #define MEGA 1
// used for final compression

#ifndef CHUNK_SIZE
#define CHUNK_SIZE (8 * 1024 * 1024 * RECORD_SIZE) // 1MB * RECORD_SIZE
#endif

// #define NUM_THREADS 4

bool DEBUG = false;
bool HASHGEN = true;
bool HASHSORT = true;

bool littleEndian = false;

size_t bucketSizeInBytes;
struct writeObject *buckets;

const int MAX_BYTES_TO_READ = 1024 * 1024 * 1024;

const int SEARCH_SIZE = 256;
long long memory_size = 1; // in GB
long long WRITE_SIZE = 16; // in KB
int BUCKET_SIZE = 1;       // Number of random records per bucket
// int NUM_BUCKETS = memory_size*1024*1024/WRITE_SIZE;      // Number of buckets
int NUM_BUCKETS = 65536;
// int PREFIX_SIZE2 = (int)(log(65536) / log(2));
int PREFIX_SIZE = 16;
unsigned long long FLUSH_SIZE = 1;
size_t BATCH_SIZE = 1;
int NUM_THREADS = 2;

// int PARTITION_SIZE = 16; //number of elements before switching to qsort from merge-sort
int PARTITION_SIZE = 1; // number of elements before switching to qsort from merge-sort
// int QSORT_SIZE = 256*1024; //number of elements to perform single threaded qsort instead of parallel merge-sort
int QSORT_SIZE = 1; // number of elements to perform single threaded qsort instead of parallel merge-sort

unsigned long long NUM_ENTRIES = 64; // Number of random records to generate

// Structure to hold a 16-byte random record
typedef struct
{
    uint8_t hash[HASH_SIZE];   // Actual random bytes
    uint8_t nonce[NONCE_SIZE]; // Nonce to store the seed (4-byte unsigned integer)
} MemoRecord;

// Structure to hold a bucket of random records
typedef struct
{
    MemoRecord *records;
    size_t count; // Number of random records in the bucket
    size_t flush; // Number of flushes of bucket
} Bucket;

typedef struct
{
    struct timeval start;
    struct timeval end;
} Timer;

typedef struct
{
    Bucket *buckets;
    int num_buckets_to_process;
    unsigned long long offset;
    int fd;
    int thread_id;
} ThreadArgs;

typedef struct
{
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    int count;
} semaphore_t;

semaphore_t semaphore_io;
