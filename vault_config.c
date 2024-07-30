//make a benchmark option that generates hashes without sorting/storing
#include "vault.h"
#include "parallel_mergesort.c"


/*
bool isLittleEndian() {
    // Create a union with an integer and an array of bytes
    union {
        uint32_t integer;
        uint8_t bytes[4];
    } test;

    // Set the integer value to 1
    test.integer = 1;

    // If the first byte is 1, the system is little-endian; otherwise, it's big-endian
    return (test.bytes[0] == 1);
}
*/

/*
int compare(const void* a, const void* b) {
    const MemoRecord* recordA = (const MemoRecord*)a;
    const MemoRecord* recordB = (const MemoRecord*)b;

    // Compare nonce fields
    //for (int i = 0; i < NONCE_SIZE; ++i) {
    //    if (recordA->hash[i] < recordB->hash[i]) return -1;
    //    if (recordA->hash[i] > recordB->hash[i]) return 1;
    //}

    // If nonces are equal, compare hash fields
    return memcmp(recordA->hash, recordB->hash, HASH_SIZE);
}*/

void resetTimer(Timer *timer) {
	gettimeofday(&timer->start, NULL);
}

double getTimer(Timer *timer) {
	gettimeofday(&timer->end, NULL);
	return (timer->end.tv_sec - timer->start.tv_sec) + (timer->end.tv_usec - timer->start.tv_usec) / 1000000.0;
}

// Function to compare two random records for sorting
int compareMemoRecords(const void *a, const void *b) {
	const MemoRecord *ra = (const MemoRecord *)a;
	const MemoRecord *rb = (const MemoRecord *)b;
	return memcmp(ra->hash, rb->hash, sizeof(ra->hash));
}

struct CircularArray {
	//unsigned char array[HASHGEN_THREADS_BUFFER][HASH_SIZE];
	MemoRecord array[HASHGEN_THREADS_BUFFER];
	size_t head;
	size_t tail;
	int producerFinished;  // Flag to indicate when the producer is finished
	pthread_mutex_t mutex;
	pthread_cond_t not_empty;
	pthread_cond_t not_full;
};

void initCircularArray(struct CircularArray *circularArray) {
	circularArray->head = 0;
	circularArray->tail = 0;
	circularArray->producerFinished = 0;
	pthread_mutex_init(&(circularArray->mutex), NULL);
	pthread_cond_init(&(circularArray->not_empty), NULL);
	pthread_cond_init(&(circularArray->not_full), NULL);
}

void destroyCircularArray(struct CircularArray *circularArray) {
	pthread_mutex_destroy(&(circularArray->mutex));
	pthread_cond_destroy(&(circularArray->not_empty));
	pthread_cond_destroy(&(circularArray->not_full));
}

void insertBatch(struct CircularArray *circularArray, MemoRecord values[BATCH_SIZE]) {
	if (DEBUG)
		printf("insertBatch(): before mutex lock\n");
	pthread_mutex_lock(&(circularArray->mutex));

	if (DEBUG)
		printf("insertBatch(): Wait while the circular array is full and producer is not finished\n");
	// Wait while the circular array is full and producer is not finished
	while ((circularArray->head + BATCH_SIZE) % HASHGEN_THREADS_BUFFER == circularArray->tail && !circularArray->producerFinished) {
		pthread_cond_wait(&(circularArray->not_full), &(circularArray->mutex));
	}

	if (DEBUG)
		printf("insertBatch(): Insert values\n");
	// Insert values
	for (int i = 0; i < BATCH_SIZE; i++) {
		memcpy(&circularArray->array[circularArray->head], &values[i], sizeof(MemoRecord));
		circularArray->head = (circularArray->head + 1) % HASHGEN_THREADS_BUFFER;
	}

	if (DEBUG)
		printf("insertBatch(): Signal that the circular array is not empty\n");
	// Signal that the circular array is not empty
	pthread_cond_signal(&(circularArray->not_empty));

	if (DEBUG)
		printf("insertBatch(): mutex unlock\n");
	pthread_mutex_unlock(&(circularArray->mutex));
}

void removeBatch(struct CircularArray *circularArray, MemoRecord *result) {
	pthread_mutex_lock(&(circularArray->mutex));

	// Wait while the circular array is empty and producer is not finished
	while (circularArray->tail == circularArray->head && !circularArray->producerFinished) {
		pthread_cond_wait(&(circularArray->not_empty), &(circularArray->mutex));
	}

	// Remove values
	for (int i = 0; i < BATCH_SIZE; i++) {
		memcpy(&result[i], &circularArray->array[circularArray->tail], sizeof(MemoRecord));
		circularArray->tail = (circularArray->tail + 1) % HASHGEN_THREADS_BUFFER;
	}

	// Signal that the circular array is not full
	pthread_cond_signal(&(circularArray->not_full));

	pthread_mutex_unlock(&(circularArray->mutex));
}

// Thread data structure
struct ThreadData {
	struct CircularArray *circularArray;
	int threadID;
};

// Function to generate a pseudo-random record using BLAKE3 hash
void generateBlake3(MemoRecord *record, unsigned long long seed) {
	// Store seed into the nonce
	memcpy(record->nonce, &seed, sizeof(record->nonce));
	//for (int i = 0; i < sizeof(record->nonce); ++i) {
	//    record->nonce[i] = (seed >> (i * 8)) & 0xFF;
	//    }

	// Generate random bytes
	blake3_hasher hasher;
	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, &record->nonce, sizeof(record->nonce));
	//blake3_hasher_update(&hasher, &seed, sizeof(seed));
	blake3_hasher_finalize(&hasher, record->hash, RECORD_SIZE - NONCE_SIZE);
}


// Function to be executed by each thread for array generation
void *arrayGenerationThread(void *arg) {
	struct ThreadData *data = (struct ThreadData *)arg;
	if (DEBUG)
		printf("arrayGenerationThread %d\n",data->threadID);
	int hashObjectSize = sizeof(MemoRecord);
	//unsigned char batch[BATCH_SIZE][HASH_SIZE];
	MemoRecord batch[BATCH_SIZE];
	long long NUM_HASHES_PER_THREAD = (long long)(NUM_ENTRIES / NUM_THREADS);
	unsigned char hash[HASH_SIZE];
	unsigned long long hashIndex = 0;
	long long i = 0;
	while(data->circularArray->producerFinished == 0)
	{
		if (DEBUG)
			printf("arrayGenerationThread(), inside while loop %llu...\n",i);
		//for (unsigned int i = 0; i < NUM_HASHES_PER_THREAD; i += BATCH_SIZE) {
		for (long long j = 0; j < BATCH_SIZE; j++) {
			if (DEBUG)
				printf("arrayGenerationThread(), inside for loop %llu...\n",j);
			hashIndex = (long long)(NUM_HASHES_PER_THREAD * data->threadID + i + j);
			generateBlake3(&batch[j], hashIndex);
			//batch[j].prefix = byteArrayToInt(hash,0);
			//memcpy(batch[j].data.byteArray, hash+PREFIX_SIZE, HASH_SIZE-PREFIX_SIZE);
			//batch[j].data.NONCE = hashIndex;
		}
		//should add hashIndex as NONCE to hashObject
		if (DEBUG)
			printf("insertBatch()...\n");
		insertBatch(data->circularArray, batch);
		i += BATCH_SIZE;
	}

	if (DEBUG)
		printf("finished generating hashes on thread id %d, thread exiting...\n",data->threadID);
	return NULL;
}



// Function to write a bucket of random records to disk
void writeBucketToDisk(const Bucket *bucket, int fd, off_t offset) {


	//bool buffered = false;

	//struct iovec iov[1];
	//    iov[0].iov_base = bucket->records;
	//    iov[0].iov_len = sizeof(MemoRecord) * bucket->count;

	if (lseek(fd, offset * sizeof(MemoRecord) * BUCKET_SIZE * FLUSH_SIZE + sizeof(MemoRecord) * BUCKET_SIZE * bucket->flush, SEEK_SET) < 0) {
		printf("writeBucketToDisk(): Error seeking in file at offset %llu; more details: %llu %llu %lu %d %zu\n",offset * sizeof(MemoRecord) * BUCKET_SIZE * FLUSH_SIZE + sizeof(MemoRecord) * BUCKET_SIZE * bucket->flush,offset,FLUSH_SIZE,sizeof(MemoRecord),BUCKET_SIZE,bucket->flush);
		close(fd);
		exit(EXIT_FAILURE);
	}

	// Write the entire bucket to the file
	//    ssize_t bytes_written = writev(fd, iov, 1);
	//    if (bytes_written < 0) {
	//        perror("Error writing to file");
	//        close(fd);
	//        exit(EXIT_FAILURE);
	//    } else if (bytes_written != sizeof(MemoRecord) * bucket->count) {
	//        fprintf(stderr, "Incomplete write to file\n");
	//        close(fd);
	//        exit(EXIT_FAILURE);
	//    }

	unsigned long long bytesWritten = write(fd, bucket->records, sizeof(MemoRecord) * bucket->count);
	if (bytesWritten < 0) {
		//perror("Error writing to file");
		printf("Error writing bucket at offset %llu to file; bytes written %llu when it expected %lu\n",offset,bytesWritten,sizeof(MemoRecord) * bucket->count);
		close(fd);
		exit(EXIT_FAILURE);
	}
}


//off_t getBucketIndex(const uint8_t *hash) {
//    // Calculate the bucket index based on the first 2-byte prefix
//    return (hash[0] << 8) | hash[1];
//}


/*
off_t byteArrayToUnsignedLongLongBigEndian(const uint8_t *byteArray, size_t bits) {
    off_t result = 0;

    // Calculate the number of bytes to process (up to 8 bytes)
    //size_t numBytes = size < sizeof(uint64_t) ? size : sizeof(uint64_t);

    // Calculate the number of bytes to process (up to 8 bytes)
    size_t numBytes = (bits + 7) / 8;
    size_t max_value = pow(2, bits);

    // Combine the bytes into the resulting unsigned long long integer
    for (size_t i = 0; i < numBytes; ++i) {
        result |= ((off_t)byteArray[i]) << (i * 8);
    }

    return result%max_value;
}

off_t byteArrayToUnsignedLongLongLittleEndian(const uint8_t *byteArray, size_t bits) {
    off_t result = 0;

    // Calculate the number of bytes to process (up to 8 bytes)
    size_t numBytes = (bits + 7) / 8;
    size_t max_value = pow(2, bits);

    // Combine the bytes into the resulting unsigned long long integer in little-endian format
    for (size_t i = 0; i < numBytes; ++i) {
        result |= ((off_t)byteArray[i]) << (i * 8);
    }

    return result%max_value;
}
*/
/*
uint64_t byteArrayToUnsignedLongLongBigEndian(uint8_t byteArray[]) {
    uint64_t result = 0;
    
    result |= ((uint64_t)byteArray[0]) << 56;
    result |= ((uint64_t)byteArray[1]) << 48;
    result |= ((uint64_t)byteArray[2]) << 40;
    result |= ((uint64_t)byteArray[3]) << 32;
    result |= ((uint64_t)byteArray[4]) << 24;
    result |= ((uint64_t)byteArray[5]) << 16;
    result |= ((uint64_t)byteArray[6]) << 8;
    result |= byteArray[7];
    
    return result;
}

uint64_t byteArrayToUnsignedLongLongLittleEndian(uint8_t byteArray[]) {
    uint64_t result = 0;
    
    result |= ((uint64_t)byteArray[0]) << 0;
    result |= ((uint64_t)byteArray[1]) << 8;
    result |= ((uint64_t)byteArray[2]) << 16;
    result |= ((uint64_t)byteArray[3]) << 24;
    result |= ((uint64_t)byteArray[4]) << 32;
    result |= ((uint64_t)byteArray[5]) << 40;
    result |= ((uint64_t)byteArray[6]) << 48;
    result |= ((uint64_t)byteArray[7]) << 56;
    
    return result;
}

uint32_t byteArrayToUnsignedIntLittleEndian(uint8_t byteArray[]) {
    uint32_t result = 0;
    
    result |= ((uint32_t)byteArray[3]) << 24;
    result |= ((uint32_t)byteArray[2]) << 16;
    result |= ((uint32_t)byteArray[1]) << 8;
    result |= byteArray[0];
    
    return result;
}
*/

void printBytes(const uint8_t *bytes, size_t length)
{
	for (size_t i = 0; i < length; i++)
	{
		printf("%02x", bytes[i]);
	}
	//printf("\n");
}


void print_binary(const uint8_t *byte_array, size_t array_size, int b) {
    int printed = 0;
    for (size_t i = 0; i < array_size; ++i) {
        uint8_t byte = byte_array[i];
        for (int j = 7; j >= 0; --j) {
            if (printed < b)
            {
            	printf("%d", (byte >> j) & 1);
            	printed++;
            }
            else
            	break;
            
        }
        //printf(" ");
    }
    printf("\n");
}

off_t byteArrayToUnsignedIntBigEndian(const uint8_t *byteArray, int b) {
	print_binary(byteArray,HASH_SIZE,b);
    off_t result = 0;

    size_t byteIndex = b / 8; // Determine the byte index
    size_t bitOffset = b % 8; // Determine the bit offset within the byte

	printf("byteArrayToUnsignedIntBigEndian(): byteIndex=%lu\n",byteIndex);
	printf("byteArrayToUnsignedIntBigEndian(): bitOffset=%lu\n",bitOffset);
    // Extract the bits from the byte array
    for (size_t i = 0; i < byteIndex; ++i) {
        result |= (uint32_t)byteArray[i] << ((byteIndex - i - 1) * 8);
    }
	printf("byteArrayToUnsignedIntBigEndian(): result1=%lld\n",result);
    
    // Extract the remaining bits
    if (bitOffset > 0) {
        result |= (uint32_t)(byteArray[byteIndex] >> (8 - bitOffset));
    }
    printf("byteArrayToUnsignedIntBigEndian(): result2=%lld\n",result);

    return result;
}

/*off_t byteArrayToUnsignedIntLittleEndian(const uint8_t *byteArray, int b) {
    off_t result = 0;

    size_t byteIndex = b / 8; // Determine the byte index
    size_t bitOffset = b % 8; // Determine the bit offset within the byte

    // Extract the bits from the byte array in little-endian format
    for (size_t i = 0; i < byteIndex; ++i) {
        result |= (uint32_t)byteArray[i] << (i * 8);
    }
    
    // Extract the remaining bits
    if (bitOffset > 0) {
        result |= (uint32_t)(byteArray[byteIndex] >> bitOffset);
    }

    return result;
}*/

off_t binaryByteArrayToULL(const uint8_t *byteArray, size_t array_size, int b) {
	if (DEBUG)
		print_binary(byteArray,array_size,b);
    off_t result = 0;
    int bits_used = 0; // To keep track of how many bits we've used so far

    for (size_t i = 0; i < array_size; ++i) {
        for (int j = 7; j >= 0 && bits_used < b; --j) {
            result = (result << 1) | ((byteArray[i] >> j) & 1);
            bits_used++;
        }
    }
	if (DEBUG)
		printf("binaryByteArrayToULL(): result=%lld\n",result);
    return result;
}


off_t getBucketIndex(const uint8_t *byteArray, int b) {
//printf("getBucketIndex(): ");
//printBytes(byteArray,HASH_SIZE);

off_t result = 0;
//littleEndian = false;
//if (littleEndian)
//	result = byteArrayToUnsignedIntLittleEndian(byteArray,b);
//else
	//result = byteArrayToUnsignedIntBigEndian(byteArray,b);
	result = binaryByteArrayToULL(byteArray,HASH_SIZE,b);
	if (DEBUG)
		printf("getBucketIndex(): %lld %d\n",result,b);

//printf(" : %lld\n",result);
    return result;
}


off_t getBucketIndex_old(const uint8_t *hash, int num_bits) {

	//printf("getBucketIndex(): ");
	//printBytes(hash,HASH_SIZE);
	
	off_t index = 0;
	int shift_bits = 0;

	// Calculate the bucket index based on the specified number of bits
	for (int i = 0; i < num_bits / 8; i++) {
		index |= ((off_t)hash[i] << shift_bits);
		shift_bits += 8;
	}

	// Handle the remaining bits if num_bits is not a multiple of 8
	if (num_bits % 8 != 0) {
		index |= ((off_t)hash[num_bits / 8] & ((1 << (num_bits % 8)) - 1)) << shift_bits;
	}
	//printf(" : %lld\n",index);
	return index;
}



long long getFileSize(const char *filename)
{
	struct stat st;

	if (stat(filename, &st) == 0)
	{
		return st.st_size;
	}
	else
	{
		perror("Error getting file size");
		return -1; // Return -1 to indicate an error
	}
}

// Function to print the contents of the file
void printFile(const char *filename, int numRecords) {
	FILE *file = fopen(filename, "rb");
	if (file == NULL) {
		printf("Error opening file for reading!\n");
		return;
	}

	MemoRecord number;
	// uint8_t array[16];

	unsigned long long recordsPrinted = 0;

	while (recordsPrinted < numRecords && fread(&number, sizeof(MemoRecord), 1, file) == 1)
	{
		// Interpret nonce as unsigned long long
		unsigned long long nonceValue = 0;
		for (int i = 0; i < sizeof(number.nonce); i++) {
			nonceValue |= (unsigned long long)number.nonce[i] << (i * 8);
		}

		// Print hash
		printf("[%llu] Hash: ",recordsPrinted*sizeof(MemoRecord));
		for (int i = 0; i < sizeof(number.hash); i++) {
			printf("%02x", number.hash[i]);
		}
		printf(" : ");

		// Print nonce
		for (int i = 0; i < sizeof(number.nonce); i++) {
			printf("%02x", number.nonce[i]);
		}

		// Print nonce as unsigned long long
		printf(" : %llu\n", nonceValue);

		recordsPrinted++;

	}
	//printf("all done!\n");

	fclose(file);
}

// Function to print the contents of the file
void printFileTail(const char *filename, int numRecords) {
	FILE *file = fopen(filename, "rb");
	if (file == NULL) {
		printf("Error opening file for reading!\n");
		return;
	}
	
	long long fileSize = getFileSize(filename);

	off_t offset = fileSize - numRecords*RECORD_SIZE;
	
			if (fseek(file, offset, SEEK_SET) < 0)
		{
			printf("printFileTail(): Error seeking in file at offset %llu\n",offset);
			fclose(file);
			exit(EXIT_FAILURE);
		}


	MemoRecord number;
	// uint8_t array[16];

	unsigned long long recordsPrinted = 0;

	while (recordsPrinted < numRecords && fread(&number, sizeof(MemoRecord), 1, file) == 1)
	{
		// Interpret nonce as unsigned long long
		unsigned long long nonceValue = 0;
		for (int i = 0; i < sizeof(number.nonce); i++) {
			nonceValue |= (unsigned long long)number.nonce[i] << (i * 8);
		}

		// Print hash
		printf("[%llu] Hash: ",offset + recordsPrinted*sizeof(MemoRecord));
		for (int i = 0; i < sizeof(number.hash); i++) {
			printf("%02x", number.hash[i]);
		}
		printf(" : ");

		// Print nonce
		for (int i = 0; i < sizeof(number.nonce); i++) {
			printf("%02x", number.nonce[i]);
		}

		// Print nonce as unsigned long long
		printf(" : %llu\n", nonceValue);

		recordsPrinted++;

	}
	//printf("all done!\n");

	fclose(file);
}



// Binary search function to search for a hash from disk
int binarySearch(const uint8_t *targetHash, size_t targetLength, int fileDescriptor, long long filesize, int *seekCount, bool bulk)
{
	if (DEBUG)
	{
		printf("binarySearch()=");
		printBytes(targetHash, targetLength);
		printf("\n");
	}
	//should use filesize to determine left and right
	// Calculate the bucket index based on the first 2-byte prefix
	off_t bucketIndex = getBucketIndex(targetHash, PREFIX_SIZE);
	//int bucketIndex = (targetHash[0] << 8) | targetHash[1];
	if (DEBUG)
		printf("bucketIndex=%lld\n",bucketIndex);


	//filesize
	long long FILESIZE = filesize;
	if (DEBUG)
		printf("FILESIZE=%lld\n", FILESIZE);

	//int RECORD_SIZE = sizeof(MemoRecord);
	if (DEBUG)
		printf("RECORD_SIZE=%d\n", RECORD_SIZE);


	unsigned long long NUM_ENTRIES = FILESIZE/RECORD_SIZE;
	if (DEBUG)
		printf("NUM_ENTRIES=%lld\n", NUM_ENTRIES);

	int BUCKET_SIZE = (FILESIZE)/(RECORD_SIZE*NUM_BUCKETS);
	//BUCKET_SIZE = 1024;
	if (DEBUG)
		printf("BUCKET_SIZE=%d\n", BUCKET_SIZE);

	//left and right are record numbers, not byte offsets
	off_t left = bucketIndex*BUCKET_SIZE;
	if (DEBUG)
		printf("left=%lld\n",left);
	off_t right = (bucketIndex+1)*BUCKET_SIZE-1;
	if (DEBUG)
		printf("right=%lld\n",right);


	//off_t right = filesize / sizeof(MemoRecord);
	off_t middle;
	MemoRecord number;

	if (bulk == false)
		*seekCount = 0; // Initialize seek count

	while (left <= right && right - left > SEARCH_SIZE)
	{
		middle = left + (right - left) / 2;
		if (DEBUG)
			printf("left=%lld middle=%lld right=%lld\n",left, middle, right);

		// Increment seek count
		(*seekCount)++;
		//if (DEBUG)
		//	printf("seekCount=%lld \n",seekCount);

		if (DEBUG)
			printf("lseek=%lld %lu\n",middle*sizeof(MemoRecord),sizeof(MemoRecord));
		// Seek to the middle position
		if (lseek(fileDescriptor, middle*sizeof(MemoRecord), SEEK_SET) < 0)
		{
			printf("binarySearch(): Error seeking in file at offset %llu\n",middle*sizeof(MemoRecord));
			exit(EXIT_FAILURE);
		}

		// Read the hash at the middle position
		if (read(fileDescriptor, &number, sizeof(MemoRecord)) < 0)
		{
			perror("Error reading from file");
			exit(EXIT_FAILURE);
		}

		// Compare the target hash with the hash read from file

		if (DEBUG)
		{
			printf("memcmp(targetHash)=");
			printBytes(targetHash, targetLength);
			printf("\n");
			printf("memcmp(number.hash)=");
			printBytes(number.hash, targetLength);
			printf("\n");
		}
		int cmp = memcmp(targetHash, number.hash, targetLength);

		if (DEBUG)
		{
			printf("memcmp()=%d\n",cmp);
			printf("nonce=");
			// Print nonce
			for (int i = 0; i < sizeof(number.nonce); i++) {
				printf("%02x", number.nonce[i]);
			}
			printf("\n");
		}

		if (cmp == 0)
		{
			// Hash found
			return middle;
		}
		else if (cmp < 0)
		{
			// Search the left half
			right = middle - 1;
		}
		else
		{
			// Search the right half
			left = middle + 1;
		}
	}

	// If the remaining data to search is less than 1024 bytes, perform a brute force search
	if (right - left <= SEARCH_SIZE)
	{
		// Increment seek count
		(*seekCount)++;
		// Seek to the left position
		if (lseek(fileDescriptor, left*sizeof(MemoRecord), SEEK_SET) < 0)
		{
			printf("binarySearch(2): Error seeking in file at offset %llu\n",left*sizeof(MemoRecord));
			exit(EXIT_FAILURE);
		}

		// Perform a brute force search in the remaining 1024 bytes
		while (left <= right)
		{
			// Read the hash at the current position
			if (read(fileDescriptor, &number, sizeof(MemoRecord)) < 0)
			{
				perror("Error reading from file");
				exit(EXIT_FAILURE);
			}

			// Compare the target hash with the hash read from file
			int cmp = memcmp(targetHash, number.hash, targetLength);
			if (cmp == 0)
			{
				// Hash found
				return left;
			}

			// Move to the next position
			left++;
		}
	}

	// Hash not found
	return -1;
}

uint8_t *hexStringToByteArray(const char *hexString, uint8_t *byteArray, size_t byteArraySize)
{
	size_t hexLen = strlen(hexString);
	if (hexLen % 2 != 0)
	{
		return NULL; // Error: Invalid hexadecimal string length
	}

	size_t byteLen = hexLen / 2;
	if (byteLen > byteArraySize)
	{
		return NULL; // Error: Byte array too small
	}

	for (size_t i = 0; i < byteLen; ++i)
	{
		if (sscanf(&hexString[i * 2], "%2hhx", &byteArray[i]) != 1)
		{
			return NULL; // Error: Failed to parse hexadecimal string
		}
	}

	return byteArray;
}

unsigned long long byteArrayToLongLong(const uint8_t *byteArray, size_t length) {
	unsigned long long result = 0;
	for (size_t i = 0; i < length; ++i) {
		result = (result << 8) | (unsigned long long)byteArray[i];
	}
	return result;
}

void longLongToByteArray(unsigned long long value, uint8_t *byteArray, size_t length) {
	for (size_t i = length - 1; i >= 0; --i) {
		byteArray[i] = value & 0xFF;
		value >>= 8;
	}
}


char *removeFilename(const char *path) {
    // Find the position of the last directory separator
    const char *last_separator = strrchr(path, '/');
    if (last_separator == NULL) {
		if (DEBUG)
		printf("No directory separator found, return a copy of the original string: %s\n",path);
		return "./";
        // No directory separator found, return a copy of the original string
        //return strdup(path);
    }

    // Calculate the length of the directory path
    size_t dir_length = last_separator - path;

    // Allocate memory for the directory path
    char *directory_path = (char *)malloc((dir_length + 1) * sizeof(char));
    if (directory_path == NULL) {
		printf("Error allocating memory\n");
        exit(EXIT_FAILURE);
    }

    // Copy the directory path to the new string
    strncpy(directory_path, path, dir_length);
    directory_path[dir_length] = '\0'; // Null-terminate the string
    //printf("%s %s %ld\n",directory_path,path,dir_length);

    return directory_path;
}

unsigned long long getDiskSpace(const char *path) {

   char *result = removeFilename(path);

    // Retrieve file system statistics for the directory path
    struct statvfs stat;

    if (statvfs(result, &stat) != 0) {
        printf("Error getting file system statistics from %s\n",result);
        exit(EXIT_FAILURE);
    }

    unsigned long long bytes_free = stat.f_bavail * stat.f_frsize;
    return bytes_free;


    
}



// Function to read a chunk of data from a file using pread
ssize_t readChunk(int fd, char *buffer, off_t offset, size_t chunkSize) {
    ssize_t bytesRead;

    // Read a chunk of data from the file at the specified offset
    bytesRead = pread(fd, buffer, chunkSize, offset);
    if (bytesRead == -1) {
        perror("Error reading file");
        return -1;
    }

    return bytesRead;
}

// Function to verify if the records in the buffer are sorted
int verifySorted(char *buffer, size_t bytesRead) {
    int i;
    for (i = RECORD_SIZE; i < bytesRead; i += RECORD_SIZE) {
        if (memcmp(buffer + i - RECORD_SIZE, buffer + i, HASH_SIZE) > 0) {
        
        			printf("verifySorted failed: ");
			printBytes((uint8_t *)(buffer + i - RECORD_SIZE), HASH_SIZE);
			printf(" !< ");
			printBytes((uint8_t *)(buffer + i), HASH_SIZE);
			printf("\n");

        
            return 0; // Records are not sorted
        }
    }
    return 1; // Records are sorted
}



void printUsage() {
	printf("Usage: ./vault -f <filename> -t <num_threads_hash> -o <num_threads_sort> -i <num_threads_io> -m <memorysize_GB> -s <filesize_GB>\n");
	printf("Usage: ./vault -p <num_records> -f <filename>\n");
	printf("Usage: ./vault -f <filename> -p 10\n");
	printf("Usage: ./vault -f <filename> -v true\n");
	printf("Usage: ./vault -f <filename> -b 10\n");
}

void printHelp() {
	printf("Help:\n");
	printf("  -t <num_threads_hash>: Specify the number of threads to generate hashes\n");
	printf("  -o <num_threads_sort>: Specify the number of threads to sort hashes\n");
	printf("  -i <num_threads_io>: Specify the number of threads for reading and writing buckets\n");
	printf("  -f <filename>: Specify the filename\n");
	//printf("  -w <writesize>: Specify the write size in KB as an integer\n");
	printf("  -m <memorysize>: Specify the memory size as an integer\n");
	printf("  -s <filesize>: Specify the filesize as an integer\n");
	printf("  -p <num_records>: Specify the number of records to print from head, must specify -f <filename>\n");
	printf("  -r <num_records>: Specify the number of records to print from tail, must specify -f <filename>\n");
	printf("  -a <search_hash>: Specify the number of records to print, must specify -f <filename>\n");
	printf("  -l <prefix_length>: Specify the number of records to print, must specify -f <filename>\n");
	printf("  -c <search_records>: Specify the number of records to print, must specify -f <filename>\n");
	printf("  -d <bool> turns on debug mode with true, off with false \n");
	printf("  -x <bool> turns hash generation on with true, off with false; default is on \n");
	printf("  -b <num_records>: verify hashes as correct BLAKE3 hashes \n");
	printf("  -v <bool> verify hashes from file, off with false, on with true; default is off \n");
	printf("  -h: Display this help message\n");
}

double min(double a, double b) {
	return (a < b) ? a : b;
}



int main(int argc, char *argv[])
{
	//assumes both values are set to false initially
	//littleEndian = isLittleEndian();
	//littleEndian = false;
	//printf("littleEndian=%s\n",littleEndian ? "true" : "false");

	Timer timer;
	double elapsedTime;

	//struct timeval start_all_walltime, end_all_walltime;

	char *FILENAME = NULL; // Default value
	long long FILESIZE = 0; // Default value
	int num_threads_sort = 1;
	int num_threads_io = 1;

	long long print_records = 0;

	uint8_t byteArray[10];
	uint8_t *targetHash = NULL;
	size_t prefixLength = 10;

	int search_records = 0;
	
	bool head = false;
	bool tail = false;
	
	bool verify_records = false;

	int verify_records_num = 0;


	int opt;
	while ((opt = getopt(argc, argv, "t:o:m:f:s:p:r:a:l:c:d:i:x:v:b:h")) != -1) {
		switch (opt) {
		/*case 'w':
			WRITE_SIZE = (long long)atoi(optarg);
			printf("WRITE_SIZE=%lld\n", WRITE_SIZE);
			break;
			*/
		case 't':
			NUM_THREADS = atoi(optarg);
			printf("NUM_THREADS=%d\n", NUM_THREADS);

			BATCH_SIZE = HASHGEN_THREADS_BUFFER/NUM_THREADS;
			printf("BATCH_SIZE=%ld\n", BATCH_SIZE);

			if (NUM_THREADS == 1)
			{
				printf("multi-threading with only 1 thread is not supported at this time, exiting\n");
				//return 1;

			}
			break;
		case 'o':
			num_threads_sort = atoi(optarg);
			printf("num_threads_sort=%d\n", num_threads_sort);
			if (num_threads_sort > 1)
			{
				printf("multi-threading for sorting has not been implemented, exiting\n");
				//return 1;

			}

			break;
		case 'i':
			num_threads_io = atoi(optarg);
			printf("num_threads_io=%d\n", num_threads_io);
			if (num_threads_io > 1)
			{
				printf("multi-threading for I/O has not been implemented, exiting\n");
				//return 1;

			}

			break;
		case 'm':
			memory_size = atoi(optarg);
			printf("memory_size=%lld GB\n", memory_size);
			break;
		case 'f':
			FILENAME = optarg;
			
			printf("FILENAME=%s\n", FILENAME);
			break;
		case 's':
			FILESIZE = atoi(optarg);
			printf("FILESIZE=%lld GB\n", FILESIZE);
			break;
		case 'p':
			print_records = atoi(optarg);
			printf("print_records=%lld\n", print_records);
			head = true;
			break;
		case 'r':
			print_records = atoi(optarg);
			tail = true;
			printf("print_records=%lld\n", print_records);
			break;
		case 'a':
			// Validate the length of the hash
			if (strlen(optarg) != 20) // Each byte represented by 2 characters in hexadecimal
			{
				printf("Invalid hexadecimal hash format\n");
				return 1;
			}
			// Convert the hexadecimal hash from command-line argument to binary

			targetHash = hexStringToByteArray(optarg, byteArray, sizeof(byteArray));
			if (targetHash == NULL)
			{
				printf("Error: Byte array too small\n");
				return 1;
			}


			printf("Hash_search=");
			printBytes(byteArray, sizeof(byteArray));
			printf("\n");


			//printf("print_records=%lld\n", print_records);
			break;
		case 'l':
			// Get the length of the prefix
			prefixLength = atoi(optarg);
			if (prefixLength <= 0 || prefixLength > 10)
			{
				printf("Invalid prefix length\n");
				return 1;
			}
			printf("prefixLength=%zu\n", prefixLength);
			break;
		case 'c':
			// Get the length of the prefix
			search_records = atoi(optarg);
			if (search_records <= 0)
			{
				printf("Invalid search records\n");
				return 1;
			}
			printf("search_records=%d\n", search_records);
			break;
		case 'x':
			if (strcmp(optarg, "true") == 0) {
				HASHGEN = true;
			} else if (strcmp(optarg, "false") == 0) {
				HASHGEN = false;
			} else {
				HASHGEN = true;
			}
			printf("HASHGEN=%s\n",HASHGEN ? "true" : "false");
			break;
		case 'd':
			if (strcmp(optarg, "true") == 0) {
				DEBUG = true;
			} else if (strcmp(optarg, "false") == 0) {
				DEBUG = false;
			} else {
				DEBUG = false;
			}
			printf("DEBUG=%s\n",DEBUG ? "true" : "false");
			break;
		case 'b':
			// Get the length of the prefix
			verify_records_num = atoi(optarg);
			if (verify_records_num <= 0)
			{
				printf("Invalid verify records num\n");
				return 1;
			}
			if (DEBUG)
			printf("verify_records_num=%d\n", verify_records_num);
			break;
		case 'v':
			if (strcmp(optarg, "true") == 0) {
				verify_records = true;
			} else if (strcmp(optarg, "false") == 0) {
				verify_records = false;
			} else {
				verify_records = false;
			}
			printf("verify_records=%s\n",verify_records ? "true" : "false");
			break;
		case 'h':
			printHelp();
			return 0;
		default:
			printUsage();
			return 1;
		}
	}
	
	if (FILENAME == NULL) {
		printf("Error: filename (-f) is mandatory.\n");
		printUsage();
		return 1;
	}
	if (FILENAME != NULL && print_records == 0 && verify_records == false && targetHash == NULL && verify_records_num == 0 && search_records == 0 && (NUM_THREADS <= 0 || num_threads_sort <=0 || FILESIZE < 0 || memory_size < 0))
	{
		printf("Error: mandatory command line arguments have not been used, try -h for more help\n");
		printUsage();
		return 1;
	}

	const char *path = FILENAME; // Example path, you can change it to any valid path

	unsigned long long bytes_free = getDiskSpace(path);
	if (bytes_free > 0) {
		if (DEBUG)
			printf("Free disk space on %s: %llu bytes\n", path, bytes_free);
	}
	printf("bytes_free=%lld\n", bytes_free);
	
	if (FILESIZE == 0 || FILESIZE*1024*1024*1024 > bytes_free)
		FILESIZE = (int)(bytes_free/(1024*1024*1024));	
	
	//if (FILESIZE*1024*1024*1024 > bytes_free)	
	//	{
	//		printf("not enough storage space\n");
	//		return 0;
	//	}
	//printf()
	

	long long sort_memory = 0;
	long long EXPECTED_TOTAL_FLUSHES = 0;
	bool found_good_config = false;
	//WRITE_SIZE = 1024*1024; //unit KB, max is 1GB
	//for (int i=0; i<N; i++)
	//{
	//	memory_size
	//}
	
	//memory and file sizes must be divisible
	
	
	int ratio = (int)ceil((double)FILESIZE/memory_size);
	printf("ratio=%d\n", ratio);	
	long long FILESIZE_byte = FILESIZE*1024*1024*1024;
	long long memory_size_byte = FILESIZE*1024*1024*1024/ratio;
	
			printf("memory_size_byte=%lld\n", memory_size_byte);			
			printf("FILESIZE_byte=%lld\n", FILESIZE_byte);				
	

	for (WRITE_SIZE= 1024*1024; WRITE_SIZE > 0; WRITE_SIZE = WRITE_SIZE/2)
	{
		//FLUSH_SIZE = FILESIZE_byte/memory_size_byte;
		FLUSH_SIZE = ratio;
		BUCKET_SIZE = (long long)WRITE_SIZE*1024*FLUSH_SIZE;
		NUM_BUCKETS = (long long)FILESIZE_byte/BUCKET_SIZE;
		PREFIX_SIZE = (int)(log(NUM_BUCKETS) / log(2))+1;
		NUM_BUCKETS = pow(2, PREFIX_SIZE);
		PREFIX_SIZE = (int)(log(NUM_BUCKETS) / log(2));
		BUCKET_SIZE = (long long)FILESIZE_byte/NUM_BUCKETS;
		
	
		EXPECTED_TOTAL_FLUSHES = (long long)FILESIZE_byte/(WRITE_SIZE*1024);
		sort_memory = (long long)BUCKET_SIZE*num_threads_sort;
		
		//valid configuration
		if (sort_memory <= memory_size_byte && NUM_BUCKETS >= 64)
		{
			//fix all numbers to ensure proper fitting of all hashes		
			WRITE_SIZE = (long long)floor(memory_size_byte / NUM_BUCKETS);
			WRITE_SIZE = (long long)(WRITE_SIZE / 16) * 16;
			BUCKET_SIZE = (long long)WRITE_SIZE*FLUSH_SIZE;
			memory_size_byte = (long long)WRITE_SIZE*NUM_BUCKETS;
			FILESIZE_byte = (long long)BUCKET_SIZE*NUM_BUCKETS;
			sort_memory = (long long)BUCKET_SIZE*num_threads_sort;
			NUM_ENTRIES = (long long)FILESIZE_byte/RECORD_SIZE;
		
			printf("-------------------------\n");
			printf("found good configuration:\n");
			printf("memory_size_byte=%lld\n", memory_size_byte);			
			printf("FILESIZE_byte=%lld\n", FILESIZE_byte);			
			printf("WRITE_SIZE=%lld\n", WRITE_SIZE);			
			printf("FLUSH_SIZE=%llu\n", FLUSH_SIZE);			
			printf("BUCKET_SIZE=%d\n", BUCKET_SIZE);			
			printf("NUM_BUCKETS=%d\n", NUM_BUCKETS);
			printf("PREFIX_SIZE=%d\n", PREFIX_SIZE);			
			printf("EXPECTED_TOTAL_FLUSHES=%lld\n", EXPECTED_TOTAL_FLUSHES);			
			printf("sort_memory=%lld\n", sort_memory);
			printf("NUM_ENTRIES=%lld\n", NUM_ENTRIES);		
			
    FILE *config_file = fopen("config.txt", "w"); // Open or create the config file for writing

    if (config_file == NULL) {
        printf("Error opening the config file!\n");
        return 1;
    }

    // Write the variables A, B, and C, each initialized to 0, to the config file
    fprintf(config_file, "FILESIZE_byte=%lld\n", FILESIZE_byte);
    fprintf(config_file, "NUM_BUCKETS=%d\n", NUM_BUCKETS);
    fprintf(config_file, "BUCKET_SIZE=%d\n", BUCKET_SIZE);

    fclose(config_file); // Close the config file

			
			
			found_good_config = true;	
			break;
			//return 0;
			
		}

	}

	if (found_good_config == false)
	{
			printf("no valid configuration found... this should never happen\n");
	
	printf("exiting...\n");
			return 1;
			}

	

}
