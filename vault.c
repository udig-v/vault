// make a benchmark option that generates hashes without sorting/storing
#include "vault.h"
#include <semaphore.h>

void semaphore_init(semaphore_t *sem, int initial_count)
{
	pthread_mutex_init(&sem->mutex, NULL);
	pthread_cond_init(&sem->condition, NULL);
	sem->count = initial_count;
}

void semaphore_wait(semaphore_t *sem)
{
	pthread_mutex_lock(&sem->mutex);
	while (sem->count <= 0)
	{
		pthread_cond_wait(&sem->condition, &sem->mutex);
	}
	sem->count--;
	pthread_mutex_unlock(&sem->mutex);
}

void semaphore_post(semaphore_t *sem)
{
	pthread_mutex_lock(&sem->mutex);
	sem->count++;
	pthread_cond_signal(&sem->condition);
	pthread_mutex_unlock(&sem->mutex);
}

void resetTimer(Timer *timer)
{
	gettimeofday(&timer->start, NULL);
}

double getTimer(Timer *timer)
{
	gettimeofday(&timer->end, NULL);
	return (timer->end.tv_sec - timer->start.tv_sec) + (timer->end.tv_usec - timer->start.tv_usec) / 1000000.0;
}

// Function to compare two random records for sorting
int compareMemoRecords(const void *a, const void *b)
{
	const MemoRecord *ra = (const MemoRecord *)a;
	const MemoRecord *rb = (const MemoRecord *)b;
	return memcmp(ra->hash, rb->hash, sizeof(ra->hash));
}

struct CircularArray
{
	MemoRecord *array;
	size_t head;
	size_t tail;
	int producerFinished; // Flag to indicate when the producer is finished
	pthread_mutex_t mutex;
	pthread_cond_t not_empty;
	pthread_cond_t not_full;
};

void initCircularArray(struct CircularArray *circularArray)
{
	circularArray->array = malloc(HASHGEN_THREADS_BUFFER * sizeof(MemoRecord));
	circularArray->head = 0;
	circularArray->tail = 0;
	circularArray->producerFinished = 0;
	pthread_mutex_init(&(circularArray->mutex), NULL);
	pthread_cond_init(&(circularArray->not_empty), NULL);
	pthread_cond_init(&(circularArray->not_full), NULL);
}

void destroyCircularArray(struct CircularArray *circularArray)
{
	free(circularArray->array);
	pthread_mutex_destroy(&(circularArray->mutex));
	pthread_cond_destroy(&(circularArray->not_empty));
	pthread_cond_destroy(&(circularArray->not_full));
}

void insertBatch(struct CircularArray *circularArray, MemoRecord values[BATCH_SIZE])
{
	if (DEBUG)
		printf("insertBatch(): before mutex lock\n");
	pthread_mutex_lock(&(circularArray->mutex));

	if (DEBUG)
		printf("insertBatch(): Wait while the circular array is full and producer is not finished\n");
	// Wait while the circular array is full and producer is not finished
	while ((circularArray->head + BATCH_SIZE) % HASHGEN_THREADS_BUFFER == circularArray->tail && !circularArray->producerFinished)
	{
		pthread_cond_wait(&(circularArray->not_full), &(circularArray->mutex));
	}

	if (DEBUG)
		printf("insertBatch(): Insert values\n");
	// Insert values
	for (int i = 0; i < BATCH_SIZE; i++)
	{
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

void removeBatch(struct CircularArray *circularArray, MemoRecord *result)
{
	pthread_mutex_lock(&(circularArray->mutex));

	// Wait while the circular array is empty and producer is not finished
	while (circularArray->tail == circularArray->head && !circularArray->producerFinished)
	{
		pthread_cond_wait(&(circularArray->not_empty), &(circularArray->mutex));
	}

	// Remove values
	for (int i = 0; i < BATCH_SIZE; i++)
	{
		memcpy(&result[i], &circularArray->array[circularArray->tail], sizeof(MemoRecord));
		circularArray->tail = (circularArray->tail + 1) % HASHGEN_THREADS_BUFFER;
	}

	// Signal that the circular array is not full
	pthread_cond_signal(&(circularArray->not_full));

	pthread_mutex_unlock(&(circularArray->mutex));
}

// Thread data structure
struct ThreadData
{
	struct CircularArray *circularArray;
	int threadID;
};

// Function to generate a pseudo-random record using BLAKE3 hash
void generateBlake3(MemoRecord *record, unsigned long long seed)
{
	// Store seed into the nonce
	memcpy(record->nonce, &seed, sizeof(record->nonce));

	// Generate random bytes
	blake3_hasher hasher;
	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, &record->nonce, sizeof(record->nonce));
	blake3_hasher_finalize(&hasher, record->hash, RECORD_SIZE - NONCE_SIZE);
}

// Function to be executed by each thread for array generation
void *arrayGenerationThread(void *arg)
{
	struct ThreadData *data = (struct ThreadData *)arg;
	if (DEBUG)
		printf("arrayGenerationThread %d\n", data->threadID);
	int hashObjectSize = sizeof(MemoRecord);
	MemoRecord batch[BATCH_SIZE];
	long long NUM_HASHES_PER_THREAD = (long long)(NUM_ENTRIES / NUM_THREADS);
	unsigned char hash[HASH_SIZE];
	unsigned long long hashIndex = 0;
	long long i = 0;
	while (data->circularArray->producerFinished == 0)
	{
		if (DEBUG)
			printf("arrayGenerationThread(), inside while loop %llu...\n", i);
		for (long long j = 0; j < BATCH_SIZE; j++)
		{
			if (DEBUG)
				printf("arrayGenerationThread(), inside for loop %llu...\n", j);
			hashIndex = (long long)(NUM_HASHES_PER_THREAD * data->threadID + i + j);
			generateBlake3(&batch[j], hashIndex);
		}
		// should add hashIndex as NONCE to hashObject
		if (DEBUG)
			printf("insertBatch()...\n");
		insertBatch(data->circularArray, batch);
		i += BATCH_SIZE;
	}

	if (DEBUG)
		printf("finished generating hashes on thread id %d, thread exiting...\n", data->threadID);
	return NULL;
}

// Function to write a bucket of random records to disk
void writeBucketToDisk(const Bucket *bucket, int fd, off_t offset)
{
	if (DEBUG)
		printf("writeBucketToDisk(): %lld %lu %d %lld %zu\n", offset, sizeof(MemoRecord), BUCKET_SIZE, WRITE_SIZE, bucket->flush);
	if (DEBUG)
		printf("writeBucketToDisk(): %lld\n", offset * sizeof(MemoRecord) * BUCKET_SIZE + WRITE_SIZE * bucket->flush);
	if (lseek(fd, offset * sizeof(MemoRecord) * BUCKET_SIZE + WRITE_SIZE * bucket->flush, SEEK_SET) < 0)
	{
		printf("writeBucketToDisk(): Error seeking in file at offset %llu; more details: %llu %llu %lu %lld %zu\n", offset * sizeof(MemoRecord) * BUCKET_SIZE + WRITE_SIZE * bucket->flush, offset, FLUSH_SIZE, sizeof(MemoRecord), WRITE_SIZE, bucket->flush);
		close(fd);
		exit(EXIT_FAILURE);
	}

	unsigned long long bytesWritten = write(fd, bucket->records, sizeof(MemoRecord) * bucket->count);
	if (bytesWritten < 0)
	{
		printf("Error writing bucket at offset %llu to file; bytes written %llu when it expected %lu\n", offset, bytesWritten, sizeof(MemoRecord) * bucket->count);
		close(fd);
		exit(EXIT_FAILURE);
	}
	if (DEBUG)
		printf("writeBucketToDisk(): bytesWritten=%lld %lu\n", bytesWritten, sizeof(MemoRecord) * bucket->count);
}

off_t byteArrayToUnsignedLongLongLittleEndian(const uint8_t *byteArray, size_t bits)
{
	off_t result = 0;

	// Calculate the number of bytes to process (up to 8 bytes)
	size_t numBytes = (bits + 7) / 8;
	size_t max_value = pow(2, bits);

	// Combine the bytes into the resulting unsigned long long integer in little-endian format
	for (size_t i = 0; i < numBytes; ++i)
	{
		result |= ((off_t)byteArray[i]) << (i * 8);
	}

	return result % max_value;
}

void printBytes(const uint8_t *bytes, size_t length)
{
	for (size_t i = 0; i < length; i++)
	{
		printf("%02x", bytes[i]);
	}
}

void print_binary(const uint8_t *byte_array, size_t array_size, int b)
{
	int printed = 0;
	for (size_t i = 0; i < array_size; ++i)
	{
		uint8_t byte = byte_array[i];
		for (int j = 7; j >= 0; --j)
		{
			if (printed < b)
			{
				printf("%d", (byte >> j) & 1);
				printed++;
			}
			else
				break;
		}
	}
	printf("\n");
}

off_t byteArrayToUnsignedIntBigEndian(const uint8_t *byteArray, int b)
{
	print_binary(byteArray, HASH_SIZE, b);
	off_t result = 0;

	size_t byteIndex = b / 8; // Determine the byte index
	size_t bitOffset = b % 8; // Determine the bit offset within the byte

	printf("byteArrayToUnsignedIntBigEndian(): byteIndex=%lu\n", byteIndex);
	printf("byteArrayToUnsignedIntBigEndian(): bitOffset=%lu\n", bitOffset);
	// Extract the bits from the byte array
	for (size_t i = 0; i < byteIndex; ++i)
	{
		result |= (uint32_t)byteArray[i] << ((byteIndex - i - 1) * 8);
	}
	printf("byteArrayToUnsignedIntBigEndian(): result1=%lld\n", result);

	// Extract the remaining bits
	if (bitOffset > 0)
	{
		result |= (uint32_t)(byteArray[byteIndex] >> (8 - bitOffset));
	}
	printf("byteArrayToUnsignedIntBigEndian(): result2=%lld\n", result);

	return result;
}

off_t binaryByteArrayToULL(const uint8_t *byteArray, size_t array_size, int b)
{
	if (DEBUG)
		print_binary(byteArray, array_size, b);
	off_t result = 0;
	int bits_used = 0; // To keep track of how many bits we've used so far

	for (size_t i = 0; i < array_size; ++i)
	{
		for (int j = 7; j >= 0 && bits_used < b; --j)
		{
			result = (result << 1) | ((byteArray[i] >> j) & 1);
			bits_used++;
		}
	}
	if (DEBUG)
		printf("binaryByteArrayToULL(): result=%lld\n", result);
	return result;
}

off_t getBucketIndex(const uint8_t *byteArray, int b)
{

	off_t result = 0;

	result = binaryByteArrayToULL(byteArray, HASH_SIZE, b);
	if (DEBUG)
		printf("getBucketIndex(): %lld %d\n", result, b);

	return result;
}

off_t getBucketIndex_old(const uint8_t *hash, int num_bits)
{
	off_t index = 0;
	int shift_bits = 0;

	// Calculate the bucket index based on the specified number of bits
	for (int i = 0; i < num_bits / 8; i++)
	{
		index |= ((off_t)hash[i] << shift_bits);
		shift_bits += 8;
	}

	// Handle the remaining bits if num_bits is not a multiple of 8
	if (num_bits % 8 != 0)
	{
		index |= ((off_t)hash[num_bits / 8] & ((1 << (num_bits % 8)) - 1)) << shift_bits;
	}
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
void printFile(const char *filename, int numRecords)
{
	FILE *file = fopen(filename, "rb");
	if (file == NULL)
	{
		printf("Error opening file for reading!\n");
		return;
	}

	MemoRecord number;
	unsigned long long recordsPrinted = 0;

	while (recordsPrinted < numRecords && fread(&number, sizeof(MemoRecord), 1, file) == 1)
	{
		// Interpret nonce as unsigned long long
		unsigned long long nonceValue = 0;
		for (int i = 0; i < sizeof(number.nonce); i++)
		{
			nonceValue |= (unsigned long long)number.nonce[i] << (i * 8);
		}

		// Print hash
		printf("[%llu] Hash: ", recordsPrinted * sizeof(MemoRecord));
		for (int i = 0; i < sizeof(number.hash); i++)
		{
			printf("%02x", number.hash[i]);
		}
		printf(" : ");

		// Print nonce
		for (int i = 0; i < sizeof(number.nonce); i++)
		{
			printf("%02x", number.nonce[i]);
		}

		// Print nonce as unsigned long long
		printf(" : %llu\n", nonceValue);

		recordsPrinted++;
	}
	fclose(file);
}

// Function to print the contents of the file
void printFileTail(const char *filename, int numRecords)
{
	FILE *file = fopen(filename, "rb");
	if (file == NULL)
	{
		printf("Error opening file for reading!\n");
		return;
	}

	long long fileSize = getFileSize(filename);

	off_t offset = fileSize - numRecords * RECORD_SIZE;

	if (fseek(file, offset, SEEK_SET) < 0)
	{
		printf("printFileTail(): Error seeking in file at offset %llu\n", offset);
		fclose(file);
		exit(EXIT_FAILURE);
	}

	MemoRecord number;

	unsigned long long recordsPrinted = 0;

	while (recordsPrinted < numRecords && fread(&number, sizeof(MemoRecord), 1, file) == 1)
	{
		// Interpret nonce as unsigned long long
		unsigned long long nonceValue = 0;
		for (int i = 0; i < sizeof(number.nonce); i++)
		{
			nonceValue |= (unsigned long long)number.nonce[i] << (i * 8);
		}

		// Print hash
		printf("[%llu] Hash: ", offset + recordsPrinted * sizeof(MemoRecord));
		for (int i = 0; i < sizeof(number.hash); i++)
		{
			printf("%02x", number.hash[i]);
		}
		printf(" : ");

		// Print nonce
		for (int i = 0; i < sizeof(number.nonce); i++)
		{
			printf("%02x", number.nonce[i]);
		}

		// Print nonce as unsigned long long
		printf(" : %llu\n", nonceValue);

		recordsPrinted++;
	}

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
	// should use filesize to determine left and right
	// Calculate the bucket index based on the first 2-byte prefix
	off_t bucketIndex = getBucketIndex(targetHash, PREFIX_SIZE);
	if (DEBUG)
		printf("bucketIndex=%lld\n", bucketIndex);

	// filesize
	long long FILESIZE = filesize;
	if (DEBUG)
		printf("FILESIZE=%lld\n", FILESIZE);

	if (DEBUG)
		printf("RECORD_SIZE=%d\n", RECORD_SIZE);

	unsigned long long NUM_ENTRIES = FILESIZE / RECORD_SIZE;
	if (DEBUG)
		printf("NUM_ENTRIES=%lld\n", NUM_ENTRIES);

	int BUCKET_SIZE = (FILESIZE) / (RECORD_SIZE * NUM_BUCKETS);
	if (DEBUG)
		printf("BUCKET_SIZE=%d\n", BUCKET_SIZE);

	// left and right are record numbers, not byte offsets
	off_t left = bucketIndex * BUCKET_SIZE;
	if (DEBUG)
		printf("left=%lld\n", left);
	off_t right = (bucketIndex + 1) * BUCKET_SIZE - 1;
	if (DEBUG)
		printf("right=%lld\n", right);

	off_t middle;
	MemoRecord number;

	if (bulk == false)
		*seekCount = 0; // Initialize seek count

	while (left <= right && right - left > SEARCH_SIZE)
	{
		middle = left + (right - left) / 2;
		if (DEBUG)
			printf("left=%lld middle=%lld right=%lld\n", left, middle, right);

		// Increment seek count
		(*seekCount)++;

		if (DEBUG)
			printf("lseek=%lld %lu\n", middle * sizeof(MemoRecord), sizeof(MemoRecord));
		// Seek to the middle position
		if (lseek(fileDescriptor, middle * sizeof(MemoRecord), SEEK_SET) < 0)
		{
			printf("binarySearch(): Error seeking in file at offset %llu\n", middle * sizeof(MemoRecord));
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
			printf("memcmp()=%d\n", cmp);
			printf("nonce=");
			// Print nonce
			for (int i = 0; i < sizeof(number.nonce); i++)
			{
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
		if (lseek(fileDescriptor, left * sizeof(MemoRecord), SEEK_SET) < 0)
		{
			printf("binarySearch(2): Error seeking in file at offset %llu\n", left * sizeof(MemoRecord));
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

unsigned long long byteArrayToLongLong(const uint8_t *byteArray, size_t length)
{
	unsigned long long result = 0;
	for (size_t i = 0; i < length; ++i)
	{
		result = (result << 8) | (unsigned long long)byteArray[i];
	}
	return result;
}

void longLongToByteArray(unsigned long long value, uint8_t *byteArray, size_t length)
{
	for (size_t i = length - 1; i >= 0; --i)
	{
		byteArray[i] = value & 0xFF;
		value >>= 8;
	}
}

char *removeFilename(const char *path)
{
	// Find the position of the last directory separator
	const char *last_separator = strrchr(path, '/');
	if (last_separator == NULL)
	{
		if (DEBUG)
			printf("No directory separator found, return a copy of the original string: %s\n", path);
		return "./";
		// No directory separator found, return a copy of the original string
	}

	// Calculate the length of the directory path
	size_t dir_length = last_separator - path;

	// Allocate memory for the directory path
	char *directory_path = (char *)malloc((dir_length + 1) * sizeof(char));
	if (directory_path == NULL)
	{
		printf("Error allocating memory\n");
		exit(EXIT_FAILURE);
	}

	// Copy the directory path to the new string
	strncpy(directory_path, path, dir_length);
	directory_path[dir_length] = '\0'; // Null-terminate the string

	return directory_path;
}

unsigned long long getDiskSpace(const char *path)
{

	char *result = removeFilename(path);

	// Retrieve file system statistics for the directory path
	struct statvfs stat;

	if (statvfs(result, &stat) != 0)
	{
		printf("Error getting file system statistics from %s\n", result);
		exit(EXIT_FAILURE);
	}

	unsigned long long bytes_free = stat.f_bavail * stat.f_frsize;
	return bytes_free;
}

// Function to read a chunk of data from a file using pread
ssize_t readChunk(int fd, char *buffer, off_t offset, size_t chunkSize)
{
	ssize_t bytesRead;

	// Read a chunk of data from the file at the specified offset
	bytesRead = pread(fd, buffer, chunkSize, offset);
	if (bytesRead == -1)
	{
		perror("Error reading file");
		return -1;
	}

	return bytesRead;
}

// Function to verify if the records in the buffer are sorted
int verifySorted(char *buffer, size_t bytesRead)
{
	printf("verifySorted(): %zu %d\n", bytesRead, RECORD_SIZE);
	int i;
	for (i = RECORD_SIZE; i < bytesRead; i += RECORD_SIZE)
	{
		if (memcmp(buffer + i - RECORD_SIZE, buffer + i, HASH_SIZE) > 0)
		{

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

void printUsage()
{
	printf("Usage: ./vault -f <filename> -t <num_threads_hash> -o <num_threads_sort> -i <num_threads_io> -m <memorysize_GB> -s <filesize_GB>\n");
	printf("Usage: ./vault -p <num_records> -f <filename>\n");
	printf("Usage: ./vault -f <filename> -p 10\n");
	printf("Usage: ./vault -f <filename> -v true\n");
	printf("Usage: ./vault -f <filename> -b 10\n");
}

void printHelp()
{
	printf("Help:\n");
	printf("  -t <num_threads_hash>: Specify the number of threads to generate hashes\n");
	printf("  -o <num_threads_sort>: Specify the number of threads to sort hashes\n");
	printf("  -i <num_threads_io>: Specify the number of threads for reading and writing buckets\n");
	printf("  -f <filename>: Specify the filename\n");
	printf("  -m <memorysize>: Specify the memory size as an integer in MB\n");
	printf("  -s <filesize>: Specify the filesize as an integer in MB\n");
	printf("  -k <num_records>: Specify the number of records, 2^k; this overides -s <filesize>\n");
	printf("  -y <hash_buffer_size>: Specify the number of records to buffer when generating hashes\n");
	printf("  -p <num_records>: Specify the number of records to print from head, must specify -f <filename>\n");
	printf("  -r <num_records>: Specify the number of records to print from tail, must specify -f <filename>\n");
	printf("  -a <search_hash>: Specify the number of records to print, must specify -f <filename>\n");
	printf("  -l <prefix_length>: Specify the number of records to print, must specify -f <filename>\n");
	printf("  -c <search_records>: Specify the number of records to print, must specify -f <filename>\n");
	printf("  -d <bool> turns on debug mode with true, off with false \n");
	printf("  -x <bool> turns hash generation on with true, off with false; default is on \n");
	printf("  -z <bool> turns sort on with true, off with false; default is on \n");
	printf("  -b <num_records>: verify hashes as correct BLAKE3 hashes \n");
	printf("  -v <bool> verify hashes from file, off with false, on with true; default is off \n");
	printf("  -h: Display this help message\n");
}

double min(double a, double b)
{
	return (a < b) ? a : b;
}

void *sort_bucket(void *arg)
{
	int *return_value = (int *)arg;
	ThreadArgs *args = (ThreadArgs *)arg;
	Bucket *buckets = args->buckets;
	int num_buckets_to_process = args->num_buckets_to_process;
	unsigned long long offset = args->offset;
	int threadID = args->threadID;

	// Read bucket and store it in the array of buckets
	// You can perform your actual reading logic here
	for (int b = 0; b < num_buckets_to_process; ++b)
	{
		if (b == threadID)
		{
			// if (HASHSORT)
			qsort(buckets[b].records, BUCKET_SIZE, sizeof(MemoRecord), compareMemoRecords);

			if (DEBUG)
			{
				printf("[SORT] qsort %d %lu\n", BUCKET_SIZE, sizeof(MemoRecord));
				printf("[SORT] BUCKET_SIZE=%d\n", BUCKET_SIZE);
				printf("[SORT] FLUSH_SIZE=%llu\n", FLUSH_SIZE);
				printf("[SORT] sizeof(MemoRecord)=%lu\n", sizeof(MemoRecord));
			}
		}
	}

	*return_value = 0;
	pthread_exit((void *)return_value);
}

void *read_bucket(void *arg)
{
	int *return_value = (int *)arg;
	ThreadArgs *args = (ThreadArgs *)arg;
	Bucket *buckets = args->buckets;
	int num_buckets_to_process = args->num_buckets_to_process;
	unsigned long long offset = args->offset;
	int threadID = args->threadID;
	int fd = args->fd;
	if (DEBUG)
		printf("read_bucket thread %d\n", threadID);

	// You can perform your actual reading logic here
	for (int b = 0; b < num_buckets_to_process; ++b)
	{
		if (b == threadID)
		{
			if (DEBUG)
				printf("reading bucket %d at offset %llu\n", b, offset);

			if (DEBUG)
				printf("sem_wait(%d): wait\n", b);
			semaphore_wait(&semaphore_io);
			if (DEBUG)
				printf("sem_wait(%d): found\n", b);

			long long bytesRead = 0;
			bytesRead = pread(fd, buckets[b].records, BUCKET_SIZE * sizeof(MemoRecord), offset);
			if (bytesRead < 0 || bytesRead != BUCKET_SIZE * sizeof(MemoRecord))
			{
				printf("Error reading bucket %d from file at offset %llu; bytes read %llu when it expected %lu\n", b, offset, bytesRead, BUCKET_SIZE * sizeof(MemoRecord));
				close(fd);
				*return_value = 1;
				pthread_exit((void *)return_value);
			}
			if (DEBUG)
				printf("[SORT] read %lld bytes, expecting %lu bytes\n", bytesRead, BUCKET_SIZE * sizeof(MemoRecord));
			// Release the semaphore
			if (DEBUG)
				printf("sem_post(%d): wait\n", b);
			semaphore_post(&semaphore_io);
			if (DEBUG)
				printf("sem_post(%d): found\n", b);
		}
	}

	*return_value = 0;
	pthread_exit((void *)return_value);
}

void *write_bucket(void *arg)
{
	int *return_value = (int *)arg;
	ThreadArgs *args = (ThreadArgs *)arg;
	Bucket *buckets = args->buckets;
	int num_buckets_to_process = args->num_buckets_to_process;
	unsigned long long offset = args->offset;
	int threadID = args->threadID;
	int fd = args->fd;

	// You can perform your actual reading logic here
	for (int b = 0; b < num_buckets_to_process; ++b)
	{
		if (b == threadID)
		{
			// Wait on the semaphore
			semaphore_wait(&semaphore_io);
			if (DEBUG)
				printf("writing bucket %d at offset %llu\n", b, offset);

			long long bytesRead = 0;
			bytesRead = pwrite(fd, buckets[b].records, BUCKET_SIZE * sizeof(MemoRecord), offset);
			if (bytesRead < 0 || bytesRead != BUCKET_SIZE * sizeof(MemoRecord))
			{
				printf("Error writing bucket %d from file at offset %llu; bytes written %llu when it expected %lu\n", b, offset, bytesRead, BUCKET_SIZE * sizeof(MemoRecord));
				close(fd);
				*return_value = 1;
				pthread_exit((void *)return_value);
			}
			if (DEBUG)
				printf("[SORT] write %lld bytes, expecting %lu bytes\n", bytesRead, BUCKET_SIZE * sizeof(MemoRecord));

			// Release the semaphore
			semaphore_post(&semaphore_io);
		}
	}

	*return_value = 0;
	pthread_exit((void *)return_value);
}

#ifdef __linux__
void print_free_memory()
{
	struct sysinfo memInfo;
	sysinfo(&memInfo);

	long long totalMemory = memInfo.totalram;
	totalMemory *= memInfo.mem_unit;

	long long freeMemory = memInfo.freeram;
	freeMemory *= memInfo.mem_unit;

	// printf("Total Memory: %lld bytes\n", totalMemory);
	// printf("Free Memory: %lld bytes\n", freeMemory);
}
#endif

#ifdef __APPLE__
void print_free_memory()
{
	FILE *fp = popen("vm_stat", "r");
	if (fp == NULL)
	{
		perror("Error opening pipe");
		// return 0;
	}

	char line[256];
	unsigned long long free_memory = 0;

	while (fgets(line, sizeof(line), fp) != NULL)
	{
		if (strstr(line, "Pages free"))
		{
			unsigned long long pages_free;
			if (sscanf(line, "Pages free: %llu.", &pages_free) == 1)
			{
				// Each page on macOS is 4096 bytes
				free_memory = pages_free * 4096;
				break;
			}
		}
	}

	pclose(fp);
	// return free_memory;

	printf("Free Memory: %llu bytes\n", free_memory);

	// On macOS, there's no direct way to get free memory.
	// You might need to use other methods or APIs for this purpose.
}
#endif

int main(int argc, char *argv[])
{
	Timer timer;
	double elapsedTime;
	double generating_time, sort_time;

	char *FILENAME = NULL;	// Default value
	long long FILESIZE = 0; // Default value
	long long KSIZE = 0;
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

	bool hashgen = false;

	int opt;
	while ((opt = getopt(argc, argv, "t:o:m:k:f:s:p:r:a:l:c:d:i:x:v:b:y:z:h")) != -1)
	{
		switch (opt)
		{
		case 't':
			NUM_THREADS = atoi(optarg);
			// printf("NUM_THREADS=%d\n", NUM_THREADS);

			BATCH_SIZE = HASHGEN_THREADS_BUFFER / NUM_THREADS;
			// printf("BATCH_SIZE=%ld\n", BATCH_SIZE);

			hashgen = true;

			if (NUM_THREADS == 1)
			{
				printf("multi-threading with only 1 thread is not supported at this time, exiting\n");
				return 1;
			}
			break;
		case 'o':
			num_threads_sort = atoi(optarg);
			// printf("num_threads_sort=%d\n", num_threads_sort);
			break;
		case 'y':
			HASHGEN_THREADS_BUFFER = atoi(optarg);
			// printf("HASHGEN_THREADS_BUFFER=%d\n", HASHGEN_THREADS_BUFFER);
			break;
		case 'i':
			num_threads_io = atoi(optarg);
			// printf("num_threads_io=%d\n", num_threads_io);
			break;
		case 'm':
			memory_size = atoi(optarg);
			// printf("memory_size=%lld MB\n", memory_size);
			break;
		case 'f':
			FILENAME = optarg;
			// printf("FILENAME=%s\n", FILENAME);
			break;
		case 's':
			FILESIZE = atoi(optarg);
			// printf("FILESIZE=%lld MB\n", FILESIZE);
			break;
		case 'k':
			KSIZE = atoi(optarg);
			// printf("KSIZE=%lld\n", KSIZE);
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
			if (strcmp(optarg, "true") == 0)
			{
				HASHGEN = true;
			}
			else if (strcmp(optarg, "false") == 0)
			{
				HASHGEN = false;
			}
			else
			{
				HASHGEN = true;
			}
			printf("HASHGEN=%s\n", HASHGEN ? "true" : "false");
			break;
		case 'z':
			if (strcmp(optarg, "true") == 0)
			{
				HASHSORT = true;
			}
			else if (strcmp(optarg, "false") == 0)
			{
				HASHSORT = false;
			}
			else
			{
				HASHSORT = true;
			}
			// printf("HASHSORT=%s\n", HASHSORT ? "true" : "false");
			break;
		case 'd':
			if (strcmp(optarg, "true") == 0)
			{
				DEBUG = true;
			}
			else if (strcmp(optarg, "false") == 0)
			{
				DEBUG = false;
			}
			else
			{
				DEBUG = false;
			}
			printf("DEBUG=%s\n", DEBUG ? "true" : "false");
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
			if (strcmp(optarg, "true") == 0)
			{
				verify_records = true;
			}
			else if (strcmp(optarg, "false") == 0)
			{
				verify_records = false;
			}
			else
			{
				verify_records = false;
			}
			printf("verify_records=%s\n", verify_records ? "true" : "false");
			break;
		case 'h':
			printHelp();
			return 0;
		default:
			printUsage();
			return 1;
		}
	}

	if (FILENAME == NULL)
	{
		printf("Error: filename (-f) is mandatory.\n");
		printUsage();
		return 1;
	}
	if (FILENAME != NULL && print_records == 0 && verify_records == false && targetHash == NULL && verify_records_num == 0 && search_records == 0 && (NUM_THREADS <= 0 || num_threads_sort <= 0 || FILESIZE < 0 || memory_size <= 0))
	{
		printf("Error: mandatory command line arguments have not been used, try -h for more help\n");
		printUsage();
		return 1;
	}

	const char *path = FILENAME; // Example path, you can change it to any valid path

	unsigned long long bytes_free = getDiskSpace(path);
	if (bytes_free > 0)
	{
		if (DEBUG)
			printf("Free disk space on %s: %llu bytes\n", path, bytes_free);
	}
	// printf("bytes_free=%lld\n", bytes_free);

	if (FILESIZE == 0 && KSIZE >= 20)
	{
		FILESIZE = pow(2, KSIZE - 20) * RECORD_SIZE;
		// printf("***FILESIZE=%lld\n", FILESIZE);
	}
	else if (FILESIZE == 0 && KSIZE < 20)
	{
		printf("-k must be set to 20 or greater\n");
		return 1;
	}

	if (FILESIZE == 0 || FILESIZE * 1024 * 1024 > bytes_free)
		FILESIZE = (int)(bytes_free / (1024 * 1024));

	long long sort_memory = 0;
	long long EXPECTED_TOTAL_FLUSHES = 0;
	bool found_good_config = false;

	// memory and file sizes must be divisible
	int ratio = (int)ceil((double)FILESIZE / memory_size);
	int num_times_ran = 0;
	// printf("ratio=%d\n", ratio);
	long long FILESIZE_byte = FILESIZE * 1024 * 1024;
	long long memory_size_byte = FILESIZE * 1024 * 1024 / ratio;

	// printf("memory_size_byte=%lld\n", memory_size_byte);
	// printf("FILESIZE_byte=%lld\n", FILESIZE_byte);

	if (hashgen)
	{
		for (WRITE_SIZE = 1024 * 1024 / ratio; WRITE_SIZE > 0; WRITE_SIZE = WRITE_SIZE / 2)
		{
			num_times_ran += 1;
			FLUSH_SIZE = ratio;
			if (DEBUG)
				printf("for(): 1: %llu\n", FLUSH_SIZE);
			BUCKET_SIZE = (long long)WRITE_SIZE * 1024 * FLUSH_SIZE;
			if (DEBUG)
				printf("for(): 2: %d\n", BUCKET_SIZE);
			NUM_BUCKETS = (long long)FILESIZE_byte / BUCKET_SIZE;
			if (DEBUG)
				printf("for(): 3: %d\n", NUM_BUCKETS);
			PREFIX_SIZE = (unsigned int)(log(NUM_BUCKETS) / log(2)) + 1;
			if (DEBUG)
				printf("for(): 4: %d\n", PREFIX_SIZE);
			NUM_BUCKETS = pow(2, PREFIX_SIZE);
			if (DEBUG)
				printf("for(): 5: %d\n", NUM_BUCKETS);
			PREFIX_SIZE = (unsigned int)(log(NUM_BUCKETS) / log(2));
			if (DEBUG)
				printf("for(): 6: %d\n", PREFIX_SIZE);
			BUCKET_SIZE = (long long)FILESIZE_byte / NUM_BUCKETS;
			if (DEBUG)
				printf("for(): 7: %d\n", BUCKET_SIZE);

			EXPECTED_TOTAL_FLUSHES = (long long)FILESIZE_byte / (WRITE_SIZE);
			if (DEBUG)
				printf("for(): 8\n");
			sort_memory = (long long)BUCKET_SIZE * num_threads_sort;
			if (DEBUG)
				printf("for(): 9: %d\n", sort_memory);

			// valid configuration
			if (sort_memory <= memory_size_byte && NUM_BUCKETS >= 64)
			{
				// fix all numbers to ensure proper fitting of all hashes
				WRITE_SIZE = (long long)floor(memory_size_byte / NUM_BUCKETS);
				if (DEBUG)
					printf("for(): 10\n");
				WRITE_SIZE = (long long)(WRITE_SIZE / 16) * 16;
				if (DEBUG)
					printf("for(): 11\n");
				BUCKET_SIZE = (long long)WRITE_SIZE * FLUSH_SIZE; // measured in records
				if (DEBUG)
					printf("for(): 12\n");
				memory_size_byte = (long long)WRITE_SIZE * NUM_BUCKETS;
				if (DEBUG)
					printf("for(): 13\n");
				FILESIZE_byte = (long long)BUCKET_SIZE * NUM_BUCKETS;
				if (DEBUG)
					printf("for(): 14\n");
				sort_memory = (long long)BUCKET_SIZE * num_threads_sort;
				if (DEBUG)
					printf("for(): 15\n");
				NUM_ENTRIES = (long long)FILESIZE_byte / RECORD_SIZE;
				if (DEBUG)
					printf("for(): 16\n");
				EXPECTED_TOTAL_FLUSHES = (long long)FILESIZE_byte / (WRITE_SIZE);
				if (DEBUG)
					printf("for(): 17\n");
				BUCKET_SIZE = (long long)WRITE_SIZE * FLUSH_SIZE / RECORD_SIZE;
				if (DEBUG)
					printf("for(): 18\n");

				// printf("-------------------------\n");
				// printf("number of times while loop ran: %d\n", num_times_ran);
				// printf("found good configuration:\n");
				// printf("memory_size_byte=%lld\n", memory_size_byte);
				// printf("FILESIZE_byte=%lld\n", FILESIZE_byte);
				// printf("WRITE_SIZE=%lld\n", WRITE_SIZE);
				// printf("FLUSH_SIZE=%llu\n", FLUSH_SIZE);
				// printf("BUCKET_SIZE=%d\n", BUCKET_SIZE);
				// printf("NUM_BUCKETS=%d\n", NUM_BUCKETS);
				// printf("PREFIX_SIZE=%d\n", PREFIX_SIZE);
				// printf("EXPECTED_TOTAL_FLUSHES=%lld\n", EXPECTED_TOTAL_FLUSHES);
				// printf("sort_memory=%lld\n", sort_memory);
				// printf("NUM_ENTRIES=%lld\n", NUM_ENTRIES);
				// printf("-------------------------\n");

				found_good_config = true;
				break;
			}
		}

		if (found_good_config == false)
		{
			printf("no valid configuration found... this should never happen\n");

			printf("exiting...\n");
			return 1;
		}
	}

	print_free_memory();

	long long MEMORY_MAX = memory_size_byte;
	// printf("MEMORY_MAX=%lld\n", MEMORY_MAX);
	// printf("RECORD_SIZE=%d\n", RECORD_SIZE);
	// printf("HASH_SIZE=%d\n", RECORD_SIZE - NONCE_SIZE);
	// printf("NONCE_SIZE=%d\n", NONCE_SIZE);

	num_threads_io = min(num_threads_sort, num_threads_io);
	// printf("num_threads_hash=%d\n", NUM_THREADS);
	// printf("num_threads_sort=%d\n", num_threads_sort);
	// printf("num_threads_io=%d\n", num_threads_io);

	if (verify_records)
	{
		resetTimer(&timer);
		int VERIFY_BUFFER_SIZE = 999984;
		int fd;
		char *buffer = (char *)malloc(VERIFY_BUFFER_SIZE);
		ssize_t bytesRead;
		off_t offset = 0;

		if (buffer == NULL)
		{
			fprintf(stderr, "Memory allocation failed\n");
			return 1;
		}

		fd = open(FILENAME, O_RDONLY);
		if (fd == -1)
		{
			perror("Unable to open file");
			free(buffer);
			return 1;
		}
		int num_reads = 0;
		bool all_sorted = true;

		// Read the file in chunks of BUFFER_SIZE bytes until the end of file
		while ((bytesRead = readChunk(fd, buffer, offset, VERIFY_BUFFER_SIZE)) > 0)
		{
			//  Verify if the records in the buffer are sorted
			if (!verifySorted(buffer, bytesRead))
			{
				printf("Records are not sorted at %lld offset.\n", offset);
				all_sorted = false;
				break;
			}

			// Update the offset for the next read
			offset += bytesRead;
			num_reads++;
		}

		if (bytesRead == -1)
		{
			fprintf(stderr, "Error reading buffer after reading %lld bytes\n", offset);
		}

		if (all_sorted)
			printf("Read %lld bytes and found all records are sorted.\n", offset);
		close(fd);
		free(buffer);
		elapsedTime = getTimer(&timer);
		double progress = 100.0;
		double remaining_time = 0.0;
		double throughput_MB = (offset / (1024 * 1024)) / elapsedTime;

		printf("[%.3lf][VERIFY]: %.2lf%% completed, ETA %.1lf seconds, %d flushes, %.1lf MB/sec\n", elapsedTime, progress, remaining_time, num_reads, throughput_MB);

		return 0;
	}

	// print records
	else if (print_records > 0)
	{
		// Print the contents of the specified file
		if (head)
		{
			printf("Printing first %lld of file '%s'...\n", print_records, FILENAME);
			printFile(FILENAME, print_records);
		}
		if (tail)
		{
			printf("Printing first %lld of file '%s'...\n", print_records, FILENAME);
			printFileTail(FILENAME, print_records);
		}

		return 0;
	}
	// search hash
	else if (targetHash != NULL)
	{
		// Open file for reading
		int fd = open(FILENAME, O_RDONLY);
		if (fd < 0)
		{
			perror("Error opening file for reading");
			return 1;
		}
		long long filesize = getFileSize(FILENAME);

		resetTimer(&timer);

		// Perform binary search
		int seekCount = 0;
		int index = binarySearch(targetHash, prefixLength, fd, filesize, &seekCount, false);

		elapsedTime = getTimer(&timer);

		if (index >= 0)
		{
			// Hash found
			MemoRecord foundNumber;
			lseek(fd, index * sizeof(MemoRecord), SEEK_SET);
			read(fd, &foundNumber, sizeof(MemoRecord));

			printf("Hash found at index: %d\n", index);
			printf("Number of lookups: %d\n", seekCount);
			printf("Hash search: ");
			printBytes(byteArray, sizeof(byteArray));
			printf("/%zu\n", prefixLength);
			printf("Hash found : ");
			printBytes(foundNumber.hash, sizeof(foundNumber.hash));
			printf("\n");

			unsigned long long nonceValue = 0;
			for (int i = 0; i < sizeof(foundNumber.nonce); i++)
			{
				nonceValue |= (unsigned long long)foundNumber.nonce[i] << (i * 8);
			}

			printf("Nonce: %llu/", nonceValue);
			printBytes(foundNumber.nonce, sizeof(foundNumber.nonce));
			printf("\n");
		}
		else
		{
			printf("Hash not found after %d lookups\n", seekCount);
		}
		printf("Time taken: %.2f microseconds\n", elapsedTime);

		// Close the file
		close(fd);

		return 0;
	}
	// search bulk
	else if (search_records > 0)
	{
		printf("searching for random hashes\n");
		size_t numberLookups = search_records;
		if (numberLookups <= 0)
		{
			printf("Invalid number of lookups\n");
			return 1;
		}

		// Get the length of the prefix
		if (prefixLength <= 0 || prefixLength > 10)
		{
			printf("Invalid prefix length\n");
			return 1;
		}

		// Open file for reading
		int fd = open(FILENAME, O_RDONLY);
		if (fd < 0)
		{
			perror("Error opening file for reading");
			return 1;
		}
		long filesize = getFileSize(FILENAME);

		// Seed the random number generator with the current time
		srand((unsigned int)time(NULL));
		int seekCount = 0;
		int found = 0;
		int notfound = 0;

		resetTimer(&timer);

		for (int searchNum = 0; searchNum < numberLookups; searchNum++)
		{
			uint8_t byteArray[HASH_SIZE];
			for (size_t i = 0; i < HASH_SIZE; ++i)
			{
				byteArray[i] = rand() % 256;
			}

			uint8_t *targetHash = byteArray;
			if (targetHash == NULL)
			{
				printf("Error: Byte array too small\n");
				return 1;
			}

			// Perform binary search
			int index = binarySearch(targetHash, prefixLength, fd, filesize, &seekCount, true);

			if (index >= 0)
			{
				// Hash found
				MemoRecord foundNumber;
				lseek(fd, index * sizeof(MemoRecord), SEEK_SET);
				read(fd, &foundNumber, sizeof(MemoRecord));

				found++;
				if (DEBUG)
				{
					printf("Hash found at index: %d\n", index);
					printf("Hash found : ");
					printBytes(foundNumber.hash, sizeof(foundNumber.hash));
					printf("\n");

					unsigned long long nonceValue = 0;
					for (int i = 0; i < sizeof(foundNumber.nonce); i++)
					{
						nonceValue |= (unsigned long long)foundNumber.nonce[i] << (i * 8);
					}

					printf("Nonce: %llu/", nonceValue);
					printBytes(foundNumber.nonce, sizeof(foundNumber.nonce));
					printf("\n");
				}
			}
			else
			{
				notfound++;
				if (DEBUG)
					printf("Hash not found\n");
			}

			if (DEBUG)
			{
				printf("Number of lookups: %d\n", seekCount);
				printf("Hash search: ");
				printBytes(byteArray, sizeof(byteArray));
				printf("/%zu\n", prefixLength);
			}
		}

		// Calculate elapsed time in microseconds
		elapsedTime = getTimer(&timer);
		printf("Number of total lookups: %zu\n", numberLookups);
		printf("Number of searches found: %d\n", found);
		printf("Number of searches not found: %d\n", notfound);
		printf("Number of total seeks: %d\n", seekCount);
		printf("Time taken: %.2f ms/lookup\n", elapsedTime * 1000.0 / numberLookups);
		printf("Throughput lookups/sec: %.2f\n", numberLookups / elapsedTime);

		// Close the file
		close(fd);

		return 0;
	}
	// verify bulk
	else if (verify_records_num > 0)
	{
		printf("verifying random records against BLAKE3 hashes\n");
		size_t numberLookups = verify_records_num;
		if (numberLookups <= 0)
		{
			printf("Invalid number of lookups\n");
			return 1;
		}

		// Open file for reading
		int fd = open(FILENAME, O_RDONLY);
		if (fd < 0)
		{
			perror("Error opening file for reading");
			return 1;
		}
		long filesize = getFileSize(FILENAME);
		unsigned long long num_records_in_file = filesize / RECORD_SIZE;

		// Seed the random number generator with the current time
		srand((unsigned int)time(NULL));
		int seekCount = 0;
		int found = 0;
		int notfound = 0;

		resetTimer(&timer);

		for (int searchNum = 0; searchNum < numberLookups; searchNum++)
		{
			int index = rand() % num_records_in_file;

			MemoRecord foundNumber;
			MemoRecord foundNumber2;
			lseek(fd, index * sizeof(MemoRecord), SEEK_SET);
			read(fd, &foundNumber, sizeof(MemoRecord));

			unsigned long long nonceValue = 0;
			for (int i = 0; i < sizeof(foundNumber.nonce); i++)
			{
				nonceValue |= (unsigned long long)foundNumber.nonce[i] << (i * 8);
			}

			generateBlake3(&foundNumber2, nonceValue);

			if (DEBUG)
			{
				printf("Hash found at index: %d\n", index);
				printf("Hash found : ");
				printBytes(foundNumber.hash, sizeof(foundNumber.hash));
				printf("\n");

				printf("Nonce: %llu/", nonceValue);
				printBytes(foundNumber.nonce, sizeof(foundNumber.nonce));
				printf("\n");
			}

			if (memcmp(&foundNumber, &foundNumber2, sizeof(MemoRecord)) == 0)
			{
				if (DEBUG)
					printf("hash verification succesful\n");
				found++;
			}
			else
			{
				if (DEBUG)
					printf("hash verification failed\n");
				notfound++;
			}
		}

		// Calculate elapsed time in microseconds
		elapsedTime = getTimer(&timer);
		printf("Number of total verifications: %zu\n", numberLookups);
		printf("Number of verifications successful: %d\n", found);
		printf("Number of verifications failed: %d\n", notfound);
		printf("Time taken: %.2f ms/verification\n", elapsedTime * 1000.0 / numberLookups);
		printf("Throughput verifications/sec: %.2f\n", numberLookups / elapsedTime);

		// Close the file
		close(fd);

		return 0;
	}

	// hash generation
	else
	{
		int fd = -1;
		double last_progress_update = 0.0;
		long long last_progress_i = 0;
		if (HASHGEN == true)
		{
			const char *EXTENSION = ".config";
			char *FILENAME_CONFIG = (char *)malloc((strlen(FILENAME) + strlen(EXTENSION)) * sizeof(char));

			strcat(FILENAME_CONFIG, FILENAME);
			strcat(FILENAME_CONFIG, EXTENSION);

			// printf("storing vault configuration in %s\n", FILENAME_CONFIG);
			FILE *config_file = fopen(FILENAME_CONFIG, "w"); // Open or create the config file for writing

			if (config_file == NULL)
			{
				printf("Error opening the config file!\n");
				return 1;
			}

			// Write the variables A, B, and C, each initialized to 0, to the config file
			fprintf(config_file, "FILESIZE_byte=%lld\n", FILESIZE_byte);
			fprintf(config_file, "NUM_BUCKETS=%d\n", NUM_BUCKETS);
			fprintf(config_file, "BUCKET_SIZE=%d\n", BUCKET_SIZE);
			fprintf(config_file, "RECORD_SIZE=%d\n", RECORD_SIZE);

			fclose(config_file); // Close the config file

			// printf("hash generation and sorting...\n");
			resetTimer(&timer);

			double generating_start = getTimer(&timer);

			// Open file for writing
			fd = open(FILENAME, O_RDWR | O_CREAT | O_TRUNC, 0644);
			if (fd < 0)
			{
				perror("Error opening file for writing");
				return 1;
			}

			// Array to hold the generated random records
			MemoRecord record;
			// Allocate memory for the array of Bucket structs
			Bucket *buckets = malloc(NUM_BUCKETS * sizeof(Bucket));
			if (buckets == NULL)
			{
				printf("2: Error allocating memory: %lu\n", NUM_BUCKETS * sizeof(Bucket));
				return 1;
			}

			// Initialize each bucket
			for (int i = 0; i < NUM_BUCKETS; ++i)
			{
				// Allocate memory for the records in each bucket
				buckets[i].records = malloc(BUCKET_SIZE * sizeof(MemoRecord));
				if (buckets[i].records == NULL)
				{
					printf("3 Error allocating memory %lu\n", BUCKET_SIZE * sizeof(MemoRecord));
					// Free previously allocated memory
					for (int j = 0; j < i; ++j)
					{
						free(buckets[j].records);
					}
					free(buckets);
					return 1;
				}
				buckets[i].count = 0; // Initialize the record count for each bucket
				buckets[i].flush = 0; // Initialize flush for each bucket
			}

			// printf("initializing circular array...\n");
			// Initialize the circular array
			struct CircularArray circularArray;
			initCircularArray(&circularArray);

			// printf("Create thread data structure...\n");
			// Create thread data structure
			struct ThreadData threadData[NUM_THREADS];

			// printf("Create hash generation threads...\n");
			// Create threads
			pthread_t threads[NUM_THREADS];
			for (int i = 0; i < NUM_THREADS; i++)
			{
				threadData[i].circularArray = &circularArray;
				threadData[i].threadID = i;
				pthread_create(&threads[i], NULL, arrayGenerationThread, &threadData[i]);
			}
			// printf("Hash generation threads created...\n");

			unsigned long long totalFlushes = 0;

			// Measure time taken for hash generation using gettimeofday
			last_progress_i = 0;

			elapsedTime = getTimer(&timer);
			last_progress_update = elapsedTime;

			// Write random records to buckets based on their first 2-byte prefix
			unsigned long long i = 0;
			int flushedBucketsNeeded = NUM_BUCKETS;
			MemoRecord *consumedArray;
			consumedArray = malloc(BATCH_SIZE * sizeof(MemoRecord));

			if (DEBUG)
				printf("BATCH_SIZE=%zu\n", BATCH_SIZE);
			if (DEBUG)
				printf("flushedBucketsNeeded=%d\n", flushedBucketsNeeded);

			while (flushedBucketsNeeded > 0)
			{
				if (DEBUG)
					printf("removeBatch()...\n");
				removeBatch(&circularArray, consumedArray);

				if (DEBUG)
					printf("processing batch of size %ld...\n", BATCH_SIZE);
				for (int b = 0; b < BATCH_SIZE; b++)
				{
					// Calculate the bucket index based on the first 2-byte prefix
					off_t bucketIndex = getBucketIndex(consumedArray[b].hash, PREFIX_SIZE);

					// Add the random record to the corresponding bucket
					Bucket *bucket = &buckets[bucketIndex];
					if (bucket->count < WRITE_SIZE / RECORD_SIZE)
					{
						bucket->records[bucket->count++] = (MemoRecord)consumedArray[b];
					}
					else
					{
						// Bucket is full, write it to disk
						if (bucket->flush < FLUSH_SIZE)
						{
							// should parallelize the sort and write to disk here...
							// if all buckets fit in memory, sort them now before writing to disk
							if (FLUSH_SIZE == 1)
							{
								// Sort the bucket contents
								if (DEBUG)
									printf("sorting bucket before flush %d...\n", b);

								if (HASHSORT)
									qsort(bucket->records, BUCKET_SIZE, sizeof(MemoRecord), compareMemoRecords);
							}

							writeBucketToDisk(bucket, fd, bucketIndex);

							// Reset the bucket count
							bucket->count = 0;
							bucket->flush++;
							if (bucket->flush == FLUSH_SIZE)
								flushedBucketsNeeded--;

							totalFlushes++;
						}
					}
				}

				// elapsedTime = getTimer(&timer);

				// if (elapsedTime > last_progress_update + 1.0)
				// {
				// 	double elapsed_time_since_last_progress_update = elapsedTime - last_progress_update;
				// 	last_progress_update = elapsedTime;

				// 	// Calculate and print estimated completion time
				// 	double progress = min(i * 100.0 / NUM_ENTRIES, 100.0);
				// 	double remaining_time = elapsedTime / (progress / 100.0) - elapsedTime;

				// 	// printf("[%.0lf][HASHGEN]: %.2lf%% completed, ETA %.1lf seconds, %llu/%llu flushes, %.1lf MB/sec\n", floor(elapsedTime), progress, remaining_time, totalFlushes, EXPECTED_TOTAL_FLUSHES, ((i - last_progress_i) / elapsed_time_since_last_progress_update) * (8.0 + 8.0) / (1024 * 1024));

				// 	last_progress_i = i;
				// }
				i += BATCH_SIZE;
			}

			if (DEBUG)
				printf("finished generating %llu hashes and wrote them to disk using %llu flushes...\n", i, totalFlushes);

			// Free memory allocated for records in each bucket
			for (int i = 0; i < NUM_BUCKETS; i++)
			{
				free(buckets[i].records);
			}
			free(buckets);

			free(consumedArray);

			double generating_end = getTimer(&timer);
			generating_time = generating_end - generating_start;
		}
		else
		{
			// printf("opening file for sorting...\n");

			// Open file for writing
			fd = open(FILENAME, O_RDWR | O_LARGEFILE, 0644);
			if (fd < 0)
			{
				perror("Error opening file for reading/writing");
				return 1;
			}

			resetTimer(&timer);
		}
		double sort_start = elapsedTime;

		bool doSort = true;

		elapsedTime = getTimer(&timer);
		last_progress_update = elapsedTime;

		if (DEBUG)
			print_free_memory();

		if (DEBUG)
		{
			printf("planning to allocate 1: %lu bytes memory...", num_threads_sort * sizeof(Bucket));
			printf("planning to allocate 2: %lu bytes memory...", num_threads_sort * BUCKET_SIZE * sizeof(MemoRecord));
			printf("planning to allocate 3: %lu bytes memory...", num_threads_sort * sizeof(ThreadArgs));
			printf("planning to allocate 4: %lu bytes memory...", num_threads_sort * sizeof(pthread_t));
		}

		if (FLUSH_SIZE > 1 && doSort && HASHSORT)
		{
			// printf("external sort started, expecting %llu flushes for %d buckets...\n", FLUSH_SIZE, NUM_BUCKETS);
			int EXPECTED_BUCKETS_SORTED = NUM_BUCKETS;
			if (DEBUG)
				printf("external sort started, expecting %llu flushes for %d buckets...\n", FLUSH_SIZE, NUM_BUCKETS);

			if (DEBUG)
			{
				printf("EXPECTED_BUCKETS_SORTED=%d\n", EXPECTED_BUCKETS_SORTED);
				printf("FLUSH_SIZE=%llu\n", FLUSH_SIZE);
				printf("NUM_BUCKETS=%d\n", NUM_BUCKETS);
				printf("BUCKET_SIZE=%d\n", BUCKET_SIZE);
			}

			// Allocate memory for num_threads_sort bucket
			if (DEBUG)
				printf("trying to allocate 1: %lu bytes memory...\n", num_threads_sort * sizeof(Bucket));
			Bucket *buckets = malloc(num_threads_sort * sizeof(Bucket));
			if (buckets == NULL)
			{
				perror("Memory allocation failed");
				return EXIT_FAILURE;
			}

			if (DEBUG)
				print_free_memory();

			// initialize state
			if (DEBUG)
				printf("trying to allocate 2: %lu bytes memory...\n", num_threads_sort * BUCKET_SIZE * sizeof(MemoRecord));
			for (int i = 0; i < num_threads_sort; ++i)
			{
				if (DEBUG)
					printf("trying to allocate 2:%d: %lu bytes memory...\n", i, BUCKET_SIZE * sizeof(MemoRecord));
				buckets[i].count = BUCKET_SIZE;
				buckets[i].records = malloc(BUCKET_SIZE * sizeof(MemoRecord));
				if (buckets[i].records == NULL)
				{
					printf("Memory allocation failed for %lu bytes\n", BUCKET_SIZE * sizeof(MemoRecord));
					close(fd);
					return 1;
				}

				if (DEBUG)
					print_free_memory();
			}

			if (DEBUG)
				printf("trying to allocate 3: %lu bytes memory...\n", num_threads_sort * sizeof(ThreadArgs));
			ThreadArgs *args = malloc(num_threads_sort * sizeof(ThreadArgs));
			if (args == NULL)
			{
				perror("Failed to allocate memory for args");
				return EXIT_FAILURE;
			}
			if (DEBUG)
				print_free_memory();
			if (DEBUG)
				printf("trying to allocate 4: %lu bytes memory...\n", num_threads_sort * sizeof(pthread_t));
			pthread_t *sort_threads = malloc(num_threads_sort * sizeof(pthread_t));

			if (sort_threads == NULL)
			{
				perror("Failed to allocate memory for sort threads");
				return EXIT_FAILURE;
			}
			if (DEBUG)
				print_free_memory();

			last_progress_i = 0;
			if (DEBUG)
				printf("before semaphore...\n");
			semaphore_init(&semaphore_io, num_threads_io);
			if (DEBUG)
				printf("after semaphore...\n");

			if (DEBUG)
				print_free_memory();

			for (unsigned long long i = 0; i < NUM_BUCKETS; i = i + num_threads_sort)
			{
				if (DEBUG)
					printf("inside for loop %llu...\n", i);

				if (DEBUG)
					print_free_memory();

				if (DEBUG)
					printf("processing bucket i=%llu\n", i);
				// Seek to the beginning of the bucket

				if (DEBUG)
					printf("starting reading threads...\n");
				for (int b = 0; b < num_threads_sort; b++)
				{
					args[b].buckets = buckets;
					args[b].threadID = b; // Assigning a sample value to 'test'
					args[b].num_buckets_to_process = num_threads_sort;
					args[b].offset = (i + b) * sizeof(MemoRecord) * BUCKET_SIZE;
					args[b].fd = fd;

					int status = pthread_create(&sort_threads[b], NULL, read_bucket, &args[b]);

					if (status != 0)
					{
						perror("pthread_create read_bucket");
						return EXIT_FAILURE;
					}
				}

				if (DEBUG)
					printf("waiting for reading threads...\n");
				// Wait for threads to finish
				for (int b = 0; b < num_threads_sort; b++)
				{
					void *thread_return_value;
					int status = pthread_join(sort_threads[b], &thread_return_value);
					if (status != 0)
					{
						perror("pthread_join read_bucket");
						printf("failed to wait for thread termination for thread read_bucket %d\n", b);
						return EXIT_FAILURE;
					}

					if (*(int *)thread_return_value != 0)
					{
						printf("Thread read_bucket has terminated with return value: %d\n", *((int *)thread_return_value));
						return EXIT_FAILURE;
					}
				}

				if (DEBUG)
					printf("starting sort threads...\n");
				for (int b = 0; b < num_threads_sort; b++)
				{
					args[b].buckets = buckets;
					args[b].threadID = b; // Assigning a sample value to 'test'
					args[b].num_buckets_to_process = num_threads_sort;
					args[b].offset = 0;
					args[b].fd = -1;

					int status = pthread_create(&sort_threads[b], NULL, sort_bucket, &args[b]);

					if (status != 0)
					{
						perror("pthread_create sort");
						return EXIT_FAILURE;
					}
				}

				if (DEBUG)
					printf("waiting for sort threads...\n");

				// Wait for threads to finish
				for (int b = 0; b < num_threads_sort; b++)
				{
					void *thread_return_value;
					int status = pthread_join(sort_threads[b], &thread_return_value);
					if (status != 0)
					{
						perror("pthread_join sort");
						printf("failed to wait for thread termination for thread sort %d\n", b);
						return EXIT_FAILURE;
					}

					if (*(int *)thread_return_value != 0)
					{
						printf("Thread sort has terminated with return value: %d\n", *((int *)thread_return_value));
						return EXIT_FAILURE;
					}
				}

				if (DEBUG)
					printf("starting writing threads...\n");
				for (int b = 0; b < num_threads_sort; b++)
				{
					args[b].buckets = buckets;
					args[b].threadID = b; // Assigning a sample value to 'test'
					args[b].num_buckets_to_process = num_threads_sort;
					args[b].offset = (i + b) * sizeof(MemoRecord) * BUCKET_SIZE;
					args[b].fd = fd;

					int status = pthread_create(&sort_threads[b], NULL, write_bucket, &args[b]);

					if (status != 0)
					{
						perror("pthread_create write_bucket");
						return EXIT_FAILURE;
					}
				}

				if (DEBUG)
					printf("waiting for writing threads...\n");

				// Wait for threads to finish
				for (int b = 0; b < num_threads_sort; b++)
				{
					void *thread_return_value;
					int status = pthread_join(sort_threads[b], &thread_return_value);
					if (status != 0)
					{
						perror("pthread_join write_bucket");
						printf("failed to wait for thread termination for thread write_bucket %d\n", b);
						return EXIT_FAILURE;
					}

					if (*(int *)thread_return_value != 0)
					{
						printf("Thread write_bucket has terminated with return value: %d\n", *((int *)thread_return_value));
						return EXIT_FAILURE;
					}
				}

				// elapsedTime = getTimer(&timer);

				// if (elapsedTime > last_progress_update + 1)
				// {
				// 	double elapsed_time_since_last_progress_update = elapsedTime - last_progress_update;

				// 	int totalBucketsSorted = i;
				// 	float perc_done = totalBucketsSorted * 100.0 / EXPECTED_BUCKETS_SORTED;
				// 	float eta = (elapsedTime - sort_start) / (perc_done / 100.0) - (elapsedTime - sort_start);
				// 	float diskSize = FILESIZE_byte / (1024 * 1024);
				// 	float throughput_MB = diskSize * perc_done * 1.0 / 100.0 / (elapsedTime - sort_start);
				// 	float throughput_MB_latest = (((i - last_progress_i) * BUCKET_SIZE * sizeof(MemoRecord) * 1.0) / (elapsedTime - last_progress_update)) / (1024 * 1024);
				// 	if (DEBUG)
				// 		printf("%llu %llu %d %llu %lu %f %f\n", i, last_progress_i, BUCKET_SIZE, FLUSH_SIZE, sizeof(MemoRecord), elapsedTime, last_progress_update);
				// 	float progress = perc_done;
				// 	float remaining_time = eta;
				// 	printf("[%.0lf][SORT]: %.2lf%% completed, ETA %.1lf seconds, %llu/%d flushes, %.1lf MB/sec\n", elapsedTime, progress, remaining_time, i, NUM_BUCKETS, throughput_MB_latest);

				// 	last_progress_i = i;

				// 	last_progress_update = elapsedTime;
				// }
			}
			// end of for loop

			// Destroy the semaphore
			pthread_mutex_destroy(&semaphore_io.mutex);
			pthread_cond_destroy(&semaphore_io.condition);

			// Free allocated memory
			for (int i = 0; i < num_threads_sort; ++i)
			{
				free(buckets[i].records);
			}
			free(buckets); // Free the memory allocated for the array of buckets
			double sort_end = getTimer(&timer);
			sort_time = sort_end - sort_start;
		}
		else
		{
			// if (HASHSORT == true)
			// 	printf("in-memory sort completed!\n");
		}

		close(fd);

		elapsedTime = getTimer(&timer);

		// Calculate hashes per second
		double hashes_per_second = NUM_ENTRIES / elapsedTime;

		// Calculate bytes per second
		double bytes_per_second = sizeof(MemoRecord) * NUM_ENTRIES / elapsedTime;

		// printf("Completed %lld MB vault %s in %.2lf seconds : %.2f MH/s %.2f MB/s\n", FILESIZE, FILENAME, elapsedTime, hashes_per_second / 1000000.0, bytes_per_second * 1.0 / (1024 * 1024));
		printf("%f,%f,%f\n", generating_time, sort_time, elapsedTime);

		return 0;
	}
	// end of hash generation
}
