// make a benchmark option that generates hashes without sorting/storing
#include "vault.h"
#include <semaphore.h>

// #include "parallel_mergesort.c"

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
	// unsigned char array[HASHGEN_THREADS_BUFFER][HASH_SIZE];
	// MemoRecord array[HASHGEN_THREADS_BUFFER];
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
	// for (int i = 0; i < sizeof(record->nonce); ++i) {
	//     record->nonce[i] = (seed >> (i * 8)) & 0xFF;
	//     }

	// Generate random bytes
	blake3_hasher hasher;
	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, &record->nonce, sizeof(record->nonce));
	// blake3_hasher_update(&hasher, &seed, sizeof(seed));
	blake3_hasher_finalize(&hasher, record->hash, RECORD_SIZE - NONCE_SIZE);
}

// Function to be executed by each thread for array generation
void *arrayGenerationThread(void *arg)
{
	struct ThreadData *data = (struct ThreadData *)arg;
	if (DEBUG)
		printf("arrayGenerationThread %d\n", data->threadID);
	int hashObjectSize = sizeof(MemoRecord);
	// unsigned char batch[BATCH_SIZE][HASH_SIZE];
	MemoRecord batch[BATCH_SIZE];
	long long NUM_HASHES_PER_THREAD = (long long)(NUM_ENTRIES / NUM_THREADS);
	unsigned char hash[HASH_SIZE];
	unsigned long long hashIndex = 0;
	long long i = 0;
	while (data->circularArray->producerFinished == 0)
	{
		if (DEBUG)
			printf("arrayGenerationThread(), inside while loop %llu...\n", i);
		// for (unsigned int i = 0; i < NUM_HASHES_PER_THREAD; i += BATCH_SIZE) {
		for (long long j = 0; j < BATCH_SIZE; j++)
		{
			if (DEBUG)
				printf("arrayGenerationThread(), inside for loop %llu...\n", j);
			hashIndex = (long long)(NUM_HASHES_PER_THREAD * data->threadID + i + j);
			generateBlake3(&batch[j], hashIndex);
			// batch[j].prefix = byteArrayToInt(hash,0);
			// memcpy(batch[j].data.byteArray, hash+PREFIX_SIZE, HASH_SIZE-PREFIX_SIZE);
			// batch[j].data.NONCE = hashIndex;
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

double getTimer(Timer *timer); // Ensure you have the correct prototype

void printProgress(double elapsedTime, bool benchmark, size_t totalChunksProcessed, size_t numberOfChunks, size_t bytesWritten, double lastProgressUpdate)
{
	double elapsedTimeSinceLastUpdate = elapsedTime - lastProgressUpdate;
	double progress = (totalChunksProcessed * 100.0) / numberOfChunks;
	double remainingTime = elapsedTime / (progress / 100.0) - elapsedTime;

	if (benchmark == false)
		printf("[%.0lf][COMPRESS]: %.2lf%% completed, ETA %.1lf seconds, %zu/%zu chunks, %.1lf MB/sec\n",
			   floor(elapsedTime), progress, remainingTime, totalChunksProcessed, numberOfChunks,
			   (bytesWritten / elapsedTimeSinceLastUpdate) / (1024 * 1024));
}

int strip_hash(Timer timer, bool benchmark, const char *inputFilePath, const char *outputFilePath)
{
	double elapsedTime = getTimer(&timer);
	double lastProgressUpdate = elapsedTime;

	// Open input file
	int inputFile = open(inputFilePath, O_RDONLY);
	if (inputFile == -1)
	{
		perror("Error opening input file");
		return -1;
	}

	// Get the size of the input file
	off_t fileSize = lseek(inputFile, 0, SEEK_END);
	lseek(inputFile, 0, SEEK_SET); // Reset to the beginning of the file

	// Calculate the number of MemoRecord structures
	size_t numberOfRecords = fileSize / sizeof(MemoRecord);
	if (DEBUG)
		printf("Number of MemoRecord structures to expect: %zu\n", numberOfRecords);

	// Calculate the number of chunks to process
	size_t numberOfChunks = (size_t)ceil((double)fileSize / CHUNK_SIZE);
	if (DEBUG)
		printf("Number of chunks to expect: %zu\n", numberOfChunks);

	size_t numberOfRecordsInChunk = CHUNK_SIZE / sizeof(MemoRecord);
	if (DEBUG)
		printf("Number of records in chunks: %zu\n", numberOfRecordsInChunk);

	// Open output file
	int outputFile = open(outputFilePath, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
	if (outputFile == -1)
	{
		perror("Error opening output file");
		close(inputFile);
		return -1;
	}

	// Dynamically allocate memory for the buffer and nonce buffer
	uint8_t *buffer = (uint8_t *)malloc(CHUNK_SIZE);
	uint8_t *nonceBuffer = (uint8_t *)malloc(NONCE_SIZE * numberOfRecordsInChunk); // Allocate enough for all nonces
	if (buffer == NULL || nonceBuffer == NULL)
	{
		fprintf(stderr, "Memory allocation failed 1: %d %zu\n", CHUNK_SIZE, NONCE_SIZE * numberOfRecordsInChunk);
		close(inputFile);
		close(outputFile);
		return -1;
	}

	ssize_t bytesRead;
	size_t totalChunksProcessed = 0;
	size_t noncesInBuffer = 0; // Counter for nonces added to buffer
	ssize_t bytesWritten = 0;

	while ((bytesRead = read(inputFile, buffer, CHUNK_SIZE)) > 0)
	{
		if (DEBUG)
			printf("noncesInBuffer=%zu %zu %d\n", noncesInBuffer, bytesRead, CHUNK_SIZE);

		size_t recordsInChunk = bytesRead / sizeof(MemoRecord);

		// Process the data in the buffer
		if (DEBUG)
			printf("processing %zu records in chunk %zu\n", recordsInChunk, totalChunksProcessed);
		for (size_t i = 0; i < recordsInChunk; i++)
		{
			size_t indexRaw = i * sizeof(MemoRecord);
			if (DEBUG)
				printf("indexRaw=%zu\n", indexRaw);
			MemoRecord *record = (MemoRecord *)(buffer + indexRaw);
			// Copy nonce to nonce buffer
			size_t indexCompressed = noncesInBuffer * NONCE_SIZE;
			if (DEBUG)
				printf("indexCompressed=%zu\n", indexCompressed);

			if (indexCompressed >= NONCE_SIZE * numberOfRecordsInChunk)
			{
				printf("this should not happen! %zu %zu %zu %d %zu\n", indexRaw, indexCompressed, NONCE_SIZE * numberOfRecordsInChunk, CHUNK_SIZE, NONCE_SIZE * numberOfRecordsInChunk);
				exit(0);
			}

			if (DEBUG)
				printf("memcpy() NONCE_SIZE=%d\n", NONCE_SIZE);
			memcpy(nonceBuffer + indexCompressed, record->nonce, NONCE_SIZE);
			noncesInBuffer++;
		}

		if (DEBUG)
			printf("check: %zu %zu %zu %zu\n", noncesInBuffer, recordsInChunk, totalChunksProcessed, numberOfChunks - 1);
		// Write the batched nonces to the output file when buffer is full or at the end
		if (noncesInBuffer >= recordsInChunk || totalChunksProcessed == numberOfChunks - 1)
		{
			ssize_t result = write(outputFile, nonceBuffer, noncesInBuffer * NONCE_SIZE);
			if (result == -1)
			{
				perror("Error writing to output file");
				free(buffer);
				free(nonceBuffer);
				close(inputFile);
				close(outputFile);
				return -1;
			}
			bytesWritten += result;
			if (DEBUG)
				printf("write(): %zu %d %zu\n", bytesWritten, NONCE_SIZE, noncesInBuffer);
			noncesInBuffer = 0; // Reset the nonce counter
		}

		totalChunksProcessed++;
		elapsedTime = getTimer(&timer);

		if (elapsedTime > lastProgressUpdate + 1.0)
		{
			printProgress(elapsedTime, benchmark, totalChunksProcessed, numberOfChunks, bytesWritten, lastProgressUpdate);
			lastProgressUpdate = elapsedTime; // Update last progress time
			bytesWritten = 0;
		}
	}

	// Final write for any remaining nonces in the buffer
	if (noncesInBuffer > 0)
	{
		ssize_t result = write(outputFile, nonceBuffer, noncesInBuffer * NONCE_SIZE);
		if (result == -1)
		{
			perror("Error writing to output file");
			free(buffer);
			free(nonceBuffer);
			close(inputFile);
			close(outputFile);
			return -1;
		}
		bytesWritten += result;
	}

	// Final progress update after processing
	elapsedTime = getTimer(&timer);
	printProgress(elapsedTime, benchmark, totalChunksProcessed, numberOfChunks, bytesWritten, lastProgressUpdate);

	// Free the allocated memory
	free(buffer);
	free(nonceBuffer);

	// Close files
	close(inputFile);
	close(outputFile);
	return 0; // Indicate successful execution
}

int strip_hash_fwrite(Timer timer, bool benchmark, const char *inputFilePath, const char *outputFilePath)
{
	double elapsedTime = getTimer(&timer);
	double lastProgressUpdate = elapsedTime;

	// Open input file
	FILE *inputFile = fopen(inputFilePath, "rb");
	if (inputFile == NULL)
	{
		perror("Error opening input file");
		return -1;
	}

	// Get the size of the input file
	fseek(inputFile, 0, SEEK_END);
	long fileSize = ftell(inputFile);
	fseek(inputFile, 0, SEEK_SET); // Reset to the beginning of the file

	// Calculate the number of MemoRecord structures
	size_t numberOfRecords = fileSize / sizeof(MemoRecord);
	if (DEBUG)
		printf("Number of MemoRecord structures to expect: %zu\n", numberOfRecords);

	// Calculate the number of chunks to process
	size_t numberOfChunks = (size_t)ceil((double)fileSize / CHUNK_SIZE);
	if (DEBUG)
		printf("Number of chunks to expect: %zu\n", numberOfChunks);

	size_t numberOfRecordsInChunk = CHUNK_SIZE / sizeof(MemoRecord);
	if (DEBUG)
		printf("Number of records in chunks: %zu\n", numberOfRecordsInChunk);

	// Open output file
	FILE *outputFile = fopen(outputFilePath, "wb");
	if (outputFile == NULL)
	{
		perror("Error opening output file");
		fclose(inputFile);
		return -1;
	}

	// Dynamically allocate memory for the buffer and nonce buffer
	uint8_t *buffer = (uint8_t *)malloc(CHUNK_SIZE);
	uint8_t *nonceBuffer = (uint8_t *)malloc(NONCE_SIZE * numberOfRecordsInChunk); // Allocate enough for all nonces
	if (buffer == NULL || nonceBuffer == NULL)
	{
		fprintf(stderr, "Memory allocation failed 1: %d %zu\n", CHUNK_SIZE, NONCE_SIZE * numberOfRecordsInChunk);
		fclose(inputFile);
		fclose(outputFile);
		return -1;
	}

	size_t bytesRead;
	size_t totalChunksProcessed = 0;
	size_t noncesInBuffer = 0; // Counter for nonces added to buffer
	size_t bytesWritten = 0;

	while ((bytesRead = fread(buffer, 1, CHUNK_SIZE, inputFile)) > 0)
	{
		if (DEBUG)
			printf("noncesInBuffer=%zu %zu %d\n", noncesInBuffer, bytesRead, CHUNK_SIZE);
		// make sure if this happens that everything is still ok...
		// if (bytesRead != CHUNK_SIZE)
		//{
		// printf("this should not happen! %zu %d\n",bytesRead,CHUNK_SIZE);
		//     	exit(0);
		// }
		size_t recordsInChunk = bytesRead / sizeof(MemoRecord);

		// Process the data in the buffer
		if (DEBUG)
			printf("processing %zu records in chunk %zu\n", recordsInChunk, totalChunksProcessed);
		for (size_t i = 0; i < recordsInChunk; i++)
		{
			size_t indexRaw = i * sizeof(MemoRecord);
			if (DEBUG)
				printf("indexRaw=%zu\n", indexRaw);
			MemoRecord *record = (MemoRecord *)(buffer + indexRaw);
			// Copy nonce to nonce buffer
			size_t indexCompressed = noncesInBuffer * NONCE_SIZE;
			if (DEBUG)
				printf("indexCompressed=%zu\n", indexCompressed);

			if (indexCompressed >= NONCE_SIZE * numberOfRecordsInChunk)
			{
				printf("this should not happen! %zu %zu %zu %d %zu\n", indexRaw, indexCompressed, NONCE_SIZE * numberOfRecordsInChunk, CHUNK_SIZE, NONCE_SIZE * numberOfRecordsInChunk);
				exit(0);
			}

			if (DEBUG)
				printf("memcpy() NONCE_SIZE=%d\n", NONCE_SIZE);
			memcpy(nonceBuffer + indexCompressed, record->nonce, NONCE_SIZE);
			noncesInBuffer++;
		}

		if (DEBUG)
			printf("check: %zu %zu %zu %zu\n", noncesInBuffer, recordsInChunk, totalChunksProcessed, numberOfChunks - 1);
		// Write the batched nonces to the output file when buffer is full or at the end
		// if (noncesInBuffer * NONCE_SIZE >= CHUNK_SIZE || totalChunksProcessed == numberOfChunks - 1) {
		if (noncesInBuffer >= recordsInChunk || totalChunksProcessed == numberOfChunks - 1)
		{
			// bytesWritten += fwrite(nonceBuffer, NONCE_SIZE, noncesInBuffer, outputFile);
			bytesWritten += fwrite(nonceBuffer, 1, noncesInBuffer * NONCE_SIZE, outputFile);
			if (DEBUG)
				printf("fwrite(): %zu %d %zu\n", bytesWritten, NONCE_SIZE, noncesInBuffer);
			noncesInBuffer = 0; // Reset the nonce counter
		}

		totalChunksProcessed++;
		elapsedTime = getTimer(&timer);

		if (elapsedTime > lastProgressUpdate + 1.0)
		{
			printProgress(elapsedTime, benchmark, totalChunksProcessed, numberOfChunks, bytesWritten, lastProgressUpdate);
			lastProgressUpdate = elapsedTime; // Update last progress time
			bytesWritten = 0;
		}
	}

	// Final write for any remaining nonces in the buffer
	if (noncesInBuffer > 0)
	{
		bytesWritten += fwrite(nonceBuffer, NONCE_SIZE, noncesInBuffer, outputFile);
	}

	// Final progress update after processing
	elapsedTime = getTimer(&timer);
	printProgress(elapsedTime, benchmark, totalChunksProcessed, numberOfChunks, bytesWritten, lastProgressUpdate);

	// Free the allocated memory
	free(buffer);
	free(nonceBuffer);

	// Close files
	fclose(inputFile);
	fclose(outputFile);
	return 0; // Indicate successful execution
}

void remove_file(const char *fileName)
{
	// Attempt to remove the file
	if (remove(fileName) == 0)
	{
		if (DEBUG)
			printf("File '%s' removed successfully.\n", fileName);
	}
	else
	{
		perror("Error removing file");
	}
}

// Function to write a bucket of random records to disk
void writeBucketToDisk(const Bucket *bucket, int fd, off_t offset)
{

	// bool buffered = false;

	// struct iovec iov[1];
	//     iov[0].iov_base = bucket->records;
	//     iov[0].iov_len = sizeof(MemoRecord) * bucket->count;

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
	if (bytesWritten < 0)
	{
		// perror("Error writing to file");
		printf("Error writing bucket at offset %llu to file; bytes written %llu when it expected %lu\n", offset, bytesWritten, sizeof(MemoRecord) * bucket->count);
		close(fd);
		exit(EXIT_FAILURE);
	}
	if (DEBUG)
		printf("writeBucketToDisk(): bytesWritten=%lld %lu\n", bytesWritten, sizeof(MemoRecord) * bucket->count);
}

// off_t getBucketIndex(const uint8_t *hash) {
//     // Calculate the bucket index based on the first 2-byte prefix
//     return (hash[0] << 8) | hash[1];
// }

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
	// printf("\n");
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
		// printf(" ");
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
	// printf("getBucketIndex(): ");
	// printBytes(byteArray,HASH_SIZE);

	off_t result = 0;
	// littleEndian = false;
	// if (littleEndian)
	//	result = byteArrayToUnsignedIntLittleEndian(byteArray,b);
	// else
	// result = byteArrayToUnsignedIntBigEndian(byteArray,b);
	result = binaryByteArrayToULL(byteArray, HASH_SIZE, b);
	if (DEBUG)
		printf("getBucketIndex(): %lld %d\n", result, b);

	// printf(" : %lld\n",result);
	return result;
}

off_t getBucketIndex_old(const uint8_t *hash, int num_bits)
{

	// printf("getBucketIndex(): ");
	// printBytes(hash,HASH_SIZE);

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
	// printf(" : %lld\n",index);
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
	// uint8_t array[16];

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
	// printf("all done!\n");

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
	// uint8_t array[16];

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
	// printf("all done!\n");

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
	//  Calculate the bucket index based on the first 2-byte prefix
	off_t bucketIndex = getBucketIndex(targetHash, PREFIX_SIZE);
	// int bucketIndex = (targetHash[0] << 8) | targetHash[1];
	if (DEBUG)
		printf("bucketIndex=%lld\n", bucketIndex);

	// filesize
	long long FILESIZE = filesize;
	if (DEBUG)
		printf("FILESIZE=%lld\n", FILESIZE);

	// int RECORD_SIZE = sizeof(MemoRecord);
	if (DEBUG)
		printf("RECORD_SIZE=%d\n", RECORD_SIZE);

	unsigned long long NUM_ENTRIES = FILESIZE / RECORD_SIZE;
	if (DEBUG)
		printf("NUM_ENTRIES=%lld\n", NUM_ENTRIES);

	int BUCKET_SIZE = (FILESIZE) / (RECORD_SIZE * NUM_BUCKETS);
	// BUCKET_SIZE = 1024;
	if (DEBUG)
		printf("BUCKET_SIZE=%d\n", BUCKET_SIZE);

	// left and right are record numbers, not byte offsets
	off_t left = bucketIndex * BUCKET_SIZE;
	if (DEBUG)
		printf("left=%lld\n", left);
	off_t right = (bucketIndex + 1) * BUCKET_SIZE - 1;
	if (DEBUG)
		printf("right=%lld\n", right);

	// off_t right = filesize / sizeof(MemoRecord);
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
		// if (DEBUG)
		//	printf("seekCount=%lld \n",seekCount);

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
		// return strdup(path);
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
	// printf("%s %s %ld\n",directory_path,path,dir_length);

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
	// printf("verifySorted(): %zu %d\n",bytesRead,RECORD_SIZE);
	int i;
	for (i = RECORD_SIZE; i < bytesRead; i += RECORD_SIZE)
	{
		// printf("%d %d\n",i,i - RECORD_SIZE);
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
	// printf("  -w <writesize>: Specify the write size in KB as an integer\n");
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
	printf("  -w <bool>: benchmark; default is off\n");
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
	// For demonstration purposes, let's just set a dummy value in the records array
	// buckets[thread_id].records = malloc(BUCKET_SIZE * sizeof(MemoRecord));
	// if (buckets[thread_id].records == NULL) {
	//    perror("Memory allocation failed");
	//    pthread_exit(NULL);
	//}

	// buckets[thread_id].count = BUCKET_SIZE;
	// buckets[thread_id].flush = 0;

	// You can perform your actual reading logic here
	for (int b = 0; b < num_buckets_to_process; ++b)
	{
		if (b == threadID)
		{
			// if (HASHSORT)
			qsort(buckets[b].records, BUCKET_SIZE, sizeof(MemoRecord), compareMemoRecords);
			// heapsort(buckets[b].records, BUCKET_SIZE, sizeof(MemoRecord), compareMemoRecords);
			//  Sort the bucket contents
			// parallel_quicksort(bucket.records, BUCKET_SIZE*FLUSH_SIZE);
			// parallel_merge_sort(bucket.records, BUCKET_SIZE*FLUSH_SIZE);
			// parallel_sort(bucket.records, BUCKET_SIZE*FLUSH_SIZE, sizeof(MemoRecord), compareMemoRecords, NUM_THREADS, PARTITION_SIZE, QSORT_SIZE, DEBUG);
			// tbb::parallel_sort(bucket.records, bucket.records + BUCKET_SIZE*FLUSH_SIZE*sizeof(MemoRecord), compare);

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
			// Get the value of the semaphore
			// sem_getvalue(&semaphore, &sem_value);
			// printf("Semaphore value: %d\n", sem_value);
			// Wait on the semaphore
			if (DEBUG)
				printf("sem_wait(%d): wait\n", b);
			// sem_wait(semaphore_io);
			semaphore_wait(&semaphore_io);
			if (DEBUG)
				printf("sem_wait(%d): found\n", b);

			// printf("sleeping for 1 second to slow things down for debugging...\n");
			// sleep(1);
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
			// sem_post(semaphore_io);
			semaphore_post(&semaphore_io);
			if (DEBUG)
				printf("sem_post(%d): found\n", b);

			// if (DEBUG)
			//	printf("[SORT] lseek to read bucket %d, offset %lld\n",b, offset);

			// if (lseek(fd, offset, SEEK_SET) < 0)
			//{
			//	printf("main(): Error seeking in file at offset %llu for bucket %d\n",offset,b);
			//	close(fd);
			//	*return_value = 1;
			//	pthread_exit((void *)return_value);
			// }

			// Read the bucket into memory

			// bytesRead = read(fd, buckets[b].records, BUCKET_SIZE * sizeof(MemoRecord));
		}
	}

	// pthread_exit(NULL);

	*return_value = 0;
	pthread_exit((void *)return_value);
}

/*
				for (int b = 0;b<num_threads_sort;b++)
				{
					if (DEBUG)
						printf("[SORT] lseek to read bucket %lld, offset %lld\n",i+b, (i+b) * sizeof(MemoRecord) * BUCKET_SIZE);
					if (lseek(fd, (i+b) * sizeof(MemoRecord) * BUCKET_SIZE, SEEK_SET) < 0)
					{
						printf("main(): Error seeking in file at offset %llu for bucket %llu\n",(i+b) * sizeof(MemoRecord) * BUCKET_SIZE,i+b);
						close(fd);
						return 1;
					}

					// Read the bucket into memory
					long long bytesRead = 0;
					bytesRead = read(fd, buckets[b].records, BUCKET_SIZE * sizeof(MemoRecord));
					if (bytesRead < 0 || bytesRead != BUCKET_SIZE * sizeof(MemoRecord))
					{
						printf("Error reading bucket %llu from file; bytes read %llu when it expected %lu\n",i+b,bytesRead,BUCKET_SIZE * sizeof(MemoRecord));
						close(fd);
						return 1;
					}
					if (DEBUG)
						printf("[SORT] read %lld bytes, expecting %lu bytes\n",bytesRead, BUCKET_SIZE * sizeof(MemoRecord));

				}
				*/

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
			// sem_wait(semaphore_io);
			if (DEBUG)
				printf("writing bucket %d at offset %llu\n", b, offset);
			// printf("sleeping for 1 second to slow things down for debugging...\n");
			// sleep(1);

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
			// sem_post(semaphore_io);
		}
	}

	*return_value = 0;
	pthread_exit((void *)return_value);
}
/*
				for (int b = 0;b<num_threads_sort;b++)
				{


					if (DEBUG)
						printf("[SORT] lseek to write bucket %lld, offset %lld\n",i+b, (i+b) * sizeof(MemoRecord) * BUCKET_SIZE);

					// Write the sorted bucket back to the file
					if (lseek(fd, (i+b) * sizeof(MemoRecord) * BUCKET_SIZE, SEEK_SET) < 0)
					{
						printf("main2(): Error seeking in file at offset %llu for bucket %llu\n",(i+b) * sizeof(MemoRecord) * BUCKET_SIZE,i+b);
						close(fd);
						return 1;
					}

					unsigned long long bytesWritten = write(fd, buckets[b].records, BUCKET_SIZE * sizeof(MemoRecord));
					if (bytesWritten < 0)
					{
						printf("Error writing bucket %llu to file; bytes written %llu when it expected %lu\n",i+b,bytesWritten,BUCKET_SIZE * sizeof(MemoRecord));
						close(fd);
						return 1;
					}

					if (DEBUG)
						printf("[SORT] write %lld bytes, expecting %lu bytes\n",bytesWritten, BUCKET_SIZE * sizeof(MemoRecord));

				}
*/

#ifdef __linux__
void print_free_memory()
{
	struct sysinfo memInfo;
	sysinfo(&memInfo);

	long long totalMemory = memInfo.totalram;
	totalMemory *= memInfo.mem_unit;

	long long freeMemory = memInfo.freeram;
	freeMemory *= memInfo.mem_unit;

	if (DEBUG)
		printf("Total Memory: %lld bytes\n", totalMemory);
	if (DEBUG)
		printf("Free Memory: %lld bytes\n", freeMemory);
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

	if (DEBUG)
		printf("Free Memory: %llu bytes\n", free_memory);

	// On macOS, there's no direct way to get free memory.
	// You might need to use other methods or APIs for this purpose.
}
#endif

int main(int argc, char *argv[])
{
	// assumes both values are set to false initially
	// littleEndian = isLittleEndian();
	// littleEndian = false;
	// printf("littleEndian=%s\n",littleEndian ? "true" : "false");

	Timer timer;
	double elapsedTime;
	double elapsedTimeHashGen;
	double elapsedTimeSort;
	double elapsedTimeSync;
	double elapsedTimeCompress;

	// struct timeval start_all_walltime, end_all_walltime;

	char *FILENAME = NULL;		 // Default value
	char *FILENAME_FINAL = NULL; // Default value
	long long FILESIZE = 0;		 // Default value
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
	bool benchmark = false;

	int verify_records_num = 0;

	bool hashgen = false;

	int opt;
	while ((opt = getopt(argc, argv, "t:o:m:k:f:q:s:p:r:a:l:c:d:i:x:v:b:y:z:w:h")) != -1)
	{
		switch (opt)
		{
		/*case 'w':
			WRITE_SIZE = (long long)atoi(optarg);
			printf("WRITE_SIZE=%lld\n", WRITE_SIZE);
			break;
			*/
		case 't':
			NUM_THREADS = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("NUM_THREADS=%d\n", NUM_THREADS);

			BATCH_SIZE = HASHGEN_THREADS_BUFFER / NUM_THREADS;
			if (benchmark == false)
				if (DEBUG)
					printf("BATCH_SIZE=%ld\n", BATCH_SIZE);

			hashgen = true;

			if (NUM_THREADS == 1)
			{
				printf("multi-threading with only 1 thread is not supported at this time, exiting\n");
				return 1;
			}
			break;
		case 'o':
			num_threads_sort = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("num_threads_sort=%d\n", num_threads_sort);
			// if (num_threads_sort > 1)
			//{
			//	printf("multi-threading for sorting has not been implemented, exiting\n");
			//	return 1;

			//}

			break;
		case 'y':
			HASHGEN_THREADS_BUFFER = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("HASHGEN_THREADS_BUFFER=%d\n", HASHGEN_THREADS_BUFFER);
			// if (num_threads_sort > 1)
			//{
			//	printf("multi-threading for sorting has not been implemented, exiting\n");
			//	return 1;

			//}

			break;
		case 'i':
			num_threads_io = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("num_threads_io=%d\n", num_threads_io);
			// if (num_threads_io > 1)
			//{
			//	printf("multi-threading for I/O has not been implemented, exiting\n");
			//	return 1;

			//}

			break;
		case 'm':
			// if (benchmark == true)
			//	memory_size = strtoll(optarg, NULL, 10) / (1024 * 1024);
			// int64_t memory_size_long = strtoll(argv[1], NULL, 10) / (1024 * 1024);
			// memory_size = (int)(memory_size_long/(1024*1024));
			// else
			memory_size = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("memory_size=%lld MB\n", memory_size);
			// HASHGEN_THREADS_BUFFER = min(memory_size*8*1024, 8*1024*1024)/ RECORD_SIZE;
			// printf("HASHGEN_THREADS_BUFFER=%d B\n", HASHGEN_THREADS_BUFFER);
			break;
		case 'f':
			FILENAME = optarg;

			if (benchmark == false)
				if (DEBUG)
					printf("FILENAME=%s\n", FILENAME);
			break;
		case 'q':
			FILENAME_FINAL = optarg;

			if (benchmark == false)
				if (DEBUG)
					printf("FILENAME_FINAL=%s\n", FILENAME_FINAL);
			break;
		case 's':
			FILESIZE = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("FILESIZE=%lld MB\n", FILESIZE);

			break;
		case 'k':
			KSIZE = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("KSIZE=%lld\n", KSIZE);
			break;
		case 'p':
			print_records = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("print_records=%lld\n", print_records);
			head = true;
			break;
		case 'r':
			print_records = atoi(optarg);
			tail = true;
			if (benchmark == false)
				if (DEBUG)
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

			if (benchmark == false)
			{
				printf("Hash_search=");
				printBytes(byteArray, sizeof(byteArray));
				printf("\n");
			}

			// printf("print_records=%lld\n", print_records);
			break;
		case 'l':
			// Get the length of the prefix
			prefixLength = atoi(optarg);
			if (prefixLength <= 0 || prefixLength > 10)
			{
				printf("Invalid prefix length\n");
				return 1;
			}
			if (benchmark == false)
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
			if (benchmark == false)
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
			if (benchmark == false)
				if (DEBUG)
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
			if (benchmark == false)
				if (DEBUG)
					printf("HASHSORT=%s\n", HASHSORT ? "true" : "false");
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
			if (benchmark == false)
				if (DEBUG)
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
			if (benchmark == false)
				printf("verify_records=%s\n", verify_records ? "true" : "false");
			break;
		case 'w':
			if (strcmp(optarg, "true") == 0)
			{
				benchmark = true;
			}
			else if (strcmp(optarg, "false") == 0)
			{
				benchmark = false;
			}
			else
			{
				benchmark = false;
			}
			if (benchmark == false)
				if (DEBUG)
					printf("benchmark=%s\n", benchmark ? "true" : "false");
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
	/*if (FILENAME_FINAL == NULL) {
		printf("Error: filename (-q) is mandatory.\n");
		printUsage();
		return 1;
	}*/
	if (FILENAME != NULL && print_records == 0 && verify_records == false && targetHash == NULL && verify_records_num == 0 && search_records == 0 && (NUM_THREADS <= 0 || num_threads_sort <= 0 || FILESIZE < 0 || memory_size <= 0))
	{
		printf("Error: mandatory command line arguments have not been used, try -h for more help\n");
		printUsage();
		return 1;
	}

	const char *path = FILENAME;			 // Example path, you can change it to any valid path
	const char *path_final = FILENAME_FINAL; // Example path, you can change it to any valid path

	unsigned long long bytes_free = getDiskSpace(path);
	if (bytes_free > 0)
	{
		if (DEBUG)
			printf("Free disk space on %s: %llu bytes\n", path, bytes_free);
	}
	if (benchmark == false)
		if (DEBUG)
			printf("bytes_free=%lld\n", bytes_free);

	if (FILESIZE == 0 && KSIZE >= 20)
	{
		FILESIZE = pow(2, KSIZE - 20) * RECORD_SIZE;
		if (benchmark == false)
			if (DEBUG)
				printf("***FILESIZE=%lld\n", FILESIZE);
	}
	else if (FILESIZE == 0 && KSIZE < 20)
	{
		printf("-k must be set to 20 or greater\n");
		return 1;
	}

	if (FILESIZE == 0 || FILESIZE * 1024 * 1024 > bytes_free)
		FILESIZE = (int)((bytes_free / (1024 * 1024)) / 100) * 100;

	// if (FILESIZE*1024*1024*1024 > bytes_free)
	//	{
	//		printf("not enough storage space\n");
	//		return 0;
	//	}
	// printf()

	long long sort_memory = 0;
	long long EXPECTED_TOTAL_FLUSHES = 0;
	bool found_good_config = false;
	// WRITE_SIZE = 1024*1024; //unit KB, max is 1GB
	// for (int i=0; i<N; i++)
	//{
	//	memory_size
	// }

	// memory and file sizes must be divisible

	// need to find largest memory_size that perfectly divides FILESIZE
	long long right_memory_size = 1;
	for (int i = memory_size; i > 0; i--)
	{
		if (FILESIZE % i == 0)
		{
			if (DEBUG)
				printf("found best memory size to be %d MB\n", i);
			right_memory_size = i;
			break;
		}
	}
	memory_size = right_memory_size;
	int ratio = (int)ceil((double)FILESIZE / memory_size);
	// memory_size = FILESIZE/ratio;

	if (benchmark == false)
		if (DEBUG)
			printf("ratio=%d\n", ratio);
	long long FILESIZE_byte = FILESIZE * 1024 * 1024;
	long long memory_size_byte = memory_size * 1024 * 1024;

	if (benchmark == false)
		if (DEBUG)
			printf("memory_size_byte=%lld\n", memory_size_byte);
	if (DEBUG)
		printf("memory_size=%lld\n", memory_size);
	if (benchmark == false)
		if (DEBUG)
			printf("FILESIZE_byte=%lld\n", FILESIZE_byte);
	if (DEBUG)
		printf("FILESIZE=%lld\n", FILESIZE);

	// DEBUG = true;
	if (hashgen)
	{
		for (WRITE_SIZE = 1024 * 1024 / ratio; WRITE_SIZE > 0; WRITE_SIZE = WRITE_SIZE / 2)
		{
			// FLUSH_SIZE = FILESIZE_byte/memory_size_byte;
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
				printf("for(): 9\n");

			// valid configuration
			if (sort_memory <= memory_size_byte && NUM_BUCKETS >= 64)
			{
				// fix all numbers to ensure proper fitting of all hashes
				WRITE_SIZE = (long long)floor(memory_size_byte / NUM_BUCKETS);
				for (int w = WRITE_SIZE; w > RECORD_SIZE; w--)
				{
					if (w % RECORD_SIZE == 0)
					{
						WRITE_SIZE = w;
						if (DEBUG)
							printf("found new WRITE_SIZE=%lld\n", WRITE_SIZE);
						break;
					}
				}

				// if (DEBUG) printf("for(): 10\n");
				//	WRITE_SIZE = (long long)(WRITE_SIZE / 16) * 16;
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

				if (benchmark == false)
				{
					if (DEBUG)
					{
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
					}
				}

				found_good_config = true;
				break;
				// return 0;
			}
		}

		if (found_good_config == false)
		// if (found_good_config == true)
		{
			printf("no valid configuration found... this should never happen\n");

			printf("exiting...\n");
			return 1;
		}
	}

	// DEBUG = false;

	if (benchmark == false)
		print_free_memory();

	long long MEMORY_MAX = memory_size_byte;
	if (benchmark == false)
	{
		if (DEBUG)
		{
			printf("MEMORY_MAX=%lld\n", MEMORY_MAX);
			printf("RECORD_SIZE=%d\n", RECORD_SIZE);
			printf("HASH_SIZE=%d\n", RECORD_SIZE - NONCE_SIZE);
			printf("NONCE_SIZE=%d\n", NONCE_SIZE);
		}
	}

	num_threads_io = min(num_threads_sort, num_threads_io);
	if (benchmark == false)
	{
		if (DEBUG)
		{
			printf("num_threads_hash=%d\n", NUM_THREADS);
			printf("num_threads_sort=%d\n", num_threads_sort);
			printf("num_threads_io=%d\n", num_threads_io);
		}
	}

	// printf("exiting...\n");
	// exit(EXIT_FAILURE);

	if (verify_records)
	{

		resetTimer(&timer);
		int VERIFY_BUFFER_SIZE = 1000000 - RECORD_SIZE;
		int fd;
		char *buffer = (char *)malloc(VERIFY_BUFFER_SIZE);
		ssize_t bytesRead;
		off_t offset = 0;

		if (buffer == NULL)
		{
			fprintf(stderr, "Memory allocation failed 2\n");
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
			// printf("just read %zd bytes, total bytes %lld...\n",bytesRead,offset);
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

		// Timing variables
		// struct timespec start, end;
		// double elapsedTime;

		// Get start time
		//  clock_gettime(CLOCK_MONOTONIC, &start);
		//  gettimeofday(&start_all_walltime, NULL);
		resetTimer(&timer);

		// Perform binary search
		int seekCount = 0;
		int index = binarySearch(targetHash, prefixLength, fd, filesize, &seekCount, false);

		// Get end time
		// clock_gettime(CLOCK_MONOTONIC, &end);

		// Calculate elapsed time in microseconds
		// elapsedTime = (end.tv_sec - start.tv_sec) * 1e6 + (end.tv_nsec - start.tv_nsec) / 1e3;
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
			// printf("Prefix length: %zu\n", prefixLength);
			printf("Hash found : ");
			printBytes(foundNumber.hash, sizeof(foundNumber.hash));
			printf("\n");

			unsigned long long nonceValue = 0;
			for (int i = 0; i < sizeof(foundNumber.nonce); i++)
			{
				nonceValue |= (unsigned long long)foundNumber.nonce[i] << (i * 8);
			}

			printf("Nonce: %llu/", nonceValue);
			// printf("Nonce (hex): ");
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
		// size_t prefixLength = atoi(argv[2]);
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

		// Timing variables
		// struct timespec start, end;
		// double elapsedTime;

		// Get start time
		// clock_gettime(CLOCK_MONOTONIC, &start);
		resetTimer(&timer);

		for (int searchNum = 0; searchNum < numberLookups; searchNum++)
		{

			// Convert the hexadecimal hash from command-line argument to binary
			// char hexString[length * 2 + 1];

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
					// printf("Nonce (hex): ");
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
				// printf("Prefix length: %zu\n", prefixLength);
			}
		}

		// Get end time
		// clock_gettime(CLOCK_MONOTONIC, &end);

		// Calculate elapsed time in microseconds
		// elapsedTime = (end.tv_sec - start.tv_sec) * 1e6 + (end.tv_nsec - start.tv_nsec) / 1e3;
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

		// Timing variables
		// struct timespec start, end;
		// double elapsedTime;

		// Get start time
		// clock_gettime(CLOCK_MONOTONIC, &start);
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

				// unsigned long long nonceValue = 0;
				// for (int i = 0; i < sizeof(foundNumber.nonce); i++) {
				//	nonceValue |= (unsigned long long)foundNumber.nonce[i] << (i * 8);
				// }

				printf("Nonce: %llu/", nonceValue);
				// printf("Nonce (hex): ");
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

		// Get end time
		// clock_gettime(CLOCK_MONOTONIC, &end);

		// Calculate elapsed time in microseconds
		// elapsedTime = (end.tv_sec - start.tv_sec) * 1e6 + (end.tv_nsec - start.tv_nsec) / 1e3;
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

			if (benchmark == false)
				if (DEBUG)
					printf("storing vault configuration in %s\n", FILENAME_CONFIG);
			FILE *config_file = fopen(FILENAME_CONFIG, "w"); // Open or create the config file for writing

			if (config_file == NULL)
			{
				printf("Error opening the config file!\n");
				return 1;
			}

			// Write the variables A, B, and C, each initialized to 0, to the config file
			if (DEBUG)
			{
				fprintf(config_file, "FILESIZE_byte=%lld\n", FILESIZE_byte);
				fprintf(config_file, "NUM_BUCKETS=%d\n", NUM_BUCKETS);
				fprintf(config_file, "BUCKET_SIZE=%d\n", BUCKET_SIZE);
				fprintf(config_file, "RECORD_SIZE=%d\n", RECORD_SIZE);
				fprintf(config_file, "HASH_SIZE=%d\n", HASH_SIZE);
				fprintf(config_file, "NONCE_SIZE=%d\n", NONCE_SIZE);
			}

			fclose(config_file); // Close the config file

			if (benchmark == false)
				if (DEBUG)
					printf("hash generation and sorting...\n");

			// Open file for writing
			// int fd = open(FILENAME, O_RDWR | O_CREAT | O_TRUNC  , 0644);
			fd = open(FILENAME, O_RDWR | O_CREAT | O_TRUNC, 0644);
			if (fd < 0)
			{
				perror("Error opening file for writing");
				return 1;
			}

			resetTimer(&timer);

			// Array to hold the generated random records
			MemoRecord record;
			// Allocate memory for the array of Bucket structs
			Bucket *buckets = malloc(NUM_BUCKETS * sizeof(Bucket));
			if (buckets == NULL)
			{
				printf("2: Error allocating memory: %lu\n", NUM_BUCKETS * sizeof(Bucket));
				return 1;
			}
			// printf("2: XXX allocating memory: %lu %d %lu\n",NUM_BUCKETS* sizeof(Bucket),NUM_BUCKETS,sizeof(Bucket));
			// exit(0);

			// Initialize each bucket
			for (int i = 0; i < NUM_BUCKETS; ++i)
			{
				// Allocate memory for the records in each bucket
				// TODO
				// should double check BUCKET_SIZE * sizeof(MemoRecord) logic if its indeed correct
				buckets[i].records = malloc(WRITE_SIZE);
				// modified due to failure in memory allocation
				// buckets[i].records = malloc(BUCKET_SIZE);
				// printf("3 XXX allocating memory %llu; %d %d\n",WRITE_SIZE,NUM_BUCKETS,i);
				// exit(0);
				if (buckets[i].records == NULL)
				{
					printf("3 Error allocating memory %lu; %d %d\n", BUCKET_SIZE * sizeof(MemoRecord), NUM_BUCKETS, i);

					// printf("3 Error allocating memory %lu; %lu %d\n",BUCKET_SIZE ,NUM_BUCKETS,i);
					//  Free previously allocated memory
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

			if (benchmark == false)
				if (DEBUG)
					printf("initializing circular array...\n");
			// Initialize the circular array
			struct CircularArray circularArray;
			initCircularArray(&circularArray);

			if (benchmark == false)
				if (DEBUG)
					printf("Create thread data structure...\n");
			// Create thread data structure
			struct ThreadData threadData[NUM_THREADS];

			if (benchmark == false)
				if (DEBUG)
					printf("Create hash generation threads...\n");
			// Create threads
			pthread_t threads[NUM_THREADS];
			for (int i = 0; i < NUM_THREADS; i++)
			{
				threadData[i].circularArray = &circularArray;
				threadData[i].threadID = i;
				pthread_create(&threads[i], NULL, arrayGenerationThread, &threadData[i]);
			}
			if (benchmark == false)
				if (DEBUG)
					printf("Hash generation threads created...\n");

			unsigned long long totalFlushes = 0;

			// Measure time taken for hash generation using gettimeofday
			// struct timeval start_time, end_time, last_progress_time;
			// gettimeofday(&start_time, NULL);
			// gettimeofday(&last_progress_time, NULL);
			last_progress_i = 0;
			// double elapsed_time = 0.0;

			elapsedTime = getTimer(&timer);
			last_progress_update = elapsedTime;

			// Start timing
			// clock_t start = clock();

			// Write random records to buckets based on their first 2-byte prefix
			unsigned long long i = 0;
			int flushedBucketsNeeded = NUM_BUCKETS;
			// MemoRecord consumedArray[BATCH_SIZE];
			MemoRecord *consumedArray;
			consumedArray = malloc(BATCH_SIZE * sizeof(MemoRecord));

			if (DEBUG)
				printf("BATCH_SIZE=%zu\n", BATCH_SIZE);
			if (DEBUG)
				printf("flushedBucketsNeeded=%d\n", flushedBucketsNeeded);

			while (flushedBucketsNeeded > 0)
			// for (unsigned long long i = 0; i < NUM_ENTRIES; i++)
			{

				if (DEBUG)
					printf("removeBatch()...\n");
				removeBatch(&circularArray, consumedArray);

				if (DEBUG)
					printf("processing batch of size %ld...\n", BATCH_SIZE);
				// this sequential loop is probably a bottleneck
				for (int b = 0; b < BATCH_SIZE; b++)
				{

					// Generate a random record
					// generateBlake3(&record, i + 1);

					// Calculate the bucket index based on the first 2-byte prefix
					// int bucketIndex = (record.hash[0] << 8) | record.hash[1];

					off_t bucketIndex = getBucketIndex(consumedArray[b].hash, PREFIX_SIZE);
					// printf("bucketIndex=%lld\n",bucketIndex);

					// Add the random record to the corresponding bucket
					Bucket *bucket = &buckets[bucketIndex];
					if (bucket->count < WRITE_SIZE / RECORD_SIZE)
					{
						// printf("bucket->count*RECORD_SIZE=%zu WRITE_SIZE=%lld\n",bucket->count*RECORD_SIZE,WRITE_SIZE);
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

								// if (num_threads_sort == 1)
								//{
								if (HASHSORT)
									qsort(bucket->records, BUCKET_SIZE, sizeof(MemoRecord), compareMemoRecords);
								// heapsort(bucket->records, BUCKET_SIZE, sizeof(MemoRecord), compareMemoRecords);
								// parallel_sort(bucket->records, BUCKET_SIZE, sizeof(MemoRecord), compareMemoRecords, NUM_THREADS, PARTITION_SIZE, QSORT_SIZE, DEBUG);
								// tbb::parallel_sort(bucket->records, bucket->records + BUCKET_SIZE*sizeof(MemoRecord), compare);
								// parallel_quicksort(bucket->records, BUCKET_SIZE);
								// parallel_merge_sort(bucket->records, BUCKET_SIZE);

								//}
								// else
								//{

								// Bucket *bucketBuffer = malloc(BUCKET_SIZE*sizeof(MemoRecord));
								// memcpy(&bucketBuffer, &bucket, BUCKET_SIZE*sizeof(MemoRecord));

								//}
							}

							// printf("writeBucketToDisk(): bucketIndex=%lld\n",bucketIndex);
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

				// gettimeofday(&end_time, NULL);
				elapsedTime = getTimer(&timer);
				// elapsed_time = (end_time.tv_sec - start_time.tv_sec) +
				//                       (end_time.tv_usec - start_time.tv_usec) / 1.0e6;

				if (elapsedTime > last_progress_update + 1.0)
				{
					double elapsed_time_since_last_progress_update = elapsedTime - last_progress_update;
					last_progress_update = elapsedTime;

					// Calculate and print estimated completion time
					double progress = min(i * 100.0 / NUM_ENTRIES, 100.0);
					double remaining_time = elapsedTime / (progress / 100.0) - elapsedTime;

					if (benchmark == false)
						printf("[%.0lf][HASHGEN]: %.2lf%% completed, ETA %.1lf seconds, %llu/%llu flushes, %.1lf MB/sec\n", floor(elapsedTime), progress, remaining_time, totalFlushes, EXPECTED_TOTAL_FLUSHES, ((i - last_progress_i) / elapsed_time_since_last_progress_update) * sizeof(MemoRecord) / (1024 * 1024));

					last_progress_i = i;
					// gettimeofday(&last_progress_time, NULL);
				}
				// i++;
				i += BATCH_SIZE;
			}

			// gettimeofday(&end_time, NULL);
			//         double elapsed_time_hashgen = (end_time.tv_sec - start_time.tv_sec) +
			//                               (end_time.tv_usec - start_time.tv_usec) / 1.0e6;

			if (DEBUG)
				printf("finished generating %llu hashes and wrote them to disk using %llu flushes...\n", i, totalFlushes);

			// Free memory allocated for records in each bucket
			for (int i = 0; i < NUM_BUCKETS; i++)
			{
				free(buckets[i].records);
			}
			free(buckets);

			free(consumedArray);

			elapsedTimeHashGen = getTimer(&timer);
		}
		else
		{
			if (benchmark == false)
				printf("opening file for sorting...\n");

			// Open file for writing
			// int fd = open(FILENAME, O_RDWR | O_CREAT | O_TRUNC  , 0644);
			fd = open(FILENAME, O_RDWR | O_LARGEFILE, 0644);
			if (fd < 0)
			{
				perror("Error opening file for reading/writing");
				return 1;
			}

			resetTimer(&timer);
		}

		bool doSort = true;

		elapsedTime = getTimer(&timer);
		last_progress_update = elapsedTime;
		double sort_start = elapsedTime;

		if (DEBUG)
			print_free_memory();

		if (DEBUG)
		{
			printf("planning to allocate 1: %lu bytes memory...", num_threads_sort * sizeof(Bucket));
			printf("planning to allocate 2: %lu bytes memory...", num_threads_sort * BUCKET_SIZE * sizeof(MemoRecord));
			printf("planning to allocate 3: %lu bytes memory...", num_threads_sort * sizeof(ThreadArgs));
			printf("planning to allocate 4: %lu bytes memory...", num_threads_sort * sizeof(pthread_t));
		}

		// make sure everything is flushed to disk before continuing to sort
		// maybe enable, doesn't seem to be needed
		// Flush data to disk
		// printf("flushing all writes to disk...\n");
		//    if (fsync(fd) == -1) {
		//        perror("fsync");
		// Handle error
		//    }

		if (FLUSH_SIZE > 1 && doSort && HASHSORT)
		{
			// double reading_time, sorting_time, writing_time, total_time;
			// struct timeval start_time, end_time, start_all, end_all;
			// long long elapsed;
			int EXPECTED_BUCKETS_SORTED = NUM_BUCKETS;
			// gettimeofday(&start_all, NULL);
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
			// Bucket bucket;
			if (DEBUG)
				printf("trying to allocate 1: %lu bytes memory...\n", num_threads_sort * sizeof(Bucket));
			Bucket *buckets = malloc(num_threads_sort * sizeof(Bucket));
			if (buckets == NULL)
			{
				perror("Memory allocation failed 3");
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
			// Read each bucket from the file, sort its contents, and write it back to the file
			// not sure why i < NUM_BUCKETS-1 is needed
			// need to break up buckets into smaller pieces
			// Initialize the semaphore with the maximum number of threads allowed
			// sem_init(&semaphore_io, 0, num_threads_io);
			// Create a named semaphore
			// semaphore_io = sem_open("/my_semaphore", O_CREAT, 0644, num_threads_io);
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

					// start threads to sort
					//  Create threads
					//  for (int i = 0; i < NUM_THREADS; ++i) {
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

					// start threads to sort
					//  Create threads
					//  for (int i = 0; i < NUM_THREADS; ++i) {
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

					// start threads to sort
					//  Create threads
					//  for (int i = 0; i < NUM_THREADS; ++i) {
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

				// gettimeofday(&end_time, NULL);
				// elapsed = (end_time.tv_sec - start_time.tv_sec) * 1000000LL +
				//                      (end_time.tv_usec - start_time.tv_usec);

				elapsedTime = getTimer(&timer);

				if (elapsedTime > last_progress_update + 1)
				{
					double elapsed_time_since_last_progress_update = elapsedTime - last_progress_update;

					int totalBucketsSorted = i; // i*NUM_THREADS;
					float perc_done = totalBucketsSorted * 100.0 / EXPECTED_BUCKETS_SORTED;
					float eta = (elapsedTime - sort_start) / (perc_done / 100.0) - (elapsedTime - sort_start);
					// float diskSize = NUM_BUCKETS * BUCKET_SIZE * FLUSH_SIZE * sizeof(MemoRecord) / (1024 * 1024);
					float diskSize = FILESIZE_byte / (1024 * 1024);
					float throughput_MB = diskSize * perc_done * 1.0 / 100.0 / (elapsedTime - sort_start);
					float throughput_MB_latest = (((i - last_progress_i) * BUCKET_SIZE * sizeof(MemoRecord) * 1.0) / (elapsedTime - last_progress_update)) / (1024 * 1024);
					if (DEBUG)
						printf("%llu %llu %d %llu %lu %f %f\n", i, last_progress_i, BUCKET_SIZE, FLUSH_SIZE, sizeof(MemoRecord), elapsedTime, last_progress_update);
					float progress = perc_done;
					// double elapsed_time = elapsed;
					float remaining_time = eta;
					// printf("Buckets sorted : %d in %lld sec %.2f%% ETA %lf sec => %.2f MB/sec\n", i*NUM_THREADS, elapsed, perc_done, eta, throughput_MB);
					if (benchmark == false)
						printf("[%.0lf][SORT]: %.2lf%% completed, ETA %.1lf seconds, %llu/%d flushes, %.1lf MB/sec\n", elapsedTime, progress, remaining_time, i, NUM_BUCKETS, throughput_MB_latest);

					last_progress_i = i;

					last_progress_update = elapsedTime;

					// gettimeofday(&last_progress_time, NULL);
					// printf("Buckets sorted : %zu\n", i*NUM_THREADS);
				}
			}
			// end of for loop

			// Destroy the semaphore
			// sem_unlink("/my_semaphore");
			// Destroy the semaphore
			// sem_destroy(&semaphore_io);

			// Destroy the semaphore
			pthread_mutex_destroy(&semaphore_io.mutex);
			pthread_cond_destroy(&semaphore_io.condition);

			// Free allocated memory
			for (int i = 0; i < num_threads_sort; ++i)
			{
				// printf("freeing buckets\n");
				free(buckets[i].records);
			}
			free(buckets); // Free the memory allocated for the array of buckets

			// free(bucket.records);
		}
		else
		{
			if (HASHSORT == true)
				if (benchmark == false)
					printf("in-memory sort completed!\n");
		}

		elapsedTime = getTimer(&timer);

		elapsedTimeSort = elapsedTime - elapsedTimeHashGen;

		// make sure everything is flushed to disk before continuing to sort
		// maybe enable, doesn't seem to be needed
		// Flush data to disk
		if (benchmark == false)
			// printf("flushing all writes to disk...\n");

			printf("[%.0lf][FLUSH]: 0.00%% completed, ETA 0.0 seconds, 0/0 flushes, 0.0 MB/sec\n", elapsedTime);

		if (fsync(fd) == -1)
		{
			perror("fsync");
			// Handle error
		}

		close(fd);

		elapsedTime = getTimer(&timer);

		elapsedTimeSync = elapsedTime - elapsedTimeHashGen - elapsedTimeSort;

		// final compression by stripping HASH from record and saving it to the HDD, also cleanup
		if (FILENAME_FINAL != NULL)
		{
			if (strip_hash(timer, benchmark, FILENAME, FILENAME_FINAL) == 0)
			{
				remove_file(FILENAME);
			}
		}

		elapsedTime = getTimer(&timer);

		elapsedTimeCompress = elapsedTime - elapsedTimeHashGen - elapsedTimeSort - elapsedTimeSync;

		// End timing
		// clock_t end = clock();
		// gettimeofday(&end_all_walltime, NULL);

		// double elapsed_walltime = (end_all_walltime.tv_sec - start_all_walltime.tv_sec) +
		//                               (end_all_walltime.tv_usec - start_all_walltime.tv_usec) / 1.0e6;

		// long long elapsed_walltime = (end_all_walltime.tv_sec - start_all_walltime.tv_sec) * 1000000LL +
		//                          (end_all_walltime.tv_usec - start_all_walltime.tv_usec);
		// double elapsed_walltime = elapsed_time+elapsed_time_hashgen;

		// Calculate elapsed time in seconds
		// double elapsed_seconds = (double)(end - start) / CLOCKS_PER_SEC;

		int record_size = 0;
		if (FILENAME_FINAL == NULL)
		{
			FILENAME_FINAL = FILENAME;
			record_size = RECORD_SIZE;
		}
		else
		{
			record_size = NONCE_SIZE;
		}

		// Calculate hashes per second
		double hashes_per_second = NUM_ENTRIES / elapsedTime;

		long long FILESIZE_FINAL = record_size * NUM_ENTRIES / (1024 * 1024);

		// Calculate bytes per second
		// double bytes_per_second = sizeof(MemoRecord) * NUM_ENTRIES / elapsedTime;
		double bytes_per_second = record_size * NUM_ENTRIES / elapsedTime;

		if (benchmark == false)
			printf("[%.0lf][FINISHED]: 100.00%% completed in %.1lf seconds, %lld MB vault in %s, %.2f MH/s, Effective Throughput %.2f MB/s\n", elapsedTime, elapsedTime, FILESIZE_FINAL, FILENAME_FINAL, hashes_per_second / 1000000.0, bytes_per_second * 1.0 / (1024 * 1024));

		// printf("Completed %lld MB vault %s in %.2lf seconds : %.2f MH/s %.2f MB/s\n", FILESIZE_FINAL, FILENAME_FINAL, elapsedTime, hashes_per_second/1000000.0, bytes_per_second*1.0/(1024*1024));
		else
			printf("%.3lf,%.3lf,%.3lf,%.3lf,%.3lf\n", elapsedTimeHashGen, elapsedTimeSort, elapsedTimeSync, elapsedTimeCompress, elapsedTime);
		// printf("MH/s: %.2f\n", );
		// printf("MB/s: %.2f\n", );

		return 0;
	}
	// end of hash generation
}
