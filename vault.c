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

void reset_timer(Timer *timer)
{
	gettimeofday(&timer->start, NULL);
}

double get_timer(Timer *timer)
{
	gettimeofday(&timer->end, NULL);
	return (timer->end.tv_sec - timer->start.tv_sec) + (timer->end.tv_usec - timer->start.tv_usec) / 1000000.0;
}

// Function to compare two random records for sorting
int compare_memo_records(const void *a, const void *b)
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
	int producer_finished; // Flag to indicate when the producer is finished
	pthread_mutex_t mutex;
	pthread_cond_t not_empty;
	pthread_cond_t not_full;
};

void init_circular_array(struct CircularArray *circular_array)
{
	circular_array->array = malloc(HASHGEN_THREADS_BUFFER * sizeof(MemoRecord));
	circular_array->head = 0;
	circular_array->tail = 0;
	circular_array->producer_finished = 0;
	pthread_mutex_init(&(circular_array->mutex), NULL);
	pthread_cond_init(&(circular_array->not_empty), NULL);
	pthread_cond_init(&(circular_array->not_full), NULL);
}

void destroy_circular_array(struct CircularArray *circular_array)
{
	free(circular_array->array);
	pthread_mutex_destroy(&(circular_array->mutex));
	pthread_cond_destroy(&(circular_array->not_empty));
	pthread_cond_destroy(&(circular_array->not_full));
}

void insert_batch(struct CircularArray *circular_array, MemoRecord values[BATCH_SIZE])
{
	if (DEBUG)
		printf("insert_batch(): before mutex lock\n");
	pthread_mutex_lock(&(circular_array->mutex));

	if (DEBUG)
		printf("insert_batch(): Wait while the circular array is full and producer is not finished\n");
	// Wait while the circular array is full and producer is not finished
	while ((circular_array->head + BATCH_SIZE) % HASHGEN_THREADS_BUFFER == circular_array->tail && !circular_array->producer_finished)
	{
		pthread_cond_wait(&(circular_array->not_full), &(circular_array->mutex));
	}

	if (DEBUG)
		printf("insert_batch(): Insert values\n");
	// Insert values
	for (size_t i = 0; i < BATCH_SIZE; i++)
	{
		memcpy(&circular_array->array[circular_array->head], &values[i], sizeof(MemoRecord));
		circular_array->head = (circular_array->head + 1) % HASHGEN_THREADS_BUFFER;
	}

	if (DEBUG)
		printf("insert_batch(): Signal that the circular array is not empty\n");
	// Signal that the circular array is not empty
	pthread_cond_signal(&(circular_array->not_empty));

	if (DEBUG)
		printf("insert_batch(): mutex unlock\n");
	pthread_mutex_unlock(&(circular_array->mutex));
}

void remove_batch(struct CircularArray *circular_array, MemoRecord *result)
{
	pthread_mutex_lock(&(circular_array->mutex));

	// Wait while the circular array is empty and producer is not finished
	while (circular_array->tail == circular_array->head && !circular_array->producer_finished)
	{
		pthread_cond_wait(&(circular_array->not_empty), &(circular_array->mutex));
	}

	// Remove values
	for (size_t i = 0; i < BATCH_SIZE; i++)
	{
		memcpy(&result[i], &circular_array->array[circular_array->tail], sizeof(MemoRecord));
		circular_array->tail = (circular_array->tail + 1) % HASHGEN_THREADS_BUFFER;
	}

	// Signal that the circular array is not full
	pthread_cond_signal(&(circular_array->not_full));

	pthread_mutex_unlock(&(circular_array->mutex));
}

// Thread data structure
struct ThreadData
{
	struct CircularArray *circular_array;
	int thread_id;
};

// Function to generate a pseudo-random record using BLAKE3 hash
void generate_blake3(MemoRecord *record, unsigned long long seed)
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
void *array_generation_thread(void *arg)
{
	struct ThreadData *data = (struct ThreadData *)arg; // Cast the argument to ThreadData structure
	if (DEBUG)
		printf("array_generation_thread %d\n", data->thread_id); // Print thread ID for debugging

	// int hash_object_size = sizeof(MemoRecord);								  // Size of a single hash record
	MemoRecord batch[BATCH_SIZE];											  // Array to hold a batch of generated hashes
	long long NUM_HASHES_PER_THREAD = (long long)(NUM_ENTRIES / NUM_THREADS); // Calculate hashes each thread is responsible for
	// unsigned char hash[HASH_SIZE];											  // Buffer to hold generated hash
	unsigned long long hash_index = 0; // Index for tracking which hash to generate
	long long i = 0;				   // Initialize loop counter

	// Main loop for the thread
	while (data->circular_array->producer_finished == 0) // Loop until the producer is finished
	{
		if (DEBUG)
			printf("array_generation_thread(), inside while loop %llu...\n", i);

		// Generate a batch of hashes
		for (size_t j = 0; j < BATCH_SIZE; j++)
		{
			if (DEBUG)
				printf("array_generation_thread(), inside for loop %lu...\n", j);

			hash_index = (long long)(NUM_HASHES_PER_THREAD * data->thread_id + i + j); // Calculate the hash index based on the thread ID and current iteration
			generate_blake3(&batch[j], hash_index);									   // Generate a hash using the calculated index
		}
		// should add hash_index as NONCE to hashObject
		if (DEBUG)
			printf("insert_batch()...\n");

		insert_batch(data->circular_array, batch); // Add the batch to the circular array
		i += BATCH_SIZE;
	}

	// Print message when the thread finishes generating hashes
	if (DEBUG)
		printf("finished generating hashes on thread id %d, thread exiting...\n", data->thread_id);

	return NULL; // Exit the thread
}

double get_timer(Timer *timer); // Ensure you have the correct prototype

void print_progress(double elapsed_time, bool benchmark, size_t total_chunks_processed, size_t number_of_chunks, size_t bytes_written, double last_progress_update)
{
	double elapsed_time_since_last_update = elapsed_time - last_progress_update;
	double progress = (total_chunks_processed * 100.0) / number_of_chunks;
	double remaining_time = elapsed_time / (progress / 100.0) - elapsed_time;

	if (benchmark == false)
		printf("[%.0lf][COMPRESS]: %.2lf%% completed, ETA %.1lf seconds, %zu/%zu chunks, %.1lf MB/sec\n",
			   floor(elapsed_time), progress, remaining_time, total_chunks_processed, number_of_chunks,
			   (bytes_written / elapsed_time_since_last_update) / (1024 * 1024));
}

int strip_hash(Timer timer, bool benchmark, const char *input_file_path, const char *output_file_path)
{
	double elapsed_time = get_timer(&timer);
	double last_progress_update = elapsed_time;

	// Open input file
	int input_file = open(input_file_path, O_RDONLY);
	if (input_file == -1)
	{
		perror("Error opening input file");
		return -1;
	}

	// Get the size of the input file
	off_t filesize = lseek(input_file, 0, SEEK_END);
	lseek(input_file, 0, SEEK_SET); // Reset to the beginning of the file

	// Calculate the number of MemoRecord structures
	size_t number_of_records = filesize / sizeof(MemoRecord);
	if (DEBUG)
		printf("Number of MemoRecord structures to expect: %zu\n", number_of_records);

	// Calculate the number of chunks to process
	size_t number_of_chunks = (size_t)ceil((double)filesize / CHUNK_SIZE);
	if (DEBUG)
		printf("Number of chunks to expect: %zu\n", number_of_chunks);

	size_t number_of_records_in_chunk = CHUNK_SIZE / sizeof(MemoRecord);
	if (DEBUG)
		printf("Number of records in chunks: %zu\n", number_of_records_in_chunk);

	// Open output file for writing (creating if it doesn't exist)
	int output_file = open(output_file_path, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
	if (output_file == -1)
	{
		perror("Error opening output file");
		close(input_file);
		return -1;
	}

	// Dynamically allocate memory for the buffer and nonce buffer
	uint8_t *buffer = (uint8_t *)malloc(CHUNK_SIZE);
	uint8_t *nonce_buffer = (uint8_t *)malloc(NONCE_SIZE * number_of_records_in_chunk); // Allocate enough for all nonces
	if (buffer == NULL || nonce_buffer == NULL)
	{
		fprintf(stderr, "Memory allocation failed 1: %d %zu\n", CHUNK_SIZE, NONCE_SIZE * number_of_records_in_chunk);
		close(input_file);
		close(output_file);
		return -1;
	}

	ssize_t bytes_read;
	size_t total_chunks_processed = 0;
	size_t nonces_in_buffer = 0; // Counter for nonces added to buffer
	ssize_t bytes_written = 0;

	// Read input file in chunks until EOF
	while ((bytes_read = read(input_file, buffer, CHUNK_SIZE)) > 0)
	{
		if (DEBUG)
			printf("nonces_in_buffer=%zu %zu %d\n", nonces_in_buffer, bytes_read, CHUNK_SIZE);

		size_t records_in_chunk = bytes_read / sizeof(MemoRecord);

		// Process each MemoRecord in the buffer
		if (DEBUG)
			printf("processing %zu records in chunk %zu\n", records_in_chunk, total_chunks_processed);
		for (size_t i = 0; i < records_in_chunk; i++)
		{
			size_t index_raw = i * sizeof(MemoRecord);
			if (DEBUG)
				printf("index_raw=%zu\n", index_raw);
			MemoRecord *record = (MemoRecord *)(buffer + index_raw);
			// Copy nonce to nonce buffer
			size_t index_compressed = nonces_in_buffer * NONCE_SIZE; // Calculate index for nonce
			if (DEBUG)
				printf("index_compressed=%zu\n", index_compressed);

			// Check to avoid buffer overflow in nonce buffer
			if (index_compressed >= NONCE_SIZE * number_of_records_in_chunk)
			{
				printf("this should not happen! %zu %zu %zu %d %zu\n", index_raw, index_compressed, NONCE_SIZE * number_of_records_in_chunk, CHUNK_SIZE, NONCE_SIZE * number_of_records_in_chunk);
				exit(0);
			}

			if (DEBUG)
				printf("memcpy() NONCE_SIZE=%d\n", NONCE_SIZE);
			// Copy nonce from the current record to nonce buffer
			memcpy(nonce_buffer + index_compressed, record->nonce, NONCE_SIZE);
			nonces_in_buffer++; // Increment nonce counter
		}

		if (DEBUG)
			printf("check: %zu %zu %zu %zu\n", nonces_in_buffer, records_in_chunk, total_chunks_processed, number_of_chunks - 1);
		// Write the batched nonces to the output file when buffer is full or at the end
		if (nonces_in_buffer >= records_in_chunk || total_chunks_processed == number_of_chunks - 1)
		{
			ssize_t result = write(output_file, nonce_buffer, nonces_in_buffer * NONCE_SIZE);
			if (result == -1)
			{
				perror("Error writing to output file");
				free(buffer);
				free(nonce_buffer);
				close(input_file);
				close(output_file);
				return -1;
			}
			bytes_written += result;
			if (DEBUG)
				printf("write(): %zu %d %zu\n", bytes_written, NONCE_SIZE, nonces_in_buffer);
			nonces_in_buffer = 0; // Reset the nonce counter
		}

		total_chunks_processed++;
		elapsed_time = get_timer(&timer);

		if (elapsed_time > last_progress_update + 1.0)
		{
			print_progress(elapsed_time, benchmark, total_chunks_processed, number_of_chunks, bytes_written, last_progress_update);
			last_progress_update = elapsed_time; // Update last progress time
			bytes_written = 0;
		}
	} // End of while loop

	// Final write for any remaining nonces in the buffer
	if (nonces_in_buffer > 0)
	{
		ssize_t result = write(output_file, nonce_buffer, nonces_in_buffer * NONCE_SIZE);
		if (result == -1)
		{
			perror("Error writing to output file");
			free(buffer);
			free(nonce_buffer);
			close(input_file);
			close(output_file);
			return -1;
		}
		bytes_written += result;
	}

	// Final progress update after processing
	elapsed_time = get_timer(&timer);
	print_progress(elapsed_time, benchmark, total_chunks_processed, number_of_chunks, bytes_written, last_progress_update);

	// Free the allocated memory
	free(buffer);
	free(nonce_buffer);

	// Close files
	close(input_file);
	close(output_file);
	return 0; // Indicate successful execution
}

// Function that uses higher-level FILE I/O functions to strip the hash from the input file; can be more efficient for handling larger files
int strip_hash_fwrite(Timer timer, bool benchmark, const char *input_file_path, const char *output_file_path)
{
	double elapsed_time = get_timer(&timer);
	double last_progress_update = elapsed_time;

	// Open input file
	FILE *input_file = fopen(input_file_path, "rb");
	if (input_file == NULL)
	{
		perror("Error opening input file");
		return -1;
	}

	// Get the size of the input file
	fseek(input_file, 0, SEEK_END);
	long filesize = ftell(input_file);
	fseek(input_file, 0, SEEK_SET); // Reset to the beginning of the file

	// Calculate the number of MemoRecord structures
	size_t number_of_records = filesize / sizeof(MemoRecord);
	if (DEBUG)
		printf("Number of MemoRecord structures to expect: %zu\n", number_of_records);

	// Calculate the number of chunks to process
	size_t number_of_chunks = (size_t)ceil((double)filesize / CHUNK_SIZE);
	if (DEBUG)
		printf("Number of chunks to expect: %zu\n", number_of_chunks);

	size_t number_of_records_in_chunk = CHUNK_SIZE / sizeof(MemoRecord);
	if (DEBUG)
		printf("Number of records in chunks: %zu\n", number_of_records_in_chunk);

	// Open output file
	FILE *output_file = fopen(output_file_path, "wb");
	if (output_file == NULL)
	{
		perror("Error opening output file");
		fclose(input_file);
		return -1;
	}

	// Dynamically allocate memory for the buffer and nonce buffer
	uint8_t *buffer = (uint8_t *)malloc(CHUNK_SIZE);
	uint8_t *nonce_buffer = (uint8_t *)malloc(NONCE_SIZE * number_of_records_in_chunk); // Allocate enough for all nonces
	if (buffer == NULL || nonce_buffer == NULL)
	{
		fprintf(stderr, "Memory allocation failed 1: %d %zu\n", CHUNK_SIZE, NONCE_SIZE * number_of_records_in_chunk);
		fclose(input_file);
		fclose(output_file);
		return -1;
	}

	size_t bytes_read;
	size_t total_chunks_processed = 0;
	size_t nonces_in_buffer = 0; // Counter for nonces added to buffer
	size_t bytes_written = 0;

	while ((bytes_read = fread(buffer, 1, CHUNK_SIZE, input_file)) > 0)
	{
		if (DEBUG)
			printf("nonces_in_buffer=%zu %zu %d\n", nonces_in_buffer, bytes_read, CHUNK_SIZE);
		size_t records_in_chunk = bytes_read / sizeof(MemoRecord);

		// Process the data in the buffer
		if (DEBUG)
			printf("processing %zu records in chunk %zu\n", records_in_chunk, total_chunks_processed);
		for (size_t i = 0; i < records_in_chunk; i++)
		{
			size_t index_raw = i * sizeof(MemoRecord);
			if (DEBUG)
				printf("index_raw=%zu\n", index_raw);
			MemoRecord *record = (MemoRecord *)(buffer + index_raw);
			// Copy nonce to nonce buffer
			size_t index_compressed = nonces_in_buffer * NONCE_SIZE;
			if (DEBUG)
				printf("index_compressed=%zu\n", index_compressed);

			if (index_compressed >= NONCE_SIZE * number_of_records_in_chunk)
			{
				printf("this should not happen! %zu %zu %zu %d %zu\n", index_raw, index_compressed, NONCE_SIZE * number_of_records_in_chunk, CHUNK_SIZE, NONCE_SIZE * number_of_records_in_chunk);
				exit(0);
			}

			if (DEBUG)
				printf("memcpy() NONCE_SIZE=%d\n", NONCE_SIZE);
			memcpy(nonce_buffer + index_compressed, record->nonce, NONCE_SIZE);
			nonces_in_buffer++;
		}

		if (DEBUG)
			printf("check: %zu %zu %zu %zu\n", nonces_in_buffer, records_in_chunk, total_chunks_processed, number_of_chunks - 1);
		// Write the batched nonces to the output file when buffer is full or at the end
		if (nonces_in_buffer >= records_in_chunk || total_chunks_processed == number_of_chunks - 1)
		{
			// bytes_written += fwrite(nonce_buffer, NONCE_SIZE, nonces_in_buffer, output_file);
			bytes_written += fwrite(nonce_buffer, 1, nonces_in_buffer * NONCE_SIZE, output_file);
			if (DEBUG)
				printf("fwrite(): %zu %d %zu\n", bytes_written, NONCE_SIZE, nonces_in_buffer);
			nonces_in_buffer = 0; // Reset the nonce counter
		}

		total_chunks_processed++;
		elapsed_time = get_timer(&timer);

		if (elapsed_time > last_progress_update + 1.0)
		{
			print_progress(elapsed_time, benchmark, total_chunks_processed, number_of_chunks, bytes_written, last_progress_update);
			last_progress_update = elapsed_time; // Update last progress time
			bytes_written = 0;
		}
	}

	// Final write for any remaining nonces in the buffer
	if (nonces_in_buffer > 0)
	{
		bytes_written += fwrite(nonce_buffer, NONCE_SIZE, nonces_in_buffer, output_file);
	}

	// Final progress update after processing
	elapsed_time = get_timer(&timer);
	print_progress(elapsed_time, benchmark, total_chunks_processed, number_of_chunks, bytes_written, last_progress_update);

	// Free the allocated memory
	free(buffer);
	free(nonce_buffer);

	// Close files
	fclose(input_file);
	fclose(output_file);
	return 0; // Indicate successful execution
}

void remove_file(const char *filename)
{
	// Attempt to remove the file
	if (remove(filename) == 0)
	{
		if (DEBUG)
			printf("File '%s' removed successfully.\n", filename);
	}
	else
	{
		perror("Error removing file");
	}
}

// Function to write a bucket of random records to disk
void write_bucket_to_disk(const Bucket *bucket, int fd, off_t offset)
{
	if (DEBUG)
		printf("write_bucket_to_disk(): %ld %lu %d %lld %zu\n", offset, sizeof(MemoRecord), BUCKET_SIZE, WRITE_SIZE, bucket->flush);
	if (DEBUG)
		printf("write_bucket_to_disk(): %lld\n", offset * sizeof(MemoRecord) * BUCKET_SIZE + WRITE_SIZE * bucket->flush);
	if (lseek(fd, offset * sizeof(MemoRecord) * BUCKET_SIZE + WRITE_SIZE * bucket->flush, SEEK_SET) < 0)
	{
		printf("write_bucket_to_disk(): Error seeking in file at offset %llu; more details: %lu %llu %lu %lld %zu\n", offset * sizeof(MemoRecord) * BUCKET_SIZE + WRITE_SIZE * bucket->flush, offset, FLUSH_SIZE, sizeof(MemoRecord), WRITE_SIZE, bucket->flush);
		close(fd);
		exit(EXIT_FAILURE);
	}

	long long bytes_written = write(fd, bucket->records, sizeof(MemoRecord) * bucket->count);
	if (bytes_written < 0)
	{
		// perror("Error writing to file");
		printf("Error writing bucket at offset %lu to file; bytes written %llu when it expected %lu\n", offset, bytes_written, sizeof(MemoRecord) * bucket->count);
		close(fd);
		exit(EXIT_FAILURE);
	}
	if (DEBUG)
		printf("write_bucket_to_disk(): bytes_written=%lld %lu\n", bytes_written, sizeof(MemoRecord) * bucket->count);
}

void print_bytes(const uint8_t *bytes, size_t length)
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
		// printf(" ");
	}
	printf("\n");
}

off_t byte_array_to_unsigned_int_big_endian(const uint8_t *byte_array, int b)
{
	print_binary(byte_array, HASH_SIZE, b);
	off_t result = 0;

	size_t byteIndex = b / 8; // Determine the byte index
	size_t bitOffset = b % 8; // Determine the bit offset within the byte

	printf("byte_array_to_unsigned_int_big_endian(): byteIndex=%lu\n", byteIndex);
	printf("byte_array_to_unsigned_int_big_endian(): bitOffset=%lu\n", bitOffset);
	// Extract the bits from the byte array
	for (size_t i = 0; i < byteIndex; ++i)
	{
		result |= (uint32_t)byte_array[i] << ((byteIndex - i - 1) * 8);
	}
	printf("byte_array_to_unsigned_int_big_endian(): result1=%ld\n", result);

	// Extract the remaining bits
	if (bitOffset > 0)
	{
		result |= (uint32_t)(byte_array[byteIndex] >> (8 - bitOffset));
	}
	printf("byte_array_to_unsigned_int_big_endian(): result2=%ld\n", result);

	return result;
}

off_t binary_byte_array_to_ull(const uint8_t *byte_array, size_t array_size, int b)
{
	if (DEBUG)
		print_binary(byte_array, array_size, b);
	off_t result = 0;
	int bits_used = 0; // To keep track of how many bits we've used so far

	for (size_t i = 0; i < array_size; ++i)
	{
		for (int j = 7; j >= 0 && bits_used < b; --j)
		{
			result = (result << 1) | ((byte_array[i] >> j) & 1);
			bits_used++;
		}
	}
	if (DEBUG)
		printf("binary_byte_array_to_ull(): result=%ld\n", result);
	return result;
}

off_t get_bucket_index(const uint8_t *byte_array, int b)
{
	off_t result = 0;
	result = binary_byte_array_to_ull(byte_array, HASH_SIZE, b);
	if (DEBUG)
		printf("get_bucket_index(): %ld %d\n", result, b);

	return result;
}

off_t get_bucket_index_old(const uint8_t *hash, int num_bits)
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
	// printf(" : %lld\n",index);
	return index;
}

long long get_file_size(const char *filename)
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
void print_file(const char *filename, int num_records)
{
	FILE *file = fopen(filename, "rb");
	if (file == NULL)
	{
		printf("Error opening file for reading!\n");
		return;
	}

	MemoRecord number;
	// uint8_t array[16];

	long long records_printed = 0;

	while (records_printed < num_records && fread(&number, sizeof(MemoRecord), 1, file) == 1)
	{
		// Interpret nonce as unsigned long long
		unsigned long long nonce_value = 0;
		for (size_t i = 0; i < sizeof(number.nonce); i++)
		{
			nonce_value |= (unsigned long long)number.nonce[i] << (i * 8);
		}

		// Print hash
		printf("[%llu] Hash: ", records_printed * sizeof(MemoRecord));
		for (size_t i = 0; i < sizeof(number.hash); i++)
		{
			printf("%02x", number.hash[i]);
		}
		printf(" : ");

		// Print nonce
		for (size_t i = 0; i < sizeof(number.nonce); i++)
		{
			printf("%02x", number.nonce[i]);
		}

		// Print nonce as unsigned long long
		printf(" : %llu\n", nonce_value);

		records_printed++;
	}

	fclose(file);
}

// Function to print the contents of the file
void print_file_tail(const char *filename, int num_records)
{
	FILE *file = fopen(filename, "rb");
	if (file == NULL)
	{
		printf("Error opening file for reading!\n");
		return;
	}

	long long filesize = get_file_size(filename);

	off_t offset = filesize - num_records * RECORD_SIZE;

	if (fseek(file, offset, SEEK_SET) < 0)
	{
		printf("print_file_tail(): Error seeking in file at offset %lu\n", offset);
		fclose(file);
		exit(EXIT_FAILURE);
	}

	MemoRecord number;

	long long records_printed = 0;

	while (records_printed < num_records && fread(&number, sizeof(MemoRecord), 1, file) == 1)
	{
		// Interpret nonce as unsigned long long
		unsigned long long nonce_value = 0;
		for (size_t i = 0; i < sizeof(number.nonce); i++)
		{
			nonce_value |= (unsigned long long)number.nonce[i] << (i * 8);
		}

		// Print hash
		printf("[%llu] Hash: ", offset + records_printed * sizeof(MemoRecord));
		for (size_t i = 0; i < sizeof(number.hash); i++)
		{
			printf("%02x", number.hash[i]);
		}
		printf(" : ");

		// Print nonce
		for (size_t i = 0; i < sizeof(number.nonce); i++)
		{
			printf("%02x", number.nonce[i]);
		}

		// Print nonce as unsigned long long
		printf(" : %llu\n", nonce_value);

		records_printed++;
	}

	fclose(file);
}

// Binary search function to search for a hash from disk
int binary_search(const uint8_t *target_hash, size_t target_length, int file_descriptor, long long filesize, int *seek_count, bool bulk)
{
	if (DEBUG)
	{
		printf("binary_search()=");
		print_bytes(target_hash, target_length);
		printf("\n");
	}
	// should use filesize to determine left and right
	//  Calculate the bucket index based on the first 2-byte prefix
	off_t bucket_index = get_bucket_index(target_hash, PREFIX_SIZE);
	if (DEBUG)
		printf("bucket_index=%ld\n", bucket_index);

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
	off_t left = bucket_index * BUCKET_SIZE;
	if (DEBUG)
		printf("left=%ld\n", left);
	off_t right = (bucket_index + 1) * BUCKET_SIZE - 1;
	if (DEBUG)
		printf("right=%ld\n", right);

	off_t middle;
	MemoRecord number;

	if (bulk == false)
		*seek_count = 0; // Initialize seek count

	while (left <= right && right - left > SEARCH_SIZE)
	{
		middle = left + (right - left) / 2;
		if (DEBUG)
			printf("left=%ld middle=%ld right=%ld\n", left, middle, right);

		// Increment seek count
		(*seek_count)++;
		// if (DEBUG)
		//	printf("seek_count=%lld \n",seek_count);

		if (DEBUG)
			printf("lseek=%ld %lu\n", middle * sizeof(MemoRecord), sizeof(MemoRecord));
		// Seek to the middle position
		if (lseek(file_descriptor, middle * sizeof(MemoRecord), SEEK_SET) < 0)
		{
			printf("binary_search(): Error seeking in file at offset %lu\n", middle * sizeof(MemoRecord));
			exit(EXIT_FAILURE);
		}

		// Read the hash at the middle position
		if (read(file_descriptor, &number, sizeof(MemoRecord)) < 0)
		{
			perror("Error reading from file");
			exit(EXIT_FAILURE);
		}

		// Compare the target hash with the hash read from file
		if (DEBUG)
		{
			printf("memcmp(target_hash)=");
			print_bytes(target_hash, target_length);
			printf("\n");
			printf("memcmp(number.hash)=");
			print_bytes(number.hash, target_length);
			printf("\n");
		}
		int cmp = memcmp(target_hash, number.hash, target_length);

		if (DEBUG)
		{
			printf("memcmp()=%d\n", cmp);
			printf("nonce=");
			// Print nonce
			for (size_t i = 0; i < sizeof(number.nonce); i++)
			{
				printf("%02x", number.nonce[i]);
			}
			printf("\n");
		}

		if (cmp == 0)
		{
			return middle; // Hash found
		}
		else if (cmp < 0)
		{
			right = middle - 1; // Search the left half
		}
		else
		{
			left = middle + 1; // Search the right half
		}
	}

	// If the remaining data to search is less than 1024 bytes, perform a brute force search
	if (right - left <= SEARCH_SIZE)
	{
		// Increment seek count
		(*seek_count)++;
		// Seek to the left position
		if (lseek(file_descriptor, left * sizeof(MemoRecord), SEEK_SET) < 0)
		{
			printf("binary_search(2): Error seeking in file at offset %lu\n", left * sizeof(MemoRecord));
			exit(EXIT_FAILURE);
		}

		// Perform a brute force search in the remaining 1024 bytes
		while (left <= right)
		{
			// Read the hash at the current position
			if (read(file_descriptor, &number, sizeof(MemoRecord)) < 0)
			{
				perror("Error reading from file");
				exit(EXIT_FAILURE);
			}

			// Compare the target hash with the hash read from file
			int cmp = memcmp(target_hash, number.hash, target_length);
			if (cmp == 0)
			{
				return left; // Hash found
			}

			left++; // Move to the next position
		}
	}

	return -1; // Hash not found
}

int generalized_search(const uint8_t *target_hash, size_t target_length, int fd,
					   long long filesize, int *seek_count, bool compressed)
{
	// Determine bucket based on prefix
	off_t bucket_index = get_bucket_index(target_hash, PREFIX_SIZE);
	printf("bucket_index=%ld\n", bucket_index);
	if (DEBUG)
		printf("bucket_index=%ld\n", bucket_index);

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

	if (compressed)
	{
		printf("Compressed Case\n");
		// Compressed Case: Only nonces stored
		// Calculate the range for the target bucket
		off_t left = bucket_index * BUCKET_SIZE;
		off_t right = (bucket_index + 1) * BUCKET_SIZE - 1;

		printf("BUCKET_SIZE=%d\n", BUCKET_SIZE);
		printf("left=%ld\n", left);
		printf("right=%ld\n", right);

		// Perform a brute-force search within the range
		for (off_t i = left; i <= right; i++)
		{
			MemoRecord record;
			lseek(fd, i * sizeof(record.nonce), SEEK_SET);
			ssize_t bytes_read = read(fd, &record.nonce, sizeof(record.nonce));
			if (bytes_read != sizeof(record.nonce))
			{
				perror("Error reading from file");
				close(fd);
				return -1;
			}

			// Convert nonce into an integer to regenerate the hash
			unsigned long long nonce_value = 0;
			for (size_t i = 0; i < sizeof(record.nonce); i++)
			{
				nonce_value |= (unsigned long long)record.nonce[i] << (i * 8);
			}
			// printf("nonce_value=%llu\n", nonce_value);

			// Regenerate the hash from nonce
			generate_blake3(&record, nonce_value);
			// printf("record.hash=");	
			int cmp = memcmp(target_hash, record.hash, target_length);

			if (cmp == 0)
				return i; // Hash found
		}
		return -1; // Hash not found
	}
	else
	{
		// Uncompressed Case: Regular Binary Search
		return binary_search(target_hash, target_length, fd, filesize, seek_count, false);
	}
}

uint8_t *hex_string_to_byte_array(const char *hex_string, uint8_t *byte_array, size_t byte_array_size)
{
	size_t hex_len = strlen(hex_string);
	if (hex_len % 2 != 0)
	{
		return NULL; // Error: Invalid hexadecimal string length
	}

	size_t byte_len = hex_len / 2;
	if (byte_len > byte_array_size)
	{
		return NULL; // Error: Byte array too small
	}

	for (size_t i = 0; i < byte_len; ++i)
	{
		if (sscanf(&hex_string[i * 2], "%2hhx", &byte_array[i]) != 1)
		{
			return NULL; // Error: Failed to parse hexadecimal string
		}
	}

	return byte_array;
}

unsigned long long byte_array_to_long_long(const uint8_t *byte_array, size_t length)
{
	unsigned long long result = 0;
	for (size_t i = 0; i < length; ++i)
	{
		result = (result << 8) | (unsigned long long)byte_array[i];
	}
	return result;
}

void long_long_to_byte_array(unsigned long long value, uint8_t *byte_array, size_t length)
{
	for (int i = length - 1; i >= 0; --i)
	{
		byte_array[i] = value & 0xFF;
		value >>= 8;
	}
}

char *remove_filename(const char *path)
{
	// Find the position of the last directory separator
	const char *last_separator = strrchr(path, '/');
	if (last_separator == NULL)
	{
		if (DEBUG)
			printf("No directory separator found, return a copy of the original string: %s\n", path);
		return "./";
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

unsigned long long get_disk_space(const char *path)
{

	char *result = remove_filename(path);

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
ssize_t read_chunk(int fd, char *buffer, off_t offset, size_t chunkSize)
{
	ssize_t bytes_read;

	// Read a chunk of data from the file at the specified offset
	bytes_read = pread(fd, buffer, chunkSize, offset);
	if (bytes_read == -1)
	{
		perror("Error reading file");
		return -1;
	}

	return bytes_read;
}

// Function to verify if the records in the buffer are sorted
int verify_sorted(char *buffer, size_t bytes_read)
{
	size_t i;

	// Iterate over the buffer starting from the second record
	for (i = RECORD_SIZE; i < bytes_read; i += RECORD_SIZE)
	{
		// Use memcmp to compare the hashes of the two consecutive records
		if (memcmp(buffer + i - RECORD_SIZE, buffer + i, HASH_SIZE) > 0)
		{
			printf("verify_sorted failed: ");
			print_bytes((uint8_t *)(buffer + i - RECORD_SIZE), HASH_SIZE);
			printf(" !< ");
			print_bytes((uint8_t *)(buffer + i), HASH_SIZE);
			printf("\n");

			return 0; // Records are not sorted
		}
	}
	return 1; // Records are sorted
}

// Function to verify if the final compressed file is sorted
int verify_sorted_compressed(char *buffer, size_t bytes_read)
{
	size_t i;
	uint8_t previous_hash[HASH_SIZE];

	for (i = 0; i < bytes_read; i += NONCE_SIZE)
	{
		// Get the current nonce
		uint8_t *current_nonce = (uint8_t *)(buffer + i);
		MemoRecord record;

		generate_blake3(&record, *((unsigned long long *)current_nonce));

		// Compare the newly generated hash with the previous hash
		if (i > 0) // Ensure there is a previous hash to compare
		{
			if (memcmp(previous_hash, record.hash, HASH_SIZE) > 0)
			{
				printf("verify_compressed_sorted failed: ");
				print_bytes(previous_hash, HASH_SIZE);
				printf(" !< ");
				print_bytes(record.hash, HASH_SIZE);
				printf("\n");
				return 0; // Records are not sorted
			}
		}

		// Store the current hash as the previous hash for the next iteration
		memcpy(previous_hash, record.hash, HASH_SIZE);
		print_bytes(previous_hash, HASH_SIZE);
		printf("\n");
	}

	return 1; // Records are sorted
}

void print_usage()
{
	printf("Usage: ./vault-t <num_threads_hash> -o <num_threads_sort> -i <num_threads_io> -f <temp_filename> -q <final_filename> -m <memorysize_GB> -k <k-value>\n");
	printf("Usage: ./vault -f <filename> -t <num_threads_hash> -o <num_threads_sort> -i <num_threads_io> -m <memorysize_GB> -s <filesize_GB>\n");
	printf("Usage: ./vault -p <num_records> -f <filename>\n");
	printf("Usage: ./vault -f <filename> -p 10\n");
	printf("Usage: ./vault -f <filename> -v true\n");
	printf("Usage: ./vault -f <filename> -b 10\n");
}

void print_help()
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
	printf("  -V <bool> verify compressed hashes from file, off with false, on with true; default is off, must specify -f <filename> and k <num_records> \n");
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
	// unsigned long long offset = args->offset;
	int thread_id = args->thread_id;

	// Loop to sort buckets
	for (int b = 0; b < num_buckets_to_process; ++b)
	{
		if (b == thread_id)
		{
			qsort(buckets[b].records, BUCKET_SIZE, sizeof(MemoRecord), compare_memo_records);

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
	int thread_id = args->thread_id;
	int fd = args->fd;
	if (DEBUG)
		printf("read_bucket thread %d\n", thread_id);

	// Loop to read buckets from disk
	for (int b = 0; b < num_buckets_to_process; ++b)
	{
		if (b == thread_id)
		{
			if (DEBUG)
				printf("reading bucket %d at offset %llu\n", b, offset);
			// Wait on the semaphore
			if (DEBUG)
				printf("sem_wait(%d): wait\n", b);
			semaphore_wait(&semaphore_io);
			if (DEBUG)
				printf("sem_wait(%d): found\n", b);

			long long bytes_read = 0;
			bytes_read = pread(fd, buckets[b].records, BUCKET_SIZE * sizeof(MemoRecord), offset);
			if (bytes_read < 0 || bytes_read != (long long int)(BUCKET_SIZE * sizeof(MemoRecord)))
			{
				printf("Error reading bucket %d from file at offset %llu; bytes read %llu when it expected %lu\n", b, offset, bytes_read, BUCKET_SIZE * sizeof(MemoRecord));
				close(fd);
				*return_value = 1;
				pthread_exit((void *)return_value);
			}
			if (DEBUG)
				printf("[SORT] read %lld bytes, expecting %lu bytes\n", bytes_read, BUCKET_SIZE * sizeof(MemoRecord));
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
	int thread_id = args->thread_id;
	int fd = args->fd;

	// Loop to write buckets to disk
	for (int b = 0; b < num_buckets_to_process; ++b)
	{
		if (b == thread_id)
		{
			// Wait on the semaphore
			semaphore_wait(&semaphore_io);
			if (DEBUG)
				printf("writing bucket %d at offset %llu\n", b, offset);

			long long bytes_read = 0;
			bytes_read = pwrite(fd, buckets[b].records, BUCKET_SIZE * sizeof(MemoRecord), offset);
			if (bytes_read < 0 || bytes_read != (long long int)(BUCKET_SIZE * sizeof(MemoRecord)))
			{
				printf("Error writing bucket %d from file at offset %llu; bytes written %llu when it expected %lu\n", b, offset, bytes_read, BUCKET_SIZE * sizeof(MemoRecord));
				close(fd);
				*return_value = 1;
				pthread_exit((void *)return_value);
			}
			if (DEBUG)
				printf("[SORT] write %lld bytes, expecting %lu bytes\n", bytes_read, BUCKET_SIZE * sizeof(MemoRecord));

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
	struct sysinfo mem_info;
	sysinfo(&mem_info);

	long long total_memory = mem_info.totalram;
	total_memory *= mem_info.mem_unit;

	long long free_memory = mem_info.freeram;
	free_memory *= mem_info.mem_unit;

	if (DEBUG)
		printf("Total Memory: %lld bytes\n", total_memory);
	if (DEBUG)
		printf("Free Memory: %lld bytes\n", free_memory);
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
	Timer timer;
	double elapsed_time;
	double elapsed_time_hash_gen = 0;
	double elapsed_time_sort;
	double elapsed_time_sync;
	double elapsed_time_compress;

	char *FILENAME = NULL;		 // Default value
	char *FILENAME_FINAL = NULL; // Default value
	long long FILESIZE = 0;		 // Default value
	long long KSIZE = 0;
	int num_threads_sort = 1;
	int num_threads_io = 1;

	long long print_records = 0;

	uint8_t *byte_array = NULL;
	size_t hash_len = 0;
	uint8_t *target_hash = NULL;
	size_t prefix_length = 10;

	int search_records = 0;

	bool head = false;
	bool tail = false;

	bool verify_records = false, verify_compressed_records = false;
	bool benchmark = false;

	int verify_records_num = 0;

	bool hashgen = false;

	int opt;
	while ((opt = getopt(argc, argv, "t:o:m:k:f:q:s:p:r:a:l:c:d:i:x:v:b:y:z:w:h:V:")) != -1)
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
			break;
		case 'y':
			HASHGEN_THREADS_BUFFER = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("HASHGEN_THREADS_BUFFER=%d\n", HASHGEN_THREADS_BUFFER);
			break;
		case 'i':
			num_threads_io = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("num_threads_io=%d\n", num_threads_io);
			break;
		case 'm':
			memory_size = atoi(optarg);
			if (benchmark == false)
				if (DEBUG)
					printf("memory_size=%lld MB\n", memory_size);
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
			hash_len = strlen(optarg) / 2;
			// Validate the length of the hash
			if (strlen(optarg) % 2 != 0 || strlen(optarg) > 20) // Each byte represented by 2 characters in hexadecimal
			{
				printf("Invalid hexadecimal hash format\n");
				return 1;
			}

			// Allocate memory for byte_array based on byte_array_len
			byte_array = malloc(hash_len);
			if (!byte_array)
			{
				printf("Memory allocation failed\n");
				return 1;
			}

			// Convert the hexadecimal hash from command-line argument to binary
			target_hash = hex_string_to_byte_array(optarg, byte_array, sizeof(byte_array));
			if (target_hash == NULL)
			{
				printf("Error: Byte array too small\n");
				return 1;
			}

			if (benchmark == false)
			{
				printf("Hash_search=");
				print_bytes(byte_array, hash_len);
				printf("\n");
			}
			break;
		case 'l':
			// Get the length of the prefix
			prefix_length = atoi(optarg);
			if (prefix_length <= 0 || prefix_length > 10)
			{
				printf("Invalid prefix length\n");
				return 1;
			}
			if (benchmark == false)
				printf("prefix_length=%zu\n", prefix_length);
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
		case 'V':
			if (optarg == NULL)
			{
				printf("Option -e requires an argument (true/false)\n");
				return 1;
			}
			if (strcmp(optarg, "true") == 0)
			{
				verify_compressed_records = true;
			}
			else if (strcmp(optarg, "false") == 0)
			{
				verify_compressed_records = false;
			}
			else
			{
				verify_compressed_records = false;
			}
			if (benchmark == false)
				printf("verify_compressed_records=%s\n", verify_compressed_records ? "true" : "false");
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
			print_help();
			return 0;
		default:
			print_usage();
			return 1;
		}
	}

	if (FILENAME == NULL)
	{
		printf("Error: filename (-f) is mandatory.\n");
		print_usage();
		return 1;
	}
	if (FILENAME != NULL && print_records == 0 && verify_records == false && verify_compressed_records == false && target_hash == NULL && verify_records_num == 0 && search_records == 0 && (NUM_THREADS <= 0 || num_threads_sort <= 0 || FILESIZE < 0 || memory_size <= 0))
	{
		printf("Error: mandatory command line arguments have not been used, try -h for more help\n");
		print_usage();
		return 1;
	}

	const char *path = FILENAME;

	long long bytes_free = get_disk_space(path);
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

	long long sort_memory = 0;
	long long EXPECTED_TOTAL_FLUSHES = 0;
	bool found_good_config = false;

	// piece of code to find memory size that is a multiple of FILESIZE
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

	// assign memory size to be a multiple of FILESIZE
	memory_size = right_memory_size;
	int ratio = (int)ceil((double)FILESIZE / memory_size);

	if (benchmark == false)
		if (DEBUG)
			printf("ratio=%d\n", ratio);

	// convert memory size and FILESIZE to bytes
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
			}
		}

		if (found_good_config == false)
		{
			printf("no valid configuration found... this should never happen\n");

			printf("exiting...\n");
			return 1;
		}
	}

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

	// verify uncompressed hashes from file
	if (verify_records)
	{
		reset_timer(&timer);
		int VERIFY_BUFFER_SIZE = 1000000 - RECORD_SIZE; // value that can hold multiple MemoRecord structures minus the size of one MemoRecord, allowing room for reading multiple records at once.
		int fd;
		char *buffer = (char *)malloc(VERIFY_BUFFER_SIZE); // Allocate memory for the verification buffer
		ssize_t bytes_read;								   // Variable to store the number of bytes read from the file
		off_t offset = 0;								   // Variable to track the current offset in the file

		if (buffer == NULL)
		{
			fprintf(stderr, "Memory allocation failed 2\n");
			return 1;
		}

		// Open the file in read-only mode
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
		while ((bytes_read = read_chunk(fd, buffer, offset, VERIFY_BUFFER_SIZE)) > 0)
		{
			//  Verify if the records in the buffer are sorted
			if (!verify_sorted(buffer, bytes_read))
			{
				printf("Records are not sorted at %ld offset.\n", offset);
				all_sorted = false;
				break;
			}

			// Update the offset for the next read
			offset += bytes_read;
			num_reads++;
		}

		if (bytes_read == -1)
		{
			fprintf(stderr, "Error reading buffer after reading %ld bytes\n", offset);
		}

		// If all records were sorted, print a success message
		if (all_sorted)
			printf("Read %ld bytes and found all records are sorted.\n", offset);

		// Close the file descriptor
		close(fd);
		free(buffer); // Free the allocated buffer

		elapsed_time = get_timer(&timer);
		double progress = 100.0;
		double remaining_time = 0.0;
		double throughput_MB = (offset / (1024 * 1024)) / elapsed_time;

		printf("[%.3lf][VERIFY]: %.2lf%% completed, ETA %.1lf seconds, %d flushes, %.1lf MB/sec\n", elapsed_time, progress, remaining_time, num_reads, throughput_MB);

		return 0;
	}
	// verify compressed hashes from file
	else if (verify_compressed_records)
	{
		reset_timer(&timer);
		int fd;
		int VERIFY_BUFFER_SIZE = 1000000 - NONCE_SIZE;
		char *buffer = (char *)malloc(VERIFY_BUFFER_SIZE); // Allocate memory for the verification buffer
		ssize_t bytes_read;								   // Variable to store the number of bytes read from the file
		off_t offset = 0;								   // Variable to track the current offset in the file
		bool all_sorted = true;

		fd = open(FILENAME, O_RDONLY);
		if (fd == -1)
		{
			perror("Unable to open file");
			free(buffer);
			return 1;
		}

		// Read the file in chunks of BUFFER_SIZE bytes until the end of file
		while ((bytes_read = read_chunk(fd, buffer, offset, VERIFY_BUFFER_SIZE)) > 0)
		{
			// Verify if the records in the buffer are sorted
			if (!verify_sorted_compressed(buffer, bytes_read))
			{
				printf("Compressed records are not sorted at %ld offset.\n", offset);
				all_sorted = false;
				break;
			}

			// Update the offset for the next read
			offset += bytes_read;
		}

		if (bytes_read == -1)
		{
			fprintf(stderr, "Error reading buffer after reading %ld bytes\n", offset);
		}

		// If all compressed records were sorted, print a success message
		if (all_sorted)
			printf("Read %ld bytes and found all compressed records are sorted.\n", offset);

		// Close the file descriptor
		close(fd);
		free(buffer); // Free the allocated buffer

		elapsed_time = get_timer(&timer);
		printf("Compressed verification completed in %.3lf seconds.\n", elapsed_time);
	}
	// print records
	else if (print_records > 0)
	{
		// Print the contents of the specified file
		if (head)
		{
			printf("Printing first %lld of file '%s'...\n", print_records, FILENAME);
			print_file(FILENAME, print_records);
		}
		if (tail)
		{
			printf("Printing first %lld of file '%s'...\n", print_records, FILENAME);
			print_file_tail(FILENAME, print_records);
		}

		return 0;
	}
	// search hash
	else if (target_hash != NULL)
	{
		printf("Searching for hash\n");
		// Open file for reading
		int fd = open(FILENAME, O_RDONLY);
		if (fd < 0)
		{
			perror("Error opening file for reading");
			return 1;
		}

		long long filesize = get_file_size(FILENAME);
		reset_timer(&timer);

		// Specify whether the hashes are compressed
		bool is_compressed = true;

		// Perform the generalized search
		int seek_count = 0;
		int index = generalized_search(target_hash, prefix_length, fd, filesize, &seek_count, is_compressed);
		printf("index=%d\n", index);

		// Get end time
		elapsed_time = get_timer(&timer);

		if (index >= 0)
		{
			// Hash found
			MemoRecord found_number;

			// if compressed, seek to nonce location only; otherwise, seek to full MemoRecord
			if (is_compressed)
			{
				lseek(fd, index * sizeof(found_number.nonce), SEEK_SET);
				ssize_t bytes_read = read(fd, &found_number.nonce, sizeof(found_number.nonce));
				if (bytes_read != sizeof(found_number.nonce))
				{
					printf("Error reading nonce from file\n");
					close(fd);
					return -1;
				}

				// Convert nonce into an integer to regenerate the hash
				unsigned long long nonce_value = 0;
				for (size_t i = 0; i < sizeof(found_number.nonce); i++)
				{
					nonce_value |= (unsigned long long)found_number.nonce[i] << (i * 8);
				}

				// Regenerate the hash
				generate_blake3(&found_number, nonce_value);
			}
			else
			{
				lseek(fd, index * sizeof(MemoRecord), SEEK_SET);
				ssize_t bytes_read = read(fd, &found_number, sizeof(MemoRecord));
				if (bytes_read != sizeof(MemoRecord))
				{
					printf("Error reading MemoRecord from file\n");
					close(fd);
					return -1;
				}
			}

			printf("Hash found at index: %d\n", index);
			printf("Number of lookups: %d\n", seek_count);
			printf("Hash search: ");
			print_bytes(byte_array, sizeof(byte_array));
			printf("/%zu\n", prefix_length);

			printf("Hash found : ");
			print_bytes(found_number.hash, sizeof(found_number.hash));
			printf("\n");

			unsigned long long nonce_value = 0;
			for (size_t i = 0; i < sizeof(found_number.nonce); i++)
			{
				nonce_value |= (unsigned long long)found_number.nonce[i] << (i * 8);
			}

			printf("Nonce: %llu/", nonce_value);
			print_bytes(found_number.nonce, sizeof(found_number.nonce));
			printf("\n");
		}
		else
		{
			printf("Hash not found after %d lookups\n", seek_count);
		}
		printf("Time taken: %.2f microseconds\n", elapsed_time);

		// Close the file
		close(fd);

		if (byte_array != NULL)
		{
			free(byte_array); // Free allocated memory for byte_array before exiting
		}

		return 0;
	}
	// search bulk
	else if (search_records > 0)
	{
		printf("searching for random hashes\n");
		size_t number_lookups = search_records;
		if (number_lookups <= 0)
		{
			printf("Invalid number of lookups\n");
			return 1;
		}

		// Get the length of the prefix
		if (prefix_length <= 0 || prefix_length > 10)
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
		long filesize = get_file_size(FILENAME);

		// Seed the random number generator with the current time
		srand((unsigned int)time(NULL));
		int seek_count = 0;
		int found = 0;
		int notfound = 0;

		reset_timer(&timer);

		for (size_t search_num = 0; search_num < number_lookups; search_num++)
		{
			uint8_t byte_array[HASH_SIZE];
			for (size_t i = 0; i < HASH_SIZE; ++i)
			{
				byte_array[i] = rand() % 256;
			}

			uint8_t *target_hash = byte_array;
			if (target_hash == NULL)
			{
				printf("Error: Byte array too small\n");
				return 1;
			}

			// Perform binary search
			int index = binary_search(target_hash, prefix_length, fd, filesize, &seek_count, true);

			if (index >= 0)
			{
				// Hash found
				MemoRecord found_number;
				lseek(fd, index * sizeof(MemoRecord), SEEK_SET);
				ssize_t bytes_read = read(fd, &found_number, sizeof(MemoRecord));
				if (bytes_read != sizeof(MemoRecord))
				{
					printf("Error reading MemoRecord from file\n");
					close(fd);
					return -1;
				}

				found++;
				if (DEBUG)
				{
					printf("Hash found at index: %d\n", index);
					printf("Hash found : ");
					print_bytes(found_number.hash, sizeof(found_number.hash));
					printf("\n");

					unsigned long long nonce_value = 0;
					for (size_t i = 0; i < sizeof(found_number.nonce); i++)
					{
						nonce_value |= (unsigned long long)found_number.nonce[i] << (i * 8);
					}

					printf("Nonce: %llu/", nonce_value);
					print_bytes(found_number.nonce, sizeof(found_number.nonce));
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
				printf("Number of lookups: %d\n", seek_count);
				printf("Hash search: ");
				print_bytes(byte_array, sizeof(byte_array));
				printf("/%zu\n", prefix_length);
				// printf("Prefix length: %zu\n", prefix_length);
			}
		}

		elapsed_time = get_timer(&timer);
		printf("Number of total lookups: %zu\n", number_lookups);
		printf("Number of searches found: %d\n", found);
		printf("Number of searches not found: %d\n", notfound);
		printf("Number of total seeks: %d\n", seek_count);
		printf("Time taken: %.2f ms/lookup\n", elapsed_time * 1000.0 / number_lookups);
		printf("Throughput lookups/sec: %.2f\n", number_lookups / elapsed_time);

		// Close the file
		close(fd);

		return 0;
	}
	// verify bulk
	else if (verify_records_num > 0)
	{
		printf("verifying random records against BLAKE3 hashes\n");
		size_t number_lookups = verify_records_num;
		if (number_lookups <= 0)
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
		long filesize = get_file_size(FILENAME);
		unsigned long long num_records_in_file = filesize / RECORD_SIZE;

		// Seed the random number generator with the current time
		srand((unsigned int)time(NULL));
		// int seek_count = 0;
		int found = 0;
		int notfound = 0;

		reset_timer(&timer);

		for (size_t search_num = 0; search_num < number_lookups; search_num++)
		{
			int index = rand() % num_records_in_file;

			MemoRecord found_number;
			MemoRecord found_number2;
			lseek(fd, index * sizeof(MemoRecord), SEEK_SET);
			ssize_t bytes_read = read(fd, &found_number, sizeof(MemoRecord));
			if (bytes_read != sizeof(MemoRecord))
			{
				printf("Error reading MemoRecord from file\n");
				close(fd);
				return -1;
			}

			unsigned long long nonce_value = 0;
			for (size_t i = 0; i < sizeof(found_number.nonce); i++)
			{
				nonce_value |= (unsigned long long)found_number.nonce[i] << (i * 8);
			}

			generate_blake3(&found_number2, nonce_value);

			if (DEBUG)
			{
				printf("Hash found at index: %d\n", index);
				printf("Hash found : ");
				print_bytes(found_number.hash, sizeof(found_number.hash));
				printf("\n");

				printf("Nonce: %llu/", nonce_value);
				print_bytes(found_number.nonce, sizeof(found_number.nonce));
				printf("\n");
			}

			if (memcmp(&found_number, &found_number2, sizeof(MemoRecord)) == 0)
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

		elapsed_time = get_timer(&timer);
		printf("Number of total verifications: %zu\n", number_lookups);
		printf("Number of verifications successful: %d\n", found);
		printf("Number of verifications failed: %d\n", notfound);
		printf("Time taken: %.2f ms/verification\n", elapsed_time * 1000.0 / number_lookups);
		printf("Throughput verifications/sec: %.2f\n", number_lookups / elapsed_time);

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
			// configuration file setup
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

			// Open the main file for writing, creating it if it doesn't exist and truncating if it does
			fd = open(FILENAME, O_RDWR | O_CREAT | O_TRUNC, 0644);
			if (fd < 0)
			{
				perror("Error opening file for writing");
				return 1;
			}

			reset_timer(&timer); // Start the timer for performance measurement

			// Array to hold the generated random records
			// MemoRecord record;
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
				// TODO: should double check BUCKET_SIZE * sizeof(MemoRecord) logic if its indeed correct
				buckets[i].records = malloc(WRITE_SIZE);
				if (buckets[i].records == NULL)
				{
					printf("3 Error allocating memory %lu; %d %d\n", BUCKET_SIZE * sizeof(MemoRecord), NUM_BUCKETS, i);

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

			// Initialize the circular array for managing records
			struct CircularArray circular_array;
			init_circular_array(&circular_array);

			// Print debug message before creating thread data structure
			if (benchmark == false)
				if (DEBUG)
					printf("Create thread data structure...\n");

			// Create thread data structure to hold thread-specific data
			struct ThreadData thread_data[NUM_THREADS];

			// Print debug message before creating hash generation threads
			if (benchmark == false)
				if (DEBUG)
					printf("Create hash generation threads...\n");

			// Create threads for parallel hash generation
			pthread_t threads[NUM_THREADS];
			for (int i = 0; i < NUM_THREADS; i++)
			{
				thread_data[i].circular_array = &circular_array;							 // Assign the circular array to each thread
				thread_data[i].thread_id = i;												 // Set the thread ID
				pthread_create(&threads[i], NULL, array_generation_thread, &thread_data[i]); // Start the thread
			}

			// Print debug message after thread creation
			if (benchmark == false)
				if (DEBUG)
					printf("Hash generation threads created...\n");

			unsigned long long total_flushes = 0; // Track total number of flushes
			last_progress_i = 0;				  // Reset last processed index

			elapsed_time = get_timer(&timer);	 // Get initial elapsed time
			last_progress_update = elapsed_time; // Update last progress time

			// Write random records to buckets based on their first 2-byte prefix
			unsigned long long i = 0;											  // Initialize index for tracking records
			int flushed_buckets_needed = NUM_BUCKETS;							  // Track how many buckets need flushing
			MemoRecord *consumed_array = malloc(BATCH_SIZE * sizeof(MemoRecord)); // Array for consumed records

			if (DEBUG)
				printf("BATCH_SIZE=%zu\n", BATCH_SIZE);
			if (DEBUG)
				printf("flushed_buckets_needed=%d\n", flushed_buckets_needed);

			while (flushed_buckets_needed > 0) // Continue processing until all buckets have been flushed
			{
				if (DEBUG)
					printf("remove_batch()...\n"); // Indicate that a batch is being removed from the circular array

				remove_batch(&circular_array, consumed_array); // Remove a batch of records from the circular array and store in consumed_array

				if (DEBUG)
					printf("processing batch of size %ld...\n", BATCH_SIZE); // Log the size of the processed batch

				// this sequential loop is probably a bottleneck
				for (size_t b = 0; b < BATCH_SIZE; b++)
				{
					// Get the index of the bucket where the current record should be stored
					off_t bucket_index = get_bucket_index(consumed_array[b].hash, PREFIX_SIZE);

					Bucket *bucket = &buckets[bucket_index]; // Reference the corresponding bucket

					// Check if the bucket has space for more records
					if (bucket->count < (size_t)(WRITE_SIZE / RECORD_SIZE))
					{
						// Add the current record to the bucket
						bucket->records[bucket->count++] = consumed_array[b];
					}
					else // If the bucket is full
					{
						// Check if this bucket can be flushed to disk
						if (bucket->flush < FLUSH_SIZE)
						{
							// Note: should parallelize the sort and write to disk here...

							// if all buckets fit in memory, sort them now before writing to disk
							if (FLUSH_SIZE == 1)
							{
								if (DEBUG)
									printf("sorting bucket before flush %ld...\n", b);

								// Sort the bucket contents if sorting is enabled
								if (HASHSORT)
									qsort(bucket->records, BUCKET_SIZE, sizeof(MemoRecord), compare_memo_records);

								// Alternative sorting methods
								// heapsort(bucket->records, BUCKET_SIZE, sizeof(MemoRecord), compare_memo_records);
								// parallel_sort(bucket->records, BUCKET_SIZE, sizeof(MemoRecord), compare_memo_records, NUM_THREADS, PARTITION_SIZE, QSORT_SIZE, DEBUG);
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

							// Write the contents of the bucket to disk
							write_bucket_to_disk(bucket, fd, bucket_index);

							// Reset the bucket count after flushing
							bucket->count = 0;
							bucket->flush++; // Increment flush count for this bucket
							if (bucket->flush == FLUSH_SIZE)
								flushed_buckets_needed--; // Decrease the count of buckets that still need flushing

							total_flushes++; // Increment the total number of flushes
						}
					}
				}

				elapsed_time = get_timer(&timer);

				// Update progress every second
				if (elapsed_time > last_progress_update + 1.0)
				{
					double elapsed_time_since_last_progress_update = elapsed_time - last_progress_update;
					last_progress_update = elapsed_time; // Update the last progress timestamp

					// Calculate and print estimated completion time
					double progress = min(i * 100.0 / NUM_ENTRIES, 100.0);
					double remaining_time = elapsed_time / (progress / 100.0) - elapsed_time;

					if (benchmark == false)
						printf("[%.0lf][HASHGEN]: %.2lf%% completed, ETA %.1lf seconds, %llu/%llu flushes, %.1lf MB/sec\n", floor(elapsed_time), progress, remaining_time, total_flushes, EXPECTED_TOTAL_FLUSHES, ((i - last_progress_i) / elapsed_time_since_last_progress_update) * sizeof(MemoRecord) / (1024 * 1024));

					last_progress_i = i;
				}
				i += BATCH_SIZE; // Increment the processed index by the batch size
			}

			if (DEBUG)
				printf("finished generating %llu hashes and wrote them to disk using %llu flushes...\n", i, total_flushes);

			// Free memory allocated for records in each bucket
			for (int i = 0; i < NUM_BUCKETS; i++)
			{
				free(buckets[i].records);
			}
			free(buckets); // Free the array of buckets

			free(consumed_array); // Free the consumed array holding the batch of records

			elapsed_time_hash_gen = get_timer(&timer); // Record the total time taken for hash generation
		}
		else // if HASHGEN is false
		{
			if (benchmark == false)
				printf("opening file for sorting...\n");

			// Open the file for reading/writing, with large file support
			fd = open(FILENAME, O_RDWR | O_LARGEFILE, 0644);
			if (fd < 0) // Check if the file opened successfully
			{
				perror("Error opening file for reading/writing");
				return 1;
			}

			reset_timer(&timer); // Reset the timer for measuring time taken
		}

		bool do_sort = true;

		elapsed_time = get_timer(&timer);
		last_progress_update = elapsed_time;
		double sort_start = elapsed_time;

		if (DEBUG)
			print_free_memory();

		if (DEBUG)
		{
			// Print planned memory allocations for various components
			printf("planning to allocate 1: %lu bytes memory...", num_threads_sort * sizeof(Bucket));
			printf("planning to allocate 2: %lu bytes memory...", num_threads_sort * BUCKET_SIZE * sizeof(MemoRecord));
			printf("planning to allocate 3: %lu bytes memory...", num_threads_sort * sizeof(ThreadArgs));
			printf("planning to allocate 4: %lu bytes memory...", num_threads_sort * sizeof(pthread_t));
		}

		if (FLUSH_SIZE > 1 && do_sort && HASHSORT) // Check if we need to perform an external sort based on flush size and sorting flags
		{
			int EXPECTED_BUCKETS_SORTED = NUM_BUCKETS; // Total number of buckets expected to be sorted
			if (DEBUG)
				printf("external sort started, expecting %llu flushes for %d buckets...\n", FLUSH_SIZE, NUM_BUCKETS);

			if (DEBUG) // Print expected sorting parameters for debugging
			{
				printf("EXPECTED_BUCKETS_SORTED=%d\n", EXPECTED_BUCKETS_SORTED);
				printf("FLUSH_SIZE=%llu\n", FLUSH_SIZE);
				printf("NUM_BUCKETS=%d\n", NUM_BUCKETS);
				printf("BUCKET_SIZE=%d\n", BUCKET_SIZE);
			}

			// Allocate memory for num_threads_sort bucket
			if (DEBUG)
				printf("trying to allocate 1: %lu bytes memory...\n", num_threads_sort * sizeof(Bucket));

			Bucket *buckets = malloc(num_threads_sort * sizeof(Bucket)); // Allocate memory for the buckets
			if (buckets == NULL)										 // Check for allocation failure
			{
				perror("Memory allocation failed 3");
				return EXIT_FAILURE;
			}

			if (DEBUG)
				print_free_memory();

			// initialize state of each bucket
			if (DEBUG)
				printf("trying to allocate 2: %lu bytes memory...\n", num_threads_sort * BUCKET_SIZE * sizeof(MemoRecord));

			for (int i = 0; i < num_threads_sort; ++i) // Loop to initialize each bucket
			{
				if (DEBUG)
					printf("trying to allocate 2:%d: %lu bytes memory...\n", i, BUCKET_SIZE * sizeof(MemoRecord));

				buckets[i].count = BUCKET_SIZE;								   // Set the initial count of records in the bucket
				buckets[i].records = malloc(BUCKET_SIZE * sizeof(MemoRecord)); // Allocate memory for records in each bucket
				if (buckets[i].records == NULL)								   // Check for allocation failure
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

			ThreadArgs *args = malloc(num_threads_sort * sizeof(ThreadArgs)); // Allocate memory for thread arguments
			if (args == NULL)
			{
				perror("Failed to allocate memory for args");
				return EXIT_FAILURE;
			}
			if (DEBUG)
				print_free_memory();

			if (DEBUG)
				printf("trying to allocate 4: %lu bytes memory...\n", num_threads_sort * sizeof(pthread_t));

			pthread_t *sort_threads = malloc(num_threads_sort * sizeof(pthread_t)); // Allocate memory for thread handles
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
			if (DEBUG)
				printf("before semaphore...\n");

			semaphore_init(&semaphore_io, num_threads_io); // Initialize semaphore for controlling IO threads
			if (DEBUG)
				printf("after semaphore...\n");

			if (DEBUG)
				print_free_memory();

			for (unsigned long long i = 0; i < (unsigned long long)NUM_BUCKETS; i = i + num_threads_sort) // Loop through buckets in increments of threads
			{
				if (DEBUG)
					printf("inside for loop %llu...\n", i);

				if (DEBUG)
					print_free_memory();

				if (DEBUG)
					printf("processing bucket i=%llu\n", i);

				// Start threads to read buckets from the file
				if (DEBUG)
					printf("starting reading threads...\n");

				for (int b = 0; b < num_threads_sort; b++) // Loop to create reading threads
				{
					// Set up arguments for each thread
					args[b].buckets = buckets;
					args[b].thread_id = b; // Assign thread ID
					args[b].num_buckets_to_process = num_threads_sort;
					args[b].offset = (i + b) * sizeof(MemoRecord) * BUCKET_SIZE;
					args[b].fd = fd;

					int status = pthread_create(&sort_threads[b], NULL, read_bucket, &args[b]); // Create reading thread

					if (status != 0)
					{
						perror("pthread_create read_bucket");
						return EXIT_FAILURE;
					}
				}

				if (DEBUG)
					printf("waiting for reading threads...\n");

				// Wait for reading threads to finish
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

				// Start threads to sort the buckets
				for (int b = 0; b < num_threads_sort; b++)
				{
					// Set up arguments for each sorting thread
					args[b].buckets = buckets;
					args[b].thread_id = b; // Assign thread ID
					args[b].num_buckets_to_process = num_threads_sort;
					args[b].offset = 0;
					args[b].fd = -1;

					int status = pthread_create(&sort_threads[b], NULL, sort_bucket, &args[b]); // Create sorting thread
					if (status != 0)
					{
						perror("pthread_create sort");
						return EXIT_FAILURE;
					}
				}

				if (DEBUG)
					printf("waiting for sort threads...\n");

				// Wait for sorting threads to finish
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
				// Start threads to write sorted buckets back to the file
				for (int b = 0; b < num_threads_sort; b++)
				{
					// Set up arguments for each writing thread
					args[b].buckets = buckets;
					args[b].thread_id = b; // Assign thread ID
					args[b].num_buckets_to_process = num_threads_sort;
					args[b].offset = (i + b) * sizeof(MemoRecord) * BUCKET_SIZE;
					args[b].fd = fd;

					int status = pthread_create(&sort_threads[b], NULL, write_bucket, &args[b]); // Create writing thread
					if (status != 0)
					{
						perror("pthread_create write_bucket");
						return EXIT_FAILURE;
					}
				}

				if (DEBUG)
					printf("waiting for writing threads...\n");

				// Wait for writing threads to finish
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

				elapsed_time = get_timer(&timer);

				if (elapsed_time > last_progress_update + 1)
				{
					// double elapsed_time_since_last_progress_update = elapsed_time - last_progress_update;

					int totalBucketsSorted = i; // i*NUM_THREADS;
					float perc_done = totalBucketsSorted * 100.0 / EXPECTED_BUCKETS_SORTED;
					float eta = (elapsed_time - sort_start) / (perc_done / 100.0) - (elapsed_time - sort_start);
					// float disk_size = FILESIZE_byte / (1024 * 1024);
					// float throughput_MB = disk_size * perc_done * 1.0 / 100.0 / (elapsed_time - sort_start);
					float throughput_MB_latest = (((i - last_progress_i) * BUCKET_SIZE * sizeof(MemoRecord) * 1.0) / (elapsed_time - last_progress_update)) / (1024 * 1024);
					if (DEBUG)
						printf("%llu %llu %d %llu %lu %f %f\n", i, last_progress_i, BUCKET_SIZE, FLUSH_SIZE, sizeof(MemoRecord), elapsed_time, last_progress_update);
					float progress = perc_done;
					float remaining_time = eta;
					if (benchmark == false)
						printf("[%.0lf][SORT]: %.2lf%% completed, ETA %.1lf seconds, %llu/%d flushes, %.1lf MB/sec\n", elapsed_time, progress, remaining_time, i, NUM_BUCKETS, throughput_MB_latest);

					last_progress_i = i;

					last_progress_update = elapsed_time;
				}
			} // end of for loop

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

			// TODO: free memory for thread data structure
			// free(args);	// Free the memory allocated for the thread arguments
			// free(sort_threads);
		}
		else
		{
			if (HASHSORT == true)
				if (benchmark == false)
					printf("in-memory sort completed!\n");
		}

		elapsed_time = get_timer(&timer);
		elapsed_time_sort = elapsed_time - elapsed_time_hash_gen;

		// Ensure that all data is flushed to disk before continuing
		// This section may be commented out if flushing is not needed
		if (benchmark == false)
			// printf("flushing all writes to disk...\n");			// Optionally print flushing status
			printf("[%.0lf][FLUSH]: 0.00%% completed, ETA 0.0 seconds, 0/0 flushes, 0.0 MB/sec\n", elapsed_time); // Print initial flush progress (0% completed)

		if (fsync(fd) == -1)
		{
			perror("fsync");
		}

		close(fd);

		elapsed_time = get_timer(&timer);
		elapsed_time_sync = elapsed_time - elapsed_time_hash_gen - elapsed_time_sort;

		// Final compression by stripping the hash from each record and saving it to disk, also includes cleanup
		if (FILENAME_FINAL != NULL)
		{
			if (strip_hash(timer, benchmark, FILENAME, FILENAME_FINAL) == 0)
			{
				remove_file(FILENAME);
			}
		}

		elapsed_time = get_timer(&timer);
		elapsed_time_compress = elapsed_time - elapsed_time_hash_gen - elapsed_time_sort - elapsed_time_sync;

		// Determine the record size based on whether a final filename is provided
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
		double hashes_per_second = NUM_ENTRIES / elapsed_time;

		// Calculate final file size in megabytes
		long long FILESIZE_FINAL = record_size * NUM_ENTRIES / (1024 * 1024);

		// Calculate bytes per second
		double bytes_per_second = record_size * NUM_ENTRIES / elapsed_time;

		if (benchmark == false)
			printf("[%.0lf][FINISHED]: 100.00%% completed in %.1lf seconds, %lld MB vault in %s, %.2f MH/s, Effective Throughput %.2f MB/s\n", elapsed_time, elapsed_time, FILESIZE_FINAL, FILENAME_FINAL, hashes_per_second / 1000000.0, bytes_per_second * 1.0 / (1024 * 1024));

		else
			printf("%.3lf,%.3lf,%.3lf,%.3lf,%.3lf\n", elapsed_time_hash_gen, elapsed_time_sort, elapsed_time_sync, elapsed_time_compress, elapsed_time);

		return 0;
	}
	// end of hash generation
}
