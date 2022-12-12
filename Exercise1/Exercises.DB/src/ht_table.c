#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht_table.h"
#include "record.h"
#include <assert.h>

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int TC(BF_ErrorCode error) {
    if (error != BF_OK) {
        BF_PrintError(error);
        return (-1);
    } else {
        return 0;
    }
}

// int HT_CreateFile(char *fileName,  int buckets){
    
// 	HT_info info;
// 	int fileDescriptor;
// 	int error;
// 	char* data;					char* hashTableData;
// 	BF_Block* block;			BF_Block_Init(&block);
// 	BF_Block* hashTableBlock;	BF_Block_Init(&hashTableBlock);

// 	int blockCounter;			int oldBlockCounter;

// 	error = TC(BF_CreateFile(fileName));
// 	if (error != 0 ) { return -1; }

// 	error = TC(BF_OpenFile(fileName, &fileDescriptor));
// 	if (error != 0 ) { return -1; }

// 	error = TC(BF_GetBlockCounter(fileDescriptor, &blockCounter));
// 	if (error != 0) return -1;
	
// 	oldBlockCounter = blockCounter;

// 	// Allocate the first block, in which we save
// 	// metadata
// 	error = TC(BF_AllocateBlock(fileDescriptor, block));
// 	if (error != 0) return -1;

// 	error = TC(BF_GetBlockCounter(fileDescriptor, &blockCounter));
// 	if (error != 0) return -1;

// 	assert(blockCounter == oldBlockCounter + 1);

// 	info.fileDesc = fileDescriptor;
// 	info.numBuckets = buckets;
// 	info.isHashFile = true;
// 	info.isHeapFile = false;
// 	info.recordsPerBlock = (sizeof(char) * BF_BLOCK_SIZE - sizeof(HT_block_info)) / sizeof(Record);

// 	error = TC(BF_GetBlock(fileDescriptor, blockCounter-1, block));
// 	if (error != 0) return -1;

// 	data = BF_Block_GetData(block);

// 	memcpy(data, &info, sizeof(HT_info));
// 	data += sizeof(HT_info);


// 	int totalSizeOfBuckets = buckets * (sizeof(int));
// 	int hashTableSize = ( sizeof(char) * BF_BLOCK_SIZE - sizeof(HT_info) );
	
// 	assert(hashTableSize >= totalSizeOfBuckets);


// 	/*
// 	Block allocated: 1 (0)
// 					Start of Hashtable
// 					˅ 
// 	_________________________________________________________________________
// 	|				|				|					|					|
// 	|				|				|					|					|
// 	|	HT_block	|	bucket[0]	|	bucket[1]		|		bucket[N]	|
// 	|				|				|					|					|
// 	|				|				|					|					|
// 	|_______________|_______________|___________________|___________________|
// 					/				|					|
// 				   |				|					|
// 	|--------------|				|					|_____________________________________________
// 	|								|____________________											  |
// 	|													˅ 											  ˅  
// 	˅                          
// 	____________________________________________		____________________________________
// 	|							|				|		|						|			|		
// 	|		HT_block_info		|	Rec[0] ... 	|		|		HT_block_info	|			|
// 	|							|				|		|						|	Rec[0]	|		....
// 	|							|				|		|						|			|
// 	|							|				|		|						|			|
// 	L-------------------------------------------|		|-----------------------|-----------|
// 	block that stored info for buckets that land in [0]
// 	*/

// 	BF_Block* allocatedBucket;
// 	BF_Block_Init(&allocatedBucket);

// 	for(int i = 0; i < buckets; i++) {
// 		printf("\nAllocating Bucket\n");
// 		HT_block_info info;

// 		// The new bucket doesn't have a overflow
// 		info.overflow = -1;

// 		// And can fit this many records inside
// 		info.recordsCount = (sizeof(char) * BF_BLOCK_SIZE - sizeof(HT_block_info)) / (sizeof(Record));
// 		char* bucketData;

// 		// Allocate it
// 		error = TC(BF_AllocateBlock(fileDescriptor, allocatedBucket));
// 		if (error != 0) return -1;
		
// 		// Firstly place its header
// 		bucketData = BF_Block_GetData(allocatedBucket);

// 		memcpy(bucketData, &info, sizeof(HT_block_info));

// 		// Now connect the bucket to the hashTable in the header
// 		// data points to the hashTable position

// 		int newBucketIn; BF_GetBlockCounter(fileDescriptor, &newBucketIn);
// 		newBucketIn--;
// 		printf("New allocated bucket is in position: %d\n", newBucketIn);

// 		memcpy(data, &newBucketIn, sizeof(int));
// 		data += sizeof(int);

// 		BF_Block_SetDirty(allocatedBucket);
// 		BF_UnpinBlock(allocatedBucket);
// 	}
	
// 	BF_Block_Destroy(&allocatedBucket);

// 	return 0;

// }

int HT_CreateFile(char *fileName,  int buckets){
	// Create a file with name fileName
	BF_CreateFile(fileName);

	// Open the file with name fileName
	int fileDescriptor;
	BF_OpenFile(fileName, &fileDescriptor);

	// Write to the first block to make it a Hash File
	BF_Block* block;
	BF_Block_Init(&block);
	BF_AllocateBlock(fileDescriptor, block); // Allocate the first block

	// Create a HT_info struct to write to the first block
	HT_info info; 
	info.fileDesc = fileDescriptor;  // Write the file descriptor
	info.numBuckets = buckets;		// Write the number of buckets
	info.isHashFile = true; 	   // Write that this is a Hash File
	info.isHeapFile = false;  	  // Write that this is not a Heap File
	info.recordsPerBlock = (sizeof(char) * BF_BLOCK_SIZE - sizeof(HT_block_info)) / (sizeof(Record));
	
	// Get the data of the first block
	char* data = BF_Block_GetData(block);
	memcpy(data, &info, sizeof(HT_info)); // Write the HT_info struct to the first block
	data += sizeof(HT_info); // Move the pointer to the next position

	// Allocate momory for the buckets
	for (int i=0; i<buckets; i++) {
		BF_Block* bucket;
		BF_Block_Init(&bucket);
		BF_AllocateBlock(fileDescriptor, bucket); // Allocate a block for the bucket

		HT_block_info info; // Create a HT_block_info struct to write to the bucket
		info.overflow = -1; // The new bucket doesn't have a overflow
		info.recordsCount = (sizeof(char) * BF_BLOCK_SIZE - sizeof(HT_block_info)) / (sizeof(Record)); // And can fit this many records inside

		char* bucketData = BF_Block_GetData(bucket);  	   // Get the data of the bucket
		memcpy(bucketData, &info, sizeof(HT_block_info)); // Write the HT_block_info struct to the bucket

		int newBucketIn; BF_GetBlockCounter(fileDescriptor, &newBucketIn); // Get the position of the bucket
		newBucketIn--; // Decrease it by one

		memcpy(data, &newBucketIn, sizeof(int)); // Write the position of the bucket to the hashTable in the first block
		data += sizeof(int); // Move the pointer to the next position

		BF_Block_SetDirty(bucket); 	 // Mark the block as dirty
		BF_UnpinBlock(bucket); 		// Unpin the block because we don't need it anymore
		BF_Block_Destroy(&bucket); // Destroy the block
	}

	// close the file
	BF_CloseFile(fileDescriptor);
	BF_Block_Destroy(&block);

	return 0;
}

HT_info* HT_OpenFile(char *fileName){
	// Open the file with name fileName
	int fileDescriptor; // The file descriptor
	BF_OpenFile(fileName, &fileDescriptor);

	BF_Block* block; 		// Create a block
	BF_Block_Init(&block); // Initialize the block
	HT_info* toReturn = (HT_info*) malloc(sizeof(HT_info)); // Allocate memory for the returning HT_info struct

	BF_GetBlock(fileDescriptor, 0, block);  // Get the first block
	char* data = BF_Block_GetData(block);  // Get the data of the first block
	HT_info* infoSaved = (HT_info*) data; // Cast the data to HT_info

	// If the file is not a Hash File, return NULL
	if (infoSaved->isHeapFile) { return NULL; }

	// Copy the data from the first block to the returning HT_info struct
	memcpy(toReturn, infoSaved, sizeof(HT_info)); 

	BF_UnpinBlock(block); 	   // Unpin the first block because we don't need it anymore
	BF_Block_Destroy(&block); // Destroy the block

    return toReturn;
}


int HT_CloseFile( HT_info* HT_info ){
	int fileDescriptor = HT_info->fileDesc; // Get the file descriptor
	BF_CloseFile(fileDescriptor); // Close the file
	free(HT_info); // Free the memory of the HT_info struct

    return 0;
}

int HT_InsertEntry(HT_info* ht_info, Record record){
    return 0;
}

int HT_GetAllEntries(HT_info* ht_info, void *value ){
    return 0;
}


uint hash_string(void* value) {
	// djb2 hash function, απλή, γρήγορη, και σε γενικές γραμμές αποδοτική
    uint hash = 5381;
    for (char* s = value; *s != '\0'; s++)
		hash = (hash << 5) + hash + *s;			// hash = (hash * 33) + *s. Το foo << 5 είναι γρηγορότερη εκδοχή του foo * 32.
    return hash;
}


