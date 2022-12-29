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
	
	int totalSizeOfBuckets = buckets * (sizeof(int));
	int hashTableSize = ( sizeof(char) * BF_BLOCK_SIZE - sizeof(HT_info) );

	assert(totalSizeOfBuckets <= hashTableSize);

	info.hashTable = malloc(sizeof(int) * buckets);
	// Get the data of the first block
	char* data = BF_Block_GetData(block);

	// Allocate momory for the buckets
	for (int i=0; i<buckets; i++) {
		BF_Block* bucket;
		BF_Block_Init(&bucket);
		BF_AllocateBlock(fileDescriptor, bucket); // Allocate a block for the bucket

		HT_block_info blockInfo; // Create a HT_block_info struct to write to the bucket
		// blockInfo.overflow = -1; // The new bucket doesn't have a overflow
		blockInfo.recordsCount = (sizeof(char) * BF_BLOCK_SIZE - sizeof(HT_block_info)) / (sizeof(Record)); // And can fit this many records inside
		blockInfo.currentRecords = 0; // Has no records inside
		blockInfo.nextBlock = -1; // Has no next block
		
		char* bucketData = BF_Block_GetData(bucket);  	   // Get the data of the bucket
		memcpy(bucketData, &blockInfo, sizeof(HT_block_info)); // Write the HT_block_info struct to the bucket

		printf("Can hold: %d, holds: %d\n", blockInfo.recordsCount, blockInfo.currentRecords);
		assert(blockInfo.currentRecords == 0);
		
		// last bucket is in position counter-1. so hashTable[i] = counter-1
		int newBucketIn; BF_GetBlockCounter(fileDescriptor, &newBucketIn); // Get the position of the bucket
		newBucketIn--; // Decrease it by one
		
		info.hashTable[i] = newBucketIn;
		printf("Created bucket for [%d], saved in block: %d\n", i, info.hashTable[i]);


		BF_Block_SetDirty(bucket); 	 // Mark the block as dirty
		BF_UnpinBlock(bucket); 		// Unpin the block because we don't need it anymore
		BF_Block_Destroy(&bucket); // Destroy the block
	}

	memcpy(data, &info, sizeof(HT_info)); // Write the HT_info struct to the first block
	
	BF_Block_SetDirty(block);
	BF_UnpinBlock(block);
	
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
	toReturn->fileDesc = fileDescriptor;

	printf("Opened file\n");

	BF_UnpinBlock(block); 	   // Unpin the first block because we don't need it anymore
	BF_Block_Destroy(&block); // Destroy the block

    return toReturn;
}


int HT_CloseFile( HT_info* HT_info ){
	int fileDescriptor = HT_info->fileDesc; // Get the file descriptor
	BF_Block* block;	BF_Block_Init(&block);
	
	printf("HT close\n");
	BF_GetBlock(fileDescriptor, 0, block); // Get the first block
	char* data = BF_Block_GetData(block); 	// Get the data of the first block
	memcpy(data, HT_info, sizeof(HT_info)); // Copy the data from the HT_info struct to the first block
	
	BF_UnpinBlock(block);

	free(HT_info->hashTable);
	BF_Block_Destroy(&block);

	free(HT_info); // Free the memory of the HT_info struct
	BF_CloseFile(fileDescriptor); // Close the file

    return 0;
}

int hashFunc(int key, int buckets) {
	return key % buckets;
}

int HT_InsertEntry(HT_info* ht_info, Record record){
	
	BF_Block* block; 		// Create a block
	BF_Block_Init(&block); // Initialize the block
	int fileDescriptor = ht_info->fileDesc; // Get the file descriptor
	int hash = hashFunc(record.id, ht_info->numBuckets); // Get the hash of the record
	int bucket = ht_info->hashTable[hash]; // Get the number of the bucket that contains the record
	int returnBlockId;

	BF_GetBlock(fileDescriptor, bucket, block);
	char* blockData = BF_Block_GetData(block);
	
	HT_block_info* blockInfoRead = (HT_block_info *) blockData;
	
	printf("Record hashed in bucket: %d, saved in block: %d\n", hash, bucket);

	int recordsInBlock = blockInfoRead->currentRecords;
	printf("Block can hold: %d records, currently holds: %d\n", blockInfoRead->recordsCount, blockInfoRead->currentRecords);
	// If records fits in block, just place it inside
	if (recordsInBlock < blockInfoRead->recordsCount) {
		printf("No overflow in bucket: %d saved in block: %d\n", hash, bucket );
		char* data = blockData +  sizeof(HT_block_info) + recordsInBlock * (sizeof(Record));
		memcpy(data, &record, sizeof(Record));
		blockInfoRead->currentRecords++;
		BF_Block_SetDirty(block); // Mark the block as dirty
		
		// return the block id
		returnBlockId = bucket;

	} else {
		printf("Allocating new block\n");
		// If records doesn't fit in block, create a new block and place it there
		BF_Block* newBlock; 		// Create a block
		BF_Block_Init(&newBlock); // Initialize the block
		BF_AllocateBlock(fileDescriptor, newBlock); // Allocate a block for the new record
		// Get block number of last allocated block ( = blockCounter - 1)
		int blockCounter; BF_GetBlockCounter(fileDescriptor,&blockCounter ); // Get the number of the last allocated block
		blockCounter--; // Get the number of the last allocated block
		
		char* newBlockData = BF_Block_GetData(newBlock); // Get the data of the new block
		HT_block_info* newBlockInfo = (HT_block_info *) newBlockData; // Cast the data to HT_block_info
		newBlockInfo->currentRecords = 1; // Set the current records to 1

		// Connect newly allocated block with the previous block in place
		newBlockInfo->nextBlock = bucket; // Set the next block to previous bucket (reverse chaining)		
		newBlockInfo->recordsCount = (sizeof(char) * BF_BLOCK_SIZE  - sizeof(HT_block_info) )/ sizeof(Record); // Set the records count to the maximum number of records that can fit in a block
		
		char* data = newBlockData +  sizeof(HT_block_info); // Get the data of the new block
		memcpy(data, &record, sizeof(Record)); // Copy the data from the record to the new block
		BF_Block_SetDirty(newBlock); // Mark the new block as dirty
		BF_UnpinBlock(newBlock); // Unpin the new block because we don't need it anymore
		BF_Block_Destroy(&newBlock); // Destroy the new block
		
		ht_info->hashTable[hash] = blockCounter; // Set the bucket to the new block
		
		printf("Header file for bucket %d now points to: %d\n", hash, blockCounter);

		// retunrBlockId is the id of the last inserted block (saved in blockCounter)
		returnBlockId = blockCounter;
	}
	BF_UnpinBlock(block); // Unpin the block because we don't need it anymore
	BF_Block_Destroy(&block); // Destroy the block
	
	
    return returnBlockId; // Return the block id
}

int HT_GetAllEntries(HT_info* ht_info, int* value ){
	int fileDescriptor = ht_info->fileDesc; // Get the file descriptor
	BF_Block* block; 		// Create a block
	BF_Block_Init(&block); // Initialize the block
	
	int blocksRead = 0;
	int hashValue = hashFunc((int) * ((int*) value), ht_info->numBuckets); // Get the hash of the value
	int bucket = ht_info->hashTable[hashValue];

	
	// Get block number of last allocated block ( = blockCounter - 1)
	BF_GetBlock(fileDescriptor, bucket, block);
	char* blockData = BF_Block_GetData(block);
	HT_block_info* info = (HT_block_info*) blockData;
	

	while ( true ) {
		// Iterate through all records of the bucket
		blocksRead++;
		for(int i = 0; i < info->currentRecords; i++) {
			// Check every record in block
			char* data = blockData +  sizeof(HT_block_info) + i * (sizeof(Record));
			Record rec = (Record) *( (Record*) data); // Cast the data to Record
			if (rec.id == (int) * ((int*)value) ) {
				printf("Found\n");
			}
		}
		// Check if there is a next block (overflow)
		if ( info->nextBlock == -1) 
			break;
		else {
			BF_UnpinBlock(block);
			BF_GetBlock(fileDescriptor, info->nextBlock, block);
			blockData = BF_Block_GetData(block);
			info = (HT_block_info*) blockData;
		}
	}

	BF_UnpinBlock(block);
	BF_Block_Destroy(&block);

    return blocksRead;
}

int HashStatistics (char* filename) {
	// Open file
	int fileDesc;
	BF_ErrorCode code = BF_OpenFile(filename, &fileDesc);
	if (code != BF_OK) {
		BF_PrintError(code);
		return -1;
	}

	// Get block 0
	BF_Block* block;
	BF_Block_Init(&block);
	code = BF_GetBlock(fileDesc, 0, block);
	if (code != BF_OK) {
		BF_PrintError(code);
		return -1;
	}

	// Get block data
	char* blockData = BF_Block_GetData(block);
	HT_info* info = (HT_info*) blockData;

	// Get number of blocks
	int blockCounter;
	code = BF_GetBlockCounter(fileDesc, &blockCounter);
	if (code != BF_OK) {
		BF_PrintError(code);
		return -1;
	}

	// Get number of buckets
	int buckets = info->numBuckets;

	// Get number of records
	int records = 0;
	for (int i = 0; i < buckets; i++) {
		records += info->hashTable[i];
	}

	// Get number of blocks
	int blocks = blockCounter;

	// Get number of records per block
	int recordsPerBlock = (sizeof(char) * BF_BLOCK_SIZE  - sizeof(HT_block_info) )/ sizeof(Record);

	// Get number of blocks per bucket
	int blocksPerBucket = blocks / buckets;

	// Get number of records per block per bucket
	int recordsPerBlockPerBucket = recordsPerBlock / buckets;

	// Get number of records per bucket
	int averageRecordsPerBucket = records / buckets;

	// Get Min records per bucket
	int minRecordsPerBucket = averageRecordsPerBucket;
	for (int i = 0; i < buckets; i++) {
		if (info->hashTable[i] < minRecordsPerBucket) {
			minRecordsPerBucket = info->hashTable[i];
		}
	}

	// Get Max records per bucket
	int maxRecordsPerBucket = averageRecordsPerBucket;
	for (int i = 0; i < buckets; i++) {
		if (info->hashTable[i] > maxRecordsPerBucket) {
			maxRecordsPerBucket = info->hashTable[i];
		}
	}

	// Get the count of buckets with overflow
	int bucketsWithOverflow = 0;
	for (int i = 0; i < buckets; i++) {
		if (info->hashTable[i] > recordsPerBlock) {
			bucketsWithOverflow++;
		}
	}

	// Get the count of blocks with overflow
	int blocksWithOverflow = 0;
	for (int i = 0; i < buckets; i++) {
		if (info->hashTable[i] > recordsPerBlock) {
			blocksWithOverflow += (info->hashTable[i] - recordsPerBlock) / recordsPerBlock;
		}
	}

	// Print statistics
	printf("Number of buckets: %d\n", buckets);
	printf("Number of records: %d\n", records);
	printf("Number of blocks: %d\n", blocks);
	printf("Number of records per block: %d\n", recordsPerBlock);
	printf("Number of min records per bucket: %d\n", minRecordsPerBucket);
	printf("Number of average records per bucket: %d\n", averageRecordsPerBucket);
	printf("Number of max records per bucket: %d\n", maxRecordsPerBucket);
	printf("Number of buckets with overflow: %d\n", bucketsWithOverflow);
	printf("Number of blocks with overflow: %d\n", blocksWithOverflow);
	printf("Number of blocks per bucket: %d\n", blocksPerBucket);
	printf("Number of records per block per bucket: %d\n", recordsPerBlockPerBucket);

	// Close file
	code = BF_CloseFile(fileDesc);
	if (code != BF_OK) {
		BF_PrintError(code);
		return -1;
	}

	return 0;
}


