#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
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

typedef struct {
  int blockId;
  char name[16];
} secIndexEntry;


int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){
  BF_CreateFile(sfileName);
	int fileDescriptor;
  

  BF_OpenFile(sfileName, &fileDescriptor);

  // Write to the first block to make it a Hash File
	BF_Block* block;
	BF_Block_Init(&block);
	BF_AllocateBlock(fileDescriptor, block); // Allocate the first block

  int counter;
  BF_GetBlockCounter(fileDescriptor, &counter);
  printf("At sht create, allocated first block in index: %d\n", counter - 1);

  SHT_info info;
  info.fileDesc = fileDescriptor;
  info.numBuckets = buckets;
  info.isHashFile = true;
  info.recordsPerBlock = (sizeof(char) * BF_BLOCK_SIZE - sizeof(SHT_block_info)) / sizeof(secIndexEntry);
  // totalSizeOfBuckets => How big the hashTable has to be
  int totalSizeOfBuckets = buckets * sizeof(int);
  // How much space is available in total for the hashTable
  int hashTableSize = (sizeof(char) * BF_BLOCK_SIZE - sizeof(SHT_info));

  info.hashTable = malloc(sizeof(int) * buckets);
	char* data = BF_Block_GetData(block);

  // Allocate momory for the buckets
	for (int i=0; i<buckets; i++) {
		BF_Block* bucket;
		BF_Block_Init(&bucket);

    // Allocate a block for the bucket
		BF_AllocateBlock(fileDescriptor, bucket); 


		SHT_block_info blockInfo; // Create a SHT_block_info struct to write to the bucket
		blockInfo.recordsCount = (sizeof(char) * BF_BLOCK_SIZE - sizeof(SHT_block_info)) / (sizeof(secIndexEntry)); // And can fit this many records inside
		blockInfo.currentRecords = 0; // Has no records inside
		blockInfo.nextBlock = -1; // Has no next block
		
		char* bucketData = BF_Block_GetData(bucket);  	        // Get the data of the bucket
		memcpy(bucketData, &blockInfo, sizeof(SHT_block_info)); // Write the HT_block_info struct to the bucket

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

  memcpy(data, &info, sizeof(SHT_info));

  BF_Block_SetDirty(block);
	BF_UnpinBlock(block);
	
	// close the file
	BF_CloseFile(fileDescriptor);
	BF_Block_Destroy(&block);

	return 0;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName){
  int fileDescriptor; // The file descriptor
	BF_OpenFile(indexName, &fileDescriptor);

	BF_Block* block; 		// Create a block
	BF_Block_Init(&block); // Initialize the block
	SHT_info* toReturn = (SHT_info*) malloc(sizeof(SHT_info)); // Allocate memory for the returning HT_info struct

	BF_GetBlock(fileDescriptor, 0, block);  // Get the first block
	char* data = BF_Block_GetData(block);  // Get the data of the first block
	SHT_info* infoSaved = (SHT_info*) data; // Cast the data to HT_info

  // for(int i = 0; i < infoSaved->numBuckets; i++)
  //   assert(infoSaved->hashTable[i] == -1);

	// If the file is not a Hash File, return NULL
	// if (infoSaved->isHeapFile) { return NULL; }
  printf("Fd SHT: %d\n", fileDescriptor);

	// Copy the data from the first block to the returning HT_info struct
	memcpy(toReturn, infoSaved, sizeof(SHT_info)); 
  
  toReturn->fileDesc = fileDescriptor;

	printf("Opened file\n");
  printf("Fd SHT after: %d\n", infoSaved->fileDesc);


	BF_UnpinBlock(block); 	   // Unpin the first block because we don't need it anymore
	BF_Block_Destroy(&block); // Destroy the block

  return toReturn;
}


int SHT_CloseSecondaryIndex( SHT_info* SHT_info ){
  printf("At sht close\n");
  int fileDescriptor = SHT_info->fileDesc; // Get the file descriptor
	BF_Block* block;	BF_Block_Init(&block);
  
	BF_GetBlock(fileDescriptor, 0 , block); // Get the first block
	char* data = BF_Block_GetData(block); 	// Get the data of the first block
  printf("At sht close\n");
	
  assert(SHT_info != NULL);
  assert(data != NULL);
  memcpy(data, SHT_info, sizeof(SHT_info)); // Copy the data from the HT_info struct to the first block
	
	BF_UnpinBlock(block);

	free(SHT_info->hashTable);
	BF_Block_Destroy(&block);
	free(SHT_info); // Free the memory of the HT_info struct
  printf("To close SHT with fd: %d\n", fileDescriptor);
	int error = TC(BF_CloseFile(fileDescriptor)); // Close the file

  return 0;
}



int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){
  int fileDescriptor = sht_info->fileDesc;
  int buckets = sht_info->numBuckets;
  int recordsPerBlock = sht_info->recordsPerBlock;
  int* hashTable = sht_info->hashTable;

  printf("Inserting in sec index: <%s, %d>\n", record.name, block_id);
  int hash = hash_string(record.name) % buckets;
  int bucket = hashTable[hash];
	printf("Record hashed in bucket: %d, saved in block: %d\n", hash, bucket);

  secIndexEntry toInsert;
  toInsert.blockId = block_id;
  strcpy(toInsert.name, record.name);
  printf("After\n");

  BF_Block* block; 		// Create a block
	BF_Block_Init(&block); // Initialize the block

	BF_GetBlock(fileDescriptor, bucket, block);
	char* blockData = BF_Block_GetData(block);

  SHT_block_info* blockInfoRead = (SHT_block_info *) blockData;
  SHT_block_info* first = blockInfoRead;

  bool insertEntryInIndex = true;
  
  while ( true ) {
		// Iterate through all records of the bucket
		for(int i = 0; i < first->currentRecords; i++) {
			// Check every record in block
			char* data = blockData +  sizeof(SHT_block_info) + i * (sizeof(secIndexEntry));
			secIndexEntry entry = (secIndexEntry) *( (secIndexEntry*) data); // Cast the data to Record
			// Check if tuple <name, block_id> exists 
      if (entry.blockId == block_id && strcmp(entry.name, record.name) == 0) {
        insertEntryInIndex = false;
        printf("\n\n\nThere already exists an entry: <%s,%d> in the index\n\n\n", record.name, block_id);
        break;
      }
		}
		// Check if there is a next block (overflow)
    if (insertEntryInIndex == false) break;

		if ( first->nextBlock == -1) 
			break;
		else {
      // Before I get the next block in the chain, unpin the current one
      BF_UnpinBlock(block);
      // Now get the next block
			BF_GetBlock(fileDescriptor, first->nextBlock, block);

			blockData = BF_Block_GetData(block);
			first = (SHT_block_info*) blockData;
		}
	}

  BF_UnpinBlock(block);
  	
  if (insertEntryInIndex) {

	  int recordsInBlock = blockInfoRead->currentRecords;
	  printf("Block can hold: %d records, currently holds: %d\n", blockInfoRead->recordsCount, blockInfoRead->currentRecords);
    // If records fits in block, just place it inside
	  if (recordsInBlock < blockInfoRead->recordsCount) {
	  	printf("No overflow in bucket: %d saved in block: %d\n", hash, bucket );
	  	char* data = blockData +  sizeof(SHT_block_info) + recordsInBlock * (sizeof(secIndexEntry));
	  	memcpy(data, &toInsert, sizeof(secIndexEntry));
	  	blockInfoRead->currentRecords++;
	  	BF_Block_SetDirty(block); // Mark the block as dirty

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
	  	newBlockInfo->recordsCount = (sizeof(char) * BF_BLOCK_SIZE  - sizeof(SHT_block_info) )/ sizeof(secIndexEntry); // Set the records count to the maximum number of records that can fit in a block
  
	  	char* data = newBlockData +  sizeof(SHT_block_info); // Get the data of the new block
	  	memcpy(data, &toInsert, sizeof(secIndexEntry)); // Copy the data from the record to the new block
	  	BF_Block_SetDirty(newBlock); // Mark the new block as dirty
	  	BF_UnpinBlock(newBlock); // Unpin the new block because we don't need it anymore
	  	BF_Block_Destroy(&newBlock); // Destroy the new block
  
	  	sht_info->hashTable[hash] = blockCounter; // Set the bucket to the new block
  
	  	printf("Header file for bucket %d now points to: %d\n", hash, blockCounter);

	  }
  }
	BF_UnpinBlock(block); // Unpin the block because we don't need it anymore
	BF_Block_Destroy(&block); // Destroy the block
	
	return 0;
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name){
  int hash = hash_string(name) % sht_info->numBuckets;
  int bucket = sht_info->hashTable[hash];
  printf("Bucket: %d\n", bucket);

  BF_Block* block; 		// Create a block
	BF_Block_Init(&block); // Initialize the block

	BF_GetBlock(sht_info->fileDesc, bucket, block);
	char* blockData = BF_Block_GetData(block);
  BF_UnpinBlock(block);

  SHT_block_info* blockInfoRead = (SHT_block_info *) blockData;
  int blockRead = 0;

  // Go down the chain of blocks in the SECONDARY INDEX
  while ( true ) {
    
    // Iterate through all entries inside the bucket of the SECONDARY INDEX
    for(int i = 0; i < blockInfoRead->currentRecords; i++) {
      
      // Check every entry inside the SECONDARY INDEX
      char* data = blockData +  sizeof(SHT_block_info) + i * (sizeof(secIndexEntry));
      secIndexEntry entry = (secIndexEntry) *( (secIndexEntry*) data); // Cast the data to Record
      
      if (strcmp(entry.name, name) == 0) {
        printf("Found entry: <%s,%d> in the index\n", entry.name, entry.blockId); // Print the record
        Record record; // Create a record
        BF_GetBlock(ht_info->fileDesc, entry.blockId, block); // Get the block of the record
        HT_block_info* HT_header = (HT_block_info*) BF_Block_GetData(block);   // Get the data of the block
        blockRead++; // Increase the number of blocks read

        // Iterate through all records of the block of the PRIMARY INDEX
        // To find if there is a record inside, with the same name
        for (int i = 0; i < HT_header->currentRecords; i++) { 
			      char* data = (char*) HT_header +  sizeof(HT_block_info) + i * (sizeof(Record)); 
          	Record record = (Record) *( (Record*) data); // Cast the data to Record
          	// If the name of the record is the same as the name we are looking for
          	if ( strcmp(name, record.name ) == 0) {
		    	    printf("%d \t\t %s \t %s \t %s \n", record.id, record.name, record.surname, record.city);
            }
        }
        BF_UnpinBlock(block);
      }
    }
    // Check if there is a next block (overflow)
	  if ( blockInfoRead->nextBlock == -1) 
			break; // If there is no next block, break the loop
	  else {
      // Before I get the next block in the chain, unpin the current one
      // BF_UnpinBlock(block); // Unpin the block because we don't need it anymore
      // Now get the next block
			BF_GetBlock(sht_info->fileDesc, blockInfoRead->nextBlock, block);

			blockData = BF_Block_GetData(block); // Get the data of the block
			blockInfoRead = (SHT_block_info*) blockData; // Cast the data to HT_block_info
		}
	}
}

unsigned int hash_string(void* value) {
	// djb2 hash function, απλή, γρήγορη, και σε γενικές γραμμές αποδοτική
    unsigned int hash = 5381;
    for (char* s = value; *s != '\0'; s++)
		hash = (hash << 5) + hash + *s;			// hash = (hash * 33) + *s. Το foo << 5 είναι γρηγορότερη εκδοχή του foo * 32.
    return hash;
}


