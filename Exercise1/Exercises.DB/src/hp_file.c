#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"
#include <assert.h>

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

int HP_CreateFile(char *fileName){

	/*
	Record block structure:
	_____________________________________________________
	|						|							|
	|						|							|
	|	HP Info Header		|	nextBlock				|
	|		(HP_Info)		|			(int)			|
	|						|							|
	-----------------------------------------------------

	*/

	HP_info info;
	int fileDescriptor;
	int blockCounter;
	int oldBlockCounter;
	BF_Block* block; BF_Block_Init(&block);
	char* data;
	int error;


	error = TC(BF_CreateFile(fileName));
	error += TC(BF_OpenFile(fileName, &fileDescriptor));
	error += TC(BF_GetBlockCounter(fileDescriptor, &blockCounter));
	if (error != 0) return -1;
	
	oldBlockCounter = blockCounter;

	// Allocate the first block, in which we save
	// metadata
	error += TC(BF_AllocateBlock(fileDescriptor, block));
	error += TC(BF_GetBlockCounter(fileDescriptor, &blockCounter));
	if (error != 0) return -1;

	assert(oldBlockCounter + 1 == blockCounter);

	// Info we want to keep in the header as metadata
	info.fileDesc = fileDescriptor;
	info.headerPosition = oldBlockCounter;
	info.isHash = false;
	info.isHeapFile = true;
	info.recordsPerBlock = ( BF_BLOCK_SIZE - sizeof(int) * 2 ) / sizeof(Record);
	info.lastBlock = -1;

	printf("Records per block = %d\n", info.recordsPerBlock);
	
	error += TC(BF_GetBlock(fileDescriptor, blockCounter - 1, block));
	if (error != 0) return -1;
	data = BF_Block_GetData(block);
	
	memcpy(data, &info, sizeof(HP_info));
	int nextBlock = -1;
	
	// Move the pointer sizeof(HP_Info) accross
	data += sizeof(HP_info);
	memcpy(data, &nextBlock, sizeof(int));
	assert ( *((int*) data) == -1 );


	BF_Block_SetDirty(block);
	error = TC(BF_UnpinBlock(block));
	if (error == -1) return -1;

	BF_Block_Destroy(&block);

	error = TC(BF_CloseFile(fileDescriptor));
	if (error == -1 ) return -1;

}

HP_info* HP_OpenFile(char *fileName){
    
	int fileDescriptor;
	int error;
	char* data;
	BF_Block* block; BF_Block_Init(&block);
	HP_info* toReturn = (HP_info* ) malloc(sizeof(HP_info));

	error = TC(BF_OpenFile(fileName, &fileDescriptor));
	if (error == -1) return NULL;

	error = TC(BF_GetBlock(fileDescriptor, 0, block));
	if (error == -1) return NULL;
	
	data = BF_Block_GetData(block);
	HP_info* infoSaved = (HP_info *) data;

	// Assert we are talking about a heap file
	if ( infoSaved->isHash ) { return NULL; }
	assert(infoSaved->isHeapFile);
	// assert (infoSaved->fileDesc == fileDescriptor);

	memcpy(toReturn, infoSaved, sizeof(HP_info));

	error = TC(BF_UnpinBlock(block));
	if (error == -1) return NULL;
	BF_Block_Destroy(&block);

	return toReturn;
}


int HP_CloseFile( HP_info* hp_info ){
	int fileDescriptor;
    int error;

	fileDescriptor = hp_info->fileDesc;
	free(hp_info);

	error = TC(BF_CloseFile(fileDescriptor));
	if (error == -1) return -1;
	
	return 0;
}

int HP_InsertEntry(HP_info* hp_info, Record record){

	/*
	Record block structure:
	________________________________________________________________________________
	|						|							|			|				|
	|						|							|			|				|
	|	recordsInBlock		|	Pointer to next block	|	Record1	|	Record...	|
	|		(int)			|			(int)			|			|				|
	|						|							|			|				|
	---------------------------------------------------------------------------------

	________________________________________________________________________________
	|			|			|		|					|							|
	|			|			|		|					|							|
	|	Rec[0]	| 	Rec[1] 	|	...	| recordsInBlock	|	Pointer to next block	|
	|	(Rec)	|			|		|		(int)		|			(int)			|
	|			|			|		|					|							|
	L_______________________________________________________________________________|
	^								^					^							^
	|								|					|							|
	data				data + BS - 2 * sizeof(int)		data+BS-sizeof(int) 	data + BLOCK_SIZE 
	(after Block_GetData)
*/
	int fileDescriptor;
	int error;
	char* data;
	int recordCounter;
	int blockCounter;
	BF_Block* block; BF_Block_Init(&block);

	fileDescriptor = hp_info->fileDesc;

	error = TC(BF_GetBlockCounter(fileDescriptor, &blockCounter));

	// Only the header metadata block is present
	if (blockCounter == 1) {
		// printf("Inserting record, no blocks for records yet\n");
		int blockCounter;
		int nextBlock;

		error = TC( BF_AllocateBlock(fileDescriptor, block) );
		error = TC( BF_GetBlockCounter(fileDescriptor, &blockCounter));


		// Read metadata to update the pointer to the next block
		error = TC(BF_GetBlock(fileDescriptor, hp_info->headerPosition, block));
		data = BF_Block_GetData(block);
		HP_info* new_info = (HP_info*) data;
		// new_info.lastBlock = blockCounter - 1;
		new_info->lastBlock = blockCounter - 1;
		// printf("Now the header points to the last block which is: %d\n", new_info->lastBlock);
		// memcpy(data, &new_info, sizeof(HP_info));

		data += sizeof(HP_info);
		nextBlock = blockCounter - 1;
		// Header block now points to new allocated block
		memcpy(data, &nextBlock, sizeof(int));
		// printf("Header now points to block: %d\n", nextBlock);

		assert(nextBlock == new_info->lastBlock);

		BF_Block_SetDirty(block);
		BF_UnpinBlock(block);

		// Now get the new allocated block, saved at blockCounter - 1 position
		error = TC(BF_GetBlock(fileDescriptor, blockCounter-1, block));
		data = BF_Block_GetData(block);
		// printf("Data is sitting at: %p for segment %d\n", data, blockCounter-1);

		int recordsInBlock = 1;
		int newBlock = -1;

		// printf("Getting data pointer for block: %d\n", blockCounter-1);

		// Copy the records inside
		memcpy(data, &record, sizeof(Record) );
		Record recInserted =  *( (Record*) data);

		// Go to the end minus 2 ints, to place how many records the block stores
		data += sizeof(char) * BF_BLOCK_SIZE - (2*sizeof(int));
		memcpy(data, &recordsInBlock, sizeof(int));
		int recs = *((int*) data);
		// printf("Recs In Block saved at: %p\n", data);

		assert(recs == 1);

		// Go to the end minus 1 int (so 1 int forward from before)
		// To place pointer to new block

		data += sizeof(int);
		memcpy(data, &newBlock, sizeof(int));
		// printf("NextBlockPointer saved at: %p\n", data);

		int nextBlockPointer = *((int*) data);
		assert(nextBlockPointer == -1);

		assert (nextBlockPointer == newBlock);
		assert (recordsInBlock == recs);
		assert (recInserted.id == record.id);
		
		printf("Successfuly inserted: \n");
		printf("%d \t\t %s \t %s \t %s \n", record.id, record.name, record.surname, record.city);
		// printf("Now the first block for records has saved: %d\n", recordsInBlock);
		
		// HP_GetAllEntries(hp_info, record.id);

		BF_Block_SetDirty(block);
		BF_UnpinBlock(block);
		return nextBlock;

	} else {
		// There exist more block records, not
		// only the header
		// printf("Now we don't only have the header block inside\n");
		// printf("We got: %d blocks inside the file\n", blockCounter);
		
		BF_Block* lBlock; BF_Block_Init(&lBlock);
		// Open the header block, read the HP_info for the position of the last Block and Unpin
		error = TC(BF_GetBlock(fileDescriptor, hp_info->headerPosition, block));
		data = BF_Block_GetData(block);
		HP_info* oldInfo = (HP_info*) data;
		int lastBlock = oldInfo->lastBlock;
		
		// printf("Got that last block is: %d\n", lastBlock);
		error = TC(BF_UnpinBlock(block));

		// Go to the last position
		error = TC(BF_GetBlock(fileDescriptor, lastBlock, lBlock));
		data = BF_Block_GetData(lBlock);
		char* dataInit = data;	// save initial pointer

		data += sizeof(char) * BF_BLOCK_SIZE - (1 * sizeof(int));
		int nextBlock = *((int*) data);
		// printf("It points to: %d\n", nextBlock);

		data -= sizeof(int);
		int recordsInsideBlock = *((int*) data );

		data = sizeof(Record) * recordsInsideBlock + dataInit;
		// printf("Last block has: %d saved records\n", recordsInsideBlock);
		// If final block reached, break

		if (recordsInsideBlock < hp_info->recordsPerBlock) {
			// We have less records inside the block
			// than a block can take, then we can attach it to
			// the current block
			// printf("Found empty spot\n");
			memcpy(data, &record, sizeof(Record));
			data = dataInit + sizeof(char) * BF_BLOCK_SIZE - (2* sizeof(int));
			int newRecordsInBlock = recordsInsideBlock + 1;
			memcpy(data, &newRecordsInBlock, sizeof(int));
			// printf("Block now has: %d records\n", newRecordsInBlock);
			
			int pointsTo;
			data = dataInit + sizeof(char) * BF_BLOCK_SIZE - (1 * sizeof(int));
			pointsTo = * ((int*) data);
			assert (pointsTo == -1);

			printf("Successfuly inserted: \n");
			printf("%d \t\t %s \t %s \t %s \n", record.id, record.name, record.surname, record.city);

			BF_Block_SetDirty(lBlock);
			BF_UnpinBlock(lBlock);
			BF_Block_Destroy(&lBlock);
			BF_Block_Destroy(&block);
			return lastBlock;
		} else {
			// We have to allocate a new block
			printf("\n\n NOW WE WILL HAVE TO ALLOCATE NEW BLOCK\n");

			BF_Block* oldLast; BF_Block_Init(&oldLast);
			
			// Allocate new block
			BF_Block* allocatedBlock; BF_Block_Init(&allocatedBlock);

			// Allocation, and copy data into the newly allocated block

			error = TC(BF_AllocateBlock(fileDescriptor, allocatedBlock));
			data = BF_Block_GetData(allocatedBlock);
			
			// Copy record
			memcpy(data, &record, sizeof(Record));

			int recordsInBlock = 1;
			int newBlock = -1;

			printf("Successfuly inserted: \n");
			printf("%d \t\t %s \t %s \t %s \n", record.id, record.name, record.surname, record.city);

			// And now copy nextBlock pointer and recordsInBlcok
			data += sizeof(char) * BF_BLOCK_SIZE - (2*sizeof(int));
			memcpy(data, &recordsInBlock, sizeof(int));
			int recs = *((int*) data);
		
			// Go to the end minus 1 int (so 1 int forward from before)
			// To place pointer to new block
			data += sizeof(int);
			memcpy(data, &newBlock, sizeof(int));
			int nextBlockPointer = *((int*) data);
			
			assert(nextBlockPointer == -1);
			assert(nextBlockPointer == newBlock);
			assert(recs == 1);
			assert(recs == recordsInBlock);
			

			BF_Block_SetDirty(allocatedBlock);
			BF_UnpinBlock(allocatedBlock);
			BF_Block_Destroy(&allocatedBlock);
			// End of copying into newly allocated block


			// Now time to change 2 blocks:
			// 1) Header file's last block
			// 2) LastBlock's new next block pointer (now it's no longer the last one, so it has a next)

			int blockCounter;
			error = TC(BF_GetBlock(fileDescriptor, hp_info->headerPosition, block));
			data = BF_Block_GetData(block);
			HP_info* oldInfo = (HP_info*) data;
			error = TC(BF_GetBlockCounter(fileDescriptor, &blockCounter));

			// Save which is the last block, before we change it
			int oldLastBlock = oldInfo->lastBlock;
			
			// Update the header's last block to the (counter - 1), a.k.a the newly allocated block
			oldInfo->lastBlock = blockCounter - 1;
			// printf("Successfully changed new last block to: %d\n", oldInfo->lastBlock);
			
			// Now read the old last block, to change its
			// next block pointer to the newly allocated one
			error = TC(BF_GetBlock(fileDescriptor, oldLastBlock, oldLast));
			data = BF_Block_GetData(oldLast);
			
			data += sizeof(char) * BF_BLOCK_SIZE - sizeof(int);
			// Its nextBlockCounter should have been -1, it hasn't changed yet
			// memcpy(data, &(oldInfo->lastBlock) , sizeof(int));
			int* lastBlockNext = (int*) data;
			assert (  (*((int*) data)) == -1 );
			*(lastBlockNext) = oldInfo->lastBlock;

			assert( *(lastBlockNext) == blockCounter - 1);

			// printf("Now the previous last block has a new next of: %d\n", oldInfo->lastBlock);
			
			BF_Block_SetDirty(oldLast);
			BF_UnpinBlock(oldLast);
			BF_Block_Destroy(&oldLast);

			BF_Block_SetDirty(block);
			error = TC(BF_UnpinBlock(block));
			BF_Block_Destroy(&block);
			
			// End of changing header and last block's pointer

			return lastBlock;
		}
	}
}

int HP_GetAllEntries(HP_info* hp_info, int value){
	int fileDescriptor;
	int error;
	char* data;
	BF_Block* block;
	BF_Block_Init(&block);


	fileDescriptor = hp_info->fileDesc;

	error = TC(BF_GetBlock(fileDescriptor, hp_info->headerPosition, block));
	data = BF_Block_GetData(block);

	HP_info info = *((HP_info*) data);

	data += sizeof(HP_info);
	int nextBlock = *((int*) data);

	printf("\nHEADER NEXT: %d\n", nextBlock);
	BF_UnpinBlock(block);

	printf("\n%s \t\t %s \t %s \t %s\n", "ID", "NAME" , "SURNAME", "CITY");

	if (nextBlock == -1) return -1;

	int blocksRead = 1;
	bool found = false;

	while(nextBlock != -1) {
		
		printf("Current block: %d\n", nextBlock);
		error = TC(BF_GetBlock(fileDescriptor, nextBlock, block));
		data = BF_Block_GetData(block);

		char* dataInit = data;

		data += sizeof(char) * BF_BLOCK_SIZE - (2*sizeof(int));
		
		int recordsInBlock = *( (int*) data);
		data += sizeof(int);
		nextBlock = *( (int*) data);
		printf("Next block is: %d\n", nextBlock);
		data = dataInit;

		for (int i = 0; i < recordsInBlock; i++) {
			Record recInside = *((Record*) data);
			
			printf("Currently in: <%d,%s,%s,%s> \n", recInside.id, recInside.name, recInside.surname, recInside.city);

			if (recInside.id == value) {
				found = true;
				printf("FOUND!!!\n");
				printf("%d \t\t %s \t %s \t %s \n", recInside.id, recInside.name, recInside.surname, recInside.city);
				break;
			}
			data += sizeof(Record);
		}

		BF_UnpinBlock(block);
		if (found) break;
		blocksRead++;
	}


	BF_Block_Destroy(&block);
	return (found) ? blocksRead : -1;
}

int TC(BF_ErrorCode error) {
    if (error != BF_OK) {
        BF_PrintError(error);
        return (-1);
    } else {
        return 0;
    }
}
