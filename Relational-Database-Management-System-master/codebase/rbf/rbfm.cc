#include "rbfm.h"
#include "pfm.h"
#include <cstring>
#include <iostream>
#include <cstdlib>
#include <cmath>
#include <cstdio>
#include <algorithm>
using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
	// pfm instance created for methods Create/destroy/open/close
	pfmInstance = PagedFileManager::instance();

}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	if (pfmInstance->createFile(fileName) == 0) {
		return 0;
	}
	return -1;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	if (pfmInstance->destroyFile(fileName) == 0) {
		return 0;
	}
	return -1;
}

RC RecordBasedFileManager::openFile(const string &fileName,
		FileHandle &fileHandle) {
	if (pfmInstance->openFile(fileName, fileHandle) == 0) {
		return 0;
	}

	return -1;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	if (pfmInstance->closeFile(fileHandle) == 0) {
		//cout<< pfmInstance->closeFile(fileHandle)
		return 0;
	}

	return -1;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	//cacluate size of record
	short int fieldsNum = (short int) recordDescriptor.size(); //gives the number of fields in the record
	void *record = malloc(PAGE_SIZE); //points to the record data
memset(record,0,PAGE_SIZE);
	short int offset = ceil((double) fieldsNum / CHAR_BIT);

	short int recordSize = 0;
//record format - actual data starts after few bytes (having the offsets to the fields for O(1) access)
	short int offsetInRecord = 0;

	memcpy((char *) record + offsetInRecord, &fieldsNum, sizeof(short int));
	offsetInRecord += (fieldsNum + 1) * sizeof(short int); //update offset

	for (short int i = 0; i < fieldsNum; i++) {
		//find if the field is Null using the null indicators bytes in the data

		if (((char*) data)[i / 8] & (1 << (7 - (i % 8)))) {
			short int off = -1; //having offset -1 for field with null data
			memcpy((char*) record + ((i + 1) * sizeof(short int)), &off,
					sizeof(short int));
			continue;
		}
		//check the variable type from record descriptor
		switch (recordDescriptor[i].type) {
		case TypeInt: {
			int num = 0;
			memcpy(&num, (char*) data + offset, recordDescriptor[i].length);
			memcpy((char*) record + offsetInRecord, &num, sizeof(int));
			offset += recordDescriptor[i].length;
			offsetInRecord += sizeof(int);
			//update the corresponding  offset that points to the end of the field
			short int endOffsetField = offsetInRecord - 1;
			memcpy((char *) record + i * sizeof(short int) + sizeof(short int),
					&endOffsetField, sizeof(short int));

			//update the offset value
		}
			break;

		case TypeVarChar: {
			int variableLength = 0; // to calculat the variable elngth of the field

			memcpy(&variableLength, (char*) data + offset, sizeof(int)); //first find the length of the string which is stored in the data
			offset += sizeof(int);
			memcpy((char *) record + offsetInRecord, (char*) data + offset,
					variableLength); //copy the actual string into the record
			offsetInRecord += variableLength;
			offset += variableLength;
			//cout << variableLength << endl;

			//update the corresponding  offset that points to the end of the field
			short int endOffsetField = offsetInRecord - 1;
			memcpy((char *) record + i * sizeof(short int) + sizeof(short int),
					&endOffsetField, sizeof(short int));

		}
			break;
		case TypeReal: {
			float num1 = 0;
			memcpy(&num1, (char *) data + offset, recordDescriptor[i].length);
			memcpy((char *) record + offsetInRecord, &num1, sizeof(float));
			offset += recordDescriptor[i].length;
			offsetInRecord = offsetInRecord + sizeof(float); //update the offset value
			//cout << num1 << endl;

			//update the corresponding  offset that points to the end of the field
			short int endOffsetField = offsetInRecord - 1;
			memcpy((char *) record + i * sizeof(short int) + sizeof(short int),
					&endOffsetField, sizeof(short int));
		}
			break;
		default:
			break;
		}

	}
	if (offsetInRecord < 10) { //since tomb_stone size is 10 bytes, we need to have record space atleast 10 bytes to store tomb_stones in case the record gets updated
		offsetInRecord = 10;
	}

	recordSize = offsetInRecord;

	int pgNum = fileHandle.getNumberOfPages() - 1;

	//Inserting record into the page
	bool writeStatus = false;
	//If the file is emplty, append a new page and insert the record

	if (pgNum == -1) {
		void *pgData = malloc(PAGE_SIZE);
memset(pgData,0,PAGE_SIZE);
		memcpy((char*) pgData, (char*) record, recordSize); //copy record data to page data
		//update freespace pointer in the last 2B
		memcpy((char*) pgData + PAGE_SIZE - sizeof(short int), &recordSize,
				sizeof(short int));
		//update no. of records in the page in second last 2B
		short int rec = 1; //only one record at this point of time
		memcpy((char*) pgData + PAGE_SIZE - (2 * sizeof(short int)), &rec,
				sizeof(short int));
		//update the record offset in the directory slot
		short int pointBeg = 0; //first record ; hence points to beginning
		memcpy((char*) pgData + PAGE_SIZE - (3 * sizeof(short int)), &pointBeg,
				sizeof(short int));
		if (fileHandle.appendPage(pgData) == -1) {
			free(record);
			return -1;
		}

		writeStatus = true;
		free(record);
		rid.pageNum = 0; // page number and slot number indexes start from 0
		rid.slotNum = 0;
free(pgData);
		return 0;
	}
	//bool writeStatus = false;
	short int freeSpaceSize = 0;
	short int offsetOfFreeSpace = 0;
	short int freeSpaceEnding = 0;
	short int numRecsPerPage = 0;
	// check if last page has enough space t add record
	getAvailableFreeSpace(fileHandle, pgNum, &freeSpaceSize, &offsetOfFreeSpace,
			&freeSpaceEnding, &numRecsPerPage); //calling method to find free space in a page

	if (freeSpaceSize > recordSize + (short int) sizeof(short int)) { ///last page has free space available to fit in the record
		void* pgData = malloc(PAGE_SIZE);
memset(pgData,0,PAGE_SIZE);
		int numOfPages = fileHandle.getNumberOfPages();
		fileHandle.readPage(numOfPages - 1, pgData);
		memcpy((char*) pgData + offsetOfFreeSpace, (char*) record, recordSize); // writing data
		//add the slot entry
		memcpy((char*) pgData + freeSpaceEnding - sizeof(short int) + 1,
				&offsetOfFreeSpace, sizeof(short int));
		offsetOfFreeSpace += recordSize;
		// update the number of records
		numRecsPerPage += 1;
		memcpy((char*) pgData + PAGE_SIZE - (2 * sizeof(short int)),
				&numRecsPerPage, sizeof(short int));
		//update free space pointer
		memcpy((char*) pgData + PAGE_SIZE - sizeof(short int),
				&offsetOfFreeSpace, sizeof(short int));
		rid.pageNum = numOfPages - 1;
		rid.slotNum = numRecsPerPage - 1;
		if ((fileHandle.writePage(numOfPages - 1, pgData)) == -1) {
			free(record);
			return -1;
		}
		free(pgData);

		writeStatus = true;
		free(record);
		return 0;
	}
	for (short int i = 0;
			writeStatus == false
					&& i < (short int) fileHandle.getNumberOfPages(); i++) {
		getAvailableFreeSpace(fileHandle, i, &freeSpaceSize, &offsetOfFreeSpace,
				&freeSpaceEnding, &numRecsPerPage);
		if (freeSpaceSize > recordSize + (short int) sizeof(short int)) { ///last page has free space available to fit in the record
			void* pgData = malloc(PAGE_SIZE);
memset(pgData,0,PAGE_SIZE);
			fileHandle.readPage(i, pgData);
			memcpy((char*) pgData + offsetOfFreeSpace, (char*) record,
					recordSize); // writing data
			//add the slot entry
			memcpy((char*) pgData + freeSpaceEnding - sizeof(short int) + 1,
					&offsetOfFreeSpace, sizeof(short int));
			offsetOfFreeSpace += recordSize;
			// update the number of records
			numRecsPerPage += 1;
			memcpy((char*) pgData + PAGE_SIZE - (2 * sizeof(short int)),
					&numRecsPerPage, sizeof(short int));
			//update free space pointer
			memcpy((char*) pgData + PAGE_SIZE - sizeof(short int),
					&offsetOfFreeSpace, sizeof(short int));
			rid.pageNum = i;
			rid.slotNum = numRecsPerPage - 1;
			if ((fileHandle.writePage(i, pgData)) == -1) {
				free(record);
				return -1;
			}
			free(pgData);

			writeStatus = true;
			free(record);
			return 0;
		}
	}

//if there is no space in any of the pages, append a new page and write data
	if (writeStatus == false) {
		void* pgData = malloc(PAGE_SIZE);
memset(pgData,0,PAGE_SIZE);
		memcpy((char*) pgData, (char*) record, recordSize); //copy record data to page data

		//update freespace pointer in the last 2B
		memcpy((char*) pgData + PAGE_SIZE - sizeof(short int), &recordSize,
				sizeof(short int));
		//update no. of records in the page in second last 2B
		short int rec = 1; //only one record at this point of time
		memcpy((char*) pgData + PAGE_SIZE - (2 * sizeof(short int)), &rec,
				sizeof(short int));
		//update the record offset in the directory slot
		short int pointBeg = 0; //first record ; hence points to beginning
		memcpy((char*) pgData + PAGE_SIZE - (3 * sizeof(short int)), &pointBeg,
				sizeof(short int));
		if (fileHandle.appendPage(pgData) == -1) {
			free(record);
			return -1;
		}
free(pgData);
		writeStatus = true;
		free(record);

		rid.pageNum = fileHandle.getNumberOfPages() - 1;
		// page number and slot number indexes start from 0
		rid.slotNum = 0;
		return 0;
	}
	if (writeStatus == false) {
		free(record);
		return -1;
	}
	free(record);
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	RID OriginalRID = getOriginalRID(fileHandle, recordDescriptor, rid);
	// first read the respective record
	int pageNum = OriginalRID.pageNum;
	int slotNum = OriginalRID.slotNum;
	if (slotNum == DEL) { //record is not present
		return -1;
	}
	void *pgData = malloc(PAGE_SIZE);

	fileHandle.readPage(pageNum, pgData);
	short int numRecsPerPage = 0;
	memcpy(&numRecsPerPage,
			(char*) pgData + PAGE_SIZE - (2 * sizeof(short int)),
			sizeof(short int));
	short int offsetBegin = 0;
	short int offsetEnd = 0;
	memcpy(&offsetBegin,
			(char*) pgData + PAGE_SIZE - (2 * sizeof(short int))
					- ((slotNum + 1) * sizeof(short int)), sizeof(short int));
	if (numRecsPerPage - 1 == slotNum) {
		memcpy(&offsetEnd, (char*) pgData + PAGE_SIZE - (sizeof(short int)),
				sizeof(short int)); //if it is the last record , then the freespace pointer points to record end
	} else {
		//else get the end from the directory
		/*memcpy(&offsetEnd,
		 (char*) pgData + PAGE_SIZE - (2 * sizeof(short int))
		 - ((slotNum + 1 + i) * sizeof(short int)),
		 sizeof(short int));*/
		for (int i = 1; i <= numRecsPerPage - slotNum - 1; i++) { //pass deleted records
			memcpy(&offsetEnd,
					(char*) pgData + PAGE_SIZE - (2 * sizeof(short int))
							- ((slotNum + 1 + i) * sizeof(short int)),
					sizeof(short int));
			if (offsetEnd >= 0)
				break;
		}
		if (offsetEnd < 0) {
			memcpy(&offsetEnd, (char*) pgData + PAGE_SIZE - sizeof(short int),
					sizeof(short int));

		}
	}
	//cout << offsetEnd<<endl;
	short int recLength = offsetEnd - offsetBegin;
	void *record = malloc(recLength);
memset(record,0,recLength);
	memcpy((char*) record, (char*) pgData + offsetBegin, recLength);
	// convert the record read <no.of fields><offset to the fields><actualdata> into the format <null bits><actual data>
	short int numberOfFields = recordDescriptor.size();
	short int start = (numberOfFields + 1) * sizeof(short int);
	int nullBitSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
	short int actualDataSize = nullBitSize;
	unsigned char* nullBitBytes = (unsigned char*) malloc(nullBitSize);
	memset(nullBitBytes, 0, nullBitSize);
	//creation of null bits vetcor
	for (short int i = 0; i < numberOfFields; i++) {
		short int offset = 0;
		memcpy(&offset, (char*) record + ((i + 1) * sizeof(short int)),
				sizeof(short int));
		if (offset != -1) { //check if offset -1; which means field has null value
			short int attributeLen = offset - start + 1;
			actualDataSize += attributeLen;
			start = offset + 1;
		} else {
			nullBitBytes[i / 8] = nullBitBytes[i / 8] | (1 << (7 - (i % 8)));
		}

	}
	//parse the record

	memcpy((char*) data, nullBitBytes, nullBitSize);
	short int dataOffset = nullBitSize;
	//now copy the attribute values
	start = (numberOfFields + 1) * sizeof(short int);
	for (short int i = 0; i < numberOfFields; i++) {
		short int offset = 0;
		memcpy(&offset, (char*) record + ((i + 1) * sizeof(short int)),
				sizeof(short int));
		if (offset != -1) {

			switch (recordDescriptor[i].type) {
			case TypeVarChar: {
				int attributeLen = offset - start + 1; //in data it has to be length of varchar followed by the varchar value
				
				memcpy((char*) data + dataOffset, &attributeLen, sizeof(int));
				dataOffset += sizeof(int);
				
				memcpy((char*) data + dataOffset, (char*) record + start,
						attributeLen);
				dataOffset += attributeLen;
				start += attributeLen;
			}
				break;
			case TypeInt:
			case TypeReal: {
				
				memcpy((char*) data + dataOffset, (char*) record + start,
						recordDescriptor[i].length);
				start += recordDescriptor[i].length;
				dataOffset += recordDescriptor[i].length;
			}
				break;
			default:
				break;
			}
		}
	}
	free(nullBitBytes);
	free(record);
	free(pgData);
	return 0;
}

RC RecordBasedFileManager::printRecord(
		const vector<Attribute> &recordDescriptor, const void *data) {

	int nullBitSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
	int numberOfFields = recordDescriptor.size();
	int offset = nullBitSize;
	for (int i = 0; i < numberOfFields; i++) {
		//check if the field is null
		if (((char*) data)[i / 8] & (1 << (7 - (i % 8)))) {
			cout << recordDescriptor[i].name << ": NULL" << endl;
			continue;
		}
		switch (recordDescriptor[i].type) {

		case TypeVarChar: {
			int length; // first find the length
			// second is the varchar value itself
			memcpy(&length, (char*) data + offset, sizeof(int));
			offset += sizeof(int);
			char *str = new char[length + 1]();
			memcpy(str, (char*) data + offset, length);
			offset += length;
			cout << recordDescriptor[i].name << ": " << str;
			delete[] str;
		}
			break;

		case TypeInt: {
			int num;
			memcpy(&num, (char*) data + offset, recordDescriptor[i].length);
			offset = offset + recordDescriptor[i].length;
			cout << recordDescriptor[i].name << ": " << num;
		}
			break;
		case TypeReal: {
			float num1;
			memcpy(&num1, (char*) data + offset, recordDescriptor[i].length);
			offset = offset + recordDescriptor[i].length;
			cout << recordDescriptor[i].name << ": " << num1;
		}
			break;
		default:
			break;
		}
		cout << "\t";

	}
	cout << endl;
	return 0;

}


RC RecordBasedFileManager::getAvailableFreeSpace(FileHandle &fileHandle,
		int pageNum, short int *freeSpaceSize, short int *offsetOfFreeSpace,
		short int *freeSpaceEnding, short int *numRecsPerPage) {

	void *pgData = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, pgData);
	//check if there is enough free space to fit in the record
	short int offBeg = 0;
	short int numberOfSlots = 0;
	memcpy(&offBeg, (char*) pgData + PAGE_SIZE - sizeof(short int),
			sizeof(short int));
	*offsetOfFreeSpace = offBeg;
	memcpy(&numberOfSlots, (char*) pgData + PAGE_SIZE - (2 * sizeof(short int)),
			sizeof(short int));
	*numRecsPerPage = numberOfSlots;
	*freeSpaceEnding = PAGE_SIZE - (2 * sizeof(short int))
			- (numberOfSlots * sizeof(short int)) - 1;
	*freeSpaceSize = (*freeSpaceEnding) - offBeg + 1;
	free(pgData);
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {

	RID newRID = getOriginalRID(fileHandle, recordDescriptor, rid);
	if (newRID.slotNum == DEL) { //trying to delete an existing record
		return -1;
	}
	//now delete the data
	RC retval = deleteRecUtility(fileHandle, recordDescriptor, newRID);
	if (retval != 0)
		return -1;
	if (newRID.pageNum != rid.pageNum && newRID.slotNum != rid.slotNum) { // then delete the first tomb_stone
		retval = deleteRecUtility(fileHandle, recordDescriptor, rid);
		if (retval != 0)
			return -1;
	}
	return 0;
}

RC RecordBasedFileManager::deleteRecUtility(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {
	

	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;
	
	void *pgData = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, pgData);
	short int numRecsPerPage = 0;
	memcpy(&numRecsPerPage,
			(char*) pgData + PAGE_SIZE - (2 * sizeof(short int)),
			sizeof(short int));
	short int startRec, endRec;
	memcpy(&startRec,
			(char*) pgData + PAGE_SIZE - (2 * sizeof(short int))
					- ((slotNum + 1) * sizeof(short int)), sizeof(short int));
	if (numRecsPerPage - 1 == slotNum) { //last record in the page
		memcpy(&endRec, (char*) pgData + PAGE_SIZE - sizeof(short int),
				sizeof(short int));
	} else { //endRec points to the start of the next record
		for (int count = 1; count <= numRecsPerPage - slotNum - 1;
				count++) {
			memcpy(&endRec,
					(char*) pgData + PAGE_SIZE - (2 * sizeof(short int))
							- ((slotNum + 1 + count) * sizeof(short int)),
					sizeof(short int));
			if (endRec >= 0) {
				break;
			}
		}
		if (endRec < 0) {
			memcpy(&endRec,
					(char*) pgData + PAGE_SIZE - sizeof(short int),
					sizeof(short int));
		}
	}
	short int lengthOfRecord = endRec - startRec;
	
	int startCopy = endRec;
	int startOverwrite = startRec;
	short int freeSpaceStart;
	memcpy(&freeSpaceStart, (char*) pgData + PAGE_SIZE - sizeof(short int),
			sizeof(short int));
	short int newfreeSpaceStart = freeSpaceStart - lengthOfRecord;
	short int spaceToShift = freeSpaceStart - startCopy;
	
	memmove((char*) pgData + startOverwrite, (char*) pgData + startCopy,
			spaceToShift);
	
	memcpy((char*) pgData + PAGE_SIZE - sizeof(short int),
			&newfreeSpaceStart, sizeof(short int));
	
	short int minus = -1;
	memcpy(
			(char*) pgData + PAGE_SIZE - (2 * sizeof(short int))
					- ((slotNum + 1) * sizeof(short int)), &minus,
			sizeof(short int));
	
	for (short int count = 1; count <= numRecsPerPage - slotNum - 1;
			count++) {
		short int currentOffset;
		//get the existing offset
		memcpy(&currentOffset,
				(char*) pgData + PAGE_SIZE - (2 * sizeof(short int))
						- ((slotNum + 1 + count) * sizeof(short int)),
				sizeof(short int));
		if (currentOffset != -1) { //for a record that was already deleted we shouldn't modify the offset
			currentOffset -= lengthOfRecord;
			//decrement by the appropriate length and restore
			memcpy(
					(char*) pgData + PAGE_SIZE - (2 * sizeof(short int))
							- ((slotNum + 1 + count) * sizeof(short int)),
					&currentOffset, sizeof(short int));
		}
	}
	fileHandle.writePage(pageNum, pgData);
	free(pgData);
	return 0;
}

RID RecordBasedFileManager::getOriginalRID(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {

	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;
	void *pgData = malloc(PAGE_SIZE);
memset(pgData,0,PAGE_SIZE);
	fileHandle.readPage(pageNum, pgData);
	//first check if the record is present or its tomb stone
	RID delRID;
	delRID.pageNum = -1; //pageNum -1 indicates record is deleted
	delRID.slotNum = DEL; //this slot number is not possible because all records are < 4K size(defined this as 10000)
	short int startRec;
	memcpy(&startRec,
			(char*) pgData + PAGE_SIZE - (2 * sizeof(short int))
					- ((slotNum + 1) * sizeof(short int)), sizeof(short int));
	if (startRec < 0) { //trying to read a deleted record
		free(pgData);
		return delRID;
	}
	short int numOfAttr;
	memcpy(&numOfAttr, (char*) pgData + startRec, sizeof(short int));
	if (numOfAttr == -1) { 
		RID newRID;
		int newPageNum, newSlotNum;
		memcpy(&newPageNum,
				(char*) pgData + startRec + sizeof(short int),
				sizeof(int));
		memcpy(&newSlotNum,
				(char*) pgData + startRec + sizeof(short int)
						+ sizeof(int), sizeof(int));
		newRID.pageNum = newPageNum;
		newRID.slotNum = newSlotNum;
		RID retRID = getOriginalRID(fileHandle, recordDescriptor, newRID);
		if (retRID.slotNum == DEL) {
			free(pgData);
			return delRID;
		} else {
			free(pgData);
			return retRID;
		}
	} else {
		free(pgData);
		return rid;
	}
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid,
		const string &attributeName, void *data) {
	void *pgData=malloc(PAGE_SIZE);
		readRecord(fileHandle,recordDescriptor,rid,pgData); 
		int numOfAttr=recordDescriptor.size();
		char nullBytes=0;
		int recordOffset=ceil((double) recordDescriptor.size()/CHAR_BIT);

		for(int i=0;i<numOfAttr;i++){
			if(recordDescriptor[i].name.compare(attributeName)==0){ 
				if(( (char*) pgData )[i/8] & ( 1<< (7-(i%8) ) )){ //the field is null
					nullBytes = nullBytes | (1<<7);
				}
				else{
					if(recordDescriptor[i].type==TypeInt){
						int d=0;
						memcpy(&d,(char*)pgData+recordOffset,recordDescriptor[i].length);
						memcpy((char*)data+1,&d,recordDescriptor[i].length); // +1 for the null bit vector which is of size 1 always
					}
					else if(recordDescriptor[i].type==TypeReal){
						float f = 0;
						memcpy(&f,(char*)pgData+recordOffset,recordDescriptor[i].length);
						memcpy((char *)data+1,&f,recordDescriptor[i].length);
					}
					else if(recordDescriptor[i].type==TypeVarChar){
						int length=0;
						memcpy(&length,(char *)pgData+recordOffset,sizeof(int));
						recordOffset+=sizeof(int);
						memcpy((char*)data+1,&length,sizeof(int));
						memcpy((char*)data+1+sizeof(int),(char*)pgData+recordOffset,length);// +1 for null bit vector, sizeofint for the length of the string

					}
				}
				break;
			}
			else{ //just move the offset to point to next attribute's data
				if(( (char*) pgData )[i/8] & ( 1<< (7-(i%8) ) )){
					continue;
				}
				else if(recordDescriptor[i].type==TypeInt)
					{
					recordOffset+=recordDescriptor[i].length;
				}
				else if (recordDescriptor[i].type==TypeReal){
					recordOffset+=recordDescriptor[i].length;
				}
				else if(recordDescriptor[i].type==TypeVarChar){
					int length=0;
					memcpy(&length,(char *)pgData+recordOffset,sizeof(int));
					recordOffset+=(length+sizeof(int));
				}
			}
		}
		memcpy((char*)data,&nullBytes,sizeof(char));
		free(pgData);
		return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){

	
	RID OriginalRID=getOriginalRID(fileHandle,recordDescriptor,rid);
	int pageNum=OriginalRID.pageNum;
	int slotNum=OriginalRID.slotNum;
	if(slotNum==DEL){//trying to update a deleted record
		return -1;
	}
	void *pgData=malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum,pgData);

	short int currRecSize,startRec,endRec;
	
	memcpy(&startRec,(char*)pgData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1)*sizeof(short int)),sizeof(short int));
	short int numRecsPerPage;
	memcpy(&numRecsPerPage,(char*)pgData+PAGE_SIZE-(2*sizeof(short int)),sizeof(short int));
	if(numRecsPerPage-1==slotNum){
		memcpy(&endRec,(char*)pgData+PAGE_SIZE-sizeof(short int),sizeof(short int));
	}
	else{
		memcpy(&endRec,(char*)pgData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+2)*sizeof(short int)),sizeof(short int));
	}
	currRecSize=endRec-startRec;
	
	short int numberOfFields=(short int)recordDescriptor.size();
	
	short int recordSize=0;
	short int offset=ceil((double) numberOfFields/CHAR_BIT);
	void *record=malloc(PAGE_SIZE);
memset(record,0,PAGE_SIZE);
	short int offsetOfRec=0;
	
	memcpy((char *)record+offsetOfRec,&numberOfFields,sizeof(short int));
	offsetOfRec+=(numberOfFields+1)*sizeof(short int);
	offset=ceil((double) numberOfFields/CHAR_BIT);
	for(short int i=0;i<numberOfFields;i++){
		if(((char*)data)[i/8] & (1<<(7-(i%8)))){ //this field is NULL
			
			short int length=-1;
			memcpy((char*)record+((i+1)*sizeof(short int)),&length,sizeof(short int));
			continue;
		}
		if(recordDescriptor[i].type==TypeVarChar){
			int length=0;
			memcpy(&length,(char *) data+offset,sizeof(int)); 
			offset+=sizeof(int);
			// store the varchar data into record
			memcpy((char *)record+offsetOfRec,(char*)data+offset,length);
			offsetOfRec+=length;
			offset+=length;
			
			short int offsetOfFieldEnd=offsetOfRec-1;
			memcpy((char *)record+i*sizeof(short int)+sizeof(short int),&offsetOfFieldEnd,sizeof(short int));
		}
		else if(recordDescriptor[i].type==TypeInt){
			// store the integer into record
			int num=0;
			memcpy(&num,(char *)data+offset,recordDescriptor[i].length);
			memcpy((char *)record+offsetOfRec,&num,sizeof(int));
			offset+=recordDescriptor[i].length;
			offsetOfRec+=sizeof(int);
			//update the corresponding directory with offset that points to the end of the record
			short int offsetOfFieldEnd=offsetOfRec-1;
			memcpy((char *)record+i*sizeof(short int)+sizeof(short int),&offsetOfFieldEnd,sizeof(short int));
		}
		else if(recordDescriptor[i].type==TypeReal){
			//store the float value into record
			float fl=0;
			memcpy(&fl,(char*)data+offset,recordDescriptor[i].length);
			memcpy((char *)record+offsetOfRec,&fl,sizeof(float));
			offset+=recordDescriptor[i].length;
			offsetOfRec+=sizeof(float);
			
			short int offsetOfFieldEnd=offsetOfRec-1;
			memcpy((char *)record+i*sizeof(short int)+sizeof(short int),&offsetOfFieldEnd,sizeof(short int));
		}
	}
	if(offsetOfRec<10){ //since we keep tomb_stone to be 10 bytes, if the offset is less than 10 bytes, atleast keep 10 bytes storage and then copy data
		offsetOfRec=10;
	}
	recordSize=offsetOfRec;

	if(recordSize < currRecSize){
		memcpy((char*)pgData+startRec,(char*)record,recordSize);

		short int freeSpaceStart;
		memcpy(&freeSpaceStart,(char*)pgData+PAGE_SIZE-sizeof(short int),sizeof(short int));
		short int spaceToShift=freeSpaceStart-endRec;
		memmove((char*)pgData+startRec+recordSize,(char*)pgData+endRec,spaceToShift);
		//update the free space pointer
		short int extraMem=currRecSize-recordSize;
		short int newfreeSpaceStart=freeSpaceStart-extraMem;
		memcpy((char*)pgData+PAGE_SIZE-sizeof(short int),&newfreeSpaceStart,sizeof(short int));
		
		for(int count=1;count<=numRecsPerPage-slotNum-1;count++){
			short int currentOffset;
			memcpy(&currentOffset,(char*)pgData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),sizeof(short int));
			if(currentOffset!=-1){
				currentOffset-=extraMem;
				memcpy((char*)pgData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),&currentOffset,sizeof(short int));
			}
		}
	}
	else if(recordSize == currRecSize){
		//just overwrite the record
		memcpy((char*)pgData+startRec,(char*)record,recordSize);
	}
	else if(recordSize > currRecSize){
		short int freeSpaceSize,freeSpaceEnding,freeSpaceStart,numRecsPerPage2;
		getAvailableFreeSpace(fileHandle,pageNum,&freeSpaceSize,&freeSpaceStart,&freeSpaceEnding,&numRecsPerPage2);
		short int neededMemory=recordSize-currRecSize;
		short int spaceToShift=freeSpaceStart-endRec;
		if(freeSpaceSize > neededMemory){
			memmove((char*)pgData+endRec+neededMemory,(char*)pgData+endRec,spaceToShift);

			memcpy((char*)pgData+startRec,(char*)record,recordSize);

			short int newfreeSpaceStart=freeSpaceStart+neededMemory;
			memcpy((char*)pgData+PAGE_SIZE-sizeof(short int),&newfreeSpaceStart,sizeof(short int));

			for(int count=1;count<=numRecsPerPage-slotNum-1;count++){
				short int currentOffset;
				memcpy(&currentOffset,(char*)pgData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),sizeof(short int));
				if(currentOffset!=-1){
					currentOffset+=neededMemory;
					memcpy((char*)pgData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),&currentOffset,sizeof(short int));
				}
			}
		}
		else{
			RID ridOfmovedRec;
			insertRecord(fileHandle,recordDescriptor,data,ridOfmovedRec);
			void* tomb_stone=malloc(PAGE_SIZE);
memset(tomb_stone, 0 , PAGE_SIZE);
			short int minus=-1;
			
			memcpy((char*)tomb_stone,&minus,sizeof(short int));
			int newPageNum=ridOfmovedRec.pageNum;
			int newSlotNum=ridOfmovedRec.slotNum;
			memcpy((char*)tomb_stone+sizeof(short int),&newPageNum,sizeof(int));
			memcpy((char*)tomb_stone+sizeof(short int)+sizeof(int),&newSlotNum,sizeof(int));
			short int sizeOfTombStone=sizeof(short int)+2*sizeof(int);
			
			memcpy((char*)pgData+startRec,(char*)tomb_stone,sizeOfTombStone);
			memcpy(&freeSpaceStart,(char*)pgData+PAGE_SIZE-sizeof(short int),sizeof(short int));
			short int extraMem=endRec-(startRec+sizeOfTombStone);
			short int newfreeSpaceStart=freeSpaceStart-extraMem;

			short int spaceToShift=freeSpaceStart-endRec;
			memmove((char*)pgData+startRec+sizeOfTombStone,(char*)pgData+endRec,spaceToShift);

			memcpy((char*)pgData+PAGE_SIZE-sizeof(short int),&newfreeSpaceStart,sizeof(short int));

			for(int count=1;count<=numRecsPerPage-slotNum-1;count++){
				short int currentOffset;
				memcpy(&currentOffset,(char*)pgData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),sizeof(short int));
				if(currentOffset!=-1){
					currentOffset-=extraMem;
					memcpy((char*)pgData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),&currentOffset,sizeof(short int));
				}
			}
			free(tomb_stone);
		}
	}
	fileHandle.writePage(pageNum,pgData);
	free(pgData);
	free(record);
	return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,
    const void *value,
    const vector<string> &attributeNames,
    RBFM_ScanIterator &rbfm_ScanIterator){

	rbfm_ScanIterator.fileHandle = fileHandle;
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.conditionAttribute = conditionAttribute;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.value = value;
	rbfm_ScanIterator.attributeNames = attributeNames;
rbfm_ScanIterator.numOfPages = fileHandle.getNumberOfPages();
rbfm_ScanIterator.currentPage = 0;
rbfm_ScanIterator.nextSlot = 0;
rbfm_ScanIterator.isEOF = false;

	return 0;

}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {

	void * recData = malloc(PAGE_SIZE);
memset(recData,0,PAGE_SIZE);
	bool validRec = true;
	do{
		getNextUtility(rid, recData);
		if(isEOF){
			free(recData);
			return RBFM_EOF;
		}
		validRec = isValid(recData);

	}while(!validRec);

	int numOfAttrs=attributeNames.size();
	int nullBitByteSize=ceil((double)numOfAttrs/CHAR_BIT);
	memset(data,0,nullBitByteSize);
	int dataOffset,recordOffset;
	dataOffset=nullBitByteSize;
	short int numOfAttrsRec;
	memcpy(&numOfAttrsRec,(char*)recData,sizeof(short int));
	recordOffset=(int)(numOfAttrsRec+1)*sizeof(short int);
	for(int j=0;j<attributeNames.size();j++){
		recordOffset=(int)(numOfAttrsRec+1)*sizeof(short int);
		for(int i=0;i<recordDescriptor.size();i++){
			bool isAttrNeeded=false;
			if(recordDescriptor[i].name==attributeNames[j]){
				isAttrNeeded=true;
			}
			if(isAttrNeeded){
				short int endOfAttr;
				memcpy(&endOfAttr,(char*)recData+sizeof(short int)+(i*sizeof(short int)),sizeof(short int));
				if(endOfAttr<0){
					((char*)data)[i/8] |= ( 1 << (7-(i%8)) );
				}
				else{
					if(recordDescriptor[i].type==TypeInt){
						int val;
						memcpy(&val,(char*)recData+recordOffset,recordDescriptor[i].length);
						recordOffset+=recordDescriptor[i].length;
						memcpy((char*)data+dataOffset,&val,sizeof(int));
						dataOffset+=sizeof(int);
					}
					else if(recordDescriptor[i].type==TypeReal){
						float val;
						memcpy(&val,(char*)recData+recordOffset,recordDescriptor[i].length);
						recordOffset+=recordDescriptor[i].length;
						memcpy((char*)data+dataOffset,&val,sizeof(float));
						dataOffset+=sizeof(float);
					}
					else if(recordDescriptor[i].type==TypeVarChar){
						int len=endOfAttr-recordOffset+1;
						memcpy((char*)data+dataOffset,&len,sizeof(int));
						dataOffset+=sizeof(int);
						memcpy((char*)data+dataOffset,(char*)recData+recordOffset,len);
						recordOffset+=len;
						dataOffset+=len;
					}
				}
				break;
			}
			else{
				//first check if the attr is present in record
				short int endOfAttr;
				memcpy(&endOfAttr,(char*)recData+sizeof(short int)+(i*sizeof(short int)),sizeof(short int));
				if(endOfAttr>=0){
					if(recordDescriptor[i].type==TypeInt)
						{

						recordOffset+=recordDescriptor[i].length;
					}
					else if(recordDescriptor[i].type==TypeReal){

						recordOffset+=recordDescriptor[i].length;
						}
					else if(recordDescriptor[i].type==TypeVarChar){
						int len=endOfAttr-recordOffset+1;
						recordOffset+=len;
					}
				}
			}
		}
	}
	free(recData);
	return 0;
}

RC RBFM_ScanIterator::getNextUtility(RID &rid, void * recordDataFormat){

	if(currentPage < numOfPages){

			void* pgData = malloc(PAGE_SIZE);
			fileHandle.readPage(currentPage, pgData);
			int numOfRecs = 0;
			memcpy(&numOfRecs, (char*)pgData + ( PAGE_SIZE - 2 * sizeof(short)), sizeof(short));
			if(nextSlot < numOfRecs){
				rid.pageNum=currentPage;
				rid.slotNum=nextSlot;
				readRecFromPg(pgData, nextSlot, numOfRecs, recordDataFormat);
				nextSlot ++;
			}else{ //(nextSlot == numOfRecs) is true. Already read the last record of the current page
				bool isLastPage = (currentPage == numOfPages -1);
				currentPage ++;
				if(!isLastPage){
					nextSlot = 0; //initialize the slot number to zero, since it is a new page
					fileHandle.readPage(currentPage, pgData);
					rid.pageNum=currentPage;
					rid.slotNum=nextSlot;
					memcpy(&numOfRecs, (char*)pgData + ( PAGE_SIZE - 2 * sizeof(short)), sizeof(short));

					readRecFromPg(pgData, nextSlot, numOfRecs, recordDataFormat);
					nextSlot ++;
				}else{ // it is a last page and all the records have been read.
					isEOF = true;
				}

			}

			free(pgData);
	}
	else{
		isEOF = true;
	}
	return 0;
}


RC RBFM_ScanIterator::readRecFromPg(void* pgData, int slotNum, int numOfRecs, void* recData){

	short int startOfRec = 0;
	memcpy(&startOfRec, (char*) pgData + (PAGE_SIZE - (2 + slotNum + 1)*sizeof(short)), sizeof(short));
	if(startOfRec<0){
		short int l1=-1;
		memcpy((char*)recData,&l1,sizeof(short int));
		return 0;
	}
	short int endOfRec = 0;
	if(slotNum + 1 == numOfRecs){
		int freeSpacePointer = 0;
		memcpy(&freeSpacePointer, (char*)pgData + (PAGE_SIZE - sizeof(short)), sizeof(short));
		endOfRec = freeSpacePointer;
	}else{
		for(int i=1;i<=numOfRecs-slotNum-1;i++){
			memcpy(&endOfRec,(char*)pgData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+i)*sizeof(short int)),sizeof(short int));

		}
		if(endOfRec<0){
			memcpy(&endOfRec, (char*)pgData + (PAGE_SIZE - sizeof(short)), sizeof(short));
		}
	}

	memcpy(recData, (char*)pgData + startOfRec, endOfRec - startOfRec);
	return 0;
}

bool RBFM_ScanIterator::isValid(void * recordDataFormat){

	int indexAttr;
	short int isTombStone;
	memcpy(&isTombStone,(char*)recordDataFormat,sizeof(short int));
	if(isTombStone<0){
		return false;
	}
	if(compOp==NO_OP){
		return true;
	}
	for(int i=0;i<recordDescriptor.size();i++){
		if(recordDescriptor[i].name.compare(conditionAttribute) == 0){
			indexAttr=i;
			break;
		}
	}
	short int endOfAttr;
	memcpy(&endOfAttr,(char*)recordDataFormat+((indexAttr+1)*sizeof(short int)),sizeof(short int));
	if(endOfAttr==-1) return false;
	short int startOfAttr=0;
	for(int i=indexAttr-1;i>=0;i--){
		memcpy(&startOfAttr,(char*)recordDataFormat+((i+1)*sizeof(short int)),sizeof(short int));
		if(startOfAttr>=0){
			startOfAttr+=1;
			break;
		}
	}
	if(startOfAttr<=0){//the attribute is first
		short int numOfAttrs;
		memcpy(&numOfAttrs,(char*)recordDataFormat,sizeof(short int));
		startOfAttr=(numOfAttrs+1)*sizeof(short int);
	}
	switch (recordDescriptor[indexAttr].type){
	case TypeInt:
		{
		int recValue;
		memcpy(&recValue,(char*)recordDataFormat+startOfAttr,sizeof(int));
		int toverify;
		memcpy(&toverify,(char*)value,sizeof(int));
		if(compOp==EQ_OP){
			if(toverify==recValue){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LT_OP){
			if(recValue<toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LE_OP){
			if(recValue<=toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GT_OP){
			if(recValue>toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GE_OP){
			if(recValue >= toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NE_OP){
			if(recValue!=toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NO_OP){
			return true;
		}

	}
		break;
	case TypeReal:
{
		float recValue;
		memcpy(&recValue,(char*)recordDataFormat+startOfAttr,sizeof(float));
		float toverify;
		memcpy(&toverify,(char*)value,sizeof(float));
		if(compOp==EQ_OP){
			if(toverify==recValue){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LT_OP){
			if(recValue<toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LE_OP){
			if(recValue<=toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GT_OP){
			if(recValue>toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GE_OP){
			if(recValue >= toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NE_OP){
			if(recValue!=toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NO_OP){
			return true;
		}
	}
break;
	case TypeVarChar:
	{
		int stringLength=endOfAttr-startOfAttr+1;
		char* str=(char*)malloc(stringLength+1);
memset(str,0,stringLength+1);
		memcpy(str,(char*)recordDataFormat+startOfAttr,stringLength);
		str[stringLength]='\0';
		string recValue(str);
		free(str);
		int length;
		memcpy(&length,(char*)value,sizeof(int));
		char * str2=(char*)malloc(length+1);
memset(str2,0,length+1);
		memcpy(str2,(char*)value+sizeof(int),length);
		str2[length]='\0';
		string toverify(str2);
		free(str2);
		if(compOp==EQ_OP){
			if(toverify==recValue){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LT_OP){
			if(recValue<toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LE_OP){
			if(recValue<=toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GT_OP){
			if(recValue>toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GE_OP){
			if(recValue >= toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NE_OP){
			if(recValue!=toverify){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NO_OP){
			return true;
		}

	}
	break;
default:
break;
}
	return false;
}

RC RBFM_ScanIterator::close(){
	fclose(fileHandle.filePointer);
	return 0;
}
