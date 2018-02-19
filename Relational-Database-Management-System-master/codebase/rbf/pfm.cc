#include "pfm.h"
#include <iostream>
#include <string.h>
#include <string>
#include <stdio.h>
#include <cmath>
#include <math.h>

using namespace std;

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance() {
	if (!_pf_manager)
		_pf_manager = new PagedFileManager();

	return _pf_manager;
}

PagedFileManager::PagedFileManager() {
}

PagedFileManager::~PagedFileManager() {
}

RC PagedFileManager::createFile(const string &fileName) {
	//check if the filename already exists
	FILE* file = fopen(fileName.c_str(), "rb");
	if (file) {
		fclose(file);
		return -1;
	} else {
		file = fopen(fileName.c_str(), "wb");
		if (file) {
			fclose(file);
			return 0;
		}

		fclose(file);
	}
	return -1;
}

RC PagedFileManager::destroyFile(const string &fileName) {

	if (remove(fileName.c_str()) == 0) {
		return 0;
	}
	return -1;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	//check if file handler is handling some other open file
	if (fileHandle.filePointer != NULL) {
		return -1;
	}
	fileHandle.filePointer = fopen(fileName.c_str(), "r+b");
	if (fileHandle.filePointer) {
		return 0;
	}

	return -1;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {

	fflush(fileHandle.filePointer);
	if (fclose(fileHandle.filePointer) == 0)
		return 0;
	else
		return -1;
}

FileHandle::FileHandle() {
	filePointer = NULL; //initialise filepointer
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
}

/*FileHandle::FileHandle(IXFileHandle &ixFileHandle)
{
	readPageCounter = ixFileHandle.readPageCounter;
	writePageCounter = ixFileHandle.writePageCounter;
   	appendPageCounter = ixFileHandle.appendPageCounter;;

}*/

FileHandle::~FileHandle() {
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
	//check if the page number exists in the file
	if (pageNum > (unsigned) this->getNumberOfPages()-1)
		return -1;

	fseek(this->filePointer, pageNum * PAGE_SIZE, SEEK_SET);
	unsigned int size = fread(data, sizeof(char), PAGE_SIZE, this->filePointer);
	if (size != PAGE_SIZE) {
		return -1;
	}
	readPageCounter++;
	return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
	int pagecount=this->getNumberOfPages();
	//cout<<"Page Num"<<pageNum<<"unsigned of pagecoutn"<<(unsigned)pagecount<<endl;
		if(pageNum >= (unsigned)pagecount){
			cout<<"return"<<endl;
			return -1;
		}
		int seekstatus=fseek(this->filePointer,pageNum*PAGE_SIZE,SEEK_SET);
		if(seekstatus!=0){
			return -1;
		}
		int writecount=fwrite(data,sizeof(char),PAGE_SIZE,this->filePointer);
		if(writecount!=PAGE_SIZE){
			return -1;
		}
		writePageCounter+=1;
	    return 0;
}

RC FileHandle::appendPage(const void *data) {
	fseek(filePointer, 0, SEEK_END);

	int size = fwrite(data,sizeof(char),PAGE_SIZE,filePointer);
	if (size!=PAGE_SIZE) {
		return -1;
	}
	appendPageCounter++;
	return 0;
}

unsigned FileHandle::getNumberOfPages() {
	fseek(filePointer, 0, SEEK_END);
	long int size = ftell(filePointer); //size in bytes
//convert to number of pages
		//	cout << size << " " << (unsigned)size/4096 << " " << ceil((unsigned)size/4096);

	return ceil((unsigned) size / 4096);

}

RC FileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
	return 0;
}
