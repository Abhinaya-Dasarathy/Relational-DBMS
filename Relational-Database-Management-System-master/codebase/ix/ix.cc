
#include "ix.h"


#include <iostream>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
pfm = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}
RC IndexManager::createFile(const string &fileName)
{
	RC rc;
	rc = pfm->createFile(fileName);
	if (rc == 0) {
		
		return 0;
	}
    return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
	RC rc;
	rc = pfm->destroyFile(fileName);
	if (rc == 0)
		return 0;
	return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	RC rc;
	rc = pfm->openFile(fileName, ixfileHandle.fileHandle);
	if (rc == 0) {
		return 0;
	}
    return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	RC rc;
	rc = pfm->closeFile(ixfileHandle.fileHandle);
	if (rc == 0)
		return 0;
    return -1;
}

RC prepareEntry(const Attribute &attribute, const void *key, const RID &rid, char* record, int &recsize)
{
	switch(attribute.type)
	{
		case TypeInt: {
			memcpy((char*) record, (char*)key, attribute.length);
			memcpy((char*) record+attribute.length, &rid, sizeof(RID));
			recsize+=attribute.length+sizeof(RID);
			break;
		}
		case TypeReal: {
			memcpy((char*) record, (char*)key, attribute.length);
			memcpy((char*) record+attribute.length, &rid, sizeof(RID));
			recsize+=attribute.length+sizeof(RID);
			break;
		}
		case TypeVarChar: {
memset(record,0,PAGE_SIZE);
			int variableLength = 0; // to calculate the variable length of the field
			memcpy(&variableLength, (char*) key, sizeof(int)); //first find the length of the string which is stored in the data

			memcpy((char*) record, &variableLength, sizeof(int)); //copy the length of string to record
			memcpy((char*) record + sizeof(int), (char*)(key + sizeof(int)), variableLength); //copy the string into the record

			memcpy((char*) record + sizeof(int) + variableLength, &rid, sizeof(RID));


			recsize+=sizeof(int)+variableLength+sizeof(RID);
			break;	
		}
	}
	return 0;		
}

RC compare(const void* key, const void* key_i, Attribute attribute, int key_len, int keyi_len)	//returns -1 for key<keyval, 1 for >=
{
	switch(attribute.type)
	{
		case TypeInt: {
			int val = 0,nodeval=0;
			
			//val=(int*)key;
			memcpy(&val, (char*)key, attribute.length);
			
			memcpy(&nodeval, (char*)key_i, attribute.length);
			
			if(val<nodeval)	//if key<K, return -1 else 1
				return -1;
			else if(val>=nodeval)
				return 1;
			break;
		}
		case TypeReal: {
			float val=0,nodeval=0;
			memcpy(&val, (char*)key, attribute.length);
			memcpy(&nodeval, (char*)key_i, attribute.length);
			if(val<nodeval)
				return -1;
			else if(val>=nodeval)
				return 1;
			break;
		}
		case TypeVarChar: {
			char* val = (char*)malloc(PAGE_SIZE);
			memset(val,0,PAGE_SIZE);
			memcpy(val, (char*)key+sizeof(int), key_len-sizeof(int));
		
			char* nodeval = (char*)malloc(PAGE_SIZE);
			memset(nodeval, 0, PAGE_SIZE);
			memcpy(nodeval, (char*)key_i, keyi_len-sizeof(int));

			int i=0,j=0;
			while(i<key_len-sizeof(int) && j<keyi_len-sizeof(int))
			{
				
				if(val[i]>nodeval[j])
					return 1;
				i++;j++;
				
			}
if(key_len<keyi_len)	return -1;
return 1;
//int return_val=strcmp(val,nodeval);

			free(val);
			free(nodeval);

			//return return_val;
			break;
		}
	}
}

RC insertIndex(IXFileHandle &ixfileHandle,int &node, const void* key, const RID rid, int &newnode, char* newval,const Attribute attribute, const int key_len)
{
	

	char *pgData = (char*)malloc(PAGE_SIZE);
	memset(pgData,0,PAGE_SIZE);
		char* buffer = (char*) malloc(PAGE_SIZE);
		memset(buffer,0,PAGE_SIZE);
	short int no_of_rec=0;
	short int free_space=0;

	char* record = (char*)malloc(PAGE_SIZE);
	//memset(record,0,PAGE_SIZE);
	int recsize=0;

	void *key_i = malloc(PAGE_SIZE);
	memset(key_i,0,PAGE_SIZE);

	
	RC rc;
	char type;

	if(node!=-1){
	RC rc=ixfileHandle.fileHandle.readPage(node,pgData);
	memcpy(&type, pgData, 1);
	memcpy(&no_of_rec, pgData + PAGE_SIZE - 2*sizeof(short int), sizeof(short int));}
	
	


	if(node==-1)	//inserting new leaf page
	{
		
		
		
		//update type of page in the first B
		char t='l';
		memcpy((char*) pgData, &t, sizeof(char));

		//write record
		prepareEntry(attribute, key, rid, record, recsize);	//returns  record as <key,rid> with size, recsize
		memcpy((char*) pgData + 1, record, recsize);


		//update sibling pointer
		int sibling_ptr=-1;
		memcpy((char*) pgData + PAGE_SIZE - 2 * sizeof(short int) - sizeof(int), &sibling_ptr, sizeof(int));

		//update no. of records in the page in second last 2B
		no_of_rec = 1; //only one record at this point of time
		memcpy((char*) pgData + PAGE_SIZE - (2 * sizeof(short int)), &no_of_rec, sizeof(short int));

		//update freespace pointer in the last 2B
		free_space=PAGE_SIZE-1-recsize-2*sizeof(short int) - sizeof(int);
		memcpy((char*) pgData + PAGE_SIZE - sizeof(short int), &free_space, sizeof(short int));

		if (ixfileHandle.fileHandle.appendPage(pgData) == -1)
		{
			free(record);
			return -1;
		}
		
		
		free(record);
		free(pgData);
		free(key_i);
free(buffer);
		return 0;		
	}

	else if(type=='l')
	{
		
		memset(key_i,0,PAGE_SIZE);

		int i=1;
		int readPos=1, found=0, add=0;
short int free_space=-1;
int recsize=0;
		memcpy(&free_space, pgData + PAGE_SIZE - 2, sizeof(short int));
		

		prepareEntry(attribute, key, rid, record, recsize);
		
		if(recsize<=free_space)	//record size < free_space in leaf node
		{
			
			
			i=1;
			readPos=1;
			found=0;
			add=0;
			while(i<=no_of_rec && !found)
			{

				memset(key_i,0,PAGE_SIZE);
				switch(attribute.type)
				{
					case TypeInt:
					case TypeReal: {
						memcpy(key_i, (char*) pgData + readPos, attribute.length);	//reading K1, K2...
						add=attribute.length;
						break;
					}

					case TypeVarChar: {
						int keyi_len=0;


						memcpy(&keyi_len, (char*) pgData + readPos ,sizeof(int));

						memcpy(key_i, (char*) pgData + readPos + sizeof(int), keyi_len);

						add=sizeof(int) + keyi_len;

						break;
					}
				}
				
				RC rc = compare(key, key_i, attribute, key_len, add);

				if(rc<0)
				{
					
					//memmove(pgData + readPos + recsize, pgData + readPos, recsize);
					//memcpy(pgData + readPos, record, recsize);
					found=1;

short int fsp = PAGE_SIZE-free_space-(2*sizeof(short int))-sizeof(int);
					memset(buffer,0,PAGE_SIZE);
					memcpy(buffer, pgData, readPos);
					memcpy(buffer + readPos, record, recsize);

					memcpy(buffer + readPos + recsize, pgData + readPos, fsp-readPos);
	memcpy((char*)buffer+PAGE_SIZE-2*sizeof(short int)-sizeof(int),(char*)pgData+PAGE_SIZE-2*sizeof(short int)-sizeof(int), sizeof(int));

					memcpy(pgData, buffer, PAGE_SIZE);
					//free(buffer);

					break;
				}
				readPos+=add+sizeof(RID);

				i++;		
			}
			
			if(i==no_of_rec+1)	//if new entry at the end
			{
				
				memcpy(pgData + readPos, record, recsize);
			}
			
			//update no. of records in the page in second last 2B
			//short int rec = 0; 
			//memcpy(&rec, pgData + PAGE_SIZE - (2 * sizeof(short int)), sizeof(short int));
			//rec+=1;
			no_of_rec++;

			memcpy((char*) pgData + PAGE_SIZE - (2 * sizeof(short int)), &no_of_rec, sizeof(short int));

			//update freespace pointer in the last 2B
			free_space-=recsize;
			memcpy((char*) pgData + PAGE_SIZE - sizeof(short int), &free_space, sizeof(short int));
			ixfileHandle.fileHandle.writePage(node, pgData);

		newnode=-1;

		free(pgData);

		free(key_i);

		free(buffer);

		free(record);

//		return 0;
		}
		//if no space & split
		else
		{
			
			int first=1;
			char t='l';

			memset(buffer,0,PAGE_SIZE);

			memcpy(buffer, &t, 1);
			short int buffer_no_of_rec=0, buffer_recsize=0;

			char* newPgData=(char*)malloc(PAGE_SIZE);
			memset(newPgData,0,PAGE_SIZE);
			memcpy(newPgData,&t,1);
			short int newPgData_no_of_rec=0, newPgData_recsize=0;

			i=1;
			readPos=1;
			found=0;
			add=0;
		
			while(i<=no_of_rec && buffer_recsize<(PAGE_SIZE-1-2*sizeof(short int)-sizeof(int))/2)
			{

				switch(attribute.type)
				{
					case TypeInt:
					case TypeReal: {
						memcpy(key_i, (char*) pgData + readPos, attribute.length);	//reading K1, K2...
						add=attribute.length+sizeof(rid);
						break;
					}

					case TypeVarChar: {
						int keyi_len=0;
						memcpy(&keyi_len, (char*) pgData + readPos ,sizeof(int));
						memcpy(key_i, (char*) pgData + readPos +sizeof(int), keyi_len);
						add=sizeof(int) + keyi_len+ sizeof(rid);
						break;
					}
				}
		
				if(found==0)
					
					rc= compare(key, key_i, attribute, key_len, add-sizeof(rid));

				if(rc<0 && found==0)
				{

					if(buffer_recsize+recsize<(PAGE_SIZE-1-2*sizeof(short int)-sizeof(int))/2)
					{
					found=1;
					memcpy(buffer + buffer_recsize+1, record, recsize);
					buffer_no_of_rec++;
					buffer_recsize+=recsize;
					}
					else
						break;
					/*memcpy(buffer + buffer_recsize+1, pgData + readPos, add);
					buffer_no_of_rec++;
					buffer_recsize+=add;
					break;*/
				}
				else
				{

					if(buffer_recsize+add<(PAGE_SIZE-1-2*sizeof(short int)-sizeof(int))/2)
					{
					memcpy(buffer + buffer_recsize+1, pgData + readPos, add);	//add is the size of record
					buffer_no_of_rec++;
					buffer_recsize+=add;
					readPos+=add;
					i++;
					}
					else
						break;
				}
			
			}
			short int buffer_fsp = PAGE_SIZE - 1 - buffer_recsize - 2*sizeof(short int) - sizeof(int);
			memcpy(buffer + PAGE_SIZE - 2*(sizeof(short int)), &buffer_no_of_rec, sizeof(short int));
			memcpy(buffer + PAGE_SIZE - sizeof(short int), &buffer_fsp, sizeof(short int));
			//ixfileHandle.fileHandle.writePage(node, buffer);

			

			first=1;
			while(i<=no_of_rec)	//to copy second half to newPgData
			{
				switch(attribute.type)
				{
					case TypeInt:
					case TypeReal: {
						memcpy(key_i, (char*) pgData + readPos, attribute.length);	//reading K1, K2...
						add=attribute.length+sizeof(rid);
						break;
					}

					case TypeVarChar: {
						int keyi_len=0;
						memcpy(&keyi_len, (char*) pgData + readPos ,sizeof(int));
						memcpy(key_i, (char*) pgData + readPos + sizeof(int), keyi_len);
						add=sizeof(int) + keyi_len + sizeof(rid);
						break;
					}
				}
				if(found==0)
					 rc = compare(key, key_i, attribute, key_len, add-sizeof(rid));
				if(rc<0 && found==0)
				{

					found=1;
					if(first==1)
					{	
						first=0;							
						memcpy(newval, (char*)key, key_len);	//copying only the key to move to non-leaf node
						
					}

					memcpy(newPgData + newPgData_recsize+1, record, recsize);
					newPgData_no_of_rec++;
					newPgData_recsize+=recsize;
				
					memcpy(newPgData + newPgData_recsize+1, pgData + readPos, add);
				
					newPgData_no_of_rec++;
					newPgData_recsize+=add;
					readPos+=add;
					i++;
					break;
				}
				else
				{
					if(first==1)
					{	
						first=0;							
						memcpy(newval, (char*) pgData + readPos, add);
				
					}

					memcpy(newPgData + newPgData_recsize+1, pgData + readPos, add);	//add is the size of record
					newPgData_no_of_rec++;
					newPgData_recsize+=add;
					readPos+=add;
					i++;
				}
			}
			if(found==0)
			{
				memcpy(newPgData + newPgData_recsize+1, record, recsize);
				newPgData_no_of_rec++;
				newPgData_recsize+=recsize;

			}
			int num=-1;
			memcpy(newPgData + PAGE_SIZE - 2*sizeof(short int) - sizeof(int), &num, sizeof(int));
			short int newPgData_fsp = PAGE_SIZE - 1 - newPgData_recsize - 2*sizeof(short int)-sizeof(int);
			memcpy(newPgData + PAGE_SIZE - 2*(sizeof(short int)), &newPgData_no_of_rec, sizeof(short int));
			memcpy(newPgData + PAGE_SIZE - sizeof(short int), &newPgData_fsp, sizeof(short int));
			
		

			ixfileHandle.fileHandle.appendPage(newPgData);
			int pgNum = ixfileHandle.fileHandle.getNumberOfPages()-1;	//getting page number for sibling
			//setting sibling pointer
			memcpy((char*) buffer + PAGE_SIZE - 2*(sizeof(short int)) - sizeof(int), &pgNum, sizeof(int));
			ixfileHandle.fileHandle.writePage(node, buffer);
		
			

			newnode = (int) pgNum;
			//free(buffer);
			free(newPgData);
			free(pgData);
			free(key_i);
			free(record);
free(buffer);
			
			//return 0;

		}

return 0;
	}	

	else if(type=='n' || type=='r')
	{
		
		char* newentry=(char*)malloc(PAGE_SIZE);	
		memset(newentry,0,PAGE_SIZE);
		int newentry_len=0;		

		//Finding Pi such that ki<=key<ki2
		int i=1;
		int readPos=1, found=0, ptr=0, ptr_pos=0, pgNum=0, keyPos=0,add=0,first=0;
		short int newPgData_no_of_rec=0, newPgData_recsize=0, buffer_no_of_rec=0, buffer_recsize=0;
		char t ='n';

		char* newPgData=(char*)malloc(PAGE_SIZE);
		memset(newPgData,0,PAGE_SIZE);
		//memcpy(newPgData,&t,1);
		
		//char* buffer=(char*)malloc(PAGE_SIZE);
		memset(buffer,0,PAGE_SIZE);

		

		while(i<=no_of_rec && !found)
		{
			add=0;
			memcpy(&ptr, (char*) pgData + readPos, sizeof(int));	//reading P0, P1, P2..
			ptr_pos=readPos;
			readPos+=sizeof(int);
			keyPos=readPos;
			switch(attribute.type)
			{
			case TypeInt:
			case TypeReal: {
				memcpy(key_i, (char*) pgData + readPos, attribute.length);	//reading K1, K2...
				readPos+=attribute.length;
				add=attribute.length;
				break;
			}
			case TypeVarChar: {
				add=0;

				memcpy(&add, (char*) pgData + readPos ,sizeof(int));
				readPos += sizeof(int);

				memcpy(key_i, (char*) pgData + readPos, add);

				readPos+=add;
				add+=sizeof(int);

				break;
			}
			}
			
			RC rc = compare(key, key_i, attribute, key_len, add);
			
			if(rc<0)
			{

				found=1;

				insertIndex(ixfileHandle, ptr, key, rid, newnode, newval, attribute, key_len);
				break;
			}
			i++;
		}
		if(i==no_of_rec+1)
		{

			memcpy(&ptr, (char*) pgData + readPos, sizeof(int));	//reading Pk
			

			ptr_pos=readPos;
			readPos+=sizeof(int);
			keyPos=readPos;
			insertIndex(ixfileHandle, ptr, key, rid, newnode, newval, attribute, key_len);
		}
		if(ptr==-1) 	//need to update ptr to reflect new page if created, when creating first leaf node for a pointer
		{
			
			pgNum = ixfileHandle.fileHandle.getNumberOfPages()-1;
			
			memcpy((char*) pgData + ptr_pos, &pgNum, sizeof(int));
			ixfileHandle.fileHandle.writePage(node, pgData);
//free(pgData);

		}

		//handle non-leaf node remaining
		if(newnode == -1)
		{
			
		
			free(record);
			free(newPgData);
			free(newentry);
			free(pgData);
			free(key_i);
			free(buffer);
			//return 0;
		}
		
		else
		{


				//the key that came from lower leaf or non-leaf node to be inserted
				//int keyi_len=0;

				memset(newentry,0,PAGE_SIZE);
				newentry_len=0;
				switch(attribute.type)
				{
					case TypeInt:
					case TypeReal: {
						memcpy(newentry, (char*) newval, attribute.length);	
						newentry_len=attribute.length;
						break;
					}	
					case TypeVarChar: {
						memcpy(&newentry_len, (char*) newval,sizeof(int));

memcpy((char*)newentry, &newentry_len, sizeof(int));
						memcpy(newentry +sizeof(int), (char*) newval + sizeof(int), newentry_len);

						newentry_len+=sizeof(int);
						break;
					}
				}
			
			//node has space
			memcpy(&free_space, pgData + PAGE_SIZE - sizeof(short int), sizeof(short int));
			if(newentry_len+sizeof(int)<=free_space)
			{
				
				//char* buffer=(char*)malloc(PAGE_SIZE);
				memset(buffer,0,PAGE_SIZE);
				//short int buffer_no_of_rec=0, buffer_recsize=0;

				short int fsp = PAGE_SIZE-free_space-(2*sizeof(short int));
				memcpy((char*)buffer, (char*)pgData, keyPos);	//copying old page till pointer page (including pointer page)


			memcpy((char*)buffer + keyPos, (char*)newentry,newentry_len);	//new entry value from lower non-leaf or leaf node

				memcpy((char*)buffer + keyPos + newentry_len, &newnode, sizeof(int));	//pointer to new page created	
				memcpy((char*)buffer + keyPos + newentry_len + sizeof(int), pgData + keyPos, fsp-keyPos);
				
				
				free_space-=(newentry_len+sizeof(int));
				
memcpy(buffer + PAGE_SIZE - sizeof(short int), &free_space, sizeof(short int));
				
				no_of_rec++;
				
memcpy(buffer + PAGE_SIZE - 2*sizeof(short int), &no_of_rec, sizeof(short int));

				
	ixfileHandle.fileHandle.writePage(node, buffer); 

				newnode=-1;
			}
		
			
			else	//split node
			{
			
			char t ='n';

			memset(buffer,0,PAGE_SIZE);
			memcpy(buffer, &t, 1);
			buffer_no_of_rec=0;
			buffer_recsize=0;

			memset(newPgData,0,PAGE_SIZE);
			memcpy(newPgData,&t,1);
			newPgData_no_of_rec=0;
			newPgData_recsize=0;

			
			readPos=1;
			found=0;
			ptr=0;
			ptr_pos=0;
			pgNum=0;
			keyPos=0;
			add=0;
			int flag =0,next1 = 0;
			int i=1;
			 
			
			while(i<=no_of_rec && buffer_recsize<((PAGE_SIZE-1-2*sizeof(short int))/2))
			{
				
				memset(key_i,0,PAGE_SIZE);
				memcpy(buffer + buffer_recsize + 1, (char*) pgData + readPos, sizeof(int));	//reading P0, P1, P2..
				buffer_recsize+=sizeof(int);
				readPos+=sizeof(int);
				keyPos=readPos;
			
				switch(attribute.type)
				{
					case TypeInt:
					case TypeReal: {
						memcpy(key_i, (char*) pgData + readPos, attribute.length);	//reading K1, K2...
						add=attribute.length;
						break;
					}

					case TypeVarChar: {
						int keyi_len=0;
						memcpy(&keyi_len, (char*) pgData + readPos ,sizeof(int));
						memcpy(key_i, (char*) pgData + readPos + sizeof(int), keyi_len);
						add=sizeof(int) + keyi_len;
						break;
					}
				}
				if(found==0)
					rc = compare(newentry, key_i, attribute, newentry_len, add);
				
				if(rc<0 && found==0)
				{

					if(buffer_recsize+newentry_len+sizeof(int)<(PAGE_SIZE-1-2*sizeof(short int))/2)
					{
					//next1=0;
					found=1;
					memcpy(buffer + buffer_recsize+1, newentry, newentry_len);
					buffer_no_of_rec++;
					buffer_recsize+=newentry_len;
				
					memcpy(buffer + buffer_recsize +1, &newnode, sizeof(int));
					buffer_recsize += sizeof(int);
					}
					else
					{
						//next=0;
						flag=1;	break;
					}
				}

				else 
				{
				if(buffer_recsize+add+sizeof(int)<(PAGE_SIZE-1-2*sizeof(short int))/2)
				{	

					//next1=1;
					memcpy(buffer + buffer_recsize+1, pgData + readPos, add);	//add is the size of record
					buffer_no_of_rec++;
					buffer_recsize+=add;
					readPos+=add;
					i++;
				}
				else
					break;
				}
			}
			if(flag==1)
			{
				memset(newval,0,PAGE_SIZE);
				memcpy(newval, newentry, newentry_len);
				memcpy(newPgData + newPgData_recsize + 1, &newnode, sizeof(int));
				newPgData_recsize+=sizeof(int);
				memcpy(newPgData + newPgData_recsize +1, pgData+readPos, add);
				readPos+=add;
				newPgData_recsize+=add;
				newPgData_no_of_rec++;
				i++;
			}
			else//check
			{
				memset(newval,0,PAGE_SIZE);
				memcpy(newval, (char*) pgData + readPos, add);
				readPos+=add;
				i++;
			}
			
			short int buffer_fsp = PAGE_SIZE - 1 - buffer_recsize - 2*sizeof(short int);
			memcpy(buffer + PAGE_SIZE - 2*(sizeof(short int)), &buffer_no_of_rec, sizeof(short int));
			memcpy(buffer + PAGE_SIZE - sizeof(short int), &buffer_fsp, sizeof(short int));
			
			first=1;			
			while(i<=no_of_rec)	//to copy second half to newPgData
			{
				memset(key_i,0,PAGE_SIZE);
				memcpy(newPgData + newPgData_recsize + 1, (char*) pgData + readPos, sizeof(int));	//reading P0, P1, P2..
				newPgData_recsize+=sizeof(int);
				readPos+=sizeof(int);
				keyPos=readPos;
				
				//if(i==no_of_rec) break;
			
				switch(attribute.type)
				{
					case TypeInt:
					case TypeReal: {
						memcpy(key_i, (char*) pgData + readPos, attribute.length);	//reading K1, K2...
						add=attribute.length;
						break;
					}

					case TypeVarChar: {
						int keyi_len=0;
						memcpy(&keyi_len, (char*) pgData + readPos ,sizeof(int));
						memcpy(key_i, (char*) pgData + readPos + sizeof(int), keyi_len);
						add=sizeof(int) + keyi_len;
						break;
					}
				}
				if(found==0)
					rc = compare(newentry, key_i, attribute, newentry_len, add);
				if(rc<0 && found==0)
				{

					found=1;
					memcpy(newPgData + newPgData_recsize+1, newentry, newentry_len);
					newPgData_no_of_rec++;
					newPgData_recsize+=newentry_len;
				
					memcpy(newPgData + newPgData_recsize +1, &newnode, sizeof(int));
					newPgData_recsize += sizeof(int);
				}
						
					memcpy(newPgData + newPgData_recsize+1, pgData + readPos, add);	//add is the size of record
					newPgData_no_of_rec++;
					newPgData_recsize+=add;
					readPos+=add;
					i++;
			}
			memcpy(newPgData + newPgData_recsize+1, pgData + readPos, sizeof(int)); //copying Pk
			newPgData_recsize+=sizeof(int);
			if(found==0)
			{
				found=1;
				memcpy(newPgData + newPgData_recsize+1, newentry, newentry_len);
				newPgData_no_of_rec++;
				newPgData_recsize+=newentry_len;
				
				memcpy(newPgData + newPgData_recsize +1, &newnode, sizeof(int));
				newPgData_recsize += sizeof(int);
			}

			
			short int newPgData_fsp = PAGE_SIZE - 1 - newPgData_recsize - 2*sizeof(short int);
			memcpy(newPgData + PAGE_SIZE - 2*(sizeof(short int)), &newPgData_no_of_rec, sizeof(short int));
			memcpy(newPgData + PAGE_SIZE - sizeof(short int), &newPgData_fsp, sizeof(short int));
		
			ixfileHandle.fileHandle.writePage(node,buffer);
			ixfileHandle.fileHandle.appendPage(newPgData);
			newnode=ixfileHandle.fileHandle.getNumberOfPages()-1;
		
			
			
		if(type=='r')
		{
			
	   		 memset(buffer,0,PAGE_SIZE);
			 memset(newentry, 0, PAGE_SIZE);
			 newentry_len=0;

			//update type of page in the first B
			char t='r';
			memcpy((char*) buffer, &t, sizeof(char));

	
			switch(attribute.type)
			{
					case TypeInt:
					case TypeReal: {
						memcpy(newentry, (char*) newval, attribute.length);	
						newentry_len=attribute.length;
						break;
					}	
					case TypeVarChar: {
						memcpy(&newentry_len, (char*) newval,sizeof(int));
						memcpy(newentry, &newentry_len, sizeof(int));
						memcpy((char*)newentry+sizeof(int), (char*) newval + sizeof(int), newentry_len);
						newentry_len+=sizeof(int);
						break;
					}
			}
		
		memset(newPgData,0,PAGE_SIZE);
		
		ixfileHandle.fileHandle.readPage(node,newPgData);	
		ixfileHandle.fileHandle.appendPage(newPgData);

		pgNum = ixfileHandle.fileHandle.getNumberOfPages()-1;
		

		//first entry with left and right pointers

		memcpy((char*) buffer + 1, &pgNum, sizeof(int));		
		memcpy((char*) buffer + 1 + sizeof(int), newentry, newentry_len);
		memcpy((char*) buffer + 1 + sizeof(int) + newentry_len, &newnode, sizeof(int));

		
		//update freespace pointer in the last 2B
		short int free_space=PAGE_SIZE-1-2*sizeof(int)-newentry_len-2*sizeof(short int);
		//-1 for type, key_len for entry, 2 for pointers, 4 for freespace, no. of entries
		memcpy((char*) buffer + PAGE_SIZE - sizeof(short int), &free_space, sizeof(short int));

		//update no. of records in the page in second last 2B
		short int rec = 1; //only one record at this point of time
		memcpy((char*) buffer + PAGE_SIZE - (2 * sizeof(short int)), &rec,sizeof(short int));
	
		ixfileHandle.fileHandle.writePage(0,buffer);
			
		newnode=-1;
		//return 0;
		}
//return 0;
	}
free(record);
			
	free(newPgData);

			free(newentry);

			free(pgData);

			free(key_i);

		free(buffer);

			
	}
			
return 0;
			
     }

	return 0;
}

RC findKeyLength(const void* key, const Attribute attribute, int &key_len)
{
	key_len=0;
	switch(attribute.type)
	{
		case TypeInt:
		case TypeReal: {
			key_len+=attribute.length;
			break;
		}
		case TypeVarChar: {
			int variableLength = 0; // to calculate the variable length of the field
			memcpy(&variableLength, (char*) key, sizeof(int)); //first find the length of the string which is stored in the data
			key_len+=sizeof(int)+variableLength;
			break;	
		}
	}
	return 0;		
	
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
cout<<"entering insert entry"<<endl;
    char *pgData = (char*)malloc(PAGE_SIZE);
    memset(pgData,0,PAGE_SIZE);
    int pgNum = ixfileHandle.fileHandle.getNumberOfPages() - 1;
    int key_len=0;
    findKeyLength(key,attribute,key_len);    //returns length of key
    
    int newnode=-1;
    char* newval=(char*)malloc(PAGE_SIZE);
    memset(newval,0,PAGE_SIZE);

    //If the file is empty, append a new page and insert the root
    if (pgNum == -1) {

	

    //update type of page in the first B
    char type='r';
    memcpy((char*) pgData, &type, sizeof(char));

    //first entry with left and right pointers as -1
    int left=-1;
    memcpy((char*) pgData + 1, &left, sizeof(int));
    memcpy((char*) pgData + 1 + sizeof(int), (char*)key, key_len);
    int right=-1;
    memcpy((char*) pgData + 1 + sizeof(int) + key_len, &right, sizeof(int));
       
    //update freespace pointer in the last 2B
    short int free_space=PAGE_SIZE-1-2*sizeof(int)-key_len-2*sizeof(short int);
    //-1 for type, key_len for entry, 2 for pointers, 4 for freespace, no. of entries
    memcpy((char*) pgData + PAGE_SIZE - sizeof(short int), &free_space, sizeof(short int));

    //update no. of records in the page in second last 2B
    short int rec = 1; //only one record at this point of time
    memcpy((char*) pgData + PAGE_SIZE - (2 * sizeof(short int)), &rec,sizeof(short int));
       
    if (ixfileHandle.fileHandle.appendPage(pgData) == -1) {
        free(pgData);
        return -1;
    }       
    }
    int root =0;
    insertIndex(ixfileHandle, root, key, rid, newnode, newval, attribute, key_len);


    free(pgData);
    free(newval);
    return 0;
}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

	RC retVal = searchAndDelete(ixfileHandle, ROOT_PAGE, attribute, key, rid);
return retVal;

}

//recursive delete function used to find where the key is and delete the same
RC IndexManager::searchAndDelete(IXFileHandle &ixfileHandle, int nodePg, const Attribute &attribute,const void *key, const RID &rid){
	void *dataNode = malloc(PAGE_SIZE);
memset(dataNode,0,PAGE_SIZE);
	ixfileHandle.readPage(nodePg, dataNode);
	char nodeType= ' ';
	memcpy(&nodeType,(char*) dataNode, sizeof(char));

	if(nodeType != 'l'){
//get the child page number form the non leaf node 
		int childPtr;
		searchKeyInNonLeafNode(attribute, dataNode, key, childPtr);

		RC returnValue = searchAndDelete(ixfileHandle, childPtr, attribute, key, rid);
		free(dataNode);
		return returnValue;

	}else{

//delete from leaf directly and update the fsp and number of records for the node
//return -1 if the entry is not there

		RC retVal = deleteEntryFromLeaf(ixfileHandle, dataNode, nodePg, attribute, key, rid);
		free(dataNode);
		return retVal;
			
	}

}
RC IndexManager::deleteEntryFromLeaf(IXFileHandle &ixfileHandle, void *dataNode, int nodePg,
		const Attribute &attribute, const void *key, const RID &rid)
{
short int fsp_bytes;
	memcpy(&fsp_bytes, (char*) dataNode + PAGE_SIZE - sizeof(short int), sizeof(short int));
short int fsp;
fsp = PAGE_SIZE-fsp_bytes-(2*sizeof(short int))-sizeof(int)-1;
	short int numOfRecords = 0;
	memcpy(&numOfRecords, (char*) dataNode + PAGE_SIZE - 2*sizeof(short int), sizeof(short int));
	bool deletedEntry=false;
	if(attribute.type == TypeInt){

		int keyToDel=-1;
		int offsetPosToDelAt = -1;
		memcpy(&keyToDel, (char*) key, sizeof(int));

		for(int i=0;i<numOfRecords;i++){
			int offset = i*3*sizeof(int);
			int keyToCheck = -1;
int RidPg = -1;
int RidSl = -1;
			memcpy(&keyToCheck, (char*) dataNode + sizeof(char) + offset, sizeof(int));

			offset += sizeof(int);
			memcpy(&RidPg, (char*) dataNode + sizeof(char) + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&RidSl, (char*) dataNode + sizeof(char) + offset, sizeof(int));

			if(keyToCheck == keyToDel && RidPg == rid.pageNum && RidSl == rid.slotNum){ //check if thsi is the key to be deleted
				offsetPosToDelAt = i*3*sizeof(int);
//if(keyToDel==255) {cout<<"offsetpos"<<offsetPosToDelAt<<endl;}				
break;
			}
		}
		if(offsetPosToDelAt == -1){
			//key to be deleted not found

			deletedEntry = false;
		}
		else{
//compaction and updation of fsp and number of records
			int shiftStart = offsetPosToDelAt + 3*sizeof(int);

			int lengthToShift = fsp - shiftStart;

			//shift the data
			memmove((char*) dataNode + sizeof(char) + offsetPosToDelAt, (char*) dataNode + sizeof(char) + shiftStart, lengthToShift);
			fsp -= 3*sizeof(int);//update
fsp_bytes += 3*sizeof(int);

			memcpy((char*) dataNode + PAGE_SIZE - sizeof(short int), &fsp_bytes, sizeof(short int));
			numOfRecords -= 1;
			memcpy((char*) dataNode + PAGE_SIZE - 2*sizeof(short int), &numOfRecords, sizeof(short int));
			deletedEntry = true;
		}
	}
	else if(attribute.type == TypeReal){
		float keyToDel = -1;
		memcpy(&keyToDel, (char*) key, sizeof(float));
		int offsetPosToDelAt = -1;
		for(int i=0;i<numOfRecords;i++){
			int offset = i*(sizeof(float) + 2*sizeof(int));
			float keyToCheck = -1;
			int RidPg = -1;
int RidSl = -1;
			memcpy(&keyToCheck, (char*) dataNode + sizeof(char) + offset, sizeof(float));
			offset += sizeof(float);
			memcpy(&RidPg, (char*) dataNode + sizeof(char) + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&RidSl, (char*) dataNode + sizeof(char) +  offset, sizeof(int));
			if(keyToCheck == keyToDel && RidPg == rid.pageNum && RidSl == rid.slotNum){
				offsetPosToDelAt = i*(sizeof(float) + 2*sizeof(int));
				break;
			}
		}
		if(offsetPosToDelAt == -1){
			
			deletedEntry = false;
		}
		else{
			int shiftStart = offsetPosToDelAt + sizeof(float) + 2*sizeof(int);
			int lengthToShift = fsp - shiftStart;
			memmove((char*) dataNode + sizeof(char) + offsetPosToDelAt, (char*) dataNode + sizeof(char) + shiftStart, lengthToShift);
			fsp -= sizeof(float) + 2*sizeof(int);
fsp_bytes += sizeof(float) + 2*sizeof(int);
			memcpy((char*) dataNode + PAGE_SIZE - sizeof(short int), &fsp_bytes, sizeof(short int));
			numOfRecords -= 1;
			memcpy((char*) dataNode + PAGE_SIZE - 2*sizeof(short int), &numOfRecords, sizeof(short int));
			deletedEntry = true;
		}
	}
//varchar
	else{
		int lenOfStr;
int lenOfStrToDel;
		memcpy(&lenOfStr, (char*) key, sizeof(int));
		char* str1 = (char*) malloc(PAGE_SIZE);
		memcpy(str1, (char*) key + sizeof(int), lenOfStr);
		str1[lenOfStr]='\0';
		string keyToDel(str1);
		free(str1);
		int offset = 0, offsetPosToDelAt = -1;
		for(int i=0;i<numOfRecords;i++){
			int len;
int RidPg = -1, RidSl = -1;
			memcpy(&len, (char*) dataNode + sizeof(char) + offset, sizeof(int));
			offset += sizeof(int);
			char* strToCheck = (char*) malloc(PAGE_SIZE);
			memcpy(strToCheck, (char*) dataNode + sizeof(char) + offset, len);
			offset += len;
			strToCheck[len]='\0';
			string keyToCheck(strToCheck);
			memcpy(&RidPg, (char*) dataNode + sizeof(char) + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&RidSl, (char*) dataNode + sizeof(char) +offset, sizeof(int));
			offset += sizeof(int);
			free(strToCheck);
			if(keyToDel == keyToCheck && RidPg == rid.pageNum && RidSl == rid.slotNum){
				offsetPosToDelAt = offset - 3*sizeof(int) - len;
				lenOfStrToDel = len;
				break;
			}
		}
		if(offsetPosToDelAt == -1){
			//key to be deleted not found
			deletedEntry = false;
		}
		else{
			int shiftStart = offsetPosToDelAt + sizeof(int) + lenOfStrToDel + 2*sizeof(int);
			int lenToShift = fsp - shiftStart -1;
			memmove((char*) dataNode + sizeof(char) + offsetPosToDelAt, (char*) dataNode + sizeof(char) + shiftStart, lenToShift);
			fsp -= (sizeof(int) + lenOfStrToDel + 2*sizeof(int));
fsp_bytes+= (sizeof(int) + lenOfStrToDel + 2*sizeof(int));
			memcpy((char*) dataNode + PAGE_SIZE - sizeof(short int), &fsp_bytes, sizeof(short int));
			numOfRecords -= 1;
			memcpy((char*) dataNode + PAGE_SIZE - 2*sizeof(short int), &numOfRecords, sizeof(short int));
			deletedEntry = true;
		}
	}
	if(!deletedEntry){
//free(dataNode);
		return -1;  //not found
	}
	ixfileHandle.writePage(nodePg, dataNode);

//free(dataNode);
	return 0;
}
RC IndexManager::searchKeyInNonLeafNode(const Attribute &attribute, const void *dataNode, const void *key, int &KeyPtr){
//cout<<"entering searchkey in non leaf"<<endl;
	short int numOfRecords =0;
	memcpy(&numOfRecords, (char*) dataNode + PAGE_SIZE - 2*sizeof(short int), sizeof(short int));
	if(attribute.type == TypeInt){
		int keyToSearch =-1;
		memcpy(&keyToSearch, (char*) key, sizeof(int));
		for(int i=0;i<numOfRecords;i++){
			int keyValue = -1;
			memcpy(&keyValue,(char*)dataNode + sizeof(char) + sizeof(int) + i*(sizeof(int) + sizeof(int)),sizeof(int));
			if(keyToSearch > keyValue && ( i + 1 )< numOfRecords){ //keep searching further
				continue;
			}
			else{
				if(keyToSearch >= keyValue){
					int pageNo;
					memcpy(&pageNo, (char*) dataNode + sizeof(char)+ sizeof(int) + i*(sizeof(int) +sizeof(int)) + sizeof(int),sizeof(int)); //get the right pointer
					KeyPtr = pageNo;
				}
				else{
					int pageNo;
					memcpy(&pageNo, (char*) dataNode + sizeof(char)+ sizeof(int) + i*(sizeof(int) +sizeof(int)) - sizeof(int),sizeof(int)); //get the left pointer
					KeyPtr = pageNo;
				}
				break;
			}
		}
	}
	else if(attribute.type == TypeReal){
		float floatkeyToSearch = -1;
		memcpy(&floatkeyToSearch, (char*)key, sizeof(float));
		for(int i=0;i<numOfRecords;i++){
			float floatKeyValue = -1;
			memcpy(&floatKeyValue,(char*)dataNode + sizeof(char) + sizeof(int) + i*(sizeof(float) + sizeof(int)),sizeof(float));
			if(floatkeyToSearch > floatKeyValue && (i+1) < numOfRecords){
				continue;
			}
			else{
				if(floatkeyToSearch >= floatKeyValue){
					int pageNo;
					memcpy(&pageNo, (char*)dataNode + sizeof(char) + sizeof(int) + i*(sizeof(float) + sizeof(int)) + sizeof(int), sizeof(int));
					KeyPtr = pageNo;
				}
				else{
					int pageNo;
					memcpy(&pageNo, (char*)dataNode + sizeof(char) + sizeof(int) + i*(sizeof(float) + sizeof(int)) - sizeof(int), sizeof(int));
					KeyPtr = pageNo;
				}
				break;
			}
		}
	}
	else{ 
		char* keyToSearch= (char*)malloc(PAGE_SIZE);
		int strLength;
		memcpy(&strLength, (char*) key, sizeof(int));
		memcpy(keyToSearch, (char*) key + sizeof(int), strLength);
		keyToSearch[strLength]='\0';
		string searchString(keyToSearch);
		free(keyToSearch);
		int currentOffset=sizeof(int);
		for(int i=0;i<numOfRecords;i++){
			char* keyValueString= (char*)malloc(PAGE_SIZE);
			int length;
			memcpy(&length, (char*) dataNode + sizeof(char) + currentOffset, sizeof(int));
			currentOffset += sizeof(int);
			memcpy(keyValueString, (char*) dataNode +sizeof(char) + currentOffset, length);
			currentOffset += length;
			keyValueString[length]='\0';
			string keyString(keyValueString);
			free(keyValueString);
			if(searchString > keyString && (i+1) < numOfRecords){
				currentOffset+=sizeof(int);
				continue;
			}
			else{
				if(searchString >= keyString){
					int pageNo;
					memcpy(&pageNo, (char*) dataNode + sizeof(char) + currentOffset, sizeof(int));
					KeyPtr=pageNo;
				}
				else{
					int pageNo;
					memcpy(&pageNo, (char*) dataNode + sizeof(char) + currentOffset - length - sizeof(int) - sizeof(int), sizeof(int));
					KeyPtr=pageNo;
				}
				break;
			}
		}
	}
	return 0;
}






RC IndexManager::printTree(IXFileHandle &ixfileHandle, int pgNum, const Attribute &attribute, int tabcount) const{
	
	void* dataNode = malloc(PAGE_SIZE);
//added
memset(dataNode,0,PAGE_SIZE);
	ixfileHandle.readPage(pgNum, dataNode);
char nodeType = ' ';

	memcpy(&nodeType, (char*) dataNode, sizeof(char));

	if(nodeType == 'l'){
		printLeaf(dataNode, attribute, tabcount+1);
		
	}
	else{
		printNonLeaf(dataNode, attribute, tabcount); //go to non leaf node and print children: 
		short int numOfRecords;
		memcpy(&numOfRecords, (char*) dataNode + PAGE_SIZE - 2*sizeof(short int), sizeof(short int));

		if(attribute.type == TypeInt || attribute.type == TypeReal){

			for(int i=0;i<=numOfRecords;i++){

				int offset= i*(sizeof(int)+sizeof(int));

				int nextPage;
				memcpy(&nextPage, (char*) dataNode + sizeof(char) + offset, sizeof(int));

				if(nextPage < 0){
					continue;
				}


				printTree(ixfileHandle,nextPage,attribute,tabcount+1);
				if(i<numOfRecords){
					cout<<","<<endl;
				}
				else{
					cout<<endl;
				}
			}
		}
		else{
			int offset=0;
			for(int i=0;i<=numOfRecords;i++){
				int nextPage;
				memcpy(&nextPage, (char*) dataNode + sizeof(char) + offset, sizeof(int));
				if(i!=numOfRecords){
					offset += sizeof(int);
					int length;
					memcpy(&length, (char*) dataNode + sizeof(char) + offset, sizeof(int));
					offset += sizeof(int) + length;
				}
				if(nextPage < 0){
					continue;
				}
				printTree(ixfileHandle, nextPage, attribute, tabcount+1);
				if(i<numOfRecords){
					cout<<","<<endl;
				}
			}
		}
		for(int i=0;i<tabcount;i++) cout<<"\t";
		cout<<"]}"<<endl;
	}
	free(dataNode);
	return 0;
}
void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
  if(ixfileHandle.getNumberOfPages() == 0){//no index exists yet
		cout<<"{\"keys\": []}"<<endl;
		return;
	}

	printTree(ixfileHandle, ROOT_PAGE, attribute, 0);// to start printing from the root
	cout<<endl;
}

//function for sorting
bool sort_i(const pair<int,int> &x, const pair<int,int> &y){
	if(x.first == y.first){
		if(x.second <= y.second) return true;
		return false;
	}

if(x.first < y.first) return true;
	
	return false;
}
//printing the leaf node
RC IndexManager::printLeaf(void* dataNode, const Attribute &attribute, int tabcount) const{
	for(int i=0;i<tabcount;i++) {cout<<"\t";}
	short int numOfRecords;
	memcpy(&numOfRecords, (char*) dataNode + PAGE_SIZE -2*sizeof(short int), sizeof(short int));
	cout<<"{\"keys\": [";
	if(attribute.type == TypeInt){
		vector< pair <int, pair <int,int> > > data;
		for(int i=0;i<numOfRecords;i++){
			int offset = i*(sizeof(int) + 2*sizeof(int)); //key + page num+ slot num all int
			int keyvalue,page,slot;
			memcpy(&keyvalue, (char*) dataNode + sizeof(char) + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&page, (char*) dataNode + sizeof(char) +offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&slot, (char*) dataNode + sizeof(char)+ offset, sizeof(int));
			data.push_back(make_pair(keyvalue,make_pair(page,slot)));
		}
		int len=data.size(),i=0;
		while(i<len){
			vector<pair<int,int> > ridsList;
			int key = data[i].first;
			ridsList.push_back(make_pair(data[i].second.first,data[i].second.second));
			i++;
			while(i<len && data[i].first == key){
				ridsList.push_back(make_pair(data[i].second.first,data[i].second.second));
				i++;
			}
			sort(ridsList.begin(), ridsList.end(), sort_i);
			int j=1;
			cout<<"\""<<key<<":[";
			cout<<"("<<ridsList[0].first<<","<<ridsList[0].second<<")";
			while(j<ridsList.size()){
				cout<<",("<<ridsList[j].first<<","<<ridsList[j].second<<")";
				j++;
			}
			cout<<"]\"";
			if(i<len) cout<<",";
		}
		cout<<"]}";
	}
	else if(attribute.type == TypeReal){
		vector<pair<float, pair<int,int> > > data;
		for(int i=0;i<numOfRecords;i++){
			int offset = i*(sizeof(float) + 2*sizeof(int));
			float keyvalue;
			int page,slot;
			memcpy(&keyvalue, (char*) dataNode + sizeof(char) + offset, sizeof(float));
			offset += sizeof(float);
			memcpy(&page, (char*) dataNode + sizeof(char)+ offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&slot, (char*) dataNode + sizeof(char)+ offset, sizeof(int));
			data.push_back(make_pair(keyvalue,make_pair(page,slot)));
			
		}
		int len=data.size(),i=0;
		while(i<len){
			vector<pair<int,int> > ridsList;
			float key = data[i].first;
			ridsList.push_back(make_pair(data[i].second.first,data[i].second.second));
			i++;
			while(i<len && data[i].first == key){
				ridsList.push_back(make_pair(data[i].second.first,data[i].second.second));
				i++;
			}
			sort(ridsList.begin(), ridsList.end(), sort_i);
			int j=1;
			cout<<"\""<<key<<":[";
			cout<<"("<<ridsList[0].first<<","<<ridsList[0].second<<")";
			while(j<ridsList.size()){
				cout<<",("<<ridsList[j].first<<","<<ridsList[j].second<<")";
				j++;
			}
			cout<<"]\"";
			if(i<len) cout<<",";
		}
		cout<<"]}";
	}
	else{
		int offset=0;
		vector< pair<string, pair<int,int> > > data;
		for(int i=0;i<numOfRecords;i++){
			int len;
			memcpy(&len, (char*) dataNode + sizeof(char)+offset, sizeof(int));
			offset+=sizeof(int);
			char* str1 = (char*) malloc(PAGE_SIZE);
			memcpy(str1, (char*) dataNode + sizeof(char) +offset, len);
			str1[len]='\0';
			string key(str1);
			offset+=len;
			int page,slot;
			memcpy(&page, (char*) dataNode + sizeof(char)+ offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&slot, (char*) dataNode + sizeof(char) + offset, sizeof(int));
			offset += sizeof(int);
			data.push_back(make_pair(key,make_pair(page,slot)));
			free(str1);
		}
		int len=data.size(),i=0;
		while(i<len){
			vector<pair<int,int> >ridsList;
			string key = data[i].first;
			ridsList.push_back(make_pair(data[i].second.first,data[i].second.second));
			i++;
			while(i<len && data[i].first == key){
				ridsList.push_back(make_pair(data[i].second.first,data[i].second.second));
				i++;
			}
			sort(ridsList.begin(), ridsList.end(), sort_i);
			int j=1;
			cout<<"\""<<key<<":[";
			cout<<"("<<ridsList[0].first<<","<<ridsList[0].second<<")";
			while(j<ridsList.size()){
				cout<<",("<<ridsList[j].first<<","<<ridsList[j].second<<")";
				j++;
			}
			cout<<"]\"";
			if(i<len) cout<<",";
		}
		cout<<"]}";
	}
	return 0;
}
//printing non leaf node
RC IndexManager::printNonLeaf(void* dataNode, const Attribute &attribute, int tabcount) const{
	for(int i=0;i<tabcount;i++) cout<<"\t";
	cout<<"{"<<endl;
	for(int i=0;i<tabcount;i++) cout<<"\t";
	short int numOfRecords = 0;
	memcpy(&numOfRecords, (char*) dataNode + PAGE_SIZE - 2*sizeof(short int), sizeof(short int));
	cout<<"\"keys\":[";
//integer
	if(attribute.type == TypeInt){
		for(int i=0;i<numOfRecords;i++){
			int offset = sizeof(int) + i*(sizeof(int) + sizeof(int));
			int keyvalue;
			memcpy(&keyvalue, (char*) dataNode + sizeof(char) +offset, sizeof(int));
			cout<<"\""<<keyvalue<<"\"";
			if(i!=numOfRecords-1){
				cout<<",";
			}
		}
		cout<<"],"<<endl;
		for(int i=0;i<tabcount;i++) cout<<"\t";
		cout<<"\"children\": ["<<endl;
	}
//real
	else if(attribute.type == TypeReal){
		for(int i=0;i<numOfRecords;i++){
			int offset = sizeof(int)+i*(sizeof(float) + sizeof(int));
			float keyvalue;
			memcpy(&keyvalue, (char*) dataNode + sizeof(char)+ offset, sizeof(float));
			cout<<"\""<<keyvalue<<"\"";
			if(i!=numOfRecords-1){
				cout<<",";
			}
		}
		cout<<"],"<<endl;
		for(int i=0;i<tabcount;i++) cout<<"\t";
		cout<<" \"children\": ["<<endl;
	}
//varchar
	else{
		int offset = sizeof(int);
		for(int i=0;i<numOfRecords;i++){
			int len;
//cout<<"offset"<<offset<<endl;
			memcpy(&len, (char*) dataNode + sizeof(char)+offset, sizeof(int));
			offset += sizeof(int);
			char* str1 = (char*) malloc(PAGE_SIZE);
			cout<<"length"<<len<<endl;
			memcpy((char*)str1, (char*) dataNode + sizeof(char) + offset, len);

			offset += len;
			offset += sizeof(int);
			str1[len]='\0';
			string key(str1);
			cout<<"\""<<key<<"\"";
			if(i!=numOfRecords-1){
				cout<<",";
			}
			free(str1);
		}
		cout<<"],"<<endl;
		for(int i=0;i<tabcount;i++) cout<<"\t";
		cout<<" \"children\": ["<<endl;
	}
	return 0;
}


//initialise scan iterator
RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool		lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
	//to check if no file is open currently
if(ixfileHandle.fileHandle.filePointer == NULL){ 
		return -1;
	}
	ix_ScanIterator.ixfileHandle = ixfileHandle;
	ix_ScanIterator.attribute = attribute;
	ix_ScanIterator.highKey = highKey;
	ix_ScanIterator.highKeyInclusive = highKeyInclusive;
	ix_ScanIterator.pgData = malloc(PAGE_SIZE);
memset(ix_ScanIterator.pgData,0,PAGE_SIZE);
	int leafPage;
//gte the leaf page number where the low key is present
//cout<<"leafpage"<<endl;
	leafPage = getLeafPage(ixfileHandle, ROOT_PAGE, attribute, lowKey);
//scanning should begin from this page

	ix_ScanIterator.currentPage = leafPage;
//cout<<"read page"<<endl;
	ixfileHandle.readPage(ix_ScanIterator.currentPage, ix_ScanIterator.pgData);

	updOffset(lowKey, lowKeyInclusive, ix_ScanIterator); 
//free(ix_ScanIterator.pgData);
    return 0;
}

int IndexManager::getLeafPage(IXFileHandle &ixfileHandle, int nodePgNo,const Attribute attribute,const void* key){
	void* dataNode = malloc(PAGE_SIZE);
memset(dataNode,0,PAGE_SIZE);
	ixfileHandle.readPage(nodePgNo, dataNode);
printBtree(ixfileHandle, attribute);
	char nodeType = ' ';
	memcpy(&nodeType,(char*) dataNode, sizeof(char));
	short int fsp,fsp_bytes;
	memcpy(&fsp_bytes, (char*) dataNode + PAGE_SIZE - sizeof(short int), sizeof(short int));
fsp = PAGE_SIZE-fsp_bytes-sizeof(int)-(2*sizeof(short int))-1;
//cout<<"page no"<<nodePgNo<<endl;

//cout<<"node type"<<nodeType<<endl;
	if(nodeType == 'n' || nodeType =='r'){ 
//if non leaf, then recursive call to getLeaf page function
		int keyPtr;
//cout<<"not leaf"<<endl;
		if(key == NULL){
			//read the first left pointer
//cout<<"key null"<<endl;
			memcpy(&keyPtr, (char*)dataNode + sizeof(char), sizeof(int));
//cout<<"key ptr"<<keyPtr<<endl;
			if(keyPtr < 0) 
			{
			if (attribute.type == TypeInt)
			{
//cout<<"entering if"<<endl;
			memcpy(&keyPtr, (char*)dataNode + sizeof(char) + sizeof(int)+sizeof(int), sizeof(int));
			
			}
			else if(attribute.type == TypeReal)
			{
			memcpy(&keyPtr,	(char*)dataNode + sizeof(char) + sizeof(int) +sizeof(float), sizeof(int));
			}
			else
			{
			int len;
			memcpy(&len, (char*)dataNode + sizeof(char), sizeof(int));
			memcpy(&keyPtr, (char*)dataNode + sizeof(char) + sizeof(int)+sizeof(int) + len, sizeof(int));
			}
			}
			else
			{	
			}
		}else{
//cout<<"before search"<<endl;
			searchKeyInNonLeafNode(attribute, dataNode, key, keyPtr);
		}
//cout<<"get leaf 1"<<endl;
free(dataNode);
		int retVal = getLeafPage(ixfileHandle, keyPtr, attribute, key);
		//free(dataNode);
		return retVal;
	}else if(nodeType == 'l'){
		fsp = PAGE_SIZE-fsp_bytes-sizeof(int)-2*sizeof(short int)-1;
		if(fsp == 0 ){//leaf page is empty
			int nextLeafPage;
			memcpy(&nextLeafPage, (char*) dataNode + PAGE_SIZE - 2*sizeof(short int) - sizeof(int), sizeof(int));
			int returnVal = -1;

			if(nextLeafPage != -1){
//cout<<"get leaf 2"<<endl;

				returnVal = getLeafPage(ixfileHandle, nextLeafPage, attribute, key);
			}
			free(dataNode);
			return returnVal;
		}
		free(dataNode);
		return nodePgNo;
	}
}
void IndexManager::updOffset(const void* lowKey, bool lowKeyInclusive, IX_ScanIterator &ix_ScanIterator){
	//This method is used to update offset, fsp, IS_EOF
	//stop the offset when the key is greater than lowKey ;set IS_EOF to true when the key is not lesser than highKey
	
	if(ix_ScanIterator.currentPage == -1){

		ix_ScanIterator.isEOF = true;
		return;
	}
	void *leafPgData = malloc(PAGE_SIZE);
memset(leafPgData,0,PAGE_SIZE);
	ix_ScanIterator.ixfileHandle.readPage(ix_ScanIterator.currentPage, leafPgData);
	short fsp,fsp_bytes;
	memcpy(&fsp_bytes, leafPgData + PAGE_SIZE - sizeof(short), sizeof(short));

fsp = PAGE_SIZE-fsp_bytes-sizeof(int)-2*sizeof(short int)-1;
	ix_ScanIterator.fsp = fsp;

	short numOfRecords;
	memcpy(&numOfRecords, leafPgData + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
	bool lowerLimitFound = false;
	bool lowerLimitNULL = false;
//if the lower key value is null	
if(lowKey == NULL){
		lowerLimitFound = true;
		lowerLimitNULL = true;
		ix_ScanIterator.offset = 0;
	}
	bool isUpperLimitPresent = false;
	bool upperLimitNull = false;
	if(ix_ScanIterator.highKey == NULL){
		isUpperLimitPresent = true;
		upperLimitNull = true;
		ix_ScanIterator.isEOF = false;
	}
	if(lowerLimitNULL && upperLimitNull){//if there is no upper and lower limit already updated the offset, free space pointer and isEOF
		free(leafPgData);
		return; 
	}
	if(ix_ScanIterator.attribute.type == TypeInt){
		int lowKeyVal;
		if(!lowerLimitNULL){
			memcpy(&lowKeyVal, lowKey, sizeof(int));
		}
		int highKeyVal;
		if(!upperLimitNull){
			memcpy(&highKeyVal, ix_ScanIterator.highKey, sizeof(int));
		}

		for (int i= 0; i< numOfRecords; ++i) {

			int key;
			memcpy(&key, leafPgData + sizeof(char) + i*(3* sizeof(int)), sizeof(int));

			if (!lowerLimitNULL) {
				if(lowKeyInclusive){
					lowerLimitFound = lowKeyVal <= key;
				}else{
					lowerLimitFound = lowKeyVal < key;
				}
			}

			if(lowerLimitFound){
				if(!lowerLimitNULL){
					ix_ScanIterator.offset = i* (3*sizeof(int));
				}
				if (!upperLimitNull) {
					if(ix_ScanIterator.highKeyInclusive){
						isUpperLimitPresent = key <= highKeyVal;
					}else{
						isUpperLimitPresent = key < highKeyVal;
					}
					if(isUpperLimitPresent){
						ix_ScanIterator.isEOF = false;
					}else{
						ix_ScanIterator.isEOF = true;
					}
				}
				break;
			}else{
				continue;
			}
		}
		if(!lowerLimitFound){
			ix_ScanIterator.isEOF=true;
		}
		if(numOfRecords == 0){

			ix_ScanIterator.isEOF = false;
		}

	}else if(ix_ScanIterator.attribute.type == TypeReal){
		float lowKeyVal;
		if(!lowerLimitNULL){
			memcpy(&lowKeyVal, lowKey, sizeof(float));
		}
		float highKeyVal;
		if(!upperLimitNull){
			memcpy(&highKeyVal, ix_ScanIterator.highKey, sizeof(float));
		}
		for (int i= 0; i< numOfRecords; ++i) {
			float key;
			memcpy(&key, leafPgData + sizeof(char)+ i* (3*sizeof(int)), sizeof(float));
			if(!lowerLimitNULL){
				if(lowKeyInclusive){
					lowerLimitFound = lowKeyVal <= key;
				}else{
					lowerLimitFound = lowKeyVal < key;
				}
			}
			if(lowerLimitFound){
				if(!lowerLimitNULL){
					ix_ScanIterator.offset = i*(3*sizeof(int));
				}
				if (!upperLimitNull) {
					if(ix_ScanIterator.highKeyInclusive){
						isUpperLimitPresent = key <= highKeyVal;
					}else{
						isUpperLimitPresent = key < highKeyVal;
					}
					if(!isUpperLimitPresent){
						ix_ScanIterator.isEOF = true;
					}else{
						ix_ScanIterator.isEOF = false;
					}
				}
				break;
			}else{
				continue;
			}
		}
		if(!lowerLimitFound){
			ix_ScanIterator.isEOF=true;
		}
		if(numOfRecords == 0){
			ix_ScanIterator.isEOF = false;
		}
	}else if(ix_ScanIterator.attribute.type == TypeVarChar){
		char* lowKeyVal = (char*)malloc(PAGE_SIZE);
		if (!lowerLimitNULL) {
			int lowkeyLength;
			memcpy(&lowkeyLength, lowKey, sizeof(int));
			memcpy(lowKeyVal, lowKey + sizeof(int), lowkeyLength);
			lowKeyVal[lowkeyLength] = '\0';
		}

		char* highKeyVal = (char*)malloc(PAGE_SIZE);
		if (!upperLimitNull) {
			int highkey_length;
			memcpy(&highkey_length, ix_ScanIterator.highKey, sizeof(int));
			memcpy(highKeyVal, ix_ScanIterator.highKey + sizeof(int), highkey_length);
			highKeyVal[highkey_length] = '\0';
		}

		int offset = 0;
		for (int i= 0; i< numOfRecords; ++i) {
			char* key = (char*)malloc(PAGE_SIZE);
			int key_length;
			memcpy(&key_length, leafPgData + sizeof(char) + offset, sizeof(int));
			memcpy(key, leafPgData + sizeof(char)+ offset + sizeof(int), key_length);
			key[key_length] = '\0';

			if (!lowerLimitNULL) {
				if(lowKeyInclusive){
					lowerLimitFound = string(lowKeyVal) <= string(key);
				}else{
					lowerLimitFound = string(lowKeyVal) < string(key);
				}
			}

			if(lowerLimitFound){
				if (!lowerLimitNULL) {
					ix_ScanIterator.offset = offset;
				}
				if (!upperLimitNull) {
					if(ix_ScanIterator.highKeyInclusive){
						isUpperLimitPresent = string(key) <= string(highKeyVal);
					}else{
						isUpperLimitPresent = string(key) < string(highKeyVal);
					}
					if(!isUpperLimitPresent){
						ix_ScanIterator.isEOF = true;
					}else{
						ix_ScanIterator.isEOF = false;
					}
				}
				free(key);
				break;
			}else{
				offset += sizeof(int) + key_length + 2*sizeof(int);
				free(key);
				continue;
			}
		}

		if(!lowerLimitFound){
			ix_ScanIterator.isEOF=true;
		}
		if(numOfRecords == 0){
			ix_ScanIterator.isEOF = false;
		}

		free(lowKeyVal);
		free(highKeyVal);
	}
	free(leafPgData);
}
//SCAN ITERATOR CLASS - BEGIN
IX_ScanIterator::IX_ScanIterator()
{

}

IX_ScanIterator::~IX_ScanIterator()
{
//free(pgData);
}

int IX_ScanIterator::getNextLeaf(int PgNo){

	void* data = malloc(PAGE_SIZE);
	memset(data,0,PAGE_SIZE);
	ixfileHandle.readPage(PgNo, data);
//cout<<"page 0 data "<< *(char*)data<<" " <<*(int*)(data+1)<<" " <<*(int*)(data+1+sizeof(int))<<endl;
	int nextPage;
	memcpy(&nextPage, (char*) data + PAGE_SIZE  - 2*sizeof(short int) - sizeof(int), sizeof(int));
	free(data);
//cout<<"next page"<<nextPage<<endl;
	return nextPage;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	int numOfPages = ixfileHandle.getNumberOfPages();
//cout<<"entering Ix"<<" current page"<<currentPage<<endl;
	if(isEOF){ //end of file
//cout<<"entering ix eof"<<endl;
		return IX_EOF;
	}
//cout<<"offset"<<offset<<endl;
	while(offset >= fsp){

		currentPage = getNextLeaf(currentPage);
//cout<<"current page"<<currentPage<<endl;
		if(currentPage < 0){

			isEOF=true;
			return IX_EOF;
		}
		if(currentPage >= numOfPages){
//cout<<"iseofset"<<endl;
			isEOF = true;
			return IX_EOF;
		}
		offset = 0;

		ixfileHandle.readPage(currentPage, pgData);
	
//cout<<"page 0 data "<< *(char*)pgData<<" " <<*(int*)(pgData+1)<<" " <<*(int*)(pgData+1+sizeof(int))<<endl;
		short fp,fp_bytes;
		memcpy(&fp_bytes, pgData + PAGE_SIZE - sizeof(short int), sizeof(short int));
fp = PAGE_SIZE-fp_bytes-sizeof(int)-2*sizeof(short int)-1;
		fsp = fp;
		
	}

	bool condSatisfied = false;
	bool highKeyNULL = false;
	if(highKey == NULL){
		condSatisfied = true;
		highKeyNULL = true;
	}
	if(attribute.type == TypeReal || attribute.type == TypeInt){
//cout<<"entering if"<<endl;
		if (!highKeyNULL) {
			if(attribute.type == TypeInt){
				int keyVal;
				memcpy(&keyVal, pgData +sizeof(char)+ offset, sizeof(int));

				int highKeyVal;
				memcpy(&highKeyVal, highKey, sizeof(int));

				if(highKeyInclusive){
					condSatisfied = keyVal <= highKeyVal;
				}else{
					condSatisfied = keyVal < highKeyVal;
				}
			}else if(attribute.type == TypeReal){
				float keyVal;
				memcpy(&keyVal, pgData +sizeof(char)+ offset, sizeof(int));

				float highKeyVal;
				memcpy(&highKeyVal, highKey, sizeof(int));

				if(highKeyInclusive){
					condSatisfied = keyVal <= highKeyVal;
				}else{
					condSatisfied = keyVal < highKeyVal;
				}
			}
		}

		if(!condSatisfied){
//cout<<"cond satisfied"<<endl;
			isEOF = true;
			return IX_EOF;
		}
//cout<<"iseof"<<isEOF<<endl;
		memcpy((char*)key, (char*)pgData +sizeof(char)+ offset, sizeof(int));
		offset += sizeof(int);

		int PgNo;
		memcpy(&PgNo, (char*)pgData + sizeof(char)+offset, sizeof(int));
		offset += sizeof(int);

		int slotNum;
		memcpy(&slotNum, (char*)pgData + sizeof(char)+ offset, sizeof(int));
		offset += sizeof(int);

		rid.pageNum = PgNo;
		rid.slotNum = slotNum;
//cout<<"rid"<<rid.pageNum<<" " <<rid.slotNum<<endl;
//cout<<"ending if"<<endl;
	}else{

		int keyLen;
		memcpy(&keyLen, pgData + sizeof(char)+ offset, sizeof(int));
		if (!highKeyNULL) {
			char* highKeyVal = (char*)malloc(PAGE_SIZE);
			int highkeyLength; 
			memcpy(&highkeyLength, highKey, sizeof(int));
			memcpy(highKeyVal, highKey + sizeof(int), highkeyLength);
			highKeyVal[highkeyLength] = '\0';

			char *keyVal = (char*)malloc(PAGE_SIZE);
			memcpy(keyVal, pgData + sizeof(char) + offset + sizeof(int), keyLen);
			keyVal[keyLen] = '\0';

			if(highKeyInclusive){
				condSatisfied = string(keyVal) <= string(highKeyVal);
			}else{
				condSatisfied = string(keyVal) < string(highKeyVal);
			}

			free(highKeyVal);
			free(keyVal);
		}
		if(!condSatisfied){
			isEOF = true;
			return IX_EOF;
		}
		memcpy(key, pgData + sizeof(char)+ offset, sizeof(int) + keyLen);
		offset += sizeof(int) + keyLen;

		int PgNo;
		memcpy(&PgNo, pgData + sizeof(char)+offset, sizeof(int));
		offset += sizeof(int);

		int slotNum;
		memcpy(&slotNum, pgData +sizeof(char)+ offset, sizeof(int));
		offset += sizeof(int);

		rid.pageNum = PgNo;
		rid.slotNum = slotNum;

	}


    return 0;
}

RC IX_ScanIterator::close()
{
//	fclose(ixfileHandle.fileHandle.fp);
//cout<<"freeing pgdata"<<endl;
	free(pgData);
	isEOF = true;
    return 0;
}
//SCAN ITERATOR CLASS - END
//file handle class -BEGIN

IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::readPage(PageNum PgNo, void *data){
	if(PgNo < 0) return -1;
	if(fileHandle.readPage(PgNo, data) == 0)
	{
		return 0;
	}
	return -1;
}

RC IXFileHandle::writePage(PageNum PgNo, const void *data){
	if(PgNo < 0) return -1;
	if(fileHandle.writePage(PgNo, data) == 0)
	{
		return 0;
	}
	return -1;
}
RC IXFileHandle::appendPage(const void *data){
	if(fileHandle.appendPage(data)==0)
	{
		return 0;
	}
else
	{return -1;}
}
unsigned IXFileHandle::getNumberOfPages(){
	return fileHandle.getNumberOfPages();
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount=fileHandle.readPageCounter;
	writePageCount=fileHandle.writePageCounter;
	appendPageCount=fileHandle.appendPageCounter;
	return 0;
}
//file handle class -END

