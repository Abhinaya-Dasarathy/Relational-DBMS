#include <cstdlib>
#include <iostream>
#include <cmath>
#include <cstdio>
#include "rm.h"
#include <cstring>

using namespace std;

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	rbfm = RecordBasedFileManager::instance();
	ix = IndexManager::instance();//proj4
	createTablesRecDescr(); //create the record descriptor for tables table
	createColumnsRecDescr(); //create the record descriptor for columns table
	createIndexRecDescr();//proj4
}

RelationManager::~RelationManager()
{
	tableAttr.clear();
	columnAttr.clear();
	indexAttr.clear();//proj4
}

RC RelationManager::createCatalog()
{

	RC returnVal = rbfm->createFile(TABLES_FILE);
	if(returnVal < 0){
		return returnVal;
	}

	RC returnVal2 = rbfm->createFile(COLUMNS_FILE);
	if(returnVal2 < 0){
		return returnVal2;
	}
//proj4
	RC returnVal3 = rbfm->createFile(INDEX_TABLE_FILE);
	if(returnVal3 < 0){
		return returnVal3;
	}

	FileHandle table_FileHandle;
	RC returnVal4=rbfm->openFile(TABLES_FILE, table_FileHandle);
	if(returnVal4!=0){
		return -1;
	}
	RID rid;
	void * tablesTblRec = malloc(PAGE_SIZE);
memset(tablesTblRec,0,PAGE_SIZE);
	prepareTablesRecord(tablesTblRec, TABLES_TBL_ID, TABLES_TABLE_NAME, TABLES_FILE, SYSTEM_TABLE);
	returnVal=rbfm->insertRecord(table_FileHandle, tableAttr, tablesTblRec, rid);
	if(returnVal!=0){
		return -1;
	}
	free(tablesTblRec);

	void* columnsTblRec = malloc(PAGE_SIZE);
	prepareTablesRecord(columnsTblRec, COLUMNS_TBL_ID, COLUMNS_TABLE_NAME, COLUMNS_FILE, SYSTEM_TABLE);
	rbfm->insertRecord(table_FileHandle, tableAttr, columnsTblRec, rid);
	
	free(columnsTblRec);
//proj4
	void* indexTblRec = malloc(PAGE_SIZE);
	prepareTablesRecord(indexTblRec, INDEX_TBL_ID, INDEX_TABLE_NAME, INDEX_TABLE_FILE, SYSTEM_TABLE);
	rbfm->insertRecord(table_FileHandle, tableAttr, indexTblRec, rid);
	free(indexTblRec);

	FileHandle column_FileHandle;
	rbfm->openFile(COLUMNS_FILE, column_FileHandle);

	void * columns_Record = malloc(PAGE_SIZE);
memset(columns_Record,0,PAGE_SIZE);
	for (int var = 0; var < tableAttr.size(); ++var) {
		Attribute currAttribute = tableAttr[var];
		prepareColumnsRecord(columns_Record, TABLES_TBL_ID, currAttribute.name, currAttribute.type, currAttribute.length, var+1, SYSTEM_TABLE);
		rbfm->insertRecord(column_FileHandle, columnAttr, columns_Record, rid);
	}

	for (int var = 0; var < columnAttr.size(); ++var) {
		Attribute currAttribute = columnAttr[var];
		prepareColumnsRecord(columns_Record, COLUMNS_TBL_ID, currAttribute.name, currAttribute.type, currAttribute.length, var+1, SYSTEM_TABLE);
		rbfm->insertRecord(column_FileHandle, columnAttr, columns_Record, rid);
	}
//proj4
for(int var = 0; var < indexAttr.size(); var++){
		Attribute currAttribute = indexAttr[var];
		prepareColumnsRecord(columns_Record, INDEX_TBL_ID, currAttribute.name, currAttribute.type, currAttribute.length, var+1, SYSTEM_TABLE);
		rbfm->insertRecord(column_FileHandle, columnAttr, columns_Record, rid);
	}
	free(columns_Record);
	rbfm->closeFile(table_FileHandle);
	rbfm->closeFile(column_FileHandle);
	

    return 0;
}

RC RelationManager::deleteCatalog()
{
	unsigned returnVal = rbfm->destroyFile(TABLES_FILE);
	unsigned returnVal2 = rbfm->destroyFile(COLUMNS_TABLE_NAME);
        unsigned returnVal3 = rbfm->destroyFile(INDEX_TABLE_FILE);//proj4
	
	if(returnVal < 0){
		return returnVal;
	}

	if(returnVal2 < 0){
		return returnVal2;
	}
//proj4
	if(returnVal3 <0){
		return returnVal3;
	}
	 return 0;
}

RC RelationManager::getNextTableID(int &nextTableID){
	
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute="table-id";
	CompOp compOp=NO_OP;
	vector<string> attributeNames;
	attributeNames.push_back("table-id");
	void *value=malloc(PAGE_SIZE);
memset(value,0,PAGE_SIZE);
	FileHandle table_FileHandle;
	RC returnVal=rbfm->openFile(TABLES_FILE, table_FileHandle);
if(returnVal<0)
{
return returnVal;
}
	rbfm->scan(table_FileHandle,tableAttr,conditionAttribute,compOp,value,attributeNames,rbfmIterator);
	int maxFound=INT_MIN;
	void *retData=malloc(PAGE_SIZE);
memset(retData,0,PAGE_SIZE);
	RID rid;
	while(rbfmIterator.getNextRecord(rid,retData)!=RBFM_EOF){
		int id1;
		memcpy(&id1,(char*)retData+1,sizeof(int));
		if(id1 > maxFound){
			maxFound=id1;
		}
	}
	free(value);
	free(retData);
	rbfm->closeFile(table_FileHandle);
	nextTableID=maxFound+1;
	return 0;
}
//proj4
RC RelationManager::getNextIndexID(int &nextIndexID){
//get the next index id by scanning the index-ids list
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute="index-id";
	vector<string> attributeNames;
CompOp compOp=NO_OP;
	attributeNames.push_back("index-id");
	void* value=malloc(PAGE_SIZE);
	FileHandle ixFileHandle;
	RC returnval =rbfm->openFile(INDEX_TABLE_FILE, ixFileHandle);
if(returnval<0)
{
return returnval;
}
	rbfm->scan(ixFileHandle, indexAttr, conditionAttribute, compOp, value, attributeNames, rbfmIterator);
	int maxFound = INT_MIN;
	void* retData = malloc(PAGE_SIZE);
memset(retData,0,PAGE_SIZE);
	RID rid;
	while(rbfmIterator.getNextRecord(rid,retData) != RBFM_EOF){
		int id2;
		memcpy(&id2,(char*)retData+1,sizeof(int));
		if(maxFound<id2){
			maxFound=id2;
		}
	}
	free(value);
	free(retData);
	rbfm->closeFile(ixFileHandle);
	if(maxFound == INT_MIN){
		maxFound = 0;
	}
	nextIndexID = maxFound + 1;
	return 0;
}
RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	int nextTableID;
	RC ret=getNextTableID(nextTableID);
	if(ret<0){
		return -1;
	}
	rbfm->createFile(tableName);
	void* data = malloc(PAGE_SIZE);
memset(data,0,PAGE_SIZE);
	unsigned tableId = nextTableID;

	FileHandle tableFileHandle;
	rbfm->openFile(TABLES_FILE, tableFileHandle);
	prepareTablesRecord(data, tableId, tableName, tableName, USER_TABLE);
	RID rid;
	rbfm->insertRecord(tableFileHandle, tableAttr, data, rid); 

	FileHandle column_FileHandle;
	rbfm->openFile(COLUMNS_FILE, column_FileHandle);
	for (int var = 0; var < attrs.size(); ++var) { 
		Attribute currAttribute = attrs[var];
		prepareColumnsRecord(data, tableId, currAttribute.name, currAttribute.type, currAttribute.length, var+1, USER_TABLE);
		rbfm->insertRecord(column_FileHandle, columnAttr, data, rid);
	}
	rbfm->closeFile(tableFileHandle);
	rbfm->closeFile(column_FileHandle);
	free(data);
    return 0;
}

RC RelationManager::getIDOfTable(const string &tableName, int &tabID, RID &tableRID, bool &sysTable, char *tableFileName){
	
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute="table-name";
	CompOp compOp=EQ_OP;
	void *value=malloc(PAGE_SIZE);
memset(value,0,PAGE_SIZE);
	int lengthOfTblName=tableName.length();
	char *str1=(char*)malloc(PAGE_SIZE);
memset(str1,0,PAGE_SIZE);
	for(int i=0;i<lengthOfTblName;i++){
		str1[i]=tableName[i];
	}
	tabID=-1;
	str1[lengthOfTblName]='\0';
	memcpy((char*)value,&lengthOfTblName,sizeof(int));
	memcpy((char*)value+sizeof(int),str1,lengthOfTblName);
	vector<string>attributeNames;
	attributeNames.push_back("table-id");
	attributeNames.push_back("system-table");
	attributeNames.push_back("file-name");
	FileHandle table_FileHandle;
	rbfm->openFile(TABLES_FILE, table_FileHandle);
	rbfm->scan(table_FileHandle,tableAttr,conditionAttribute,compOp,value,attributeNames,rbfmIterator);
	RID rid;
	void *retData=malloc(PAGE_SIZE);
memset(retData,0,PAGE_SIZE);
	while(rbfmIterator.getNextRecord(rid,retData)!=RBFM_EOF){
		int retID;
		memcpy(&retID,(char*)retData+1,sizeof(int));
		int sysTab;
		memcpy(&sysTab,(char*)retData+1+sizeof(int),sizeof(int));
		if(sysTab==1) sysTable=true;
		else sysTable=false;
		int lenOfFileName;
		memcpy(&lenOfFileName,(char*)retData+1+(2*sizeof(int)),sizeof(int));
		memcpy(tableFileName,(char*)retData+1+(3*sizeof(int)),lenOfFileName);
		tableFileName[lenOfFileName]='\0';
		tabID=retID;
		tableRID.pageNum=rid.pageNum;
		tableRID.slotNum=rid.slotNum;
		break;
	}
	rbfm->closeFile(table_FileHandle);
	free(retData);
	free(value);
	free(str1);
	return 0;
}

//proj4
//deleting corresponding index for table when deleting the table itself
void RelationManager::deleteIndexOfTable(int table_Id){

	vector<IndexSchema> index_det;
	getIndexesOfTable(table_Id, index_det);
	RID ridToDel;
	for(int i=0; i < index_det.size(); i++){
		ridToDel = index_det[i].first.second;
		FileHandle indexFileHandle;
		rbfm->openFile(INDEX_TABLE_FILE, indexFileHandle);
		rbfm->deleteRecord(indexFileHandle, indexAttr, ridToDel);
		rbfm->closeFile(indexFileHandle);
		ix->destroyFile(index_det[i].second.second);
	}
}

void RelationManager::getIndexesOfTable(int &tabId, vector<IndexSchema> &index_det){
	
	FileHandle indexFileHandle;
	RBFM_ScanIterator rbfmIterator;
	RID rid;
	rbfm->openFile(INDEX_TABLE_FILE, indexFileHandle);
	
	string conditionAttribute = "table-id";
	CompOp compOp = EQ_OP;
	void* tid = malloc(PAGE_SIZE);
	int tableId = tabId;
	memcpy((char*) tid, &tableId, sizeof(int));
	vector<string> attributes;
	attributes.push_back("index-id");
	attributes.push_back("attribute-name");
	attributes.push_back("filename");
	int nullBitSize=ceil((double)attributes.size()/CHAR_BIT);
	rbfm->scan(indexFileHandle, indexAttr, conditionAttribute, compOp, tid, attributes, rbfmIterator);
	void* retData = malloc(PAGE_SIZE);
	while(rbfmIterator.getNextRecord(rid, retData) != RBFM_EOF){
		int offset = nullBitSize;
		int indexId;
		memcpy(&indexId, (char*) retData + offset, sizeof(int));
		offset += sizeof(int);
		char* attributeName = (char*) malloc(PAGE_SIZE);
		int len;
		memcpy(&len, (char*) retData + offset, sizeof(int));
		offset += sizeof(int);
		memcpy(attributeName, (char*) retData + offset, len);
		offset += len;
		attributeName[len] = '\0';
		string str_AttrName(attributeName);
		char* fileName = (char*) malloc(PAGE_SIZE);
		memcpy(&len, (char*) retData + offset, sizeof(int));
		offset += sizeof(int);
		memcpy(fileName, (char*) retData + offset, len);
		offset += len;
		fileName[len] = '\0';
		string str_FileName(fileName);
		index_det.push_back(make_pair(make_pair(indexId, rid), make_pair(str_AttrName, str_FileName)));
		free(attributeName);
		free(fileName);
	}
	rbfmIterator.close();
	free(tid);
	free(retData);
}


RC RelationManager::deleteTable(const string &tableName)
{
	
	
	int tabID;
	RID tableRID;
	bool sysTable = false;
	char *tableFileName=(char*)malloc(55); 
        memset(tableFileName,0,55);
	getIDOfTable(tableName,tabID,tableRID,sysTable,tableFileName);
	if(sysTable){
		return -1;
	}
else if (tabID<0)
{
return 0;
}
	FileHandle table_FileHandle;
	rbfm->openFile(TABLES_FILE,table_FileHandle);
	rbfm->deleteRecord(table_FileHandle,tableAttr,tableRID);
	//cout<<"RID"<<tableRID.pageNum << " " << tableRID.slotNum <<"Table Name"<<tableName<<endl;
	fflush(table_FileHandle.filePointer);

	
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute="table-id";
	CompOp compOp=EQ_OP;
	void *value=malloc(PAGE_SIZE);
memset(value,0, PAGE_SIZE);
	memcpy((char*)value,&tabID,sizeof(int));
	vector<string>attributeNames;
	FileHandle column_FileHandle;
	rbfm->openFile(COLUMNS_FILE,column_FileHandle);
	rbfm->scan(column_FileHandle,columnAttr,conditionAttribute,compOp,value,attributeNames,rbfmIterator);
	RID rid;
	void *retData=malloc(PAGE_SIZE);
memset(retData,0,PAGE_SIZE);
	vector<RID> toDel;
	while(rbfmIterator.getNextRecord(rid,retData)!=RBFM_EOF){
		toDel.push_back(rid);
	}
	RID toDelRID;
	for(int i=0;i<toDel.size();i++){
		toDelRID=toDel[i];
		FileHandle columnsFH;
		rbfm->openFile(COLUMNS_FILE,columnsFH);
		rbfm->deleteRecord(columnsFH,columnAttr,toDelRID);
		rbfm->closeFile(columnsFH);
	}
	rbfm->closeFile(table_FileHandle);
	rbfm->closeFile(column_FileHandle);
	rbfm->destroyFile(tableFileName);
	free(tableFileName);
	free(value);
	free(retData);
//delete the corresponding index entries of the table -proj4
deleteIndexOfTable(tabID);
	

	return 0;
}
//proj4
RC RelationManager::insertTupleToIndex(const string &tableName, int &tabId,
		const void* data, RID &rid, vector<Attribute> &attributes){
	vector<IndexSchema> index_det; //<<index-id, rid>, <attributename, filename> >
int nullBitSize=ceil((double)attributes.size()/CHAR_BIT);	
getIndexesOfTable(tabId, index_det);
	
	void* key_value = malloc(PAGE_SIZE);
	for(int i=0; i < index_det.size(); i++){
		string attr = index_det[i].second.first;
		string ixFileName = index_det[i].second.second;
		int offset = nullBitSize;
		for(int j=0; j < attributes.size(); j++){
			if(attributes[j].name == attr){
				if( ((char*)data)[j/8] & (1 << (7-(j%8))) ){// attribute is null
					break;
				}
                                switch (attributes[j].type){
  
				case TypeInt:
				{
					int k;
					memcpy(&k, (char*) data + offset, sizeof(int));
					offset += sizeof(int);
					memcpy((char*) key_value, &k, sizeof(int));
				}
				break;
				case TypeReal:
				{
					float k;
					memcpy(&k, (char*) data + offset, sizeof(float));
					offset += sizeof(float);
					memcpy((char*) key_value, &k, sizeof(float));
				}
				break;
				case TypeVarChar:
				{
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));
					offset += sizeof(int);
					char* str = (char*) malloc(PAGE_SIZE);
					memcpy(str, (char*) data + offset, len);
					offset += len;
					str[len] = '\0';
					memcpy((char*) key_value, &len, sizeof(int));
					memcpy((char*) key_value + sizeof(int), str, len);
					free(str);
				}
				break;
				}
				IXFileHandle indexFileHandle;
				ix->openFile(ixFileName, indexFileHandle);
//cout<<"calling insert"<<endl;
				ix->insertEntry(indexFileHandle, attributes[j], key_value, rid);
				ix->closeFile(indexFileHandle);
				break;
				
			}
			else{
				if( ((char*)data)[j/8] & (1 << (7-(j%8))) ){// attribute is null
					continue;
				}
				switch(attributes[j].type){
					case TypeInt:
				{
					offset += sizeof(int);
				}
 				break;
				case TypeReal:
				{
					offset += sizeof(float);
				}
				break;
				case TypeVarChar:				
				{
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));
					offset += sizeof(int) + len;
				}
				break;
				}
			}
		}
	}
	free(key_value);
}
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	int tabID;
	RID tableRID;
	bool sysTable = false;
	char *tableFileName=(char*)malloc(55); 
        memset(tableFileName,0,55);
	getIDOfTable(tableName,tabID,tableRID,sysTable,tableFileName);
	if(tabID<0 || sysTable){
		return -1;
	}
	FileHandle fileHandle;
	int returnVal = rbfm->openFile(tableFileName, fileHandle);
	if(returnVal < 0){
		free(tableFileName);
		return returnVal;
	}
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	RC rc=rbfm->insertRecord(fileHandle, attributes, data, rid);
	if(rc<0){
//cout<<"insert faile";
		free(tableFileName);
		return -1;
	}
	rc=rbfm->closeFile(fileHandle);
	free(tableFileName);
	if(rc<0){
		return -1;
	}
	
	

//insert tuple into the index as well
RID rid2;
	rid2.pageNum = rid.pageNum;
	rid2.slotNum = rid.slotNum;
cout<<"calling inserttoindex"<<endl;
	insertTupleToIndex(tableName, tabID, data, rid2, attributes);
return 0;
}

void RelationManager::deleteTupleFromIndex(int &tableId, vector<Attribute> &attributes, const void* recordData, RID &rid){
	vector<IndexSchema> index_det; //<<index-id, rid>, <attributename, filename> >
	getIndexesOfTable(tableId, index_det);
	int nullBitSize=ceil((double)attributes.size()/CHAR_BIT);
	void* key_value = malloc(PAGE_SIZE);
	for(int i=0; i < index_det.size(); i++){
		string attr = index_det[i].second.first;
		string ixFileName = index_det[i].second.second;
		int offset = nullBitSize;
		for(int j=0; j < attributes.size(); j++){
			if(attributes[j].name == attr){
				if( ((char*)recordData)[j/8] & (1 << (7-(j%8))) ){// attribute is null
					break;
				}
				if(attributes[j].type == TypeInt){
					int k;
					memcpy(&k, (char*) recordData + offset, sizeof(int));
					offset += sizeof(int);
					memcpy((char*) key_value, &k, sizeof(int));
				}
				else if(attributes[j].type == TypeReal){
					float k;
					memcpy(&k, (char*) recordData + offset, sizeof(float));
					offset += sizeof(float);
					memcpy((char*) key_value, &k, sizeof(float));
				}
				else{
					int len;
					memcpy(&len, (char*) recordData + offset, sizeof(int));
					offset += sizeof(int);
					char* str = (char*) malloc(PAGE_SIZE);
					memcpy(str, (char*) recordData + offset, len);
					offset += len;
					str[len] = '\0';
					memcpy((char*) key_value, &len, sizeof(int));
					memcpy((char*) key_value + sizeof(int), str, len);
					free(str);
				}
				IXFileHandle ixFileHandle;
				ix->openFile(ixFileName, ixFileHandle);
				RC ret = ix->deleteEntry(ixFileHandle, attributes[j], key_value, rid);
				ix->closeFile(ixFileHandle);
				break;
			}
			else{
				if( ((char*)recordData)[j/8] & (1 << (7-(j%8))) ){// attribute is null
					continue;
				}
				if(attributes[j].type == TypeInt){
					offset += sizeof(int);
				}
				else if(attributes[j].type == TypeReal){
					offset += sizeof(float);
				}
				else{
					int len;
					memcpy(&len, (char*) recordData + offset, sizeof(int));
					offset += sizeof(int) + len;
				}
			}
		}
	}
	free(key_value);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	int tabID;
	RID tableRID;
	bool sysTable = false;
	char *tableFileName=(char*)malloc(55); 
        memset(tableFileName,0,55);
	getIDOfTable(tableName,tabID,tableRID,sysTable,tableFileName);
	if(tabID < 0 || sysTable){
		return -1;
	}
	FileHandle fileHandle;
	int returnVal = rbfm->openFile(tableFileName, fileHandle);
	if(returnVal < 0){
		return returnVal;
	}
	void* recordData = malloc(PAGE_SIZE);
memset(recordData,0,PAGE_SIZE);
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	rbfm->readRecord(fileHandle, attributes, rid, recordData);
	RC ret=rbfm->deleteRecord(fileHandle, attributes, rid);
	rbfm->closeFile(fileHandle);
	free(tableFileName);
RID rid2;
	rid2.pageNum = rid.pageNum;
	rid2.slotNum = rid.slotNum;
deleteTupleFromIndex(tabID, attributes, recordData, rid2);
	free(recordData);
	return ret;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	int tabID;
	RID tableRID;
	bool sysTable = false;
	char *tableFileName=(char*)malloc(55); 
memset(tableFileName,0,55);
	getIDOfTable(tableName,tabID,tableRID,sysTable,tableFileName);
	FileHandle fileHandle;
	int returnVal = rbfm->openFile(tableFileName, fileHandle);
	if(returnVal < 0){
		return returnVal;
	}
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	void* recordData = malloc(PAGE_SIZE);
memset(recordData,0,PAGE_SIZE);
	rbfm->readRecord(fileHandle, attributes, rid, recordData);
	RC ret=rbfm->updateRecord(fileHandle, attributes, data, rid);
	rbfm->closeFile(fileHandle);
RID rid2;
	rid2.pageNum = rid.pageNum;
	rid2.slotNum = rid.slotNum;
	deleteTupleFromIndex(tabID, attributes, recordData, rid2);
	insertTupleToIndex(tableFileName, tabID, data, rid2, attributes);
	
	free(tableFileName);
	free(recordData);
	return ret;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
//cout<<"entering read tuple"<<endl;
	int tabID;
	RID tableRID;
	bool sysTable = false;
	char *tableFileName=(char*)malloc(55);
        memset(tableFileName,0,55);
//cout<<"before get id"<<endl;
	getIDOfTable(tableName,tabID,tableRID,sysTable,tableFileName);
	FileHandle fileHandle;
	int returnVal = rbfm->openFile(tableFileName, fileHandle);
	if(returnVal < 0){
		return returnVal;
	}
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	RC ret=rbfm->readRecord(fileHandle, attributes, rid, data);
	rbfm->closeFile(fileHandle);
	free(tableFileName);
	return ret;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	int tabID;
	RID tableRID;
	bool sysTable;
	char *tableFileName=(char*)malloc(55); 
memset(tableFileName,0,55);
	getIDOfTable(tableName,tabID,tableRID,sysTable,tableFileName);
	FileHandle fileHandle;
	int returnVal = rbfm->openFile(tableFileName, fileHandle);
	if(returnVal < 0){
		free(tableFileName);
		return returnVal;
	}
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	RC ret=rbfm->readAttribute(fileHandle, attributes, rid, attributeName, data);
	rbfm->closeFile(fileHandle);
	free(tableFileName);
	return ret;

}
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	int tabID;
	RID tableRID;
	bool sysTable;
	char *tableFileName=(char*)malloc(55); 
memset(tableFileName,0,55);
	getIDOfTable(tableName,tabID,tableRID,sysTable,tableFileName);
	vector<Attribute>recordDescriptor;
	getAttributes(tableName,recordDescriptor);
	FileHandle fileHandle;
	rbfm->openFile(tableName,fileHandle);
	RC ret=rbfm->scan(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfmScanIterator);
	free(tableFileName);
	return ret;
   
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
	if(rbfmScanIterator.getNextRecord(rid,data)==RBFM_EOF){
		return RM_EOF;
	}
	else{
		 
return 0;
	}
}

RC RM_ScanIterator::close(){
	rbfmScanIterator.close();
	return 0;
}

RC RelationManager::createColumnsRecDescr(){

	Attribute table_IdAttr;
	table_IdAttr.length = 4;
	table_IdAttr.name = "table-id";
	table_IdAttr.type = TypeInt;
	columnAttr.push_back(table_IdAttr);

	Attribute column_NameAttr;
	column_NameAttr.length = 50;
	column_NameAttr.name = "column-name";
	column_NameAttr.type = TypeVarChar;
	columnAttr.push_back(column_NameAttr);

	Attribute column_TypeAttr;
	column_TypeAttr.length = 4;
	column_TypeAttr.name = "column-type";
	column_TypeAttr.type = TypeInt;
	columnAttr.push_back(column_TypeAttr);

	Attribute column_LengthAttr;
	column_LengthAttr.length = 4;
	column_LengthAttr.name = "column-length";
	column_LengthAttr.type = TypeInt;
	columnAttr.push_back(column_LengthAttr);

	Attribute column_PositionAttr;
	column_PositionAttr.length = 4;
	column_PositionAttr.name = "column-position";
	column_PositionAttr.type = TypeInt;
	columnAttr.push_back(column_PositionAttr);

	Attribute sysTableA;
	sysTableA.length = 4;
	sysTableA.name = "system-table";
	sysTableA.type = TypeInt;
	columnAttr.push_back(sysTableA);

	return 0;
}

RC RelationManager::createTablesRecDescr(){
	Attribute table_IdAttr;
	table_IdAttr.length = 4;
	table_IdAttr.name = "table-id";
	table_IdAttr.type = TypeInt;
	tableAttr.push_back(table_IdAttr);

	Attribute table_NameA;
	table_NameA.length = 50;
	table_NameA.name = "table-name";
	table_NameA.type = TypeVarChar;
	tableAttr.push_back(table_NameA);

	Attribute fileNameA;
	fileNameA.length = 50;
	fileNameA.name = "file-name";
	fileNameA.type = TypeVarChar;
	tableAttr.push_back(fileNameA);

	Attribute sysTableA;
	sysTableA.length = 4;
	sysTableA.name = "system-table";
	sysTableA.type = TypeInt;
	tableAttr.push_back(sysTableA);

	return 0;
}
//proj 4
RC RelationManager::createIndexRecDescr(){
Attribute index_IdAttr;
	index_IdAttr.length = 4;
	index_IdAttr.name = "index-id";
	index_IdAttr.type = TypeInt;
	indexAttr.push_back(index_IdAttr);

	Attribute table_IdAttr;
	table_IdAttr.length = 4;
	table_IdAttr.name = "table-id";
	table_IdAttr.type = TypeInt;
	indexAttr.push_back(table_IdAttr);

	Attribute table_NameAttr;
	table_NameAttr.length = 50;
	table_NameAttr.name = "table-name";
	table_NameAttr.type = TypeVarChar;
	indexAttr.push_back(table_NameAttr);

	Attribute attrName;
	attrName.length = 50;
	attrName.name = "attribute-name";
	attrName.type = TypeVarChar;
	indexAttr.push_back(attrName);

	Attribute attrType;
	attrType.length = 4;
	attrType.name = "attribute-type";
	attrType.type = TypeInt;
	indexAttr.push_back(attrType);

	Attribute fileName;
	fileName.length = 50;
	fileName.name = "filename";
	fileName.type = TypeVarChar;
	indexAttr.push_back(fileName);



}
RC RelationManager::prepareTablesRecord(const void*data, int tableId, string tableName, string fileName, int sysTable){
	int nullBitByteSize = ceil((double)tableAttr.size()/8);
	char *nullBitIndicator = (char *) malloc(nullBitByteSize);
	memset(nullBitIndicator, 0, nullBitByteSize);

	memcpy((char*)data, nullBitIndicator, nullBitByteSize);

	int offset = nullBitByteSize;
	memcpy((char*)data + offset, &tableId, sizeof(int));
	offset += sizeof(int);

	int lengthOfTblName = tableName.length();
	memcpy((char*)data + offset, &lengthOfTblName, sizeof(int));
	offset += sizeof(int);

	char *str=(char*)malloc(PAGE_SIZE);
memset(str,0,PAGE_SIZE);
	for(int i=0;i<lengthOfTblName;i++){
		str[i]=tableName[i];
	}
	str[lengthOfTblName]='\0';
	memcpy((char*)data + offset, str, lengthOfTblName);
	offset += lengthOfTblName;

	int lengthOfFileName = fileName.length();
	memcpy((char*)data + offset, &lengthOfFileName, sizeof(int));
	offset += sizeof(int);

	for(int i=0;i<lengthOfFileName;i++){
		str[i]=fileName[i];
	}
	str[lengthOfFileName]='\0';

	memcpy((char*)data + offset, str, lengthOfFileName);
	offset += lengthOfFileName;

	free(str);

	memcpy((char*)data + offset, &sysTable, sizeof(int));
	free(nullBitIndicator);
	return 0;

}

RC RelationManager::prepareColumnsRecord(const void*data, int tableId, string columnName,
		AttrType columnType, int columnLength, int columnPosition, int sysTable){
	int nullBitByteSize = ceil((double)columnAttr.size()/8);
	char *nullBitIndicator = (char *) malloc(nullBitByteSize);
	memset(nullBitIndicator, 0, nullBitByteSize);

	memcpy((char*)data, nullBitIndicator, nullBitByteSize);

	int offset = nullBitByteSize;
	memcpy((char*)data + offset, &tableId, sizeof(int));
	offset += sizeof(int);

	int columnNameLength = columnName.length();
	memcpy((char*)data + offset, &columnNameLength, sizeof(int));
	offset += sizeof(int);

	char *str=(char*)malloc(PAGE_SIZE);
memset(str,0,PAGE_SIZE);
	for(int i=0;i<columnNameLength;i++){
		str[i]=columnName[i];
	}
	str[columnNameLength]='\0';

	memcpy((char*)data + offset, str, columnNameLength);
	offset += columnNameLength;

	memcpy((char*)data + offset, &columnType, sizeof(int));
	offset += sizeof(int);

	memcpy((char*)data + offset, &columnLength, sizeof(int));
	offset += sizeof(int);

	memcpy((char*)data + offset, &columnPosition, sizeof(int));
	offset += sizeof(int);

	memcpy((char*)data + offset, &sysTable, sizeof(int));
	offset += sizeof(int);
	free(str);
	free(nullBitIndicator);
	return 0;

}
void RelationManager::prepareIndexRecord(void* data, int indexId, int tableId, const string &tableName,
		const string &attributeName, AttrType attributeType, const string &fileName){
	int nullBitBytes = ceil((double) indexAttr.size() / CHAR_BIT);
	memset(data, 0, nullBitBytes);
	int offset = nullBitBytes;

	memcpy((char*) data + offset, &indexId, sizeof(int));
	offset += sizeof(int);

	memcpy((char*) data + offset, &tableId, sizeof(int));
	offset += sizeof(int);

	char* str = (char*) malloc(PAGE_SIZE);
	int len = tableName.size();
	for(int i = 0; i < len; i++){
		str[i] = tableName[i];
	}
	str[len] = '\0';
	memcpy((char*) data + offset, &len, sizeof(int));
	offset += sizeof(int);
	memcpy((char*) data + offset, str, len);
	offset += len;

	len = attributeName.size();
	for(int i = 0; i < len; i++){
		str[i] = attributeName[i];
	}
	str[len] = '\0';
	memcpy((char*) data + offset, &len, sizeof(int));
	offset += sizeof(int);
	memcpy((char*) data + offset, str, len);
	offset += len;

	memcpy((char*) data + offset, &attributeType, sizeof(int));
	offset += sizeof(int);

	len = fileName.size();
	for(int i = 0; i < len; i++){
		str[i] = fileName[i];
	}
	str[len] = '\0';
	memcpy((char*) data + offset, &len, sizeof(int));
	offset += sizeof(int);
	memcpy((char*) data + offset, str, len);
	offset += len;

	free(str);
}
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	int table_ID;
	RID tableRID;
	bool sysTab;
	char *tableFileName=(char*)malloc(55);
memset(tableFileName,0,55);
	getIDOfTable(tableName, table_ID, tableRID, sysTab, tableFileName);
	FileHandle column_FileHandle;
	rbfm->openFile(COLUMNS_FILE, column_FileHandle);
	RBFM_ScanIterator scanIterator;
	void* tableId_Data = malloc(sizeof(int)+1);
memset(tableId_Data,0,sizeof(int)+1);
	memcpy((char*)tableId_Data, &table_ID, sizeof(int));
	char *conditionAttribute="table-id";
	CompOp compOp=EQ_OP;
	vector<string> attributeNames;
	for (int var = 0; var < columnAttr.size(); ++var) {
		attributeNames.push_back(columnAttr[var].name);
	}
	rbfm->scan(column_FileHandle, columnAttr, conditionAttribute, compOp, tableId_Data, attributeNames, scanIterator);

	RID rid;
	void *data=malloc(PAGE_SIZE);
memset(data,0,PAGE_SIZE);
	
	while(scanIterator.getNextRecord(rid, data) != RBFM_EOF){
		unsigned nullBitByteSize = ceil((double)columnAttr.size()/8);
		int offset = nullBitByteSize;
		offset += sizeof(int);

		int column_NameLength;
		memcpy(&column_NameLength, (char*)data+offset, sizeof(int));
		offset += sizeof(int);

		char* columnName = (char*)malloc(column_NameLength+1);
		memcpy(columnName, (char*)data+offset, column_NameLength);
		columnName[column_NameLength]='\0';
		offset += column_NameLength;

		AttrType column_Type;
		memcpy(&column_Type, (char*)data+offset, sizeof(int));
		offset += sizeof(4);

		int column_Length;
		memcpy(&column_Length, (char*)data+offset, sizeof(int));

		Attribute attr;
		attr.name = columnName;
		attr.length = column_Length;
		attr.type = column_Type;
		attrs.push_back(attr);
		free(columnName);
	}
	rbfm->closeFile(column_FileHandle);
	free(tableId_Data);
	free(tableFileName);
	free(data);
    return 0;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName){
	int tableId;
	vector<Attribute> attributes;
	RID rid;
	bool systemTable;
	char* tableFileName = (char*) malloc(PAGE_SIZE);
	getIDOfTable(tableName, tableId, rid, systemTable, tableFileName);
	getAttributes(tableName, attributes);
	AttrType attrType;
	Attribute keyvalueAttr;
	for(int i=0; i < attributes.size(); i++){
		if(attributes[i].name == attributeName){
			attrType = attributes[i].type;
			keyvalueAttr.length = attributes[i].length;
			keyvalueAttr.name = attributes[i].name;
			keyvalueAttr.type = attributes[i].type;
			break;
		}
	}
	int nextIndexID;
	getNextIndexID(nextIndexID);
	void* indexTableRecord = malloc(PAGE_SIZE);
	string fileName = tableName + "_" + attributeName + ".idx";
	prepareIndexRecord(indexTableRecord, nextIndexID, tableId, tableName, attributeName, attrType, fileName);
	FileHandle indexFileHandle;
	rbfm->openFile(INDEX_TABLE_FILE, indexFileHandle);
	rbfm->insertRecord(indexFileHandle, indexAttr, indexTableRecord, rid);
	rbfm->closeFile(indexFileHandle);
	free(indexTableRecord);

	//now insert the entries of the table into the index
	ix->createFile(fileName);
	IXFileHandle ixFileHandle;
	ix->openFile(fileName, ixFileHandle);

	FileHandle tableFileHandle;
	rbfm->openFile(tableFileName, tableFileHandle);
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute = attributes[0].name;
	CompOp compOp = NO_OP;
	void* value = malloc(PAGE_SIZE);
	vector<string> attributeNames;
	attributeNames.push_back(attributeName);
	rbfm->scan(tableFileHandle, attributes, conditionAttribute, compOp, value, attributeNames, rbfmIterator);
	RID retRid;
	void* retData = malloc(PAGE_SIZE);
	void* key_value = malloc(PAGE_SIZE);
	while(rbfmIterator.getNextRecord(retRid, retData) != RBFM_EOF){
		if(attrType == TypeInt){
			memcpy((char*) key_value, (char*) retData + 1, sizeof(int));
		}
		else if(attrType == TypeReal){
			memcpy((char*) key_value, (char*) retData + 1, sizeof(float));
		}
		else{
			int len;
			memcpy(&len, (char*) retData + 1, sizeof(int));
			memcpy((char*) key_value, &len, sizeof(int));
			memcpy((char*) key_value + sizeof(int), (char*) retData + 1 + sizeof(int), len);
		}
//cout<<"calling insert entry"<<endl;
		ix->insertEntry(ixFileHandle, keyvalueAttr, key_value, retRid);
ix->printBtree(ixFileHandle,keyvalueAttr);
	}
	ix->closeFile(ixFileHandle);
	rbfm->closeFile(tableFileHandle);
	free(value);
	free(retData);
	free(key_value);
	free(tableFileName);
	return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName){
	int tableId;
	RID tableRid;
	bool systemTable;
	char* tableFileName = (char*) malloc(PAGE_SIZE);
	getIDOfTable(tableName, tableId, tableRid, systemTable, tableFileName);
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	FileHandle indexFileHandle;
	rbfm->openFile(INDEX_TABLE_FILE, indexFileHandle);
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute = "table-id";
	CompOp compOp = EQ_OP;
	void* value = malloc(PAGE_SIZE);
	memcpy((char*) value, &tableId, sizeof(int));
	vector<string> attributeNames;
	attributeNames.push_back("attribute-name");
	attributeNames.push_back("filename");
	rbfm->scan(indexFileHandle, indexAttr, conditionAttribute, compOp, value, attributeNames, rbfmIterator);
	RID retRid;
	void* retData = malloc(PAGE_SIZE);
	RID toDeleteRid;
	toDeleteRid.pageNum = -1;
	while(rbfmIterator.getNextRecord(retRid, retData) != RBFM_EOF){
		int len;
		memcpy(&len, (char*) retData + 1, sizeof(int));
		char* attrName = (char*) malloc(100);
		memcpy(attrName, (char*) retData + 1 + sizeof(int), len);
		attrName[len] = '\0';
		string ixAttrName(attrName);
		free(attrName);
		if(attributeName == ixAttrName){
			int offset = 1 + sizeof(int) + len;
			int slen;
			memcpy(&slen, (char*) retData + offset, sizeof(int));
			offset += sizeof(int);
			char* fname = (char*) malloc(100);
			memcpy(fname, (char*) retData + offset, slen);
			fname[slen] = '\0';
			string ixFileName(fname);
			ix->destroyFile(ixFileName);
			toDeleteRid.pageNum = retRid.pageNum;
			toDeleteRid.slotNum = retRid.slotNum;
			free(fname);
			break;
		}
	}
	rbfmIterator.close();
	RC ret;
	if(toDeleteRid.pageNum != -1){
		ret = rbfm->deleteRecord(indexFileHandle, indexAttr, toDeleteRid);
	}
	else{
		ret = -1;
	}
	rbfm->closeFile(indexFileHandle);
	return ret;
}

RC RelationManager::indexScan(const string &tableName, const string &attributeName, const void* lowkey_value, const void* highkey_value,
		bool lowkey_valueInclusive, bool highkey_valueInclusive, RM_IndexScanIterator &rm_IndexScanIterator){
	int tabId;
	RID rid;
	bool systemTable;
	char* tableFileName = (char*) malloc(100);
	getIDOfTable(tableName, tabId, rid, systemTable, tableFileName);
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	Attribute attr;
	for(int i = 0; i < attributes.size(); i++){
		if(attributes[i].name == attributeName){
			attr.length = attributes[i].length;
			attr.name = attributes[i].name;
			attr.type = attributes[i].type;
		}
	}
	FileHandle indexFileHandle;
	rbfm->openFile(INDEX_TABLE_FILE, indexFileHandle);
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute = "table-id";
	CompOp compOp = EQ_OP;
	void* value = malloc(PAGE_SIZE);
	memcpy((char*) value, &tabId, sizeof(int));
	vector<string> attributeNames;
	attributeNames.push_back("attribute-name");
	attributeNames.push_back("filename");
//cout<<"beofre rbfm scan"<<endl;
	rbfm->scan(indexFileHandle, indexAttr, conditionAttribute, compOp, value, attributeNames, rbfmIterator);
	RID retRid;
	void* retData = malloc(PAGE_SIZE);
	string indexFileName = "";
	while(rbfmIterator.getNextRecord(retRid, retData) != RBFM_EOF){
		int len;
		memcpy(&len, (char*) retData + 1, sizeof(int));
		char* attrName = (char*) malloc(100);
		memcpy(attrName, (char*) retData + 1 + sizeof(int), len);
		attrName[len] = '\0';
		string ixAttrName(attrName);
		free(attrName);
		if(attributeName == ixAttrName){
			int offset = 1 + sizeof(int) + len;
			int slen;
			memcpy(&slen, (char*) retData + offset, sizeof(int));
			offset += sizeof(int);
			char* fname = (char*) malloc(100);
			memcpy(fname, (char*) retData + offset, slen);
			fname[slen] = '\0';
			string ixFileName(fname);
			indexFileName = ixFileName;
			free(fname);
			break;
		}
	}
	IXFileHandle ixFileHandle;
	ix->openFile(indexFileName, ixFileHandle);
//cout<<"before ix scan"<<endl;
	ix->scan(ixFileHandle, attr, lowkey_value, highkey_value, lowkey_valueInclusive,
			highkey_valueInclusive, rm_IndexScanIterator.ixScanIterator);
//cout<<"after ix scan"<<endl;
	rbfm->closeFile(indexFileHandle);
//ix->closeFile(ixFileHandle);
	free(tableFileName);
	free(value);
	free(retData);
	return 0;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void* key_value){
//cout<<"enterind rm scan"<<endl;
	  if(ixScanIterator.getNextEntry(rid,key_value) != IX_EOF){
//cout<<"completing ix scan"<<endl;
		  return 0;
	  }
	  return RM_EOF;
}

RC RM_IndexScanIterator::close(){
	  ixScanIterator.close();
	  indexManager->closeFile(ixScanIterator.ixfileHandle);
	  return 0;
}
