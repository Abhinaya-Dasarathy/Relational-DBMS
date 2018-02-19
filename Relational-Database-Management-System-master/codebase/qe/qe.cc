#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cmath>
#include "qe.h"
#include <map>

using namespace std;
//filter implementation

RC Filter::getNextTuple(void *data){
	if(condition.lhsAttr.size() == 0)//if the lhs of where condition size is 0
	{
cout<<"entering eof1"<<endl;
        		return QE_EOF;
        	}
    	bool condSatisfied = false;
        	do{
        		RC returnVal = input->getNextTuple(data);
cout<<"got next tuple"<<endl;
				if(returnVal == RM_EOF){
cout<<"entering eof"<<endl;
					return QE_EOF;
				}
				//check if the condition gets satisfied
				vector<Attribute> attrs;
			getAttributes(attrs);
			int numOfAttributes = attrs.size();
			int nullBitBytes = ceil((double)numOfAttributes / CHAR_BIT);//get the number of null bits
			int dataOffset = nullBitBytes;
			if(!condition.bRhsIsAttr){ // if the right side of condition is not an attribute and is a value,
cout<<"entering correctly"<<endl;
				for (int i = 0; i < numOfAttributes; ++i) {
					Attribute attribute_i = attrs[i];//get all the attributes in the index and check if the lhs attribute is same as the attribute name to be checked

					bool null = ((char*)data)[i / 8] & ( 1 << ( 7 - ( i % 8 )));//check if the attribute value is empty
					//if this attribute is the lhs attribute
					if(attribute_i.name == condition.lhsAttr){
						//if the rhs value in condition is null or the lhs attribute is null in the input data
						if(condition.rhsValue.data == NULL && null){
							condSatisfied = true;
						}
						else 
						switch(condition.rhsValue.type){
							case TypeInt:{
							int valueInData;
							memcpy(&valueInData, data + dataOffset, sizeof(int));
							int valueInrhs;
							memcpy(&valueInrhs, condition.rhsValue.data, sizeof(int));
							if(condition.op == EQ_OP){
								condSatisfied = valueInData == valueInrhs;
							}else if(condition.op == LT_OP){
								condSatisfied = valueInData < valueInrhs;
							}else if(condition.op == LE_OP){
								condSatisfied = valueInData <= valueInrhs;
							}else if(condition.op == GT_OP){
								condSatisfied = valueInData > valueInrhs;
							}else if(condition.op == GE_OP){
								condSatisfied = valueInData >= valueInrhs;
							}else if(condition.op == NE_OP){
								condSatisfied = valueInData != valueInrhs;
							}else if(condition.op == NO_OP){
								condSatisfied = true;
							}
							}break;
							case TypeReal:{
							float valueInData;
							memcpy(&valueInData, data + dataOffset, sizeof(float));
							float valueInrhs;
							memcpy(&valueInrhs, condition.rhsValue.data, sizeof(float));
							if(condition.op == EQ_OP){
								condSatisfied = valueInData == valueInrhs;
							}else if(condition.op == LT_OP){
								condSatisfied = valueInData < valueInrhs;
							}else if(condition.op == LE_OP){
								condSatisfied = valueInData <= valueInrhs;
							}else if(condition.op == GT_OP){
								condSatisfied = valueInData > valueInrhs;
							}else if(condition.op == GE_OP){
								condSatisfied = valueInData >= valueInrhs;
							}else if(condition.op == NE_OP){
								condSatisfied = valueInData != valueInrhs;
							}else if(condition.op == NO_OP){
								condSatisfied = true;
							}
							}break;
							case TypeVarChar:{
							int length;
							memcpy(&length, data + dataOffset, sizeof(int));
							char * charValue = (char*)malloc(PAGE_SIZE);
							memcpy(charValue, data + dataOffset + sizeof(int), length);
							charValue[length] = '\0';
							string valueInData = string(charValue);

							int charightLenInCond;
							memcpy(&charightLenInCond, condition.rhsValue.data, sizeof(int));
							char* charInCond = (char*) malloc(PAGE_SIZE);
							memcpy(charInCond, (char*) condition.rhsValue.data + sizeof(int), charightLenInCond);
							charInCond[charightLenInCond] = '\0';
							string valueInrhs = string(charInCond);

							if(condition.op == EQ_OP){
								condSatisfied = valueInData == valueInrhs;
							}else if(condition.op == LT_OP){
								condSatisfied = valueInData < valueInrhs;
							}else if(condition.op == LE_OP){
								condSatisfied = valueInData <= valueInrhs;
							}else if(condition.op == GT_OP){
								condSatisfied = valueInData > valueInrhs;
							}else if(condition.op == GE_OP){
								condSatisfied = valueInData >= valueInrhs;
							}else if(condition.op == NE_OP){
								condSatisfied = valueInData != valueInrhs;
							}else if(condition.op == NO_OP){
								condSatisfied = true;
							}
							
							free(charValue);
							free(charInCond);
							}break;
						}
					}else{
						//if the attribute is not the given lhs attribute in condition skip
						if(null)//if the attribute value is null in the input data
						{
							//don't update anything
						}else if(attrs[i].type == TypeInt){
							dataOffset += sizeof(int);
						}
						else if(attrs[i].type == TypeReal){
							dataOffset += sizeof(float);
						}
						else{
							int len;
							memcpy(&len, (char*) data + dataOffset, sizeof(int));
							dataOffset += sizeof(int) + len;
						}
					}
				}
			}
			else{ //if rhs is also an attribute
				
				void* lhsValue = NULL;
				void* rhsValue = NULL;
				AttrType lhsAttrType, rhsAttrType;
				int dataOffset2 = nullBitBytes;
				for(int i = 0; i < attrs.size(); i++){
					if(attrs[i].name == condition.lhsAttr){
						lhsAttrType = attrs[i].type;
						if( ((char*)data)[i/8] & ( 1 << (7 - (i%8) ) ) ){
							lhsValue = NULL;
						}
						lhsValue = malloc(PAGE_SIZE);
						switch(attrs[i].type){
							case TypeInt:{
							int val;
							memcpy(&val, (char*) data + dataOffset2, sizeof(int));
							dataOffset2 += sizeof(int);
							memcpy((char*) lhsValue, &val, sizeof(int));
							}break;
							case TypeReal:{
							float val;
							memcpy(&val, (char*) data + dataOffset2, sizeof(float));
							dataOffset2 += sizeof(float);
							memcpy((char*) lhsValue, &val, sizeof(float));
							}break;
							case TypeVarChar:{
							int len;
							memcpy(&len,(char*) data + dataOffset2, sizeof(int));
							memcpy((char*) lhsValue, (char*) data + dataOffset2, sizeof(int) + len);
							dataOffset2 += sizeof(int) + len;
							}break;
						}
					}
					else{
						if( ((char*)data)[i/8] & ( (1 << (7 - (i%8) ) ) ) ){
							continue;
						}
						else if(attrs[i].type == TypeInt){
							dataOffset2 += sizeof(int);
						}
						else if(attrs[i].type == TypeReal){
							dataOffset2 += sizeof(float);
						}
						else{
							int len;
							memcpy(&len, (char*) data + dataOffset2, sizeof(int));
							dataOffset2 += sizeof(int) + len;
						}
					}
				}
				//Now get the RHS value similarly
				int dataOffset3 = nullBitBytes;
				for(int i = 0; i < attrs.size(); i++){
					if(attrs[i].name == condition.rhsAttr){
						rhsAttrType = attrs[i].type;
						if( ((char*)data)[i/8] & ( 1 << (7 - (i%8) ) ) ){
							rhsValue = NULL;
						}
						rhsValue = malloc(PAGE_SIZE);
						switch(attrs[i].type){
							case TypeInt:{
							int val;
							memcpy(&val, (char*) data + dataOffset3, sizeof(int));
							dataOffset3 += sizeof(int);
							memcpy((char*) rhsValue, &val, sizeof(int));
							}break;
							case TypeReal:{
							float val;
							memcpy(&val, (char*) data + dataOffset3, sizeof(float));
							dataOffset3 += sizeof(float);
							memcpy((char*) rhsValue, &val, sizeof(float));
							}break;
							case TypeVarChar:{
							int len;
							memcpy(&len,(char*) data + dataOffset3, sizeof(int));
							memcpy((char*) rhsValue, (char*) data + dataOffset3, sizeof(int) + len);
							dataOffset3 += sizeof(int) + len;
							}break;
						}	
					}
					else{
						if( ((char*)data)[i/8] & ( (1 << (7 - (i%8) ) ) ) ){
							continue;
						}
						else if(attrs[i].type == TypeInt){
							dataOffset3 += sizeof(int);
						}
						else if(attrs[i].type == TypeReal){
							dataOffset3 += sizeof(float);
						}
						else{
							int len;
							memcpy(&len, (char*) data + dataOffset2, sizeof(int));
							dataOffset3 += sizeof(int) + len;
						}
					}
				}
				//if the left of cond value is 0
				if(lhsValue == NULL){
					condSatisfied = false;
				}
				else if(lhsAttrType != rhsAttrType){
					condSatisfied = true;
				}
				switch(lhsAttrType){
					case TypeInt:{
					int leftCondValue,rightCondValue;
					memcpy(&leftCondValue, (char*) lhsValue, sizeof(int));
					memcpy(&rightCondValue, (char*) rhsValue, sizeof(int));
					if(condition.op == EQ_OP){
						condSatisfied = (leftCondValue == rightCondValue);
					}
					else if(condition.op == LT_OP){
						condSatisfied = (leftCondValue < rightCondValue);
					}
					else if(condition.op == LE_OP){
						condSatisfied = (leftCondValue <= rightCondValue);
					}
					else if(condition.op == GT_OP){
						condSatisfied = (leftCondValue > rightCondValue);
					}
					else if(condition.op == GE_OP){
						condSatisfied = (leftCondValue >= rightCondValue);
					}
					else if(condition.op == NE_OP){
						condSatisfied = (leftCondValue != rightCondValue);
					}
					else if(condition.op == NO_OP){
						condSatisfied = true;
					}
					}break;
					case TypeReal:{
					float leftCondValue,rightCondValue;
					memcpy(&leftCondValue, (char*) lhsValue, sizeof(float));
					memcpy(&rightCondValue, (char*) rhsValue, sizeof(float));
					if(condition.op == EQ_OP){
						condSatisfied = (leftCondValue == rightCondValue);
					}
					else if(condition.op == LT_OP){
						condSatisfied = (leftCondValue < rightCondValue);
					}
					else if(condition.op == LE_OP){
						condSatisfied = (leftCondValue <= rightCondValue);
					}
					else if(condition.op == GT_OP){
						condSatisfied = (leftCondValue > rightCondValue);
					}
					else if(condition.op == GE_OP){
						condSatisfied = (leftCondValue >= rightCondValue);
					}
					else if(condition.op == NE_OP){
						condSatisfied = (leftCondValue != rightCondValue);
					}
					else if(condition.op == NO_OP){
						condSatisfied = true;
					}
					}break;
					case TypeVarChar:{
					char* leftStr = (char*) malloc(PAGE_SIZE);
					char* rightStr = (char*) malloc(PAGE_SIZE);
					int leftLen, rightLen;
					memcpy(&leftLen, (char*) lhsValue, sizeof(int));
					memcpy(&rightLen, (char*) rhsValue, sizeof(int));
					memcpy(leftStr, (char*) lhsValue + sizeof(int), leftLen);
					memcpy(rightStr, (char*) rhsValue + sizeof(int), rightLen);
					leftStr[leftLen] = '\0';
					rightStr[rightLen] = '\0';
					string leftCondValue(leftStr);
					string rightCondValue(rightStr);
					
					if(condition.op == EQ_OP){
						condSatisfied = (leftCondValue == rightCondValue);
					}
					else if(condition.op == LT_OP){
						condSatisfied = (leftCondValue < rightCondValue);
					}
					else if(condition.op == LE_OP){
						condSatisfied = (leftCondValue <= rightCondValue);
					}
					else if(condition.op == GT_OP){
						condSatisfied = (leftCondValue > rightCondValue);
					}
					else if(condition.op == GE_OP){
						condSatisfied = (leftCondValue >= rightCondValue);
					}
					else if(condition.op == NE_OP){
						condSatisfied = (leftCondValue != rightCondValue);
					}
					else if(condition.op == NO_OP){
						condSatisfied = true;
					}
					free(leftStr);
					free(rightStr);
					}break;
				}
			}
        	}while(condSatisfied == false);
        	return 0;
}

void Filter::getAttributes(vector<Attribute> &attrs) const{
        	input->getAttributes(attrs);
        
}

//BNL Join

BNLJoin::BNLJoin(Iterator *leftIn,TableScan *rightIn,const Condition &condition,const unsigned numPages){
//copy the inputs         	
this->leftIn = leftIn;
this->condition = condition;
        	this->numPages = numPages;
        	this->rightIn = rightIn;
        	leftIn->getAttributes(leftTableAttribute);
        	rightIn->getAttributes(rightTableAttribute);
        	RC returnCode = populateHash();

}
//gives the tuple size given the data of tuple and the attribute vector
unsigned BNLJoin::getSize(void * data, vector<Attribute> attrs){
        	int nullBitSize = ceil((double)attrs.size() / CHAR_BIT);
        	unsigned offset = nullBitSize;
        	for (int i = 0; i< attrs.size(); ++i) {
        		Attribute attr = attrs[i];
        		bool null = ((char*)data)[i / 8] & ( 1 << ( 7 - ( i % 8 )));
        		if(null){
        			//don't update offset
        		}else if(attr.type == TypeInt || attr.type == TypeReal){
        			offset += sizeof(int);
				}else if(attr.type == TypeVarChar){
					int length;
					memcpy(&length, data + offset, sizeof(int));
					offset += sizeof(int) + length;
				}
			}
        	return offset;
}

RC BNLJoin::populateHash(){

        	//emptying the hash table
        	for(std::map<int, vector<void*> >::iterator iterator1 = mapIntegers.begin(); iterator1 != mapIntegers.end(); ++iterator1){
        		vector<void*> tuples = iterator1->second;
        		for (int i = 0; i < tuples.size(); ++i) {
					free(tuples[i]);
				}
        	}
        	for(std::map<float, vector<void*> >::iterator iterator2 = mapFloat.begin(); iterator2 != mapFloat.end(); ++iterator2){
        		vector<void*> tuples = iterator2->second;
				for (int i = 0; i < tuples.size(); ++i) {
					free(tuples[i]);
				}
        	}
        	for(std::map<string, vector<void*> >::iterator iterator3 = mapString.begin(); iterator3 != mapString.end(); ++iterator3){
        		vector<void*> tuples = iterator3->second;
				for (int i = 0; i < tuples.size(); ++i) {
					free(tuples[i]);
				}
			}
mapFloat.clear();
        	mapString.clear();

        	mapIntegers.clear();
        	
        	sizeOfHashTable = 0;
//check if the left table is finished        	
if(isLeftDone){
        		return QE_EOF;
        	}
        	bool isHashTableFull = false;
        	do{
        		void* leftTableData = malloc(PAGE_SIZE);
				RC retVal = leftIn->getNextTuple(leftTableData);
				unsigned leftTupleSize = getSize(leftTableData, leftTableAttribute);
				if(retVal == QE_EOF){
					isLeftDone = true;
					if(mapIntegers.size() > 0 || mapFloat.size() > 0 || mapString.size() > 0){
						free(leftTableData);
						return 0;
					}else{
						free(leftTableData);
						return QE_EOF;
					}
				}


				int leftnullBitSize = ceil((double)leftTableAttribute.size() / CHAR_BIT);
				int leftOffset = leftnullBitSize;

				for (int i = 0; i < leftTableAttribute.size(); ++i) {
					bool null = ((char*)leftTableData)[i / 8] & ( 1 << ( 7 - ( i % 8 )));
					Attribute attrLeft = leftTableAttribute[i];
					if(attrLeft.name == condition.lhsAttr){
						if(null){
							//don't populate the map with null keys
							break;
						}
						switch(attrLeft.type){
							case TypeInt:{
							int value;
							memcpy(&value, leftTableData + leftOffset, sizeof(int));
							std::map<int, vector<void*> >::iterator iterator = mapIntegers.find(value);
							if(iterator == mapIntegers.end()){
							//the attribute value not already present in the hashtable , then add the lefttable data as a fresh entry to it.
								vector<void*> tuples;
								tuples.push_back(leftTableData);
								mapIntegers.insert(pair<int, vector<void*> >(value, tuples));
							}else{
                                                        //the attribute value present in the hashtable, then get the already present tuple associated with it and add the new one to it.
								vector<void*> tuples = iterator->second;
								tuples.push_back(leftTableData);
								mapIntegers.insert(pair<int, vector<void*> >(value, tuples));
							}
							sizeOfHashTable += leftTupleSize;
							//check if the hash table size has not crossed the buffer size given as input to BNL
							if(sizeOfHashTable >= PAGE_SIZE * numPages){
								isHashTableFull  = true;
							}

							}break;
							case TypeReal:{
							float value;
							memcpy(&value, leftTableData + leftOffset, sizeof(float));

							std::map<float, vector<void*> >::iterator iterator = mapFloat.find(value);
							if(iterator == mapFloat.end()){
								vector<void*> tuples;
								tuples.push_back(leftTableData);
								mapFloat.insert(pair<float, vector<void*> >(value, tuples));
							}else{
								vector<void*> tuples = iterator->second;
								tuples.push_back(leftTableData);
								mapFloat.insert(pair<float, vector<void*> >(value, tuples));
							}

							sizeOfHashTable += leftTupleSize;
							if(sizeOfHashTable >= PAGE_SIZE * numPages){
								isHashTableFull  = true;
							}
							}break;
							case TypeVarChar:{

							int length;
							memcpy(&length, leftTableData + leftOffset, sizeof(int));
							char* characterValue = (char*)malloc(PAGE_SIZE);
							memcpy(characterValue, leftTableData + leftOffset + sizeof(int), length);
							characterValue[length] = '\0';

							string value = string(characterValue);

							std::map<string, vector<void*> >::iterator iterator = mapString.find(value);
							if(iterator == mapString.end()){
								vector<void*> tuples;
								tuples.push_back(leftTableData);
								mapString.insert(pair<string, vector<void*> >(value, tuples));
							}else{
								vector<void*> tuples = iterator->second;
								tuples.push_back(leftTableData);
								mapString.insert(pair<string, vector<void*> >(value, tuples));
							}

							sizeOfHashTable += leftTupleSize;
							if(sizeOfHashTable >= PAGE_SIZE * numPages){
								isHashTableFull  = true;
							}
							free(characterValue);
							}break;
						}
						break;
					}else{
						//go to next attribute if this is not the attribute in the join condition
						if(null){
							
						}else if(attrLeft.type == TypeInt || attrLeft.type == TypeReal){
							leftOffset += sizeof(int);
						}else if(attrLeft.type == TypeVarChar){
							int length;
							memcpy(&length, leftTableData + leftOffset, sizeof(int));
							leftOffset += sizeof(int) + length;
						}
					}
				}
				

        	}while(isHashTableFull);
        	return 0;
}
RC BNLJoin::joinTuples(void* leftData, void* rightData, void* outputData,
		vector<Attribute> leftTableAttribute, vector<Attribute> rightTableAttribute){
	int totalAttributes = leftTableAttribute.size() + rightTableAttribute.size();
	int totalNullBitBytes = ceil((double)totalAttributes/CHAR_BIT);
	int leftNullBitBytes = ceil((double)leftTableAttribute.size()/CHAR_BIT);
	int rightNullBitBytes= ceil((double)rightTableAttribute.size()/CHAR_BIT);
	int leftOffset = leftNullBitBytes;
	int rightOffset = rightNullBitBytes;
	unsigned char* nullBitBytes = (unsigned char*) malloc(totalNullBitBytes);
	memset(nullBitBytes, 0 , totalNullBitBytes);
	//find length of left tuple
	for(int i = 0; i < leftTableAttribute.size(); i++){
		if( ((char*)leftData)[i/8] & ( 1 << (7 - (i%8) ) ) ){
			nullBitBytes[i/8] = nullBitBytes[i/8] | (1 << (7 - (i%8)));
			continue;
		}
		if(leftTableAttribute[i].type == TypeInt){
			leftOffset += sizeof(int);
		}
		else if(leftTableAttribute[i].type == TypeReal){
			leftOffset += sizeof(float);
		}
		else{
			int length;
			memcpy(&length, (char*) leftData + leftOffset, sizeof(int));
			leftOffset += sizeof(int) + length;
		}
	}
	//find length of right tuple
	for(int i = 0; i < rightTableAttribute.size(); i++){
		if( ((char*)rightData)[i/8] & ( 1 << (7 - (i%8) ) ) ){
//copy all the null bit bytes to nullBitBytes 			
	nullBitBytes[(leftTableAttribute.size() + i)/8] =
					nullBitBytes[(leftTableAttribute.size() + i)/8] | (1 << (7 - ((leftTableAttribute.size() + i)%8)));
			continue;
		}
		if(rightTableAttribute[i].type == TypeInt){
			rightOffset += sizeof(int);
		}
		else if(rightTableAttribute[i].type == TypeReal){
			rightOffset += sizeof(float);
		}
		else{
			int length;
			memcpy(&length, (char*) rightData + rightOffset, sizeof(int));
			rightOffset += sizeof(int) + length;
		}
	}
	//copy the entire null bits and then copy the left tuple data and then right tuple data
	int offset_JoinedData = 0;
	memcpy((char*) outputData + offset_JoinedData, nullBitBytes, totalNullBitBytes);
	offset_JoinedData += totalNullBitBytes;
	memcpy((char*) outputData + offset_JoinedData, (char*) leftData + leftNullBitBytes, leftOffset - leftNullBitBytes);
	offset_JoinedData += leftOffset - leftNullBitBytes;
	memcpy((char*) outputData + offset_JoinedData, (char*) rightData + rightNullBitBytes, rightOffset - rightNullBitBytes);
	free(nullBitBytes);
	return 0;
}

RC BNLJoin::getNextTuple(void *data){
bool isMatching = false;
        	if(leftTupleMatch.size() > leftTupleMatchIndex){
        		void* leftTableData = leftTupleMatch[leftTupleMatchIndex];
        		leftTupleMatchIndex ++;
        		joinTuples(leftTableData, rightTableData, data, leftTableAttribute, rightTableAttribute);
        		return 0;
        	}
        	
        	do{
        		void* rightTableData = malloc(PAGE_SIZE);
				RC returnVal2 = rightIn->getNextTuple(rightTableData);
				if(returnVal2 == QE_EOF){
					RC retVal = populateHash();
					if(retVal == QE_EOF){
						free(rightTableData);
						return QE_EOF;
					}else{
						
						rightIn->setIterator(); //a function of TableScan-set iterator of right table so that we loop through it from start again
					}


				}
				int right_nullBitBytes = ceil((double)rightTableAttribute.size() / CHAR_BIT);
				int right_offset = right_nullBitBytes;
				for (int i = 0; i < rightTableAttribute.size(); ++i) {
					Attribute attr = rightTableAttribute[i];
					bool null = ((char*)rightTableData)[i / 8] & ( 1 << ( 7 - ( i % 8 )));
					if(condition.rhsAttr == attr.name){
						if(null){
							isMatching = false;
							//the attribute value is null, so there can't be any match
							break;
						}
						if(attr.type == TypeInt){
							int value;
							memcpy(&value, rightTableData + right_offset, sizeof(int));
							std::map<int, vector<void*> >::iterator iterator = mapIntegers.find(value);
							if(iterator != mapIntegers.end()){
								isMatching = true;
								vector<void*> tuples = iterator->second;
								if(tuples.size() > 1){
									leftTupleMatch = tuples;
									leftTupleMatchIndex = 1;
									memcpy(this->rightTableData, rightTableData, PAGE_SIZE);
								}
								void* leftTuple = tuples[0];
								joinTuples(leftTuple, rightTableData, data, leftTableAttribute, rightTableAttribute);
								break;
							}else{
								isMatching = false;
								break;
							}
						}else if(attr.type == TypeReal){
							float value;
							memcpy(&value, rightTableData + right_offset, sizeof(float));
							std::map<float, vector<void*> >::iterator iterator = mapFloat.find(value);
							if(iterator != mapFloat.end()){
								isMatching = true;
								vector<void*> tuples = iterator->second;
								if(tuples.size() > 1){
									leftTupleMatch = tuples;
									leftTupleMatchIndex = 1;
									memcpy(this->rightTableData, rightTableData, PAGE_SIZE);
								}
								void* leftTuple = tuples[0];
								joinTuples(leftTuple, rightTableData, data, leftTableAttribute, rightTableAttribute);
								break;
							}else{
								isMatching = false;
								break;
							}
						}else if(attr.type == TypeVarChar){

							char* characterValue = (char*)malloc(PAGE_SIZE);
							int length;
							memcpy(&length, rightTableData + right_offset, sizeof(int));
							memcpy(characterValue, rightTableData + right_offset + sizeof(int), length);
							characterValue[length] = '\0';
							string value = string(characterValue);
							free(characterValue);

							std::map<string, vector<void*> >::iterator iterator = mapString.find(value);
							if(iterator != mapString.end()){
								isMatching = true;
								vector<void*> tuples = iterator->second;
								if(tuples.size() > 1){
									leftTupleMatch = tuples;
									leftTupleMatchIndex = 1;
									memcpy(this->rightTableData, rightTableData, PAGE_SIZE);
								}
								void* leftTuple = tuples[0];
								joinTuples(leftTuple, rightTableData, data, leftTableAttribute, rightTableAttribute);
								break;
							}else{
								isMatching = false;
								break;
							}
						}
					}else{
						if(null){
							
						}else if(attr.type == TypeInt || attr.type == TypeReal){
							right_offset += sizeof(int);
						}else if(attr.type == TypeVarChar){
							int length;
							memcpy(&length, rightTableData + right_offset, sizeof(int));
							right_offset += sizeof(int) + length;
						}
					}
				}

				free(rightTableData);
        	}while(!isMatching);
        	return 0;
}
// For attribute in vector<Attribute>, name it as rel.attr
void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
        	for (int i = 0; i < leftTableAttribute.size(); ++i) {
        		attrs.push_back(leftTableAttribute[i]);
        	}
        	for(int i = 0; i < rightTableAttribute.size(); ++i){
        		attrs.push_back(rightTableAttribute[i]);
        	}
}

//PROJECT OPERATOR
void Project::getAttributes(vector<Attribute> &attrs) const{
        	vector<Attribute> inAttributes;
        	input->getAttributes(inAttributes);
        	for (int i = 0; i < attrNames.size(); ++i) {
        		string attributeName = attrNames[i];
        		for (int j = 0; j < inAttributes.size(); ++j) {
					Attribute attr1 = inAttributes[j];
					if(attr1.name == attributeName){
						attrs.push_back(attr1);
						break;
					}
				}
			}
        
}
RC Project::getNextTuple(void *data) {
        	char* readData = (char*)malloc(PAGE_SIZE);
        	RC retVal = input->getNextTuple(readData);
        	if(retVal == QE_EOF){
        		free(readData);
        		return QE_EOF;
        	}
		//input data
		vector<Attribute> attrs;
        	input->getAttributes(attrs);
        	int numOfAttributes = attrs.size();
		int readDataNullBitBytes = ceil((double)numOfAttributes / CHAR_BIT);
		//output data
		int writeNumAttr = attrNames.size();
        	int writeNullSize = ceil((double)writeNumAttr / CHAR_BIT);
        	int writedataOffset = writeNullSize;
        	char* writeDataNullBit = (char*)malloc(writeNullSize);
        	memset(writeDataNullBit,0,writeNullSize); 

        	
        	
memcpy(data,writeDataNullBit, writeNullSize);

        	for (int i = 0; i < writeNumAttr; ++i) {
        		string attribute_i = attrNames[i];//attribute to be read
        		
        		int readDataOffset = readDataNullBitBytes;
        		for (int j = 0; j < numOfAttributes; ++j) {
        			Attribute attr = attrs[j];
					bool null = ((char*)readData)[j / 8] & ( 1 << ( 7 - ( j % 8 )));
					if(attr.name == attribute_i){
						if(null){
							writeDataNullBit[j / 8] = writeDataNullBit[j / 8] | (1 << (7 - (j % 8)));
							
						}else{
							if(attr.type == TypeInt || attr.type == TypeReal){
								memcpy(data + writedataOffset, readData + readDataOffset, sizeof(int));
								writedataOffset += sizeof(int);
								readDataOffset += sizeof(int);
							}else if(attr.type == TypeVarChar){
								int length;
								memcpy(&length, readData + readDataOffset, sizeof(int));
								memcpy(data + writedataOffset, readData + readDataOffset, sizeof(int) + length);
								writedataOffset += sizeof(int) + length;
								readDataOffset += sizeof(int) + length;
							}
						}
						break;
					}else{
						
						if(null){
							
						}else{
							if(attr.type == TypeInt || attr.type == TypeReal){
								readDataOffset += sizeof(int);
							}else if(attr.type == TypeVarChar){
								int length;
								memcpy(&length, readData + readDataOffset, sizeof(int));
								readDataOffset += sizeof(int) + length;
							}
						}
					}
				}
			}
        	free(writeDataNullBit);
        	free(readData);
        	return 0;
}


//AGGREGATE OPERATION
RC Aggregate::getNextTuple(void *data){
	//aggregation operation cannot be performed on characters
        if(aggAttr.type == TypeVarChar){
        		return QE_EOF;
        	}
	if(!isGroupBy){//basic aggregation
				void *inData = malloc(PAGE_SIZE);
				int nullBitBytes = ceil((float)inAttributes.size() / CHAR_BIT);
				while(input->getNextTuple(inData) != QE_EOF){
					int offset = nullBitBytes;
					for (int i = 0; i < inAttributes.size(); ++i) {
						bool null = ((char*)inData)[i / 8] & ( 1 << ( 7 - ( i % 8 )));
						Attribute attribute_i = inAttributes[i];
						if(attribute_i.name == aggAttr.name){
							if(null){
								break;
							}
							if(attribute_i.type == TypeInt){
								int value;
								memcpy(&value, inData + offset, sizeof(int));
								int_Vector.push_back(value);
								break;
							}else if(attribute_i.type == TypeReal){
								float value;
								memcpy(&value, inData + offset, sizeof(float));
								float_Vector.push_back(value);
								break;
							}
						}else{
							if(null){
								
							}else if(attribute_i.type == TypeInt || attribute_i.type == TypeReal){
								offset += sizeof(int);
							}else if(attribute_i.type == TypeVarChar){
								int length;
								memcpy(&length, inData + offset, sizeof(int));
								offset += sizeof(int) + length;
							}
						}
					}
				}
				
				if(int_Vector.size() == 0 && float_Vector.size() == 0){
					return QE_EOF;
				}
				calculateAggregation(data, int_Vector,float_Vector);
				free(inData);
				int_Vector.clear();
				float_Vector.clear();
			}
			else{//group by aggregate
				void* aggregateData = malloc(PAGE_SIZE);
				//group by Int and aggregate attribute Int
				if(groupAttr.type == TypeInt && aggAttr.type == TypeInt){
					if(intIntIter == intIntMap.end()){
						free(aggregateData);
						return QE_EOF;
					}
					vector<float> temp;
					calculateAggregation(aggregateData, intIntIter->second, temp);
					int value = intIntIter->first;
					memset((char*)data, 0, 1);
					int offset_data = 1;
					memcpy((char*) data + offset_data, &value, sizeof(int));
					offset_data += sizeof(int);
					memcpy((char*) data + offset_data, (char*) aggregateData + 1, sizeof(float)); //+1 is added because the output data from aggregation comes with 1 null bit
					intIntIter++;
				}
				//group by Int and aggregate Attribute Float
				else if(groupAttr.type == TypeInt && aggAttr.type == TypeReal){
					if(intFloatIter == intFloatMap.end()){
						free(aggregateData);
						return QE_EOF;
					}
					vector<int> temp;
					calculateAggregation(aggregateData, temp, intFloatIter->second);
					int val = intFloatIter->first;
					memset((char*)data, 0, 1);
					int offset_data = 1;
					memcpy((char*) data + offset_data, &val, sizeof(int));
					offset_data += sizeof(int);
					memcpy((char*) data + offset_data, (char*) aggregateData + 1, sizeof(float));
					intFloatIter++;
				}
				//group by Real and aggregate attr Int
				else if(groupAttr.type == TypeReal && aggAttr.type == TypeInt){
					if(floatIntIter == floatIntMap.end()){
						free(aggregateData);
						return QE_EOF;
					}
					vector<float> temp;
					calculateAggregation(aggregateData, floatIntIter->second, temp);
					float val = floatIntIter->first;
					memset((char*)data, 0, 1);
					int offset_data = 1;
					memcpy((char*) data + offset_data, &val, sizeof(float));
					offset_data += sizeof(float);
					memcpy((char*) data + offset_data, (char*) aggregateData + 1, sizeof(float));
					floatIntIter++;
				}
				//group by Real and aggregate attr Real
				else if(groupAttr.type == TypeReal && aggAttr.type == TypeReal){
					if(floatFloatIter == floatFloatMap.end()){
						free(aggregateData);
						return QE_EOF;
					}
					vector<int> temp;
					calculateAggregation(aggregateData, temp, floatFloatIter->second);
					float val = floatFloatIter->first;
					memset((char*)data, 0, 1);
					int offset_data = 1;
					memcpy((char*) data + offset_data, &val, sizeof(float));
					offset_data += sizeof(float);
					memcpy((char*) data + offset_data, (char*) aggregateData + 1, sizeof(float));
					floatFloatIter++;
				}
				//group by varchar and aggregate attr Int
				else if(groupAttr.type == TypeVarChar && aggAttr.type == TypeInt){
					if(stringIntIter == stringIntMap.end()){
						free(aggregateData);
						return QE_EOF;
					}
					vector<float> temp;
					calculateAggregation(aggregateData, stringIntIter->second, temp);
					string val = stringIntIter->first;
					memset((char*)data, 0, 1);
					int offset_data = 1;
					int len = val.length();
					memcpy((char*) data + offset_data, &len, sizeof(int));
					offset_data += sizeof(int);
					memcpy((char*) data + offset_data, val.c_str(), len);
					offset_data += len;
					memcpy((char*) data + offset_data, (char*) aggregateData + 1, sizeof(float));
					stringIntIter++;
				}
				//group by varchar and aggregate attr real
				else if(groupAttr.type == TypeVarChar && aggAttr.type == TypeReal){
					if(stringFloatIter == stringFloatMap.end()){
						free(aggregateData);
						return QE_EOF;
					}
					vector<int> temp;
					calculateAggregation(aggregateData, temp, stringFloatIter->second);
					string val = stringFloatIter->first;
					memset((char*)data, 0, 1);
					int offset_data = 1;
					int len = val.length();
					memcpy((char*) data + offset_data, &len, sizeof(int));
					offset_data += sizeof(int);
					memcpy((char*) data + offset_data, val.c_str(), len);
					offset_data += len;
					memcpy((char*) data + offset_data, (char*) aggregateData + 1, sizeof(float));
					stringFloatIter++;
				}
				free(aggregateData);
			}
        	return 0;
}

//hash map for aggregate function depending on the group by attribute
RC Aggregate::populateAggregateHash(){
        	void* data = malloc(PAGE_SIZE);
        	bool aggrAttrNull = false;
        	bool grpAttrNull = false;
        	void* aggrAttributeData = malloc(PAGE_SIZE);
        	void* grpAttributeData = malloc(PAGE_SIZE);
        	while(input->getNextTuple(data) != QE_EOF){
        		int offset = ceil((double)inAttributes.size()/CHAR_BIT);
				for (int i = 0; i < inAttributes.size(); ++i) {
					if(inAttributes[i].name == aggAttr.name){ 
//if the incoming data attribute is the attribute on which aggregate is performed, fill the aggregate attr data with the values from the tuples-no case of char here
						if( ((char*)data)[i/8] & (1 << (7 - (i%8)) ) ){
							aggrAttrNull = true;
							break;
						}
						if(inAttributes[i].type == TypeInt){
							int value;
							memcpy(&value, (char*) data + offset, sizeof(int));
							offset += sizeof(int);
							memcpy((char*)aggrAttributeData, &value, sizeof(int));
						}
						else if(inAttributes[i].type == TypeReal){
							float value;
							memcpy(&value, (char*) data + offset, sizeof(float));
							offset += sizeof(float);
							memcpy((char*) aggrAttributeData, &value, sizeof(float));
						}
					}
//if the incoming data attribute is the attribute on which group by is performed, fill the group attribute data with the values from the tuples-char can be there
					else if(inAttributes[i].name == groupAttr.name){
						if( ((char*)data)[i/8] & (1 << (7 - (i%8)) ) ){
							grpAttrNull = true;
							break;
						}
						if(inAttributes[i].type == TypeInt){
							int value;
							memcpy(&value, (char*) data + offset, sizeof(int));
							offset += sizeof(int);
							memcpy((char*) grpAttributeData, &value, sizeof(int));
						}
						else if(inAttributes[i].type == TypeReal){
							float value;
							memcpy(&value, (char*) data + offset, sizeof(float));
							offset += sizeof(float);
							memcpy((char*) grpAttributeData, &value, sizeof(float));
						}
						else{
							int length;
							memcpy((char*) data + offset, &length, sizeof(int));
							memcpy((char*) grpAttributeData, (char*) data + offset, sizeof(int) + length);
							offset += sizeof(int) + length;
						}
					}
					else{
//not a group by attribute not a aggregate attribute, then just skip this attribute value and go to next attribute
						if( ((char*)data)[i/8] & (1 << (7 - (i%8)) ) ){
							continue;
						}
						if(inAttributes[i].type == TypeInt || inAttributes[i].type == TypeReal){
							offset += sizeof(int);
						}
						else{
							int length;
							memcpy(&length, (char*) data + offset, sizeof(int));
							offset += sizeof(int) + length;
						}
					}
				}
				//now update the hash table
				if(aggrAttrNull || grpAttrNull){
					continue;
				}
//six cases based on the group by attribute type and aggregate attribute type
				if(groupAttr.type == TypeInt && aggAttr.type == TypeInt){
					int grpAttrValue;
					int aggrAttrValue;
					memcpy(&grpAttrValue, (char*) grpAttributeData, sizeof(int));
					memcpy(&aggrAttrValue, (char*) aggrAttributeData, sizeof(int));
					if(intIntMap.count(grpAttrValue) > 0){
						intIntMap[grpAttrValue].push_back(aggrAttrValue);
					}
					else{
						vector<int> vector_i;
						vector_i.push_back(aggrAttrValue);
						intIntMap.insert(make_pair(grpAttrValue, vector_i));
					}
				}
				else if(groupAttr.type == TypeInt && aggAttr.type == TypeReal){
					int grpAttrValue;
					float aggrAttrValue;
					memcpy(&grpAttrValue, (char*) grpAttributeData, sizeof(int));
					memcpy(&aggrAttrValue, (char*) aggrAttributeData, sizeof(float));
					if(intFloatMap.count(grpAttrValue) > 0){
						intFloatMap[grpAttrValue].push_back(aggrAttrValue);
					}
					else{
						vector<float> vector_i;
						vector_i.push_back(aggrAttrValue);
						intFloatMap.insert(make_pair(grpAttrValue, vector_i));
					}
				}
				else if(groupAttr.type == TypeReal && aggAttr.type == TypeInt){
					float grpAttrValue;
					int aggrAttrValue;
					memcpy(&grpAttrValue, (char*) grpAttributeData, sizeof(float));
					memcpy(&aggrAttrValue, (char*) aggrAttributeData, sizeof(int));
					if(floatIntMap.count(grpAttrValue) > 0){
						floatIntMap[grpAttrValue].push_back(aggrAttrValue);
					}
					else{
						vector<int> vector_i;
						vector_i.push_back(aggrAttrValue);
						floatIntMap.insert(make_pair(grpAttrValue, vector_i));
					}
				}
				else if(groupAttr.type == TypeReal && aggAttr.type == TypeReal){
					float grpAttrValue;
					float aggrAttrValue;
					memcpy(&grpAttrValue, (char*) grpAttributeData, sizeof(float));
					memcpy(&aggrAttrValue, (char*) aggrAttributeData, sizeof(float));
					if(floatFloatMap.count(grpAttrValue) > 0){
						floatFloatMap[grpAttrValue].push_back(aggrAttrValue);
					}
					else{
						vector<float> vector_i;
						vector_i.push_back(aggrAttrValue);
						floatFloatMap.insert(make_pair(grpAttrValue, vector_i));
					}
				}
				else if(groupAttr.type == TypeVarChar && aggAttr.type == TypeInt){
					char* str = (char*) malloc(PAGE_SIZE);
					int len;
					memcpy(&len, (char*) grpAttributeData, sizeof(int));
					memcpy(str, (char*) grpAttributeData + sizeof(int), len);
					str[len] = '\0';
					string grpAttrValue(str);
					free(str);
					int aggrAttrValue;
					memcpy(&aggrAttrValue, (char*) aggrAttributeData, sizeof(int));
					if(stringIntMap.count(grpAttrValue) > 0){
						stringIntMap[grpAttrValue].push_back(aggrAttrValue);
					}
					else{
						vector<int> vector_i;
						vector_i.push_back(aggrAttrValue);
						stringIntMap.insert(make_pair(grpAttrValue, vector_i));
					}
				}
				else if(groupAttr.type == TypeVarChar && aggAttr.type == TypeReal){
					char* str = (char*) malloc(PAGE_SIZE);
					int len;
					memcpy(&len, (char*) grpAttributeData, sizeof(int));
					memcpy(str, (char*) grpAttributeData + sizeof(int), len);
					str[len] = '\0';
					string grpAttrValue(str);
					free(str);
					float aggrAttrValue;
					memcpy(&aggrAttrValue, (char*) aggrAttributeData, sizeof(float));
					if(stringFloatMap.count(grpAttrValue) > 0){
						stringFloatMap[grpAttrValue].push_back(aggrAttrValue);
					}
					else{
						vector<float> vector_i;
						vector_i.push_back(aggrAttrValue);
						stringFloatMap.insert(make_pair(grpAttrValue, vector_i));
					}
				}
        	}
        	free(data);
        	free(aggrAttributeData);
        	free(grpAttributeData);
        	intIntIter = intIntMap.begin();
        	intFloatIter = intFloatMap.begin();
        	floatIntIter = floatIntMap.begin();
        	floatFloatIter = floatFloatMap.begin();
        	stringIntIter = stringIntMap.begin();
        	stringFloatIter = stringFloatMap.begin();
}             
//the output data of every query operation should be of the form null bits + actual data as per the test case checking
RC Aggregate::buildOutputData(float value, void* data){
        	int nullBitBytes = 1;
        	char* nullBit = (char*)malloc(nullBitBytes);
        	memset(nullBit, 0, nullBitBytes);
		
        	if(value == NULL){
        		nullBit[0] = nullBit[0] | (1 << 7);
			memcpy(data, nullBit, nullBitBytes);
        	}else{
		memcpy(data, nullBit, nullBitBytes);
            	memcpy(data + nullBitBytes, &value, sizeof(float));
        	}
        	
}

     
       
        // output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const{
        	if(!groupAttr.name.empty()){
        		attrs.push_back(groupAttr);
        	}
        	Attribute attr = aggAttr;
        	attr.type = TypeReal;
        	attr.length = 4;
        	string str;
        	if(operation == MIN){
        		str = "MIN";
        	}
        	else if(operation == MAX){
        		str = "MAX";
        	}
        	else if(operation == SUM){
        		str = "SUM";
        	}
        	else if(operation == AVG){
        		str = "AVG";
        	}
        	else if(operation == COUNT){
        		str = "COUNT";
        	}
        	attr.name = str + "(" + attr.name + ")";
        	attrs.push_back(attr);

};
//compute the aggregation
RC Aggregate::calculateAggregation(void* data, vector<int> int_Vector, vector<float> float_Vector){
       			
			if(operation == MIN){ //MINIMUM
				float min;
				switch(aggAttr.type){
					case TypeInt:{
					min = (float)int_Vector[0];
					for (int i = 1; i < int_Vector.size(); ++i) {
						if(int_Vector[i] < min){
							min = (float)int_Vector[i];
						}
					}
					
					}break;
					case TypeReal:{
					min = float_Vector[0];
					for (int i = 1; i < float_Vector.size(); ++i) {
						if(float_Vector[i] < min){
							min = float_Vector[i];
						}
					}
					
					}break;
					
				}
			buildOutputData(min,data);
			}else if(operation == MAX){ //MAXIMUM
				float max;
				switch(aggAttr.type){
					case TypeInt:{
					max = (float)int_Vector[0];
					for (int i= 1; i < int_Vector.size(); ++i) {
						if(int_Vector[i] > max){
							max = (float)int_Vector[i];
						}
					}
					
					}break;
					case TypeReal:{
					max = float_Vector[0];
					for (int i = 1; i < float_Vector.size(); ++i) {
						if(float_Vector[i] > max){
							max = float_Vector[i];
						//cout<<"max"<<max<<endl;
						}
					}
					
					}break;
				//buildOutputData(max, data);	
				}
			buildOutputData(max, data);
			}else if(operation == COUNT){ //COUNT
				float count;
				switch(aggAttr.type){
					case TypeInt:{
					count = (float)int_Vector.size();
					//buildOutputData(count, data);
					}break;
					case TypeReal:{
					count = float_Vector.size();

					}break;
					

				}
				buildOutputData(count, data);
			}else if(operation == SUM){ //SUM
				float sum;
				switch(aggAttr.type){
					case TypeInt:{
					sum = 0;
					for (int i = 0; i < int_Vector.size(); ++i) {
						sum += (float)int_Vector[i];
					}
					
					}break;
					case TypeReal:{
					sum = 0;
					for (int i = 0; i < float_Vector.size(); ++i) {
						sum += float_Vector[i];
					}
					//buildOutputData(sum, data);
					}break;
					
				}
				buildOutputData(sum, data);
			}else if(operation == AVG){ //AVERAGE
				float avg;
				switch(aggAttr.type){
					case TypeInt:{
					float sum = 0;
					for (int i = 0; i < int_Vector.size(); ++i) {
						sum += (float)int_Vector[i];
					}
					avg = (float)sum / int_Vector.size();
					
					}break;
					case TypeReal:{
					float sum = 0;
					for (int i = 0; i < float_Vector.size(); ++i) {
						sum += float_Vector[i];
					}
					avg = (float)sum / float_Vector.size();
					//buildOutputData(avg, data);
					}break;
					
				}
			buildOutputData(avg, data);
			}
}


RC INLJoin::findLow_Key(void* left_Data, void* low_Key, Attribute &leftAttribute, bool &low_KeyNULL){
	int offset = ceil((double)left_Attributes.size()/CHAR_BIT);
	for(int i = 0; i < left_Attributes.size(); i++){
		if(left_Attributes[i].name == condition.lhsAttr){
			leftAttribute.name = left_Attributes[i].name;
			leftAttribute.length = left_Attributes[i].length;
			leftAttribute.type = left_Attributes[i].type;
			if( ((char*)left_Data)[i/8] & ( 1 << (7 - (i%8))) ){
				low_KeyNULL = true;
				break;
			}
			if(left_Attributes[i].type == TypeInt){
				int keyVal;
				memcpy(&keyVal, (char*) left_Data + offset, sizeof(int));
				memcpy((char*)low_Key, &keyVal, sizeof(int));
			}
			else if(left_Attributes[i].type == TypeReal){
				float keyVal;
				memcpy(&keyVal, (char*) left_Data + offset, sizeof(float));
				memcpy((char*)low_Key, &keyVal, sizeof(float));
			}
			else if(left_Attributes[i].type == TypeVarChar){
				int length;
				memcpy(&length, (char*) left_Data + offset, sizeof(int));
				memcpy((char*)low_Key, (char*) left_Data + offset, sizeof(int) + length);
			}
			low_KeyNULL = false;
			break;
		}
		else{
			if( ((char*)left_Data)[i/8] & ( 1 << (7 - (i%8))) ){
				continue;
			}
			if(left_Attributes[i].type == TypeInt){
				offset += sizeof(int);
			}
			else if(left_Attributes[i].type == TypeReal){
				offset += sizeof(float);
			}
			else{
				int length;
				memcpy(&length, (char*) left_Data + offset, sizeof(int));
				offset += sizeof(int) + length;
			}
		}
	}
	return 0;
}



RC INLJoin::getNextTuple(void* data){

	if(isEOF){
cout<<"entering"<<endl;
		return QE_EOF;
	}
	bool foundJoin = false;
	void* right_Data = malloc(PAGE_SIZE);
cout<<"isEOF"<<isEOF<<" foundJoin"<< foundJoin<<endl;
	while(!isEOF && !foundJoin){
		if(rightIn->getNextTuple(right_Data) != RM_EOF){
cout<<"before joining"<<endl;
			foundJoin = true;
			//join the two tuples
			BNLJoin::joinTuples(left_Data, right_Data, data, left_Attributes, right_Attributes);
cout<<"done"<<endl;
		}
		if(!foundJoin){
			if(condition.op == NE_OP){
				if(notEqualToLeft == true){
					rightIn->setIterator(low_Key, NULL, false, true);
				}
				else{
					do{
						RC retVal = leftIn->getNextTuple(left_Data);
						if(retVal == RM_EOF){
							isEOF = true;
							break;
						}
						findLow_Key(left_Data, low_Key, leftAttribute, low_KeyNULL);
					}while(low_KeyNULL);
					if(condition.op== EQ_OP){
					rightIn->setIterator(low_Key, low_Key, true, true);
					}
					else if(condition.op == LT_OP){
					rightIn->setIterator(NULL, low_Key, true, false);
					}
					else if(condition.op == LE_OP){
					rightIn->setIterator(NULL, low_Key, true, true);
					}
					else if(condition.op == GT_OP){
					rightIn->setIterator(low_Key, NULL, false, true);
					}
					else if(condition.op == GE_OP){
					rightIn->setIterator(low_Key, NULL, true, true);
					}
					else if(condition.op == NO_OP){
					rightIn->setIterator(NULL, NULL, true, true);
					}
					else if(condition.op == NE_OP){
					rightIn->setIterator(NULL, low_Key, true, false);
		
					}
				}
				notEqualToLeft = !notEqualToLeft;
			}
			else{
				do{
					RC ret = leftIn->getNextTuple(left_Data);
					if(ret == RM_EOF){
						isEOF = true;
						break;
					}
					findLow_Key(left_Data, low_Key, leftAttribute, low_KeyNULL);
				}while(low_KeyNULL && !isEOF);
				if(!isEOF){
					if(condition.op== EQ_OP){
					rightIn->setIterator(low_Key, low_Key, true, true);
					}
					else if(condition.op == LT_OP){
					rightIn->setIterator(NULL, low_Key, true, false);
					}
					else if(condition.op == LE_OP){
					rightIn->setIterator(NULL, low_Key, true, true);
					}
					else if(condition.op == GT_OP){
					rightIn->setIterator(low_Key, NULL, false, true);
					}
					else if(condition.op == GE_OP){
					rightIn->setIterator(low_Key, NULL, true, true);
					}
					else if(condition.op == NO_OP){
					rightIn->setIterator(NULL, NULL, true, true);
					}
					else if(condition.op == NE_OP){
					rightIn->setIterator(NULL, low_Key, true, false);
					notEqualToLeft = true;
					}
							}
						}
		}
	}
cout<<"joined"<<endl;
	free(right_Data);
	if(isEOF){
		return QE_EOF;
	}
	return 0;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	for (int i = 0; i < left_Attributes.size(); ++i) {
		attrs.push_back(left_Attributes[i]);
	}
	for(int i = 0; i < right_Attributes.size(); ++i){
		attrs.push_back(right_Attributes[i]);
	}
}
