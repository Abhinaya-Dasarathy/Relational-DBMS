#ifndef _qe_h_
#define _qe_h_


#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cmath>

#include <map>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"
#include <map>
#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};



class Filter : public Iterator {
    // Filter operator
    public:
Iterator* input;
		Condition condition;
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        )
{
//cout<<"entering constructor"<<endl;
this->input=input;
this->condition =condition;
};
        ~Filter(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};


class Project : public Iterator {
    // Projection operator
    public:
Iterator *input;
vector<string> attrNames;
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames){
this->input=input;
this->attrNames=attrNames;
};   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
	unsigned sizeOfHashTable;
map<int, vector<void*> > mapIntegers;
		map<float, vector<void*> > mapFloat;
		map<string, vector<void*> > mapString;
		Iterator *leftIn;
		TableScan *rightIn;
		Condition condition;
		unsigned numPages;
		bool isLeftDone = false;

		vector<void*> leftTupleMatch;
		int leftTupleMatchIndex = 0;
		void* rightTableData = malloc(PAGE_SIZE);

		vector<Attribute> leftTableAttribute;
		vector<Attribute> rightTableAttribute;
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        );
        ~BNLJoin(){
		free(rightTableData);		
	};
	RC populateHash();
static RC joinTuples(void* leftData, void* rightData, void* outputData,
		vector<Attribute> leftTableAttribute, vector<Attribute> rightTableAttribute);
	unsigned getSize(void * data, vector<Attribute> attrs);
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
	
Iterator *input;
bool isGroupBy;
		Attribute aggAttr;
		Attribute groupAttr;
		AggregateOp operation;
		vector<Attribute> inAttributes;
		vector<int> int_Vector;
		vector<float> float_Vector;
//group by hash maps for different combinations
		map<int, vector<int> > intIntMap;
		map<float, vector<int> > floatIntMap;
		map<string, vector<int> > stringIntMap;
		map<int, vector<float> > intFloatMap;
		map<float, vector<float> > floatFloatMap;
		map<string, vector<float> > stringFloatMap;
		map<int, vector<int> >:: iterator intIntIter;
		map<float, vector<int> >:: iterator floatIntIter;
		map<string, vector<int> >:: iterator stringIntIter;
		map<int, vector<float> >:: iterator intFloatIter;
		map<float, vector<float> >:: iterator floatFloatIter;
		map<string, vector<float> >:: iterator stringFloatIter;
        // Mandatory
        // Basic aggregation
RC populateAggregateHash();
RC buildOutputData(float value, void* data);//output data with null bits
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ){
        	this->input = input;
this->operation = op;
        	this->aggAttr = aggAttr;
        	
        	input->getAttributes(inAttributes);
isGroupBy = false;//to check if it is basic or groupby hash aggregation
	};

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        ){
this->input = input;
        	this->aggAttr = aggAttr;
        	this->operation = op;
        	input->getAttributes(inAttributes);
isGroupBy =true;
        	this->groupAttr = groupAttr;
        	populateAggregateHash();
};
        ~Aggregate(){};
RC calculateAggregation(void* data, vector<int> intVector, vector<float> floatVector);
        RC getNextTuple(void *data);
        // Please name the output attribute as operation(aggAttr)
        // E.g. Relation=rel, attribute=attr, operation=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;
};



class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
vector<Attribute> left_Attributes, right_Attributes;
		Iterator* leftIn;
		IndexScan* rightIn;
		Condition condition;
		void* left_Data;
		bool isEOF;
		void* low_Key;
		Attribute leftAttribute;
		bool notEqualToLeft;
		bool low_KeyNULL;
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){
this->leftIn = leftIn;
	this->rightIn = rightIn;
isEOF = false;
	leftIn->getAttributes(left_Attributes);
	rightIn->getAttributes(right_Attributes);
	this->condition = condition;
	left_Data = malloc(PAGE_SIZE);
	low_Key = malloc(PAGE_SIZE);
	int retVal = leftIn->getNextTuple(left_Data);
	if(retVal == RM_EOF){
cout<<"if in constructor"<<endl;
		isEOF = true;
	}
	else{

		low_KeyNULL = true;

		while(low_KeyNULL){
			findLow_Key(left_Data, low_Key, leftAttribute, low_KeyNULL);
			if(low_KeyNULL)
				leftIn->getNextTuple(left_Data);
		}
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
	
};
	~INLJoin(){
		free(left_Data);
		free(low_Key);
	};
RC findLow_Key(void* left_Data, void* low_Key, Attribute &leftAttribute, bool &low_KeyNULL);
       
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      ){};
      ~GHJoin(){};

      RC getNextTuple(void *data){return QE_EOF;};
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const{};
};


#endif
