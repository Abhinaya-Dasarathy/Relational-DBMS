#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define ROOT_PAGE 0 //define root page number

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
PagedFileManager *pfm;
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

//additional methods added
RC searchAndDelete(IXFileHandle &ixfileHandle, int nodePg, const Attribute &attribute,const void *key, const RID &rid);
RC searchKeyInNonLeafNode(const Attribute &attribute, const void *dataNode, const void *key, int &KeyPtr);
RC printTreeUtil(IXFileHandle &ixfileHandle, const Attribute &attribute);
RC printTree(IXFileHandle &ixfileHandle, int pgNum, const Attribute &attribute, int tabcount) const;
RC printLeaf(void* dataNode, const Attribute &attribute, int tabcount) const;
RC printNonLeaf(void* dataNode, const Attribute &attribute, int tabcount) const;
void updOffset(const void* lowKey, bool lowKeyInclusive, IX_ScanIterator &ix_ScanIterator);
int getLeafPage(IXFileHandle &ixfileHandle, int nodePgNo,const Attribute attribute,const void* key);
RC deleteEntryFromLeaf(IXFileHandle &ixfileHandle, void *dataNode, int nodePg,const Attribute &attribute, const void *key, const RID &rid);
    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};



class IXFileHandle {
    public:
FileHandle fileHandle;
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();
RC readPage(PageNum pageNum, void *data);                           // Get a specific page
	RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
	RC appendPage(const void *data);                                    // Append a specific page
	unsigned getNumberOfPages();                                        // Get the number of pages in the file

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
	
	

};

class IX_ScanIterator {
    public:
IXFileHandle ixfileHandle;
void* pgData;
		Attribute attribute;
		const void *highKey;
		bool highKeyInclusive;
		
		short fsp;
		bool isEOF;
		int currentPage;
		int offset;
		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();
//new method
 int getNextLeaf(int pgNo);
        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



#endif
