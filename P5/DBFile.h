#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"

typedef enum {
	heap,
	sorted,
	tree
} fType;

struct SortInfo {
	
	OrderMaker *myOrder;
	int runLength;
	
};

class GenericDBFile {

protected:
	
	friend class SelectFileNode;
	
	File *file;                    // The real file pointer
	Page *bufferPage;              // The pointer to the buffer page object
	
	char *fpath;               // File path storage
	bool isWriting;                // Writing mode flag
	
	off_t pageIndex;               // The index of the buffer page
	
public:
	
	virtual int Create (const char *fpath) = 0;
	virtual int Open (char *fpath) = 0;
	virtual int Close () = 0;
	
	virtual void Load (Schema &myschema, const char *loadpath) = 0;
	
	virtual void MoveFirst () = 0;
	virtual void Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
	
	virtual ~GenericDBFile ();
	
};

class Heap : public GenericDBFile {

public:
	
	Heap ();
	~Heap ();
	
	int Create (const char *fpath);
	int Open (char *fpath);
	int Close ();
	
	void Load (Schema &myschema, const char *loadpath);
	
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};

class Sorted : public GenericDBFile {

private:
	
	OrderMaker *order;               // The order of the records in this file
	OrderMaker *query;
	
	BigQ *bigq;                      // Internal BigQ Strutre
	Pipe *inPipe, *outPipe;          // Internal Pipes to input and output for the
                                     // BigQ Strutre 
	
	int runLength;                   // The length of every run in the BigQ structure
	int buffsize;                    // The buffer size of Pipes

public:
	
	Sorted (OrderMaker *order, int runLength);
	~Sorted ();
	
	int Create (const char *fpath);
	int Open (char *fpath);
	int Close ();
	
	void Load (Schema &myschema, const char *loadpath);
	
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	
	// Get next function when query exist and valid
	int GetNextWithQuery (Record &fetchme, CNF &cnf, Record &literal);
	
	// Get next function when query is not valid
	int GetNextSequential (Record &fetchme, CNF &cnf, Record &literal);
	
	// Binary Search after query is successfully generated
	// return 0 or 1, representing found or not found
	int BinarySearch(Record &fetchme, CNF &cnf, Record &literal);
	
	// Set up the Internal BigQ for Records to add
	void SetupBigQ ();
	
	// Merge the records in the internal BigQ with
	// others already in the file. After merging, 
	// the pointer is at the start of the file.
	void Merge ();
	
	// Generate the query OrderMaker. Return 0 or 1,
	// representing generation success or failure.
	int QueryOrderGen (OrderMaker &query, OrderMaker &order, CNF &cnf);
	
};

class DBFile {
	
private:
	
	GenericDBFile *myInternalVar;         // The DB file pointer

public:
	
	DBFile ();
	~DBFile ();
	
	// Create a new DBFile instance, return 1 for success and 
	// 0 for failure
	int Create (const char *fpath, fType ftype, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
