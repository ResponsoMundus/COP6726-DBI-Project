#ifndef REL_OP_H
#define REL_OP_H

#include <iostream>
#include <vector>
#include <cstring>
#include <sstream>
#include <pthread.h>

#include "Pipe.h"
#include "DBFile.h"
#include "Schema.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class RelOp {

protected:
	
	int runLen;
	pthread_t t;
	
public:
	
	// blocks the caller until the particular relational operator 
	// has run to completion 
	virtual void WaitUntilDone ();
	
	// tells how much internal memory the operation can use
	virtual void Use_n_Pages (int n);
	
	// starts the operation
	virtual void Start () = 0;
	
};

class SelectPipe : public RelOp {
	
private:
	
	Pipe *in, *out;
	CNF *cnf;
	Record *lit;
	
public:
	
	SelectPipe () {};
	~SelectPipe () {};
	
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	
	void Start ();
	
};

class SelectFile : public RelOp {

private:
	
	DBFile *file;
	Pipe *out;
	CNF *cnf;
	Record *lit;

public:
	
	SelectFile () {};
	~SelectFile () {};
	
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	
	void Start ();
	
};

class Project : public RelOp {

private:
	
	Pipe *in, *out;
	int *attsToKeep;
	int numAttsIn, numAttsOut;

public:
	
	Project () {};
	~Project () {};
	
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	
	void Start ();
	
};

class Join : public RelOp {

private:
	
	Pipe *inL, *inR, *out;
	CNF *cnf;
	Record *lit;
	
public:
	
	Join () {};
	~Join () {};
	
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	
	void Start ();
	
};

class DuplicateRemoval : public RelOp {

private:
	
	Pipe *in, *out;
	Schema *schema;

public:
	
	DuplicateRemoval () {};
	~DuplicateRemoval () {};
	
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	
	void Start ();
	
};

class Sum : public RelOp {

private:
	
	Pipe *in, *out;
	Function *compute;

public:
	
	Sum () {};
	~Sum () {};
	
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	
	void Start ();
	
};

class GroupBy : public RelOp {

private:
	
	Pipe *in, *out;
	OrderMaker *order;
	Function *compute;

public:
	
	GroupBy () {};
	~GroupBy () {};
	
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	
	void Start ();
	
};

class WriteOut : public RelOp {

private:
	
	Pipe *in;
	FILE *file;
	Schema *schema;

public:
	
	WriteOut () {};
	~WriteOut () {};
	
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	
	void Start ();
	
};

#endif