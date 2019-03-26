#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class Run;

// Comparison Engine Wrapper for sorting
class CompareEngineWrapper {
	
private:

	OrderMaker *order;
	
public:
	
	CompareEngineWrapper (OrderMaker *order);
	bool operator () (Record *left, Record *right);
	
};

// Comparison Engine Wrapper for pirority queue to compare Runs
class CompareEngineWrapperForRun {

private:
	
	CompareEngineWrapper *comp;

public:
	
	CompareEngineWrapperForRun (CompareEngineWrapper *comp);
	bool operator () (Run *left, Run *right);
	
};

// simple struct to pass variables from BigQ to its worker thread
typedef struct {
	
	Pipe *in;
	Pipe *out;
	OrderMaker *order;
	int runlen;
	
} BigQInfo;

// a Run class for priority queue to work
class Run {
	
private:
	
	off_t curIndex;
	off_t endIndex;
	File *file;
	Page *bufferPage;
	
public:
	
	Record *record;
	
	Run (File *file, off_t curIndex, off_t endIndex);
	~Run ();
	// returns the current record in current variable
	// changes record variable to the next Record in bufferPage
	// returns 1 for success, and 0 for failure
	int Next (Record *current);
	
};

class BigQ {
	
public:
	
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	
};

#endif
