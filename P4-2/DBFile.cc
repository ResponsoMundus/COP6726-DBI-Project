#include <string>
#include <cstring>
#include <fstream>
#include <iostream>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

GenericDBFile :: ~GenericDBFile () {}

DBFile :: DBFile () {}

DBFile :: ~DBFile () {
	
	delete myInternalVar;
	
}

int DBFile :: Create (const char *fpath, fType ftype, void *startup) {
	
	ofstream md;
	
	char mdpath[100];
	
	sprintf (mdpath, "%s.md", fpath);
	md.open (mdpath);
	
	if (ftype == heap) {
		
		md << "heap" << endl;
		myInternalVar = new Heap;
		
	} else if (ftype == sorted) {
		
		md << "sorted" << endl;
		md << ((SortInfo *) startup)->runLength << endl;
		md << ((SortInfo *) startup)->myOrder->numAtts << endl;
		((SortInfo *) startup)->myOrder->PrintInOfstream (md);
		myInternalVar = new Sorted (((SortInfo *) startup)->myOrder, ((SortInfo *) startup)->runLength);
		
	} else if (ftype == tree) {
		// for late usage
		md << "tree" << endl;
		// may need more operation
	} else {
		
		md.close ();
		
		cout << "The type of file (" << fpath << ") not recognized!" << endl;
		return 0;
		
	}
	
	myInternalVar->Create (fpath);
	md.close ();
	
	return 1;
	
}

int DBFile :: Open (char *fpath) {
	
	ifstream md;
	string str;
	
	int attNum;
	char *mdpath = new char[100];
	
	sprintf (mdpath, "%s.md", fpath);
	md.open (mdpath);
	
	if (md.is_open ()) {
		
		md >> str;
		
		if (!str.compare ("heap")) {
			
			myInternalVar = new Heap;
			
		} else if (!str.compare ("sorted")){
			
			int runLength;
			
			OrderMaker *order = new OrderMaker;
			
			md >> runLength;
			md >> order->numAtts;
			
			for (int i = 0; i < order->numAtts; i++) {
				
				md >> attNum;
				md >> str;
				
				order->whichAtts[i] = attNum;
				
				if (!str.compare ("Int")) {
					
					order->whichTypes[i] = Int;
					
				} else if (!str.compare ("Double")) {
					
					order->whichTypes[i] = Double;
					
				} else if (!str.compare ("String")) {
					
					order->whichTypes[i] = String;
					
				} else {
					
					delete order;
					
					md.close ();
					
					cout << "Invalid metadata for sorted file (" << fpath << ")" << endl;
					
					return 0;
					
				}
				
			}
			
			myInternalVar = new Sorted (order, runLength);
			
		} else if (!str.compare ("tree")) {
			// Todo
			
			
		} else {
			
			md.close ();
			
			cout << "Invalid file type in metadata of file (" << fpath << ")" << endl;
			
			return 0;
			
		}
		
	} else {
		
		md.close ();
		
		cout << "Can not open file (" << fpath << ")!" << endl;
		return 0;
		
	}
	
	myInternalVar->Open (fpath);
	md.close ();
	
	return 1;
	
}

int DBFile :: Close () {
	
	return myInternalVar->Close ();
	
}

void DBFile :: Load (Schema &myschema, const char *loadpath) {
	
	myInternalVar->Load (myschema, loadpath);
	
}

void DBFile :: MoveFirst () {
	
	myInternalVar->MoveFirst ();
	
}

void DBFile :: Add (Record &addme) {
	
	myInternalVar->Add (addme);
	
}

int DBFile :: GetNext (Record &fetchme) {
	
	return myInternalVar->GetNext (fetchme);
	
}

int DBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
	return myInternalVar->GetNext (fetchme, cnf, literal);
	
}

Heap :: Heap () {
	
	file = new File ();
	bufferPage = new Page ();
	
}

Heap :: ~Heap () {
	
	delete bufferPage;
	delete file;
	
}

int Heap :: Create (const char *fpath) {
	
	isWriting = true;
	
	this->fpath = new char[100];
	strcpy (this->fpath, fpath);
	
	pageIndex = 0;
	bufferPage->EmptyItOut ();
	file->Open (0, this->fpath);
	
	return 1;
	
}

void Heap :: Load (Schema &f_schema, const char *loadpath) {
	
	FILE *tableFile = fopen(loadpath, "r");
	Record temp;
	
	if (tableFile == NULL) {
		
		cout << "Can not open file " << loadpath << "!" << endl;
		exit (0);
		
	}
	
	pageIndex = 0;
	bufferPage->EmptyItOut ();
	
	while (temp.SuckNextRecord (&f_schema, tableFile) == 1)
		Add (temp);
	
	file->AddPage (bufferPage, pageIndex);
	bufferPage->EmptyItOut ();
	
	fclose (tableFile);
	
}

int Heap :: Open (char *fpath) {
	
	this->fpath = new char[100];
	
	strcpy (this->fpath, fpath);
	pageIndex = 0;
	
	bufferPage->EmptyItOut ();
	file->Open (1, this->fpath);
	
	return 1;
	
}

void Heap :: MoveFirst () {
	
	if (isWriting && bufferPage->GetNumRecs () > 0) {
		
		file->AddPage (bufferPage, pageIndex++);
		isWriting = false;
		
	}
	
	bufferPage->EmptyItOut ();
	pageIndex = 0;
	file->GetPage (bufferPage, pageIndex);
	
}

int Heap :: Close () {
	
	if (isWriting && bufferPage->GetNumRecs () > 0) {
		
		file->AddPage (bufferPage, pageIndex++);
		bufferPage->EmptyItOut ();
		isWriting = false;
		
	}
	
	file->Close ();
	
	return 1;
	
}

void Heap :: Add (Record &rec) {
	
	if (! (bufferPage->Append (&rec))) {
		
		file->AddPage (bufferPage, pageIndex++);
		bufferPage->EmptyItOut ();
		bufferPage->Append (&rec);
		
	}
	
}

int Heap :: GetNext (Record &fetchme) {
	
	if (bufferPage->GetFirst (&fetchme)) {
		// Got the record from current page
		return 1;
		
	} else {
		// Didn't get the record from current page
		// Need to get a new page
		pageIndex++;
		if (pageIndex < file->GetLength () - 1) {
			// if not reach EOF
			file->GetPage (bufferPage, pageIndex);
			bufferPage->GetFirst (&fetchme);
			
			return 1;

		} else {
			// if already reach EOF
			return 0;
			
		}
		
	}
	
}

int Heap :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
	ComparisonEngine comp;
	
	while (GetNext (fetchme)) {
		
		if (comp.Compare (&fetchme, &literal, &cnf)){
			
			return 1;
			
		}
		
	}
	
	return 0;
	
}

Sorted :: Sorted (OrderMaker *order, int runLength) {
	
	this->query = NULL;
	this->bigq = NULL;
	this->file = new File ();
	this->bufferPage = new Page ();
	this->order = order;
	this->runLength = runLength;
	this->buffsize = 100;
	
}

Sorted :: ~Sorted () {
	
	delete query;
	delete file;
	delete bufferPage;
	
}

int Sorted :: Create (const char *fpath) {
	
	isWriting = false;
	pageIndex = 0;
	
	this->fpath = new char[100];
	strcpy (this->fpath, fpath);
	
	bufferPage->EmptyItOut ();
	file->Open (0, this->fpath);
	
	return 1;
	
}

int Sorted :: Open (char *fpath) {
	
	isWriting = false;
	pageIndex = 0;
	
	this->fpath = new char[100];
	strcpy (this->fpath, fpath);
	
	bufferPage->EmptyItOut ();
	file->Open (1, this->fpath);
	
	if (file->GetLength () > 0) {
		
		file->GetPage (bufferPage, pageIndex);
		
	}
	
	
	return 1;
	
}

int Sorted :: Close () {
	
	if (isWriting) {
		
		Merge ();
		
	}
	
	file->Close ();
	
}

void Sorted :: Add (Record &addme) {
	
	if (!isWriting) {
		
		SetupBigQ ();
		
	}
	
	inPipe->Insert (&addme);
	
}

void Sorted :: Load (Schema &myschema, const char *loadpath) {
	
	FILE *tableFile = fopen(loadpath, "r");
	Record temp;
	
	if (tableFile == NULL) {
		
		cout << "Can not open file " << loadpath << "!" << endl;
		exit (0);
		
	}
	
	pageIndex = 0;
	bufferPage->EmptyItOut ();
	
	while (temp.SuckNextRecord (&myschema, tableFile)) {
		
		Add (temp);
		
	}
	
	fclose (tableFile);
	
}

void Sorted :: MoveFirst () {
	
	if (isWriting) {
		
		Merge ();
		
	} else {
		
		bufferPage->EmptyItOut ();
		pageIndex = 0;
		
		if (file->GetLength () > 0) {
			
			file->GetPage (bufferPage, pageIndex);
			
		}
		
		if (query) {
			
			delete query;
			
		}
		
	}
	
}

int Sorted :: GetNext (Record &fetchme) {
	
	if (isWriting) {
		
		Merge ();
		
	}
	
	if (bufferPage->GetFirst (&fetchme)) {
		// Got the record from current page
		return 1;
		
	} else {
		// Didn't get the record from current page
		// Need to get a new page
		pageIndex++;
		if (pageIndex < file->GetLength () - 1) {
			// if not reach EOF
			file->GetPage (bufferPage, pageIndex);
			bufferPage->GetFirst (&fetchme);
			
			return 1;

		} else {
			// if already reach EOF
			return 0;
			
		}
		
	}
	
}

int Sorted :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
	if (isWriting) {
		
		Merge ();
		
	}
	
	ComparisonEngine comp;
	
	if (!query) {
		// cout << "No query found! Try to gen one!" << endl;
		// query does not exist
		query = new OrderMaker;
		
		if (QueryOrderGen (*query, *order, cnf) > 0) {
			// query generated successfully
			// cout << "Query Gen Success! Go Bin Search!" << endl;
			query->Print ();
			if (BinarySearch (fetchme, cnf, literal)) {
				// cout << "Found!" << endl;
				// Found
				return 1;
				
			} else {
				// binary search fails
				// cout << "Not Found!" << endl;
				return 0;
				
			}
			
		} else {
			//query generated but is empty
			// cout << "Query Gen fail! Go Sequential!" << endl;
			return GetNextSequential (fetchme, cnf, literal);
			
		}
		
	} else {
		// query exists
		if (query->numAtts == 0) {
			// invalid query
			return GetNextSequential (fetchme, cnf, literal);
			
		} else {
			// valid query
			return GetNextWithQuery (fetchme, cnf, literal);
			
		}
		
	}
	
}

int Sorted :: GetNextWithQuery (Record &fetchme, CNF &cnf, Record &literal) {
	
	ComparisonEngine comp;
	
	while (GetNext (fetchme)) {
		
		if (!comp.Compare (&literal, query, &fetchme, order)){
			
			if (comp.Compare (&fetchme, &literal, &cnf)){
				
				return 1;
				
			}
		
		} else {
			
			break;
			
		}
		
	}
	
	return 0;
	
}

int Sorted :: GetNextSequential (Record &fetchme, CNF &cnf, Record &literal) {
	
	ComparisonEngine comp;
	
	while (GetNext (fetchme)) {
		
		if (comp.Compare (&fetchme, &literal, &cnf)){
			
			return 1;
			
		}
		
	}
	
	return 0;
	
}

int Sorted :: BinarySearch(Record &fetchme, CNF &cnf, Record &literal) {
	
	off_t first = pageIndex;
	off_t last = file->GetLength () - 1;
	off_t mid = pageIndex;
	
	Page *page = new Page;
	
	ComparisonEngine comp;
	
	while (true) {
		
		mid = (first + last) / 2;
		
		file->GetPage (page, mid);
		
		if (page->GetFirst (&fetchme)) {
			
			if (comp.Compare (&literal, query, &fetchme, order) <= 0) {
				
				last = mid - 1;
				if (last <= first) break;
				
			} else {
				
				first = mid + 1;
				if (last <= first) break;
				
			}
			
		} else {
			
			break;
			
		}
		
	}
	
	if (comp.Compare (&fetchme, &literal, &cnf)) {
		
		delete bufferPage;
		
		pageIndex = mid;
		bufferPage = page;
		
		return 1;
		
	} else {
	
		delete page;
		
		return 0;
		
	}
	
}

void Sorted :: SetupBigQ () {
	
	isWriting = true;
	
	inPipe = new Pipe (buffsize);
	outPipe = new Pipe (buffsize);
	
	bigq = new BigQ(*inPipe, *outPipe, *order, runLength);
	
}

void Sorted :: Merge () {
	
	inPipe->ShutDown ();
	
	isWriting = false;
	
	if (file->GetLength () > 0) {
		
		MoveFirst ();
		
	}
	
	Record *fromPipe = new Record;
	Record *fromFile = new Record;
	
	Heap *newFile = new Heap;
	newFile->Create ("bin/temp.bin");
	
	int flagPipe = outPipe->Remove (fromPipe);
	int flagFile = GetNext (*fromFile);
	
	ComparisonEngine comp;
	
	while (flagFile && flagPipe) {
		
		if (comp.Compare (fromPipe, fromFile, order) > 0) {
			
			newFile->Add (*fromFile);
			flagFile = GetNext (*fromFile);
			
		} else {
			
			newFile->Add (*fromPipe);
			flagPipe = outPipe->Remove (fromPipe);
			
		}
		
	}
	
	while (flagFile) {
		
		newFile->Add (*fromFile);
		flagFile = GetNext (*fromFile);
		
	}
	
	while (flagPipe) {
		
		newFile->Add (*fromPipe);
		flagPipe = outPipe->Remove (fromPipe);
		
	}
	
	outPipe->ShutDown ();
	newFile->Close ();
	delete newFile;
	
	file->Close ();
	
	remove (fpath);
	rename ("bin/temp.bin", fpath);
	
	file->Open (1, fpath);
	
	MoveFirst ();
	
}

int Sorted :: QueryOrderGen (OrderMaker &query, OrderMaker &order, CNF &cnf) {
	
	query.numAtts = 0;
	bool found = false;
	
	for (int i = 0; i < order.numAtts; ++i) {
		
		
		for (int j = 0; j < cnf.numAnds; ++j) {
			
			if (cnf.orLens[j] != 1) {
				
				continue;
				
			}
			
			if (cnf.orList[j][0].op != Equals) {
				
				continue;
				
			}
			
			if ((cnf.orList[i][0].operand1 == Left && cnf.orList[i][0].operand2 == Left) ||
               (cnf.orList[i][0].operand2 == Right && cnf.orList[i][0].operand1 == Right) ||
               (cnf.orList[i][0].operand1==Left && cnf.orList[i][0].operand2 == Right) ||
               (cnf.orList[i][0].operand1==Right && cnf.orList[i][0].operand2 == Left)) {
				
                continue;
				
			}

			
			if (cnf.orList[j][0].operand1 == Left &&
				cnf.orList[j][0].whichAtt1 == order.whichAtts[i]) {
				
				query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt2;
				query.whichTypes[query.numAtts] = cnf.orList[i][0].attType;
				
				query.numAtts++;
				
				found = true;
				
				break;
				
			}
			
			if (cnf.orList[j][0].operand2 == Left &&
				cnf.orList[j][0].whichAtt2 == order.whichAtts[i]) {
				
				query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt1;
				query.whichTypes[query.numAtts] = cnf.orList[i][0].attType;
				
				query.numAtts++;
				
				found = true;
				
				break;
				
			}
			
		}
		
		if (!found) {
			
			break;
			
		}
		
	}
	
	return query.numAtts;
	
}
