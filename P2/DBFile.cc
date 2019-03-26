#include <cstring>
#include <iostream>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

DBFile::DBFile () {
	
	file = new File();
	bufferPage = new Page();
	
}

DBFile::~DBFile () {
	
	delete file;
	delete bufferPage;
	
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
	// need to change when other fTypes are introduced
	// TODO: add matadata for file
	isCreated = true;
	filetype = f_type;
	strcpy(fpath, f_path);
	pageIndex = 0;
	bufferPage->EmptyItOut ();
	file->Open (0, fpath);
	
	return 1;
	
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
	
	FILE *tableFile = fopen(loadpath, "r");
	Record temp;
	
	if (tableFile == NULL) {
		
		cout << "File " << loadpath << " do not exist! \n";
		exit(0);
		
	}
	
	pageIndex = 0;
	bufferPage->EmptyItOut ();
	
	while (temp.SuckNextRecord (&f_schema, tableFile) == 1)
		Add (temp);
	
	file->AddPage (bufferPage, pageIndex++);
	bufferPage->EmptyItOut ();
	
	fclose(tableFile);
	
}

int DBFile::Open (const char *f_path) {
	// need for change when other fTypes are introduced
	// TODO: add matadata for file
	filetype = heap;
	strcpy(fpath, f_path);
	pageIndex = 0;
	bufferPage->EmptyItOut ();
	file->Open (1, fpath);
	
	return 1;
	
}

void DBFile::MoveFirst () {
	
	bufferPage->EmptyItOut ();
	pageIndex = 0;
	file->GetPage (bufferPage, pageIndex);
	
}

int DBFile::Close () {
	
	if (isCreated && bufferPage->GetNumRecs () > 0) {
		
		file->AddPage (bufferPage, pageIndex);
		bufferPage->EmptyItOut ();
		isCreated = false;
		
	}
	
	file->Close ();
	
	return 1;
	
}

void DBFile::Add (Record &rec) {
	
	if (! (bufferPage->Append (&rec))) {
		
		file->AddPage (bufferPage, pageIndex++);
		bufferPage->EmptyItOut ();
		bufferPage->Append (&rec);
		
	}
	
}

int DBFile::GetNext (Record &fetchme) {
	
	if (bufferPage->GetFirst (&fetchme)) {
		// Got the record from current page
		return 1;
		
	} else {
		// Didn't get the record from current page
		// Need to get a new page
		if (++pageIndex < file->GetLength () - 1) {
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

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
	ComparisonEngine comp;
	
	while (GetNext (fetchme)) {
		
		if (comp.Compare (&fetchme, &literal, &cnf)){
			
			return 1;
			
		}
		
	}
	
	return 0;
	
}
