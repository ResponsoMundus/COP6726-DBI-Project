#include "BigQ.h"

CompareEngineWrapper :: CompareEngineWrapper (OrderMaker *order) {
	
	this->order = order;
	
}

bool CompareEngineWrapper :: operator () (Record *left, Record *right) {
	
	ComparisonEngine comp;
	
	return (comp.Compare (left, right, order) == 1) ? false : true;
	
}

CompareEngineWrapperForRun :: CompareEngineWrapperForRun (CompareEngineWrapper *comp) {
	
	this->comp = comp;
	
}

bool CompareEngineWrapperForRun :: operator () (Run *left, Run *right) {
	
	return !((*comp) (left->record, right->record));
	
}

void *sortingThread (void *arg) {
	
	BigQInfo *info = (BigQInfo *) arg;
	
	off_t pageIndex = 0;
	
	int curSize = sizeof(int);
	int maxSize = info->runlen * PAGE_SIZE;
	char *tmp = "tmp.bin";
	
	vector<Record *> recBuff;
	vector<off_t> runLocs;
	
	File file;
	Page page;
	Record rec;
	Record *newRec;
	
	CompareEngineWrapper comp(info->order);
	CompareEngineWrapperForRun compForPQ(&comp);
	
	page.EmptyItOut ();
	file.Open (0, tmp);
	
	// read data from in pipe sort them into runlen pages
	while (info->in->Remove (&rec) == 1) {
		
		newRec = new Record;
		
		newRec->Consume (&rec);
		curSize += newRec->GetSize ();
		
		if (curSize > maxSize) {
			
			sort(recBuff.begin (), recBuff.end (), comp);
			
			for (auto iter = recBuff.begin (); iter != recBuff.end (); ++iter) {
				
				if (page.Append (*iter) == 0) {
					
					file.AddPage (&page, pageIndex++);
					page.EmptyItOut ();
					page.Append (*iter);
					
				}
				
			}
			
			if (page.GetNumRecs () > 0) {
				
				file.AddPage (&page, pageIndex++);
				page.EmptyItOut ();
				
			}
			
			runLocs.push_back (pageIndex);
			
			recBuff.clear ();
			curSize = sizeof(int) + newRec->GetSize();
			
		}
		
		recBuff.push_back (newRec);
		
	}

	sort(recBuff.begin(), recBuff.end(), comp);
	
	
	for (auto iter = recBuff.begin (); iter != recBuff.end (); ++iter) {
	
		if (page.Append (*iter) == 0) {
			
			file.AddPage (&page, pageIndex++);
			page.EmptyItOut ();
			page.Append (*iter);
			
		}
		
	}
	
	if (page.GetNumRecs () > 0) {
		
		file.AddPage (&page, pageIndex++);
		page.EmptyItOut ();
		
	}
	
	runLocs.push_back (pageIndex);
	
	recBuff.clear ();
	
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
	
	off_t previous = 0;
	priority_queue<Run *, vector<Run *>, CompareEngineWrapperForRun> runPQ (compForPQ);
	
	for (auto iter = runLocs.begin (); iter != runLocs.end (); ++iter) {
		
		runPQ.push (new Run (&file, previous, *iter));
		previous = *iter;
		
	}
	
	while (!runPQ.empty ()) {
		
		Run *temp = runPQ.top ();
		runPQ.pop ();
		
		if (temp->Next (&rec) == 1) {
			
			runPQ.push(temp);
			
		} else {
			
			delete temp;
			
		}

		info->out->Insert (&rec);
		
	}

    // finally shut down the out pipe
	info->out->ShutDown ();
	
	file.Close ();
	remove (tmp);
	
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
	pthread_t sortThread;
	BigQInfo *info = new BigQInfo {
		&in,
		&out,
		&sortorder,
		runlen
	};
	
	pthread_create (&sortThread, NULL, sortingThread, (void *) info);
	
}

BigQ :: ~BigQ () {
}

Run :: Run (File *file, off_t startIndex, off_t endIndex) {
	
	this->file = file;
	this->curIndex = startIndex;
	this->endIndex = endIndex;
	
	this->bufferPage = new Page;
	this->file->GetPage (this->bufferPage, curIndex++);
	
	this->record = new Record;
	this->bufferPage->GetFirst (this->record);
	
}

int Run :: Next (Record *current) {
	
	current->Consume (record);
	
	if (bufferPage->GetFirst (record) == 1) {
		
		return 1;
		
	} else {
		
		if (curIndex < endIndex) {
			
			file->GetPage (bufferPage, curIndex++);
			bufferPage->GetFirst (record);
			
			return 1;
			
		} else {
			
			return 0;
			
		}
		
	}
	
	
}

Run :: ~Run () {
	
	delete record;
	delete bufferPage;
	
}
