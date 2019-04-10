#include "RelOp.h"

using namespace std;

int buffSize = 100;

// Operation Starter
void *_StartOp (void *arg) {
	
	((RelOp *)arg)->Start ();
	
}

// Set runLen
void RelOp :: Use_n_Pages (int n) {
	
	runLen = n;
	
}

// Wait Until pthread Done
void RelOp :: WaitUntilDone () {
	
	pthread_join (t, NULL);
	
}

/*                    Select Pipe                    */

void SelectPipe :: Run (
	Pipe &inPipe,
	Pipe &outPipe,
	CNF &selOp,
	Record &literal
) {
	
	in = &inPipe;
	out = &outPipe;
	cnf = &selOp;
	lit = &literal;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}

void SelectPipe :: Start () {
	
	Record *tmp = new Record ();
	ComparisonEngine comp;
	
	while (in->Remove (tmp)) {
		
		if (comp.Compare (tmp, lit, cnf)) {
			
			out->Insert (tmp);
			
		}
		
	}
	
	out->ShutDown ();
	
	delete tmp;
	
}

/*                    Select File                    */

void SelectFile :: Run (
	DBFile &inFile,
	Pipe &outPipe,
	CNF &selOp,
	Record &literal
) {
	
	file = &inFile;
	out = &outPipe;
	cnf = &selOp;
	lit = &literal;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}

void SelectFile :: Start () {
	
	Record *tmp = new Record ();
	ComparisonEngine comp;
	
	// int i = 0;
	
	while (file->GetNext (*tmp, *cnf, *lit)) {
		
		out->Insert (tmp);
		// i++;
		
	}
	
	// cout << i << " records inserted!" << endl;
	
	out->ShutDown ();
	
	delete tmp;
	
}

/*                      Project                      */
void Project :: Run (
	Pipe &inPipe,
	Pipe &outPipe,
	int *keepMe,
	int numAttsInput,
	int numAttsOutput
) {
	
	in = &inPipe;
	out = &outPipe;
	attsToKeep = keepMe;
	numAttsIn = numAttsInput;
	numAttsOut = numAttsOutput;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}
	
void Project :: Start () {
	
	// int count = 0;
	
	Record *tmp = new Record ();
	
	while (in->Remove (tmp)) {
		
		tmp->Project (attsToKeep, numAttsOut, numAttsIn);
		out->Insert (tmp);
		// count++;
		
	}
	
	// cout << count << " projected!" << endl;
	
	out->ShutDown ();
	
	delete tmp;
	
}

/*                       Join                        */
void Join :: Run (
	Pipe &inPipeL,
	Pipe &inPipeR,
	Pipe &outPipe,
	CNF &selOp,
	Record &literal
) {
	
	inL = &inPipeL;
	inR = &inPipeR;
	out = &outPipe;
	cnf = &selOp;
	lit = &literal;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}
	
void Join :: Start () {
	
	int count = 0;
	
	int leftNumAtts, rightNumAtts;
	int totalNumAtts;
	int *attsToKeep;
	
	ComparisonEngine comp;
	
	Record *fromLeft = new Record ();
	Record *fromRight = new Record ();
	
	OrderMaker leftOrder, rightOrder;
	
	cnf->GetSortOrders(leftOrder, rightOrder);
	
	if (leftOrder.numAtts > 0 && rightOrder.numAtts > 0) {
		
		// cout << "bigq Version" << endl;
		
		Pipe left (buffSize);
		Pipe right (buffSize);
		
		BigQ bigqLeft (*inL, left, leftOrder, runLen);
		BigQ bigqRight (*inR, right, rightOrder, runLen);
		
		bool isDone = false;
		
		if (!left.Remove (fromLeft)) {
			
			isDone = true;
			
		} else {
			
			leftNumAtts = fromLeft->GetLength ();
			
		}
		
		if (!isDone && !right.Remove (fromRight)) {
			
			isDone = true;
			
		} else {
			
			rightNumAtts = fromRight->GetLength ();
			totalNumAtts = leftNumAtts + rightNumAtts;
			
			attsToKeep = new int[totalNumAtts];
			
			for (int i = 0; i < leftNumAtts; i++) {
				
				attsToKeep[i] = i;
				
			}
			
			for (int i = 0; i < rightNumAtts; i++) {
				
				attsToKeep[leftNumAtts + i] = i;
				
			}
			
		}
		
		/* Move left pipe as a reference and right as a follow up
		 * (which means fromLeft is always bigger than or equal to 
		 * fromRight) until one of them is done. When 
		 * fromLeft == fromRight merge and insert.
		 */
		while (!isDone) {
			
			while (comp.Compare (fromLeft, &leftOrder, fromRight, &rightOrder) > 0) {
				
				if (!right.Remove (fromRight)) {
					
					isDone = true;
					break;
					
				}
				
			}
			
			while (!isDone && comp.Compare (fromLeft, &leftOrder, fromRight, &rightOrder) < 0) {
				
				if (!left.Remove (fromLeft)) {
					
					isDone = true;
					break;
					
				}
				
			}
			
			while (!isDone && comp.Compare (fromLeft, &leftOrder, fromRight, &rightOrder) == 0) {
				
				Record *tmp = new Record ();
				
				tmp->MergeRecords (
					fromLeft,
					fromRight,
					leftNumAtts,
					rightNumAtts,
					attsToKeep,
					totalNumAtts,
					leftNumAtts
				);
				
				// count++;
				
				out->Insert (tmp);
				
				if (!right.Remove (fromRight)) {
					
					isDone = true;
					break;
					
				}
				
			}
			
		}
		
		while (right.Remove (fromLeft));
		while (left.Remove (fromLeft));
		
	} else {
		
		// cout << "Nested Loop Version" << endl;
		
		char fileName[100];
		sprintf (fileName, "temp.tmp");
		
		Heap dbFile;
		dbFile.Create (fileName);
		
		bool isDone = false;
		
		if (!inL->Remove (fromLeft)) {
			
			isDone = true;
			
		} else {
			
			leftNumAtts = fromLeft->GetLength ();
			
		}
		
		if (!inR->Remove (fromRight)) {
			
			isDone = true;
			
		} else {
			
			rightNumAtts = fromRight->GetLength ();
			totalNumAtts = leftNumAtts + rightNumAtts;
			
			attsToKeep = new int[totalNumAtts];
			
			for (int i = 0; i < leftNumAtts; i++) {
				
				attsToKeep[i] = i;
				
			}
			
			for (int i = 0; i < rightNumAtts; i++) {
				
				attsToKeep[leftNumAtts + i] = i;
				
			}
			
		}
		
		if (!isDone) {
			
			do{
				
				dbFile.Add (*fromLeft);
				
			} while (inL->Remove (fromLeft));
			
			do{
				
				dbFile.MoveFirst ();
				
				Record *newRec = new Record ();
				
				while (dbFile.GetNext (*fromLeft)) {
					
					if (comp.Compare (fromLeft, fromRight, lit, cnf)) {
						
						newRec->MergeRecords (
							fromLeft,
							fromRight,
							leftNumAtts,
							rightNumAtts,
							attsToKeep,
							totalNumAtts,
							leftNumAtts
						);
						
						// count++;
						
						out->Insert (newRec);
						
					}
					
				}
				
				delete newRec;
				
			} while (inR->Remove (fromRight));
			
		}
		
		dbFile.Close ();
		remove ("temp.tmp");
		
	}
	
	// cout << count << " records join!" << endl;
	
	out->ShutDown ();
	
	delete fromLeft;
	delete fromRight;
	delete attsToKeep;
	
}

/*                 Duplicate Removal                 */

void DuplicateRemoval :: Run (
	Pipe &inPipe,
	Pipe &outPipe,
	Schema &mySchema
) {
	
	in = &inPipe;
	out = &outPipe;
	schema = &mySchema;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}

void DuplicateRemoval :: Start () {
	
	ComparisonEngine comp;
	
	// int inCount = 1, outCount = 0;
	
	Record *curr = new Record ();
	Record *prev = new Record ();
	OrderMaker sortOrder (schema);
	
	Pipe p (buffSize);
	BigQ bigq (*in, p, sortOrder, runLen);
	
	p.Remove (prev);
	
	while (p.Remove (curr)) {
		
		// inCount++;
		
		if (comp.Compare (prev, curr, &sortOrder)) {
			
			out->Insert (prev);
			prev->Copy (curr);
			// outCount++;
			
		}
		
	}
	
	if (curr->bits != NULL && !comp.Compare (curr, prev, &sortOrder)) {
		
		out->Insert (prev);
		prev->Copy (curr);
		// outCount++;
		
	}
	
	// cout << inCount << " records inputed!" << endl;
	// cout << outCount << " records outputed!" << endl; 
	
	out->ShutDown ();
	
	delete curr;
	delete prev;
	
}

/*                        Sum                        */
void Sum :: Run (
	Pipe &inPipe,
	Pipe &outPipe,
	Function &computeMe
) {
	
	in = &inPipe;
	out = &outPipe;
	compute = &computeMe;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}

void Sum :: Start () {
	
	Type t;
	
	stringstream ss;
	Attribute atts;
	Record *tmp = new Record ();
	
	// int count = 1;
	
	int integerSum = 0, integerRec;
	double doubleSum = 0.0, doubleRec;
	
	atts.name = "SUM";
	if (!in->Remove (tmp)) {
		
		cout << "You screwed up" << endl;
		out->ShutDown ();
		return ;
		
	}
	
	atts.myType = compute->Apply (*tmp, integerRec, doubleRec);
	
	if (atts.myType == Int) {
		
		integerSum += integerRec;
		
	} else {
		
		doubleSum += doubleRec;
		
	}
	
	while (in->Remove (tmp)) {
		
		compute->Apply (*tmp, integerRec, doubleRec);
		
		// count++;
		
		if (atts.myType == Int) {
			
			integerSum += integerRec;
			
		} else {
			
			doubleSum += doubleRec;
			
		}
		
	}
	
	// cout << doubleSum << endl;
	
	if (atts.myType == Int) {
		
		Schema sumSchema (NULL, 1, &atts);
		
		ss << integerSum << '|';
		tmp->ComposeRecord (&sumSchema, ss.str ().c_str ());
		
	} else {
		
		Schema sumSchema (NULL, 1, &atts);
		
		ss << doubleSum << '|';
		tmp->ComposeRecord (&sumSchema, ss.str ().c_str ());
		
	}
	
	// cout << count << " records sum!" << endl;
	
	out->Insert (tmp);
	out->ShutDown ();
	
}

/*                      Group By                     */
void GroupBy :: Run (
	Pipe &inPipe,
	Pipe &outPipe,
	OrderMaker &groupAtts,
	Function &computeMe
) {
	
	in = &inPipe;
	out = &outPipe;
	order = &groupAtts;
	compute = &computeMe;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}

void GroupBy :: Start () {
	
	Type t;
	Schema *sumSchema;
	Attribute att;
	stringstream ss;
	
	ComparisonEngine comp;
	
	Pipe sortPipe (buffSize);
	
	Record *prev = new Record ();
	Record *curr = new Record ();
	Record *sum = new Record ();
	Record *newRec = new Record ();
	
	// cout << "group by started" << endl;
	
	BigQ bigq (*in, sortPipe, *order, runLen);
	
	int count = 0;
	
	int integerSum = 0, integerRec;
	double doubleSum = 0.0, doubleRec;
	
	char *sumStr = new char[buffSize];
	
	int numAtts = order->numAtts;
	int *atts = order->whichAtts;
	int *attsToKeep = new int[numAtts + 1];
	
	attsToKeep[0] = 0;
	
	for (int i = 0; i < numAtts; i++) {
		
		attsToKeep[i + 1] = atts[i];
		
	}
	
	if (sortPipe.Remove (prev)) {
		
		t = compute->Apply (*prev, integerRec, doubleRec);
		
		if (t == Int) {
			
			integerSum += integerRec;
			// cout << integerRec << endl;
			
		} else {
			
			doubleSum += doubleRec;
			// cout << doubleRec << endl;
			
		}
		
	} else {
		
		cout << "No output from sortPipe!" << endl;
		
		out->ShutDown();
		
		delete sumStr;
		delete sumSchema;
		delete prev;
		delete curr;
		delete sum;
		delete newRec;
		
		exit (-1);
		
	}
	
	att.name = "SUM";
	att.myType = t;
	
	sumSchema = new Schema (NULL, 1, &att);
	
	// cout << "doing group by" << endl;
	
	while (sortPipe.Remove (curr)) {
		
		if (comp.Compare (prev, curr, order) != 0) {
			// prev != curr
			
			if (t == Int) {
				
				sprintf (sumStr, "%d|", integerSum);
				
			} else {
				
				sprintf (sumStr, "%f|", doubleSum);
				
			}
			
			sum->ComposeRecord (sumSchema, sumStr);
			newRec->MergeRecords (sum, prev, 1, prev->GetLength (), attsToKeep, numAtts + 1, 1);
			
			// cout << "Inserted!" << endl;
			out->Insert (newRec);
			
			count++;
			// cout << count << " group done!" << endl;
			
			
			doubleSum = 0.0;
			integerSum = 0;
			compute->Apply (*curr, integerRec, doubleRec);
			
			if (t == Int) {
			
				integerSum += integerRec;
				
			} else {
				
				doubleSum += doubleRec;
				
			}
			
			prev->Consume (curr);
			
			
		} else {
			// prev == curr
			
			compute->Apply (*curr, integerRec, doubleRec);
			
			if (t == Int) {
				
				integerSum += integerRec;
				
			} else {
				
				doubleSum += doubleRec;
				
			}
			
		}
		
	}
	
	if (t = Int) {
		
		sprintf (sumStr, "%d|", integerSum);
		
	} else {
		
		sprintf (sumStr, "%f|", doubleSum);
		
	}
	
	count++;
	sum->ComposeRecord (sumSchema, sumStr);
	newRec->MergeRecords (sum, prev, 1, prev->GetLength (), attsToKeep, numAtts + 1, 1);
	out->Insert (newRec);
	// cout << count << " group done!" << endl;
	
	// cout << "Inserted " << count << " groups!" << endl;
	
	out->ShutDown ();
	
	delete sumStr;
	delete sumSchema;
	delete prev;
	delete curr;
	delete sum;
	delete newRec;
	
}

/*                     Write Out                     */
void WriteOut :: Run (
	Pipe &inPipe,
	FILE *outFile,
	Schema &mySchema
) {
	
	in = &inPipe;
	file = outFile;
	schema = &mySchema;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}

void WriteOut :: Start () {
	
	Record *tmp = new Record ();
	
	while (in->Remove (tmp)) {
		
		tmp->WriteToFile (file, schema);
		
	}
	
	fclose (file);
	
	delete tmp;
	
}
