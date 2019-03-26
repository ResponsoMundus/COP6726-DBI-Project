#include "RelationOp.h"

using namespace std;

int buffSize = 100;

// Operation Starter
void *_StartOp (void *arg) {
	
	((RelationOp *)arg)->Start ();
	
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
	cnf = &cnf;
	lit = &literal;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}

void SelectPipe :: Start () {
	
	Record *temp = new Record ();
	ComparisonEngine comp;
	
	while (in->Remove (temp) == 1) {
		
		if (comp.compare (temp, lit, cnf) == 1) {
			
			out->Insert (temp);
			
		}
		
	}
	
	out->ShutDown ();
	
	delete temp;
	
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
	cnf = &cnf;
	lit = &literal;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}

void SelectFile :: Start () {
	
	Record *temp = new Record ();
	ComparisonEngine comp;
	
	while (file->GetNext (*temp, *cnf, *lit) == 1) {
		
		out->Insert (temp);
		
	}
	
	out->ShutDown ();
	
	delete temp;
	
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
	attsToKeep = &keepMe;
	numAttsIn = &numAttsInput;
	numAttsOut = &numAttsOutput;
	
	pthread_create (&t, NULL, _StartOp, (void *) this);
	
}
	
void Project :: Start () {
	
	Record *temp = new Record ();
	
	while (in->Remove (temp)) {
		
		temp->Project (attsToKeep, numAttsOut, numAttsIn);
		out->Insert (temp);
		
	}
	
	in->ShutDown ();
	out->ShutDown ();
	
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
	
	ComparisonEngine comp;
	OrderMaker leftOrder, rightOrder;
	
	Record *fromLeft = new Record ();
	Record *fromRight = new Record ();
	
	Pipe *left = new Pipe (buffSize);
	Pipe *right = new Pipe (buffSize);
	
	vector<Record *> vecLeft, vecRight;
	
	int attsNumLeft = fromLeft->GetLength ();
	int attsNumRight = fromRight->GetLength ();
	int attsNum = attsNumLeft + attsNumRight;
	int atts[attsNum];
	
	for (int i = 0; i < attsNum; i++) {
		
		atts[i] = i;
		
	}
	
	if (myCNF->GetSortOrders (leftOrder, rightOrder)) {
		
		BigQ bigqLeft (*inL, *left, leftOrder, runLen); 
		BigQ bigqRight (*inR, *right, rightOrder, runLen);
		
		left->Remove (fromLeft);
		right->Remove (fromRight);
		
		while (true) {
			
			if (comp.Compare (fromLeft, &leftOrder, fromRight, &rightOrder) < 0) {
				
				if (!left->Remove (fromLeft)) {
					
					break;
					
				}
				
			} else if (comp.Compare (fromLeft, &leftOrder, fromRight, &rightOrder) > 0) {
				
				if (!right->Remove (fromRight)) {
					
					break;
					
				}
				
			} else {
				
				bool emptyFlag = false;
				
				// Search for all records that equals the current
				// one from left Pipe
				while (true) {
					
					Record *tmp = new Record ();
					
					tmp->Consume (fromLeft);
					vecLeft.push_back (tmp);
					
					if (!left->Remove (fromLeft)) {
						
						emptyFlag = true;
						break;
						
					}
					
					if (comp.Compare (tmp, fromLeft, &leftOrder)) {
						
						break;
						
					}
					
				}
				
				// Search for all records that equals the current
				// one from right Pipe
				while (true) {
					
					Record *tmp = new Record ();
					
					tmp->Consume (fromRight);
					vecRight.push_back (tmp);
					
					if (!right->Remove (fromRight)) {
						
						emptyFlag = true;
						break;
						
					}
					
					if (comp.Compare (tmp, fromRight, &rightOrder)) {
						
						break;
						
					}
					
				}
				
				// Combine all records that is equal
				for (auto iterLeft = vecLeft.begin (); iterLeft != vecLeft.end (); iterLeft++) {
					
					for (auto iterRight = vecRight.begin (); iterRight != vecRight.end (); iterRight++) {
						
						if (lit->GetLength () && comp.Compare (*iterLeft, *iterRight, lit, cnf)) {
							
							Record *tmp = new Record ();
							
							tmp->MergeRecords (
								*iterLeft,
								*iterRight,
								attsNumLeft,
								attsNumRight,
								atts,
								attsNum,
								attsNumLeft
							);
							out->Insert (tmp);
							
						}
						
					}
					
				}
				
				vecLeft.clear ();
				vecRight.clear ();
				
				if (emptyFlag) {
					
					break;
					
				}
				
			}
			
		}
		
	} else {
		
		while (true) {
			
			Record *tmp = new Record ();
			
			tmp->Consume (fromLeft);
			vecLeft.push_back (tmp);
			
			if (!right->Remove (fromLeft)) {
				
				break;
						
			}
			
		}
		
		while (true) {
			
			Record *tmp = new Record ();
			
			tmp->Consume (fromRight);
			vecRight.push_back (tmp);
			
			if (!right->Remove (fromRight)) {
				
				break;
				
			}
		
		}
		
		for (auto iterLeft = vecLeft.begin (); iterLeft != vecLeft.end (); iterLeft++) {
			
			for (auto iterRight = vecRight.begin (); iterRight != vecRight.end (); iterRight++) {
				
				if (lit->GetLength () && comp.Compare (*iterLeft, *iterRight, lit, cnf)) {
					
					Record *tmp = new Record ();
							
					tmp->MergeRecords (
						*iterLeft,
						*iterRight,
						attsNumLeft,
						attsNumRight,
						atts,
						attsNum,
						attsNumLeft
					);
					out->Insert (tmp);
					
				}
				
			}
			
		}
		
	}
	
	out->ShutDown ();
	
}

/*                 Duplicate Removal                 */

/*                        Sum                        */

/*                      Group By                     */

/*                     Write Out                     */

