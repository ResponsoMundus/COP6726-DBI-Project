#include "gtest/gtest.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "BigQ.h"
#include "test.h"

int runlen;

int buffsz = 100;
Pipe input (buffsz);
Pipe output (buffsz);

void *producer (void *arg) {

	Pipe *myPipe = (Pipe *) arg;

	Record temp;
	int counter = 0;

	DBFile dbfile;
	dbfile.Open (rel->path ());
	cout << " producer: opened DBFile " << rel->path () << endl;
	dbfile.MoveFirst ();

	while (dbfile.GetNext (temp) == 1) {
		counter += 1;
		if (counter%100000 == 0) {
			 cerr << " producer: " << counter << endl;	
		}
		myPipe->Insert (&temp);
	}

	dbfile.Close ();
	myPipe->ShutDown ();

	cout << " producer: inserted " << counter << " recs into the pipe\n";
	
}

TEST (BIGQTEST, WIRTETEST) {
	
	int i = 0;
	
	Record rec[2];
	Record *last = NULL, *prev = NULL;
	
	ComparisonEngine comp;
	OrderMaker sortorder (rel->schema ());
	
	while(output.Remove (&rec[i%2])) {
		
		prev = last;
		last = &rec[i%2];
		
		if (prev && last) {
			
			ASSERT_NE (comp.Compare (prev, last, &sortorder), 1);
			
		}
		
		i++;
		
	}
	
	cleanup ();
	
}

int main (int argc, char **argv) {
	
	testing::InitGoogleTest(&argc, argv);
	
	setup ();
	
	relation *rel_ptr[] = {n, r, c, p, ps, o, li};
	
	int findx = 0;
	while (findx < 1 || findx > 8) {
		cout << "\n select dbfile to use: \n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. orders \n";
		cout << "\t 7. lineitem \n\t ";
		cin >> findx;
	}
	rel = rel_ptr [findx - 1];
	
	cout << "\t\n specify runlength:\n\t ";
	cin >> runlen;
	
	// using default order
	OrderMaker sortorder (rel->schema ());

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread;
	pthread_create (&thread, NULL, producer, (void *)&input);

	BigQ bq (input, output, sortorder, runlen);
	
	pthread_join (thread, NULL);
	
	return RUN_ALL_TESTS ();
	
}
