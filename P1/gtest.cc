#include "gtest/gtest.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "test.h"

relation *rel;

TEST (DBFILETEST, RECORDINTEGRITY) {
	
	FILE *file;
	
	DBFile dbfile;
	Record temp;
	Record sample;
	
	OrderMaker *order = new OrderMaker ((rel->schema ()));
	ComparisonEngine comp;
	
	dbfile.Create(rel->path(), heap, NULL);
	
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
	dbfile.Load (*(rel->schema ()), tbl_path);
	
	dbfile.MoveFirst ();
	
	file = fopen(tbl_path, "r");
	
	while (dbfile.GetNext (temp) == 1 &&
		   sample.SuckNextRecord(rel->schema (), file) == 1) {
		
		EXPECT_EQ (comp.Compare (&temp, &sample, order), 0);
		
	}
	
	EXPECT_EQ (dbfile.GetNext (temp), sample.SuckNextRecord(rel->schema (), file));
	
	fclose(file);
	
}

int main (int argc, char **argv) {
	
	testing::InitGoogleTest(&argc, argv);
	
	setup (catalog_path, dbfile_dir, tpch_dir);

	relation *rel_ptr[] = {n, r, c, p, ps, o, li};

	int findx = 0;
	while (findx < 1 || findx > 7) {
		cout << "\n select table: \n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. orders \n";
		cout << "\t 7. lineitem \n \t ";
		cin >> findx;
	}

	rel = rel_ptr [findx - 1];
	
	return RUN_ALL_TESTS ();
	
}
