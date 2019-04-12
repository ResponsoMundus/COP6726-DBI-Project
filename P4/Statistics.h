#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <string>
#include <vector>
#include <iostream>

using namespace std;

class RelInfo {

public:
	
	int partNum;
	int recNum;
	map<string, int> distinctAtts;
	
	RelInfo ();
	RelInfo (RelInfo &copyMe);
	
};

class Statistics {

private:
	
	int partitionNum;
	map<string, RelInfo> relMap;
	map<string, vector<string>> relAtts;
	map<int, vector<string>> partInfo;
	
public:
	
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *toWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	
	int GetPartitionNum ();
	map<string, RelInfo> *GetRelMap ();
	map<string, vector<string>> *GetRelAtts ();
	map<int, vector<string>> *GetPartInfo ();

};

#endif
