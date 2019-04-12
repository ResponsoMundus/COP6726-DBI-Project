#include "Statistics.h"

RelInfo :: RelInfo () {
	
	partNum = -1;
	
}

RelInfo :: RelInfo (ReelInfo &copyMe) {
	
	temp.partNum = copyMe.partNum;
	temp.tupleNum = copyMe.tupleNum;
	
	for (auto it = copyMe.distinctAtts.begin (); it != copyMe.distinctAtts.end(); it++) {
		
		temp.distinctAtts[it->first] = it->second;
		
	}
	
}

Statistics :: Statistics () {
	
	partitionNum = 0;
	
}

Statistics :: Statistics (Statistics &copyMe) {
	
	partitionNum = copyMe.GetPartitionNum ();
	
	map<string, RelInfo> *SourceRelMap = copyMe.GetRelMap ();
	map<string, vector<string>> *SourceRelAtts = CopyMe.GetRelAtts ();
	map<int, vector<string>> *SourcePartInfo = copyMe.GetPartInfo ();
	
	for (auto iter = SourceRelMap->begin (); iter != SourceRelMap->end (); iter++) {
		
		RelInfo temp (iter->second);
		
		relMap[iter->first] = temp;
		
	}
	
	for (auto iter = SourceRelAtts->begin (); iter != SourceRelAtts->end (); iter++) {
		
		vector<string> temp;
		
		for (auto it = iter->second.begin (); it != iter->second.end (); it++) {
			
			temp.push_back (*iter);
			
		}
		
		relAtts[iter->first] = temp;
		
	}
	
	for (auto iter = SourcePartInfo->begin (); iter != SourcePartInfo->end (); iter++) {
		
		vector<string> temp;
		
		for (auto it = iter->second.begin (); it != iter->second.end (); it++) {
			
			temp.push_back (*iter);
			
		}
		
		relAtts[iter->first] = temp;
		
	}
	
}

Statistics :: ~Statistics () {}

void Statistics :: AddRel (char *relName, int numTuples) {
	
	string relStr (relName);
	auto iter = relMap.find (relStr);
	
	if (iter == relMap.end ()) {
		
		RelInfo newInfo;
		newInfo.tupleNum = numTuples;
		
		relMap[relStr] = newInfo;
		
	} else {
		
		iter->second.tupleNum = numTuples;
		
	}
	
}

void Statistics :: AddAtt (char *relName, char *attName, int numDistincts) {
	
	string relStr (relName);
	string attStr (attName);
	
	auto iter = relMap.find (relStr);
	
	if (iter == relMap.end ()) {
		
		RelInfo newInfo;
		newInfo.distinctAtts[attStr] = numDistincts;
		
		relMap[relStr] = newInfo;
		
		vector<string> newVec;
		newVec.push_back (relStr);
		
		relAtts[attStr] = newVec;
		
	} else {
		
		auto it = iter->second.distinctAtts.find (attStr);
		
		if (it == iter->second.distinctAtts.end ()) {
			
			iter->second.distinctAtts[attStr] = numDistincts;
			
			vector<string> newVec;
			newVec.push_back (relStr);
			relAtts[attStr] = newVec;
			
		} else {
			
			it->second = numDistincts;
			
		}
		
	}
	
}

void Statistics :: CopyRel (char *oldName, char *newName) {
	
	string oldStr (oldName);
	string newStr (newName);
	
	auto iter = relMap.find(oldStr);
	
	if (iter == relMap.end ()) {
		
		cout << "Relation " << oldName << " don't seem to exist!" << endl;
		
		return;
		
	}
	
	RelInfo newInfo (iter->second);
	
	for (auto it = newInfo.distinctAtts.begin (); it != newInfo.distinctAtts.end (); it++) {
		
		relAtts[it->first].push_back (newStr);
		
	}
	
	relMap[newStr] = newInfo;
	
}
	
void Statistics :: Read (char *fromWhere) {
	
	
	
}

void Statistics :: Write (char *toWhere) {
	
	
	
}

void Statistics :: Apply (struct AndList *parseTree, char *relNames[], int numToJoin) {
	
	
	
	
	
	
}

double Statistics :: Estimate (struct AndList *parseTree, char **relNames, int numToJoin) {
	
	
	
}

int Statistics :: GetPartitionNum () {
	
	return partitionNum;
	
}

map<string, RelInfo> *Statistics :: GetRelMap () {
	
	return &relMap;
	
}

map<string, vector<string>> *Statistics :: GetRelAtts () {
	
	return relAtts;
	
}

map<int, vector<string>> *Statistics :: GetPartInfo () {
	
	return partInfo;
	
}
