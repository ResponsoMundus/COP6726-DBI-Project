#include "Statistics.h"

AttributeInfo :: AttributeInfo () {}

AttributeInfo :: AttributeInfo (string name, int num) {
	
	attrName = name;
	distinctTuples = num;
	
}

AttributeInfo :: AttributeInfo (AttributeInfo &copyMe) {
	
	attrName = copyeMe.name;
	distinctTuples = copyMe.distinctTuples;
	
}

AttributeInfo AttributeInfo :: operator= (AttributeInfo &copyMe) {
	
	attrName = copyeMe.name;
	distinctTuples = copyMe.distinctTuples;
	
	return *this;
	
}

RelationInfo :: RelationInfo () {
	
	isJoint = false;
	
}

RelationInfo :: RelationInfo (string name, int tuples) {
	
	isJoint = false;
	relName = name;
	numTuples = tuples;
	
}

RelationInfo :: RelationInfo (RelationInfo &copyMe) {
	
	isJoint = copyMe.isJoint;
	relName = copyMe.relName;
	numTuples = copyMe.numTuples;
	attrMap.insert (copyMe.attrMap.begin (), copyMe.attrMap.end ());
	
}

RelationInfo RelationInfo :: operator= (RelationInfo &copyMe) {
	
	isJoint = copyMe.isJoint;
	relName = copyMe.relName;
	numTuples = copyMe.numTuples;
	attrMap.insert (copyMe.attrMap.begin (), copyMe.attrMap.end ());
	
	return *this;
	
}

bool RelationInfo :: isRelationPresent (string relName) {
	
	if (relName == relName) {
		
		return true;
		
	}
	
	auto it = relJoint.find(relName);
	
	if (it != relJoint.end ()) {
		
		return true;
		
	}
	
	return false;
	
}

Statistics :: Statistics () {}

Statistics :: Statistics (Statistics &copyMe) {
	
	relMap.insert (copyMe.relMap.begin (), copyMe.relMap.end ());
	
}

Statistics :: ~Statistics () {}

Statistics Statistics :: operator= (Statistics &copyMe) {
	
	relMap.insert (copyMe.relMap.begin (), copyMe.relMap.end ());
	
	return *this;
	
}

double AndOp (AndList *andList, char *relName[], int numJoin) {
	
	if (andList == NULL) {
		
		return 1.0;
		
	}
	
	double left = 1.0;
	double right = 1.0;
	
	left = OrOp (andList->left, relName, numJoin);
	right = AndOp (andList->right, relName, numJoin);
	
	return left * right;
	
}

double OrOp (OrList *orList, char *relName[], int numJoin) {
	
	if (orList == NULL) {
		
		return 0.0
		
	}
	
	double left = 0.0;
	double right = 0.0;
	
	left = ComOp (orList->left, relName, numJoin);
	
	int count = 1;
	
	OrList *temp = orList->rightOr;
	char *attrName = orList->left->left->value;
	
	while (temp) {
		
		if (strcmp (orList->left->left->value, attrName) == 0) {
			
			count++;
			
		}
		
		temp = temp->rightOr;
		
	}
	
	if (count > 1) {
		
		return (double) count * left;
		
	}
	
	right = Or (orList->rightOr, relName, numJoin);
	
	return (double) (1.0 - (1.0 - left) * (1.0 - right));
	
}

double ComOp (ComparisonOp *comOp, char *relName[], int numJoin) {
	
	RelationInfo leftRel, rightRel;
	
	double left = 0.0;
	double right = 0.0;
	
	int leftResult = GetRelForOp (comOp->left, relName, numJoin, leftRel);
	int rightResult = GetRelForOp (comOp-right, relName, numJoin, rightRel);
	int code = comOp->code;
	
	if (comOp->left->code == NAME) {
		
		if (leftResult < 0) {
			
			cout << comOp->left->value << " not present in any relation!" << endl;
			left = 1.0;
			
		} else {
			
			left = leftRel.attrMap[comOp->left->value].distinctTuples;
			
		}
		
	} else {
		
		left = -1.0;
		
	}
	
	if (comOp->right->code == NAME) {
		
		if (rightResult < 0) {
			
			cout << comOp->right->value << " not present in any relation!" << endl;
			right = 1.0;
			
		} else {
			
			right = rightRel.attrMap[comOp->left->value].distinctTuples;
			
		}
		
	} else {
		
		right = -1.0;
		
	}
	
	if (code == LESS_THAN || code == GREATER_THAN) {
		
		return 1.0 / 3.0;
		
	} else if (code == EQUALS) {
		
		if (left > right) {
			
			return 1.0 / left;
			
		} else {
			
			return 1.0 / right;
			
		}
		
	}
	
	cout << "Error!" << endl;
	return 0.0;
	
}

int GetRelForOp (Operand *operand, char *relName[], int numJoin, RelationInfo &relInfo) {
	
	if (operand == NULL) {
		
		return -1;
		
	}
	
	if (relName == NULL) {
		
		return -1;
		
	}
	
	RelMap relMap;
	
	for (auto iter = relMap.begin (); iter != relMap.end (); iter++) {
		
		if (iter->second.attrMap.find (operand->value) != iter->second.attrMap.end()) {
			
			relation = iter->second;
			
			return 0;
			
		}
		
	}
	
	return -1;
	
}

void Statistics :: AddRel (char *relName, int numTuples) {
	
	RelationInfo temp(relName, numTuples);
	
	relMap[relName] = temp;
	
}

void Statistics :: AddAtt (char *relName, char *attrName, int numDistincts) {
	
	AttributeInfo temp(attrName, numDistincts);
	
	relMap[relName].attrMap[attrName] = temp;
	
}

void Statistics :: CopyRel (char *oldName, char *newName) {
	
	string oldStr (oldName);
	string newStr (newName);
	
	relMap[newStr] = relMap[oldStr];
	relMap[newStr].relName = newStr;
	
	AttrMap newAttrMap;
	
	for (
		auto it = relMap[newStr].attrMap.begin ();
		it != relMap[newStr].attrMap.end ();
		it++
	) {
		
		AttributeInfo temp (it->second);
		
		newAttrMap[it->first] = temp;
		
	}
	
	relMap[newStr].attrMap = newAttrMap;
	
}
	
void Statistics :: Read (char *fromWhere) {
	
	int numRel, numJoint, numAttr, numTuples, numDistincts;
	string relName, jointName, attrName;
	
	ifstream in (fromWhere);
	
	if (!in) {
		
		cout << "File \"" << fromWhere << "\" does not exist!" << endl;
		
	}
	
	relMap.clear ();
	
	in >> numRel;
	
	for (int i = 0; i < numRel; i++) {
		
		in >> relName;
		in >> numTuples;
		
		RelationInfo relation (relName, numTuples);
		relMap[relName] = relation;
		
		in >> relMap[relName].isJoint;
		
		if (relMap[relName].isJoint) {
			
			in >> numJoint;
			
			for (int j = 0; j < numJoint; j++) {
				
				in >> jointName;
				
				relMap[relName].relJoint[jointName] = jointName;
				
			}
			
		}
		
		in >> numAttr;
		
		for (int j = 0; j < numAttr; j++) {
			
			in >> attrName;
			in >> numDistincts;
			
			AttributeInfo attrInfo (attrName, numDistincts);
			
			relMap[relName].attrMap[attrName] = attrInfo;
			
		}
		
	}
	
}

void Statistics :: Write (char *toWhere) {
	
	ofstream out (toWhere);
	
	out << relMap.size () << endl;
	
	for (
		auto iter = relMap.begin ();
		iter != relMap.end ();
		iter++
	) {
		
		out << iter->second.relName << endl;
		out << iter->second.numTuples << endl;
		out << iter->second.isJoint << endl;
		
		if (iter->second.isJoint) {
			
			out << iter->second.relJoint.size () << endl;
			
			for (
				auto it = iter->second.relJoint.begin ();
				it != iter->second.relJoint.end ();
				it++
			) {
				
				out << it->second << endl;
				
			}
			
		}
		
		out << iter->second.attrMap.size () << endl;
		
		for (
			auto it = iter->second.attrMap.begin ();
			it != iter->second.attrMap.end ();
			it++
		) {
			
			out << it->second.attrName << endl;
			out << it->second.distinctTuples << endl;
			
		}
		
	}
	
	out.close ();
	
}

void Statistics :: Apply (struct AndList *parseTree, char *relNames[], int numToJoin) {
	
	int index = 0;
	int numJoin = 0;
	char *names[100];
	
	RelationInfo rel;
	
	while (index < numToJoin) {
		
		auto iter = relMap.find (relNmaes[index]);
		
		if (iter != relMap.end ()) {
			
			relation = iter->second;
			
			names[numJoin++] = relNams[index];
			
			if (!rel.isJoint) {
				
				index++;
				
			}
			
		} else {
			
			int size = rel.relJoint.size();
			
			if (size <= numToJoin) {
				
				for (int i = 0; i < numToJoin; i++) {
					
					if (rel.relJoint.find (relNames[i]) == rel.relJoint.end () &&
						rel.relJoint[relNames[i]] != rel.relJoint[relNames[index]) {
						
						cout << "Cannot be joined!" << endl;
						
						return;
						
					}
					
				}
				
			} else {
				
				cout << "Cannot be joined!" << endl;
				
			}
			
		}
		
		index++;
		
	}
	
	double estimation = Estimate (parseTree, names, numJoin);
	
	index = 1;
	string firstRelName (names[0]);
	RelationInfo firstRel = relMap[firstRelName];
	RelationInfo temp;
	
	relMap.erase (firstRelName);
	firstRel.isJoint = true;
	forstRel.numTuples = estimation;
	
	while (index < numJoin) {
		
		firstRel.relJoint[names[index]] = names[index];
		temp = relMap[names[index]];
		relMap.erase (names[index]);
		
		firstRel.attrMap.insert (temp.relMap.begin (), temp.relMap.end ());
		index++;
		
	}
	
	relMap.insert (pair<string, RelationInfo> (firstRelName, firstRel));
	
}

double Statistics :: Estimate (struct AndList *parseTree, char **relNames, int numToJoin) {
	
	int index = 0;
	
	double factor = 1.0;
	double product = 1.0;
	
	while (index < numToJoin) {
		
		if (relMap.find (relNames[index]) != relMap.end ()) {
			
			RelationInfo rel = relMa[relNames[index]]
			
			product *= (double)rel.numTuples;
			
		} else {
			
			cout << relNames[index] << " Not Found!" << endl;
			
		}
		
		index++;
	
	}
	
	if (parseTree = NULL) {
		
		return product;
		
	}
	
	factor = AndOp (parseTree, relNames, numToJoin);
	
	return factor * product;
	
}
