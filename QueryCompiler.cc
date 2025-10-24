#include <vector>
#include <unordered_map>
#include <bitset>
#include <tuple>
#include <cfloat>
#include <climits>
#include <algorithm>
#include <cstring>

#include "QueryCompiler.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

using namespace std;

/*
 * This JoinOrder class was mostly written by ChatGPT.
 *
 * Prompts:
 * 1. I'm implementing a join ordering algorithm, e.g., to optimize the join of 5 tables, in C++. Let's represent the possible joins using bitsets, such that the bitset of '11111' represents the overall join between all 5 tables. We'll use these bitsets as the keys to an unordered_map, which maps each join bitset to its estimated cardinality. This will require us to produce all combinations of bitsets having two to five 1's. We can omit the bitsets having only a single 1, since that doesn't represent a join.
 * 2. Let the cardinalities be given and used to populate [bitsetCardinalityMap]. Generate the optimal join order using the bitsetCardinalityMap.
 * 3. Encapsulate this into a new class, called JoinOrder, which has a constructor that takes a vector<int> cardinality of length up to 5, for each base relation. This is used to populate bitsetCardinalityMap, where the cardinality of a join is estimated as the product of cardinalities from the base relations. Add a callable method to the new class called, getJoin(int i), which returns the i-th join from the optimal join order.
 */

#ifndef MAX_DYNAMIC_JOINS
# define MAX_DYNAMIC_JOINS 10
#endif

bool QueryCompiler::isEqualityOnIndexedAttr(AndList* predicate, Schema& schema, string& outAttrName, int& outValue) {
	auto typeToString = [](int code) {
		switch (code) {
			case NAME: return "NAME";
			case INTEGER: return "INTEGER";
			case FLOAT: return "FLOAT";
			case STRING: return "STRING";
			default: return "UNKNOWN";
		}
	};

	for (AndList* andNode = predicate; andNode != nullptr; andNode = andNode->rightAnd) {
		ComparisonOp* comp = andNode->left;
		if (!comp) continue;

		cout << "Checking predicate: "
		     << (comp->left ? comp->left->value : "null")
		     << " " << comp->code << " "
		     << (comp->right ? comp->right->value : "null") << endl;

		cout << "LEFT code: " << comp->left->code << " [" << typeToString(comp->left->code) << "], "
		     << "RIGHT code: " << comp->right->code << " [" << typeToString(comp->right->code) << "]" << endl;

		if (comp->code == EQUALS) {
			// NAME = INTEGER
			if (comp->left->code == NAME && comp->right->code == INTEGER) {
				string fullName(comp->left->value);
				size_t dotPos = fullName.find('.');
				if (dotPos != string::npos) fullName = fullName.substr(dotPos + 1);
				SString attName(fullName.c_str());

				if (schema.Index(attName) >= 0) {
					outAttrName = fullName;
					outValue = atoi(comp->right->value);
					return true;
				}
			}
			// INTEGER = NAME
			if (comp->right->code == NAME && comp->left->code == INTEGER) {
				string fullName(comp->right->value);
				size_t dotPos = fullName.find('.');
				if (dotPos != string::npos) fullName = fullName.substr(dotPos + 1);
				SString attName(fullName.c_str());

				if (schema.Index(attName) >= 0) {
					outAttrName = fullName;
					outValue = atoi(comp->left->value);
					return true;
				}
			}
		}
	}
	return false;
}

class JoinOrder {
private:
    int numTables;  // Actual number of tables (dynamic, up to MAX_DYNAMIC_JOINS)
    unordered_map<bitset<MAX_DYNAMIC_JOINS>, float> bitsetCardinalityMap;  // Use float for cardinalities
    unordered_map<bitset<MAX_DYNAMIC_JOINS>, pair<float, pair<bitset<MAX_DYNAMIC_JOINS>, bitset<MAX_DYNAMIC_JOINS>>>> dp; // For DP
    vector<bitset<MAX_DYNAMIC_JOINS>> optimalOrder;  // To store optimal join sequence
    vector<vector<int>> adjacencyMatrix;  // Adjacency matrix for join constraints

    // Helper function to generate all subsets of a bitset
    vector<bitset<MAX_DYNAMIC_JOINS>> generateSubsets(const bitset<MAX_DYNAMIC_JOINS>& set) {
        vector<bitset<MAX_DYNAMIC_JOINS>> subsets;
        for (int i = 0; i < (1 << numTables); ++i) {
            bitset<MAX_DYNAMIC_JOINS> subset(i);
            if ((subset & set) == subset) {  // Ensure subset is valid
                subsets.push_back(subset);
            }
        }
        return subsets;
    }

    // Check if two subsets can join based on the adjacency matrix
    bool canJoin(const bitset<MAX_DYNAMIC_JOINS>& setA, const bitset<MAX_DYNAMIC_JOINS>& setB) {
        for (int i = 0; i < numTables; ++i) {
            if (setA[i]) {  // If table `i` is in setA
                for (int j = 0; j < numTables; ++j) {
                    if (setB[j] && adjacencyMatrix[i][j]) {
                        return true;  // Can join
                    }
                }
            }
        }
        return false;
    }

    // Recursive function to reconstruct the optimal join order
    void reconstructOrder(const bitset<MAX_DYNAMIC_JOINS>& join) {
        if (dp[join].second.first == join) {
            optimalOrder.push_back(join);
            return;
        }
        reconstructOrder(dp[join].second.first);
        reconstructOrder(dp[join].second.second);
        optimalOrder.push_back(join);
    }

public:
    // Constructor to populate bitsetCardinalityMap and adjacency matrix
    JoinOrder(const vector<float>& cardinality, const vector<vector<int>>& adjacency)
        : numTables(cardinality.size()), adjacencyMatrix(adjacency) {
        // Populate base relation cardinalities
        for (int i = 0; i < cardinality.size(); ++i) {
            bitset<MAX_DYNAMIC_JOINS> singleTable(1 << i);  // Single table bitset
            bitsetCardinalityMap[singleTable] = cardinality[i];
        }

        // Populate cardinalities for all joins
        for (int i = 1; i < (1 << cardinality.size()); ++i) {
            bitset<MAX_DYNAMIC_JOINS> joinBitset(i);
            float min_cardinality = FLT_MAX;
            int max_distincts = 1;
            int excluded_distincts = 1;
            for (int j = 0; j < cardinality.size(); ++j) {
                if (joinBitset[j]) {
                	int num_distincts = *max_element(adjacency[j].begin(), adjacency[j].end());
                    bitset<MAX_DYNAMIC_JOINS> singleTable(1 << j);
					float card = bitsetCardinalityMap[singleTable];
					if (card < min_cardinality) {
						min_cardinality = card;
						if (excluded_distincts > max_distincts) {
							max_distincts = excluded_distincts;
						}
						excluded_distincts = num_distincts;
					}
					else if (num_distincts > max_distincts){
						max_distincts = num_distincts;
					}
                }
            }
            bitsetCardinalityMap[joinBitset] = min_cardinality * max_distincts;
        }
    }

    // Method to calculate the optimal join order using DP
    void calculateOptimalOrder() {
        // Initialize DP for single relations
        for (unordered_map<bitset<MAX_DYNAMIC_JOINS>, float>::iterator it = bitsetCardinalityMap.begin();
             it != bitsetCardinalityMap.end(); ++it) {
            if (it->first.count() == 1) {
                dp[it->first] = make_pair(it->second, make_pair(it->first, it->first));
            }
        }

        // DP computation for larger joins
        for (int size = 2; size <= numTables; ++size) {
            for (unordered_map<bitset<MAX_DYNAMIC_JOINS>, float>::iterator it = bitsetCardinalityMap.begin();
                 it != bitsetCardinalityMap.end(); ++it) {
                if (it->first.count() == size) {
                    float minCost = FLT_MAX;
                    bitset<MAX_DYNAMIC_JOINS> bestA;
                    bitset<MAX_DYNAMIC_JOINS> bestB;
                    bool foundValidJoin = false;  // Track if a valid join exists

                    // Generate subsets of the current bitset
                    vector<bitset<MAX_DYNAMIC_JOINS>> subsets = generateSubsets(it->first);
                    for (vector<bitset<MAX_DYNAMIC_JOINS>>::iterator subsetIt = subsets.begin();
                         subsetIt != subsets.end(); ++subsetIt) {
                        bitset<MAX_DYNAMIC_JOINS> subsetA = *subsetIt;
                        bitset<MAX_DYNAMIC_JOINS> subsetB = it->first ^ subsetA;  // Complement subset
                        if (subsetA.none() || subsetB.none()) continue;

                        // Ensure subsets are valid in DP
                        if (dp.find(subsetA) == dp.end() || dp.find(subsetB) == dp.end()) continue;

                        // Check adjacency constraints
                        if (!canJoin(subsetA, subsetB)) continue;

                        // Get costs from dp table
                        float costA = dp[subsetA].first;
                        float costB = dp[subsetB].first;
                        float totalCost = costA + costB + it->second;

                        // Update minimum cost and best splits only if valid
                        if (totalCost < minCost) {
                            minCost = totalCost;
                            bestA = subsetA;
                            bestB = subsetB;
                            foundValidJoin = true;
                        }
                    }

                    // Ensure intermediate joins are in DP
                    if (foundValidJoin) {
                        dp[it->first] = make_pair(minCost, make_pair(bestA, bestB));
                    }
                }
            }
        }

        // Reconstruct the optimal order
        bitset<MAX_DYNAMIC_JOINS> fullJoin((1 << numTables) - 1);  // Full join
        if (dp.find(fullJoin) != dp.end()) {        // Ensure fullJoin exists in dp
            reconstructOrder(fullJoin);
        }
    }

    // Callable method to get the i-th join from the optimal order
    bool getJoin(int i, bitset<MAX_DYNAMIC_JOINS>& join, float& cardinality) {
        if (i < 0 || i >= optimalOrder.size()) {
            return false;
        }
        join = optimalOrder[i];
        cardinality = bitsetCardinalityMap[optimalOrder[i]];
        return true;
    }
};

void QueryCompiler::CreateGroupBy(Schema& saplingSchema, RelationalOp* &sapling, NameList* groupingAtts, FuncOperator* finalFunction) {
	// create GroupBy operators (always only a single aggregate)
	Schema schemaIn = saplingSchema;
	StringVector atts; SString attName("sum"); atts.Append(attName);
	StringVector attTypes; SString attType("FLOAT"); attTypes.Append(attType);
	IntVector distincts; SInt dist(1); distincts.Append(dist);
	Schema sumSchema(atts, attTypes, distincts);
	IntVector keepMe;
	int numAttsOutput = 0;
	for (NameList* att = groupingAtts; att != NULL; att = att->next){
		numAttsOutput += 1;
		SString attName(att->name);
		SInt idx = schemaIn.Index(attName);
		keepMe.Append(idx);
	}

	// SUM(att1), att2 -- will always do this one
	// att2, SUM(att1) -- will not handle this properly
	
	Schema schemaOut = schemaIn;
	schemaOut.Project(keepMe);
	sumSchema.Append(schemaOut);
	sumSchema.SetNoTuples(schemaOut.GetNoTuples());

	int noTuples = sumSchema.GetNoTuples();
	int ndv_prod = 1;
	for (int i=0; i<sumSchema.GetAtts().Length(); i++) {
		SString attName(sumSchema.GetAtts()[i].name);
		ndv_prod *= sumSchema.GetDistincts(attName);
	}
	SInt cardinality(min(noTuples/2, ndv_prod));
	sumSchema.SetNoTuples(cardinality);

	// Sum function
	Function compute;
	compute.GrowFromParseTree(finalFunction, schemaIn);

	// OrderMaker constructor takes int*, not IntVector
	int* keepMe_intv = new int[numAttsOutput];
	for (int i=0; i<numAttsOutput; i++) {
		keepMe_intv[i] = keepMe[i];
	}

	OrderMaker groupingAttsOrderMaker(schemaIn, keepMe_intv, numAttsOutput);

	delete [] keepMe_intv;

	RelationalOp* producer = sapling;
	sapling = new GroupBy(schemaIn, schemaOut, groupingAttsOrderMaker, compute, producer);

	saplingSchema.Swap(sumSchema);
}


void QueryCompiler::CreateFunction(Schema& saplingSchema, RelationalOp* &sapling, FuncOperator* finalFunction) {
	// create Sum operator
	Schema schemaIn = saplingSchema;
	
	StringVector atts; SString attName("sum"); atts.Append(attName);
	StringVector attTypes; SString attType("FLOAT"); attTypes.Append(attType);
	IntVector distincts; SInt dist(1); distincts.Append(dist);
	Schema schemaOut(atts, attTypes, distincts);
	SInt noTuples(1);
	schemaOut.SetNoTuples(noTuples);

	// Sum function
	Function compute;
	compute.GrowFromParseTree(finalFunction, schemaIn);

	RelationalOp* producer = sapling;
	sapling = new Sum(schemaIn, schemaOut, compute, producer);

	saplingSchema.Swap(schemaOut);
}

void QueryCompiler::CreateProject(Schema& saplingSchema, RelationalOp* &sapling, NameList* attsToSelect, int& distinctAtts) {
	// create Project operators
	Schema schemaIn = saplingSchema;
	int numAttsInput = schemaIn.GetNumAtts();
	// numAttsInput = 17;
	
	// schemaOut based on indexes of atts
	Schema schemaOut = schemaIn;
	IntVector keepMe;
	for (NameList* att = attsToSelect; att != NULL; att = att->next) {
		SString attName(att->name);
		SInt idx = schemaIn.Index(attName);
		keepMe.Append(idx);
	}
	schemaOut.Project(keepMe);
	int numAttsOutput = keepMe.Length();

	// Project constructor takes int*, not IntVector
	int* keepMe_intv = new int[numAttsOutput];
	for (int i=0; i<numAttsOutput; i++) {
		keepMe_intv[i] = keepMe[i];
	}

	RelationalOp* producer = sapling;
	sapling = new Project(schemaIn, schemaOut, numAttsInput, numAttsOutput, keepMe_intv, producer);

	if (distinctAtts != 0) {
		int noTuples = schemaOut.GetNoTuples();
		int ndv_prod = 1;
		for (int i=0; i<schemaOut.GetAtts().Length(); i++) {
			SString attName(schemaOut.GetAtts()[i].name);
			ndv_prod *= schemaOut.GetDistincts(attName);
		}
		SInt cardinality(min(noTuples/2, 1));
		schemaOut.SetNoTuples(cardinality);
		RelationalOp* producer = sapling;
		sapling = new DuplicateRemoval(schemaOut, producer);
	}

	saplingSchema.Swap(schemaOut);
}

QueryCompiler::QueryCompiler(Catalog& _catalog) : catalog(&_catalog) {
}

QueryCompiler::~QueryCompiler() {
}

// Schema& schema = current_schemas[idx]; // already fetched
void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {
	
		// save info about index in catalog
		// and also build the index???
		// write a function that builds the index -> where we put it? -> own class -> indexmanager.h
	/**************************************************
	 * create a SCAN operator for each table in the query
	 **************************************************/

	// 1a. map table names to a position
	// 1b. count the total number of tables
	int num_tables = 0;
	for (TableList* node=_tables; node != NULL; node=node->next) {
		// cout << node->tableName << endl;
		num_tables++;
	}

	// 2a. Initialize an array of Rel. Ops - one per table
	// 2b. Also track their schema
	RelationalOp** bottomup_tree = new RelationalOp*[num_tables];
	Schema* current_schemas = new Schema[num_tables];

	int idx = 0;
	for (TableList* node=_tables; node != NULL; node=node->next) {
		SString name(node->tableName);

		// 3a. Get original schema of table
		if (!catalog->GetSchema(name, current_schemas[idx])) {
			cerr << "Failed to retrieve schema of " << name << endl;
			exit(1);
		}

		// 3b. Get datafile of table
		SString filename;
		if (!catalog->GetDataFile(name, filename)) {
			cerr << "Failed to retrieve datafile of " << name << endl;
			exit(1);
		}

		// 3c. Open a handle to the datafile
		char* tmp = new char[string(filename).length() + 1];
		strcpy(tmp, string(filename).c_str());
		DBFile dbfile;
		if (dbfile.Open(tmp) < 0) {
			cerr << "Failed to open DBFile at " << tmp << endl;
		}



		// Check if the table has an index on the relevant attribute
		string attrName;
		int key;


		
		if (isEqualityOnIndexedAttr(_predicate, current_schemas[idx], attrName, key)) {
			if (catalog->HasIndex(name, attrName)) {
				cout << "Using IndexScan for " << name << "." << attrName << " = " << key << endl;
				// cout << "but not rly" << endl;
				// bottomup_tree[idx] = new Scan(current_schemas[idx], dbfile, name);

				string indexFile = catalog->GetIndexFile(name, attrName);
				BTreeIndex* index = new BTreeIndex(*catalog);
				index->Open(indexFile);		
				bottomup_tree[idx] = new IndexScan(key, *index, dbfile);
			} else {
				cout << "No index found for " << name << "." << attrName << endl;
				bottomup_tree[idx] = new Scan(current_schemas[idx], dbfile, name);
			}
		} else {
				cout << "Predicate did not match an indexable equality for table: " << name << endl;
				bottomup_tree[idx] = new Scan(current_schemas[idx], dbfile, name);
		}


		// 3d. Construct a Scan for table
		
		ComparisonOp* op_for_scan_index = nullptr;
		string identified_index_name_str = "";
	bool use_scan_index = false; // Flag to decide which operator to create
		if (op_for_scan_index = nullptr;!= nullptr && !identified_index_name_str.empty()) {
			SInt original_tuples = current_schemas[idx].GetNoTuples();
			Schema temp_schema_for_estimation = current_schemas[idx];
			SInt estimated_tuples_after_scan = EstimateCardinalityForOp(op_for_scan_index, temp_schema_for_estimation, original_tuples);

			if (op_for_scan_index->code == EQUALS) {
				use_scan_index = true;
				// cout << "LOG: Heuristic: Choosing ScanIndex for EQUALS predicate." << endl;
			} else if (op_for_scan_index->code == LESS_THAN || op_for_scan_index->code == GREATER_THAN) {
				if (original_tuples > 0 && (int)estimated_tuples_after_scan < (original_tuples / 10)) {
					use_scan_index = true;
					// cout << "LOG: Heuristic: Choosing ScanIndex for selective range predicate (est: "
					//      << (int)estimated_tuples_after_scan << " < " << (original_tuples / 10) << ")" << endl;
				} else {
					// cout << "LOG: Heuristic: Skipping ScanIndex for non-selective range predicate (est: "
					//      << estimated_tuples_after_scan << " >= " << (original_tuples / 10) << ")" << endl;
				}
			}
		}
		if (use_scan_index) {
			bottomup_tree[idx] = new ScanIndex(current_schemas[idx], *catalog, identified_index_name_str, name, op_for_scan_index);
			SInt original_tuples = current_schemas[idx].GetNoTuples(); // Get original again
			SInt estimated_tuples_after_scan = EstimateCardinalityForOp(op_for_scan_index, current_schemas[idx], original_tuples);
			current_schemas[idx].SetNoTuples(estimated_tuples_after_scan);
			// cout << "LOG: Created ScanIndex for table " << table_name_str << ". Estimated tuples: " << estimated_tuples_after_scan << endl;
		}
		
		//bool use_scan_index = false; // Flag to decide which operator to create
		//if (op_for_scan_index != nullptr && !identified_index_name_str.empty()) {
			
		//	SInt original_tuples = current_schemas[idx].GetNoTuples();
		//	Schema temp_schema_for_estimation = current_schemas[idx];
		//	SInt estimated_tuples_after_scan = EstimateCardinalityForOp(op_for_scan_index, temp_schema_for_estimation, original_tuples);

		//	if (op_for_scan_index->code == EQUALS) {
		//		use_scan_index = true;
				
		//	} else if (op_for_scan_index->code == LESS_THAN || op_for_scan_index->code == GREATER_THAN) {
		//		if (original_tuples > 0 && (int)estimated_tuples_after_scan < (original_tuples / 10)) {
					use_scan_index = true;
					// cout << "LOG: Heuristic: Choosing ScanIndex for selective range predicate (est: "
					//      << (int)estimated_tuples_after_scan << " < " << (original_tuples / 10) << ")" << endl;
		//		} else {
					// cout << "LOG: Heuristic: Skipping ScanIndex for non-selective range predicate (est: "
					//      << estimated_tuples_after_scan << " >= " << (original_tuples / 10) << ")" << endl;
		//		}
		//	}
		/}
		//cout << *bottomup_tree[idx] << endl;
		//idx++;
}

	/**************************************************
	 * push-down selections: 
	 * create a SELECT operator wherever necessary
	 **************************************************/

	// 4. Construct Select ops if tables have it
	for (int i = 0; i < num_tables; i++) {
		CNF pred_conjunction;
		Record pred_values;

		// 4a. Convert AndList of predicates into a conjunction
		// 		Extracts literal values of predicates into pred_values
		int num_preds = pred_conjunction.ExtractCNF(*_predicate,
														current_schemas[i], 
														pred_values);
		if (num_preds < 0) {
			cerr << "Failed to form conjunction of predicates" << endl;
			exit(1);
		}
		else if (num_preds > 0) { 
			// 4b. Estimate cardinality of selection
			SInt cardinality = current_schemas[i].GetNoTuples();
			for (int j = 0; j < num_preds; j++) {
				int att_idx = pred_conjunction.andList[j].whichAtt1; // assumes Left is a name
				int num_distinct = current_schemas[i].GetAtts()[att_idx].noDistinct;
				if (num_distinct < 1) {
					cerr << "Warning: 0 distincts for attribute " 
						<< att_idx << " of " << current_schemas[i] << endl;
						continue;
				}

				switch (pred_conjunction.andList[j].op) {
					case Equals:
						// 4b.i. Divide by number of distincts
						cardinality = cardinality / num_distinct;
						num_distinct = 1;
						break;
					case LessThan:
					case GreaterThan:
						// 4b.ii. Divide by a small integer
						cardinality = cardinality / min(num_distinct, 3);
						num_distinct = max(1, num_distinct / min(num_distinct, 3));
						break;
				}
				// 4b.iii. Update number of distincts
				current_schemas[i].GetAtts()[att_idx].noDistinct = num_distinct;
			}
			// 4b.iv. Update number of tuples
			current_schemas[i].SetNoTuples(cardinality);

			// 4c. Put Select on top of Scan
			RelationalOp* select = new Select(	current_schemas[i],
												pred_conjunction,
												pred_values,
												bottomup_tree[i]);
			bottomup_tree[i] = select;
			// cout << *bottomup_tree[i] << endl;
		}
	}

	/**************************************************
	 * compute the optimal join order
	 * create corresponding join operators
	 **************************************************/
	if (num_tables > 1 && num_tables <= 10) {

		vector<float> cardinality(num_tables, 0);
		for (int i = 0; i < num_tables; i++) {
			cardinality[i] = current_schemas[i].GetNoTuples();
		}

		// adjacency matrix doubles as number of distinct keys
		vector<vector<int>> adjacency_matrix(num_tables, vector<int>(num_tables, 0));
		for (int left = 0; left < num_tables; left++) {
			for (int right = left+1; right < num_tables; right++) {
				CNF join_preds;
				int num_preds = join_preds.ExtractCNF(*_predicate, current_schemas[left], current_schemas[right]);
				if (num_preds > 0) {
					int min_distincts = INT_MAX;
					for (int pred_idx = 0; pred_idx < num_preds; pred_idx++) {
						// get number of distinct from both join attributes
						// assumes left and right are attributes
						int left_att = join_preds.andList[pred_idx].whichAtt1;
						int left_distinct = current_schemas[left].GetAtts()[left_att].noDistinct;
						if (left_distinct < min_distincts) min_distincts = left_distinct;

						int right_att = join_preds.andList[pred_idx].whichAtt2;
						int right_distinct = current_schemas[right].GetAtts()[right_att].noDistinct;
						if (right_distinct < min_distincts) min_distincts = right_distinct;
					}
					adjacency_matrix[left][right] = max(1, min_distincts);
					adjacency_matrix[right][left] = max(1, min_distincts);
				} 
			}
		}

		JoinOrder join_order(cardinality, adjacency_matrix);
		join_order.calculateOptimalOrder();

		vector<bool> joined(num_tables, false);

		idx = 0;
	    bitset<MAX_DYNAMIC_JOINS> next_join;
	    float card = 0.0;
	    while (join_order.getJoin(idx++, next_join, card)) {
	        // cout << "Join " << idx << ": " << next_join << " (" << card << ")" << endl;
	        if (next_join.count() == 1) continue;

	        int left = 0;
	        int right = 0;

	        for (int i = 0; i < num_tables; i++) {
	        	if (next_join[i]) {
	        		left = i;
	        		break;
	        	}
	        }

	        for (int i = left + 1; i < num_tables; i++) {
	        	if (next_join[i]) {
	        		if (!joined[left] || !joined[i]) {
		        		right = i;
		        		break;
		        	}
	        	}
	        }

	        if (left == right) cerr << "Join optimization error" << endl;

	        // cout << "extracting join predicates " << left << ", " << right << endl;
	        CNF join_preds;
	        int num_preds = join_preds.ExtractCNF(*_predicate, current_schemas[left], current_schemas[right]);
	        // cout << current_schemas[left] << endl;
	        // cout << current_schemas[right] << endl;

	        if (num_preds > 0) {
		        // cout << "merging left and right schemas" << endl;
		        Schema schema_out;
		        schema_out.Append(current_schemas[left]);
		        schema_out.Append(current_schemas[right]);
		        SInt new_cardinality = card;
		        schema_out.SetNoTuples(new_cardinality);
		        // cout << schema_out << endl;

		        // TODO Phase-4: check whether join_preds are all equalities
		        //	if join_preds are equalities, use (symmetric) hash join
		        // 	otherwise, default to NestedLoopJoin

				bool all_equalities = true; // if we make this false, then we always create NLJ, which is safe
		        for (int hh=0; hh<join_preds.numAnds; hh++)
		        {
		        	if(join_preds.andList[hh].op != Equals)
		        		all_equalities = false;
		        }
		        // cout << "QueryCompiler all_equalities = " << all_equalities << endl;

				RelationalOp* join;
				

				// cout << "left schema: " << current_schemas[left].GetNoTuples() << endl;
				// cout << "right schema: " << current_schemas[right].GetNoTuples() << endl;

				// chicken bit
				bool flag_allow_symmetric = false;
				bool flag_allow_hash = false;
				
				if (!all_equalities || !flag_allow_hash)
				{
					join = new NestedLoopJoin(current_schemas[left],
						current_schemas[right],
						schema_out,
						join_preds,
						bottomup_tree[left],
						bottomup_tree[right]);
				}
				else{
					
					
					if(flag_allow_symmetric && 
						(current_schemas[left].GetNoTuples() > 1000) && 
						( current_schemas[right].GetNoTuples() > 1000))
					{
						join = new SymmetricHashJoin(current_schemas[left],
							current_schemas[right],
							schema_out,
							join_preds,
							bottomup_tree[left],
							bottomup_tree[right]);
					}
					else
					{
						join = new HashJoin(current_schemas[left],
							current_schemas[right],
							schema_out,
							join_preds,
							bottomup_tree[left],
							bottomup_tree[right]);
					}
										

				}
										

				


		        // cout << "updating relational operators" << endl;
		        for (int i = 0; i < num_tables; i++) {
		        	if (next_join[i]) {
		        		bottomup_tree[i] = join;
		        		current_schemas[i] = schema_out;
		        		joined[i] = true;
		        	}
		        }
		    }
		    else {
		    	cerr << "Missing join predicate" << endl;
		    	cerr << "adjacency " << adjacency_matrix[left][right] << endl;
		    }
	    }
		// Everything should now be joined (unless the query is missing a join)
	}

	// after joins, there is only one node in the forest
	RelationalOp* sapling = bottomup_tree[0];
	Schema saplingSchema = current_schemas[0];
	
	// create the remaining operators based on the query
	if (_groupingAtts != NULL) {
		CreateGroupBy(saplingSchema, sapling, _groupingAtts, _finalFunction);
	} else if (_finalFunction != NULL /* but _groupingAtts IS null */) {
		CreateFunction(saplingSchema, sapling, _finalFunction);
	} else if (_attsToSelect != NULL) {
		CreateProject(saplingSchema, sapling, _attsToSelect, _distinctAtts);
	}

	// connect everything in the query execution tree and return
	// The root will be a WriteOut operator
	// _outfile is path to text file where query results are printed
	string outfile = "output.txt";
	sapling = new WriteOut(saplingSchema, outfile, sapling);
	_queryTree.SetRoot(*sapling);

	// free the memory occupied by the parse tree since it is not necessary anymore
	delete [] bottomup_tree;
	delete [] current_schemas;
}

#ifdef MAX_DYNAMIC_JOINS
# undef MAX_DYNAMIC_JOINS
#endif
