#include "Catalog.h"
#include "QueryCompiler.h"
#include "QueryParser.h"
#include "BTreeIndex.h"
#include <iostream>
#include <string>
#include <sstream>

using namespace std;

void BuildIndexAndUpdateCatalog(Catalog& catalog, const CreateIndexInfo& info);

// Dummy definitions to satisfy linker
CreateIndexInfo createIndexInfo = {nullptr, nullptr, nullptr};
bool isCreateIndex = false;

// Add these externs at the top of main.cc
extern struct FuncOperator* finalFunction;
extern struct TableList* tables;
extern struct AndList* predicate;
extern struct NameList* groupingAtts;
extern struct NameList* attsToSelect;
extern int distinctAtts;

extern "C" int yyparse();
extern "C" int yylex_destroy();

void PrintHelp() {
    cout << "Available commands:\n"
         << "  SQL statements - Any valid SQL statement\n"
         << "  schema        - Print schema of all tables\n"
         << "  index         - Print all indexes\n"
         << "  help          - Show this help message\n"
         << "  exit/quit     - Exit the program\n";
}

void PrintSchema(Catalog& catalog) {
    cout << "\nDatabase Schema:\n";
    cout << "----------------\n";
    StringVector tables;
    catalog.GetTables(tables);
    for (int i = 0; i < tables.Length(); ++i) {
        SString& table = tables[i];
        Schema schema;
        if (catalog.GetSchema(table, schema)) {
            cout << "Table: " << table << "\n";
            cout << schema << "\n";
        }
    }
}

void PrintIndexes(Catalog& catalog) {
    cout << "\nDatabase Indexes:\n";
    cout << "----------------\n";
    StringVector tables;
    catalog.GetTables(tables);
    for (int i = 0; i < tables.Length(); ++i) {
        SString& table = tables[i];
        Schema schema;
        if (catalog.GetSchema(table, schema)) {
            AttributeVector& atts = schema.GetAtts();
            for (int j = 0; j < atts.Length(); ++j) {
                SString& attName = atts[j].name;
                string idxFile = catalog.GetIndexFile(string(table), string(attName));
                if (!idxFile.empty()) {
                    cout << "Table: " << table << ", Attribute: " << attName << ", IndexFile: " << idxFile << "\n";
                }
            }
        }
    }
}

void BuildIndexAndUpdateCatalog(Catalog& catalog, const CreateIndexInfo& info) {
    if (!info.indexName || !info.tableName || !info.attributeName) {
        std::cout << "Error: Invalid CREATE INDEX command." << std::endl;
        return;
    }
    std::string tableName(info.tableName);
    std::string attributeName(info.attributeName);
    std::string indexName(info.indexName);
    // Generate index file name (e.g., idx_<table>_<attribute>.idx)
    std::string indexFile = "idx_" + tableName + "_" + attributeName + ".idx";
    // Build the B+-tree index
    BTreeIndex index;
    if (!index.Build(catalog, tableName, attributeName, indexFile)) {
        std::cout << "Error: Failed to build index." << std::endl;
        return;
    }
    // Update the catalog with the new index
    catalog.RegisterIndex(tableName, attributeName, indexFile);
    catalog.Save();
    std::cout << "Index '" << indexName << "' created on table '" << tableName << "' (attribute '" << attributeName << ") and stored in '" << indexFile << "'." << std::endl;
}

int main() {
    SString catalogFile("catalog.sqlite");
    Catalog catalog(catalogFile);
    QueryCompiler compiler(catalog);
    
    cout << "Welcome to the Database System!\n";
    cout << "Type 'help' for available commands.\n\n";
    
    string input;
    while (true) {
        cout << "db> ";
        getline(cin, input);
        if (input.empty()) continue;
        
        if (input == "exit" || input == "quit") {
            break;
        } else if (input == "help") {
            PrintHelp();
        } else if (input == "schema") {
            PrintSchema(catalog);
        } else if (input == "index") {
            PrintIndexes(catalog);
        } else {
            // Parse the SQL query
            std::istringstream iss(input);
            std::string sql = input;
            // Set up parser input
            extern FILE* yyin;
            yyin = fmemopen((void*)sql.c_str(), sql.size(), "r");
            if (yyparse() == 0) {
                if (isCreateIndex) {
                    // Call function to build index and update catalog
                    BuildIndexAndUpdateCatalog(catalog, createIndexInfo);
                    isCreateIndex = false;
                } else {
                    QueryExecutionTree queryTree;
                    compiler.Compile(tables, attsToSelect, finalFunction, predicate, groupingAtts, distinctAtts, queryTree);
                    queryTree.ExecuteQuery();
                }
            } else {
                cout << "Error: Failed to parse query.\n";
            }
            yylex_destroy();
        }
    }
    return 0;
} 