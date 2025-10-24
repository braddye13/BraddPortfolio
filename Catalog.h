#ifndef _CATALOG_H
#define _CATALOG_H

#include <iostream>
#include <cstdio>
#include <unordered_map>
#include <map>
#include "sqlite3.h"

#include "Schema.h"

using namespace std;

class Catalog {
private:
	map<string, map<string, string>> indexData; // tableName → attributeName → indexFile
protected:
	/* Data structures to keep catalog data in memory.
	 * A series of data structures you may find useful are included.
	 * Efficient data structures are recommended.
	 * Avoid linear traversals when possible.
	 */
	string filename;
	sqlite3* db_handle;

	// useful buffers
	sqlite3_stmt* stmt_handle = 0;
	const char* stmt_leftover = 0;

	// map table to its schema 
	unordered_map<string, Schema> table_schema;

public:
	/* Catalog constructor.
	 * Initialize the catalog with the persistent data stored in _fileName.
	 * _fileName is a SQLite database containing data on tables and their attributes.
	 * _fileName is queried through SQL.
	 * Populate in-memory data structures with data from the SQLite database.
	 * All the functions work with the in-memory data structures.
	 */
	Catalog(SString& _fileName);

	/* Catalog destructor.
	 * Store all the catalog data in the SQLite database.
	 */
	virtual ~Catalog();

	/* Save the content of the in-memory catalog to the database.
	 * Return true on success, false otherwise.
	 */
	bool Save();

	/* Get/Set the number of tuples in _table.
	 * Get returns true if _table exists, false otherwise.
	 */
	bool GetNoTuples(SString& _table, SInt& _noTuples);
	void SetNoTuples(SString& _table, SInt& _noTuples);

	/* Get/Set the location of the physical file containing the data.
	 * Get returns true if _table exists, false otherwise.
	 */
	bool GetDataFile(SString& _table, SString& _path);
	void SetDataFile(SString& _table, SString& _path);

	/* Get/Set the number of distinct elements in _attribute of _table.
	 * Get returns true if _table exists, false otherwise.
	 */
	bool GetNoDistinct(SString& _table, SString& _attribute, SInt& _noDistinct);
	void SetNoDistinct(SString& _table, SString& _attribute, SInt& _noDistinct);

	/* Return the tables from the catalog.
	 */
	void GetTables(StringVector& _tables);

	/* Return the attributes of _table in _attributes.
	 * Return true if _table exists, false otherwise.
	 */
	bool GetAttributes(SString& _table, StringVector& _attributes);

	/* Return the schema of _table in _schema.
	 * Return true if _table exists, false otherwise.
	 */
	bool GetSchema(SString& _table, Schema& _schema);

	/* Add a new table to the catalog with the corresponding attributes and types.
	 * The only possible types for an attribute are: INTEGER, FLOAT, and STRING.
	 * Return true if operation successful, false otherwise.
	 * There can be a single table with a given name in the catalog.
	 * There can be a single attribute with a given name in a table.
	 */
	bool CreateTable(SString& _table, StringVector& _attributes,
		StringVector& _attributeTypes);

	/* Delete table from the catalog.
	 * Return true if operation successful, i.e., _table exists, false otherwise.
	 */
	bool DropTable(SString& _table);


	bool HasIndex(const string& table, const string& attribute);
	string GetIndexFile(const string& table, const string& attribute);
	void RegisterIndex(const string& table, const string& attribute, const string& indexFile);
	/* Overload printing operator for Catalog.
	 * Print the content of the catalog in a friendly format:
	 * table_1 \tab noTuples \tab pathToFile
	 * \tab attribute_1 \tab type \tab noDistinct
	 * \tab attribute_2 \tab type \tab noDistinct
	 * ...
	 * table_2 \tab noTuples \tab pathToFile
	 * \tab attribute_1 \tab type \tab noDistinct
	 * \tab attribute_2 \tab type \tab noDistinct
	 * ...
	 * Tables/attributes are sorted in ascending alphabetical order.
	 */
	friend ostream& operator<<(ostream& _os, Catalog& _c);
};

#endif //_CATALOG_H
