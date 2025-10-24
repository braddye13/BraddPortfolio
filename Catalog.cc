#include <iostream>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;



static int tables_callback(	void* data, 		// user-argument to the callback
							int argc, 			// number of attributes in the tuple
							char** argv, 		// the attribute values of the tuple
							char** azColName) { // the attribute names of the tuple

	// Cast data into correct type
	unordered_map<string, Schema>* table_schema = static_cast<unordered_map<string, Schema>*>(data);
	
	// expected schema of tuple is:
	// 0 - table name (char*)
	// 1 - num_tuples (int)
	// 2 - filepath (char*)
	string table_name(argv[0]);
	SInt num_tuples(atoi(argv[1]));
	SString filepath(argv[2]);

	// Insert to map 
	// Assumes schema does not yet exist
	(*table_schema)[table_name] = Schema(num_tuples, filepath);

	return 0;
}

static int indexes_callback(void* data,
							int argc,
							char** argv,
							char** azColName) {

	// cast data back into correct type
	map<string, map<string, string>>* indexData = static_cast<map<string, map<string, string>>*>(data);

	// expected schema of tuple is:
	// 0 - table_name (char*)
	// 1 - attribute_name (char*)
	// 2 - index_file (char*)
	string table_name(argv[0]);
	string attribute_name(argv[1]);
	string index_file(argv[2]);

	// Add to the index data map
	(*indexData)[table_name][attribute_name] = index_file;

	return 0;
}

static int attributes_callback(	void* data,
								int argc,
								char** argv,
								char** azColName) {

	// cast data back into correct type
	unordered_map<string, Schema>* table_schema = static_cast<unordered_map<string, Schema>*>(data);

	// expected schema of tuple is:
	// 0 - attribute name (char*)
	// 1 - attribute position (int)
	// 2 - attribute type (char*)
	// 3 - num_distinct (int)
	// 4 - table name (char*)
	SString name(argv[0]);
	SInt position(atoi(argv[1]));
	SString type(argv[2]);
	SInt num_distinct(atoi(argv[3]));
	string table_name(argv[4]);

	// check that attribute position is correct
	// assumes position is 0-indexed
	if (position != (*table_schema)[table_name].GetNumAtts()) {
		return 1;
	}

	// construct schema containing new attribute
	StringVector att_names; att_names.Append(name);
	StringVector att_types; att_types.Append(type);
	IntVector distincts; distincts.Append(num_distinct);

	Schema schema(att_names, att_types, distincts);

	// add attribute to schema
	(*table_schema)[table_name].Append(schema);

	return 0;
}

Catalog::Catalog(SString& _fileName) {
	// Opens/creates database with the given filename
	filename = _fileName;
	int return_code = sqlite3_open(filename.c_str(),
									&(db_handle));
	if (return_code != SQLITE_OK) {
		cerr << "Error ("<< return_code << ") opening database " << filename << endl;
		exit(1);
	}
	/* Recommended schema of the database: 
	 *	Tables (id INT, name VARCHAR, num_tuples INT, path VARCHAR)
	 *	Attributes (name VARCHAR, position INT, type VARCHAR, num_distinct INT, table_id INT)
	 *	.
	 *	..
	 *	...
	 *	More can be added as necessary for later phases
	 *	For simplicity, I'll not actually use numeric join keys (id, table_id) 
	 */
	// Initialize error message buffer
	char* error_buffer;
	string sql = "CREATE TABLE IF NOT EXISTS Tables (name VARCHAR, num_tuples INT, file VARCHAR);";
	return_code = sqlite3_exec(db_handle, 		// database handle
								sql.c_str(), 	// sql statement buffer
								NULL, 			// callback function
								0,				// argument to callback
								&error_buffer); // buffer for error message
	if (return_code != SQLITE_OK) {
		cerr << "Error (code " << return_code << ") when creating Tables: "<< error_buffer << endl;

		// Free buffer to avoid memory leaks
		sqlite3_free(error_buffer);
	}
	sql = "CREATE TABLE IF NOT EXISTS Attributes (table_name VARCHAR, position INT, name VARCHAR, type VARCHAR, num_distinct INT);";
	return_code = sqlite3_exec(db_handle, 		// database handle
								sql.c_str(), 	// sql statement buffer
								NULL, 			// callback function
								0,				// argument to callback
								&error_buffer); // buffer for error message
	if (return_code != SQLITE_OK) {
		cerr << "Error (code " << return_code << ") when creating Attributes: "<< error_buffer << endl;

		// Free buffer to avoid memory leaks
		sqlite3_free(error_buffer);
	}

	// Create Indexes table if it doesn't exist
	sql = "CREATE TABLE IF NOT EXISTS Indexes (table_name VARCHAR, attribute_name VARCHAR, index_file VARCHAR);";
	return_code = sqlite3_exec(db_handle, 		// database handle
								sql.c_str(), 	// sql statement buffer
								NULL, 			// callback function
								0,				// argument to callback
								&error_buffer); // buffer for error message
	if (return_code != SQLITE_OK) {
		cerr << "Error (code " << return_code << ") when creating Indexes: "<< error_buffer << endl;

		// Free buffer to avoid memory leaks
		sqlite3_free(error_buffer);
	}

	// Database schema has been created
	// Next step is to read in the data into memory

	sql = "SELECT name, num_tuples, file FROM Tables;";
	return_code = sqlite3_exec(db_handle, 				// database handle
								sql.c_str(), 			// sql statement buffer
								tables_callback, 		// callback function
								(void*)&(table_schema),	// argument to callback
								&error_buffer); 		// buffer for error message
	if (return_code != SQLITE_OK) {
		cerr << "Error (code " << return_code << ") when reading Tables: "<< error_buffer << endl;

		// Free buffer to avoid memory leaks
		sqlite3_free(error_buffer);
	}

	sql = "SELECT name, position, type, num_distinct, table_name FROM Attributes ORDER BY position;";
	return_code = sqlite3_exec(db_handle, 				// database handle
								sql.c_str(), 			// sql statement buffer
								attributes_callback, 	// callback function
								(void*)&(table_schema),	// argument to callback
								&error_buffer); 		// buffer for error message
	if (return_code != SQLITE_OK) {
		cerr << "Error (" << return_code << ") running SELECT FROM Attributes: "<< error_buffer << endl;
		sqlite3_free(error_buffer);
	}

	// Load index data from the database
	sql = "SELECT table_name, attribute_name, index_file FROM Indexes;";
	return_code = sqlite3_exec(db_handle, 				// database handle
								sql.c_str(), 			// sql statement buffer
								indexes_callback, 		// callback function
								(void*)&(indexData),	// argument to callback
								&error_buffer); 		// buffer for error message
	if (return_code != SQLITE_OK) {
		cerr << "Error (code " << return_code << ") when reading Indexes: "<< error_buffer << endl;
		sqlite3_free(error_buffer);
	}
}

Catalog::~Catalog() {
	// Closes the sqlite database on deconstruction
	sqlite3_close(db_handle);
}

bool Catalog::Save() {
	// Saves data in Catalog back into database
	// optionally group all statements into a single transaction
	sqlite3_exec(db_handle, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);

	// 1. Clear existing Catalog data
	char* error_buffer;
	string sql = "DELETE FROM Tables;";
	int return_code = sqlite3_exec(db_handle, sql.c_str(), NULL, 0, &error_buffer);
	if (return_code != SQLITE_OK) {
		cerr << "Error (code " << return_code << ") when clearing Tables: "<< error_buffer << endl;
		sqlite3_free(error_buffer);
		return false;
	}

	sql = "DELETE FROM Attributes;";
	return_code = sqlite3_exec(db_handle, sql.c_str(), NULL, 0, &error_buffer);
	if (return_code != SQLITE_OK) {
		cerr << "Error (code " << return_code << ") when clearing Attributes: "<< error_buffer << endl;
		sqlite3_free(error_buffer);
		return false;
	}


	// 2. Prepare INSERT INTO Tables
	sql = "INSERT INTO Tables VALUES(?, ?, ?);";
	return_code = sqlite3_prepare_v2(db_handle,			// database handle 
										sql.c_str(),	// sql statement buffer
										-1,				// max number of bytes to read from buffer
										&stmt_handle,	// handle for prepared statement
										&stmt_leftover);	// pointer to end of current statement in buffer
	if (return_code != SQLITE_OK) {
		cerr << "Error ("<< return_code <<") compiling INSERT statement: " << sqlite3_errmsg(db_handle) << endl;
		exit(1);
		return false;
	}

	// 3. Insert schemas from Catalog into Tables
	for (unordered_map<string, Schema>::iterator it = table_schema.begin(); it != table_schema.end(); ++it) {
		// Get data to insert
		string name = it->first;
		Schema schema = it->second;
		int num_tuples = schema.GetNoTuples();
		string filepath = schema.GetDataFile();

		// Bind data to INSERT statement
		sqlite3_reset(stmt_handle);
		sqlite3_clear_bindings(stmt_handle);
		sqlite3_bind_text(stmt_handle, 1, name.c_str(), -1, 0);	
		sqlite3_bind_int(stmt_handle, 2, num_tuples);
		sqlite3_bind_text(stmt_handle, 3, filepath.c_str(), -1, 0);

		// Run INSERT
		return_code = sqlite3_step(stmt_handle);
		if (return_code != SQLITE_DONE) {
			cerr	<< "Error ("<< return_code <<") running INSERT INTO Tables VALUES (" 
					<< name << ", " 
					<< num_tuples << ", " 
					<< filepath << "): " 
					<< sqlite3_errmsg(db_handle) << endl;
			return false;
		}
	}
	sqlite3_finalize(stmt_handle);

	// 5. Prepare INSERT INTO Attributes
	sql = "INSERT INTO Attributes VALUES(?, ?, ?, ?, ?);";
	return_code = sqlite3_prepare_v2(db_handle,			// database handle 
										sql.c_str(),	// sql statement buffer
										-1,				// max number of bytes to read from buffer
										&stmt_handle,	// handle for prepared statement
										&stmt_leftover);	// pointer to end of current statement in buffer
	if (return_code != SQLITE_OK) {
		cerr << "Error ("<< return_code <<") compiling INSERT statement: " << sqlite3_errmsg(db_handle) << endl;
		return false;
	}

	// 6. Insert schemas from Catalog into Attributes
	for (unordered_map<string, Schema>::iterator it = table_schema.begin(); it != table_schema.end(); ++it) {
		// Get data to insert
		string table_name = it->first;
		Schema schema = it->second;
		AttributeVector& attributes = schema.GetAtts();

		// Iterate through attributes
		for (int pos = 0; pos < schema.GetNumAtts(); pos++) {
			string name = attributes[pos].name;
			int num_distinct = attributes[pos].noDistinct;

			string type;
			switch (attributes[pos].type) {
				case Integer:
					type = "INTEGER";
					break;
				case Float:
					type = "FLOAT";
					break;
				case String:
					type = "STRING";
					break;
				default:
					cerr << "Invalid type (" << type << ") for attribute " << name << endl;
					exit(1);
			}

			// Bind data to INSERT statement
			sqlite3_reset(stmt_handle);
			sqlite3_clear_bindings(stmt_handle);
			sqlite3_bind_text(stmt_handle, 1, table_name.c_str(), -1, 0);
			sqlite3_bind_int(stmt_handle, 2, pos);
			sqlite3_bind_text(stmt_handle, 3, name.c_str(), -1, 0);
			sqlite3_bind_text(stmt_handle, 4, type.c_str(), -1, 0);
			sqlite3_bind_int(stmt_handle, 5, num_distinct);

			// Run INSERT
			return_code = sqlite3_step(stmt_handle);
			if (return_code != SQLITE_DONE) {
				cerr	<< "Error (" << return_code << ") running INSERT INTO Attributes VALUES (" 
						<< name << ", " 
						<< pos << ", "
						<< type << ", "
						<< num_distinct << ", "
						<< table_name << "): " 
						<< sqlite3_errmsg(db_handle) << endl;
				return false;
			}
		}
	}
	sqlite3_finalize(stmt_handle);

	// 7. Delete existing index data
	sql = "DELETE FROM Indexes;";
	return_code = sqlite3_exec(db_handle, sql.c_str(), NULL, 0, &error_buffer);
	if (return_code != SQLITE_OK) {
		cerr << "Error (code " << return_code << ") when clearing Indexes: "<< error_buffer << endl;
		sqlite3_free(error_buffer);
		return false;
	}

	// 8. Prepare INSERT INTO Indexes
	sql = "INSERT INTO Indexes VALUES(?, ?, ?);";
	return_code = sqlite3_prepare_v2(db_handle,			// database handle 
									sql.c_str(),	// sql statement buffer
									-1,				// max number of bytes to read from buffer
									&stmt_handle,	// handle for prepared statement
									&stmt_leftover);	// pointer to end of current statement in buffer
	if (return_code != SQLITE_OK) {
		cerr << "Error ("<< return_code << ") compiling INSERT statement: " << sqlite3_errmsg(db_handle) << endl;
		return false;
	}

	// 9. Insert index data from Catalog into Indexes
	for (auto tableIt = indexData.begin(); tableIt != indexData.end(); ++tableIt) {
		string tableName = tableIt->first;
		for (auto attrIt = tableIt->second.begin(); attrIt != tableIt->second.end(); ++attrIt) {
			string attributeName = attrIt->first;
			string indexFile = attrIt->second;

			// Bind data to INSERT statement
			sqlite3_reset(stmt_handle);
			sqlite3_clear_bindings(stmt_handle);
			sqlite3_bind_text(stmt_handle, 1, tableName.c_str(), -1, 0);
			sqlite3_bind_text(stmt_handle, 2, attributeName.c_str(), -1, 0);
			sqlite3_bind_text(stmt_handle, 3, indexFile.c_str(), -1, 0);

			// Run INSERT
			return_code = sqlite3_step(stmt_handle);
			if (return_code != SQLITE_DONE) {
				cerr << "Error (" << return_code << ") running INSERT INTO Indexes VALUES (" 
					<< tableName << ", " 
					<< attributeName << ", "
					<< indexFile << "): " 
					<< sqlite3_errmsg(db_handle) << endl;
				return false;
			}
		}
	}
	sqlite3_finalize(stmt_handle);

	// commit transaction
	sqlite3_exec(db_handle, "COMMIT;", nullptr, nullptr, nullptr);
	return true;
}

bool Catalog::GetNoTuples(SString& _table, SInt& _noTuples) {
	if (table_schema.count(_table) == 0) {return false;}

	_noTuples = table_schema[_table].GetNoTuples();
	return true;
}

void Catalog::SetNoTuples(SString& _table, SInt& _noTuples) {
	table_schema[_table].SetNoTuples(_noTuples);
}

bool Catalog::GetDataFile(SString& _table, SString& _path) {
	if (table_schema.count(_table) == 0) {return false;}

	_path = table_schema[_table].GetDataFile();
	return true;
}

void Catalog::SetDataFile(SString& _table, SString& _path) {
	table_schema[_table].SetDataFile(_path);
}

bool Catalog::GetNoDistinct(SString& _table, SString& _attribute, SInt& _noDistinct) {
	if (table_schema.count(_table) == 0) {return false;}
	SInt d = table_schema[_table].GetDistincts(_attribute);
	_noDistinct = table_schema[_table].GetDistincts(_attribute);
	return true;
}

void Catalog::SetNoDistinct(SString& _table, SString& _attribute, SInt& _noDistinct) {
	table_schema[_table].SetDistincts(_attribute, _noDistinct);
}

void Catalog::GetTables(StringVector& _tables) {
	for (unordered_map<string, Schema>::iterator it = table_schema.begin(); it != table_schema.end(); ++it) {
		SString name(it->first);
		_tables.Append(name);
	}
}

bool Catalog::GetAttributes(SString& _table, StringVector& _attributes) {
	if (table_schema.count(_table) == 0) {return false;}

	unordered_map<string, Schema>::iterator it = table_schema.find(_table);
	if (it == table_schema.end()) {return false;}

	for (int i = 0; i < it->second.GetAtts().Length(); i++) {
		SString att(it->second.GetAtts()[i].name);
		_attributes.Append(att);
	}

	return true;
}

bool Catalog::GetSchema(SString& _table, Schema& _schema) {
	if (table_schema.count(_table) == 0) {return false;}

	_schema = table_schema[_table];
	return true;
}

bool Catalog::CreateTable(SString& _table, StringVector& _attributes,
	StringVector& _attributeTypes) {
	if (table_schema.count(_table) != 0) {return false;}

	// Initialize the number of distinct values to 0
	IntVector distincts;
	SInt t(0);
	for (int i = 0; i < _attributes.Length(); i++) {
		distincts.Append(t);

		// Validate attribute type
		if (!((string)_attributeTypes[i] == "INTEGER" ||
						(string)_attributeTypes[i] == "FLOAT" || 
						(string)_attributeTypes[i] == "STRING")) {
			return false;
		}
	}

	table_schema[_table] = Schema(_attributes, _attributeTypes, distincts);
	return true;
}

bool Catalog::DropTable(SString& _table) {
	if (table_schema.erase(_table) == 0) {return false;} 
	return true;
}

bool Catalog::HasIndex(const string& table, const string& attribute) {
	return indexData.count(table) && indexData[table].count(attribute);
}

string Catalog::GetIndexFile(const string& table, const string& attribute) {
	return indexData[table][attribute];
}

void Catalog::RegisterIndex(const string& table, const string& attribute, const string& indexFile) {
	indexData[table][attribute] = indexFile;
	Save();
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	for (unordered_map<string, Schema>::iterator it = _c.table_schema.begin(); it != _c.table_schema.end(); ++it) {
		_os << it->first << " " << it->second << endl;
	}
	return _os;
}