#include <string>
#include <cstring>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
	fileName = f_path;
	if (f_type == Heap) {
		int ret = file.Open(0, f_path);
		if (ret == -1) {
			cerr << "Failed to create Heap file " << fileName << endl;
			return -1;
		}
		currentPagePos = 0;
		currentPage.EmptyItOut();
	}
	else {
		cerr << "Unrecognized filetype: " << f_type << endl;
		return -1;
	}
	return 0;
}

int DBFile::Open (char* f_path) {
	fileName = f_path;
	int ret = file.Open(1, f_path);
	if (ret == -1) {
		cerr << "Failed to open file: " << fileName << endl;
		return -1;
	}
	return 0;
}

void DBFile::Load (Schema& schema, char* textFile) {
	currentPagePos = 0;
	currentPage.EmptyItOut();

	FILE* f = fopen(textFile, "rt");
	Record rec;
	while (rec.ExtractNextRecord (schema, *f)) {
		// rec.print(cout, schema); cout << endl;
		AppendRecord(rec);
	}
	file.AddPage(currentPage, currentPagePos);
}

int DBFile::Close () {
	int numPages = file.Close();
	cout << "Closed file " << fileName << " with " << numPages << " pages" << endl;
	return numPages;
}

void DBFile::MoveFirst () {
	currentPagePos = 0;
	currentRecordOffset = 0;
	file.GetPage(currentPage, currentPagePos);
}

void DBFile::AppendRecord (Record& rec) {
	if (0 == currentPage.Append(rec)) {
		// save the current page to File
		file.AddPage(currentPage, currentPagePos);
		// increment currentPagepos
		currentPagePos++;
		// start a new page and add the record to this new page
		currentPage.EmptyItOut();
		if (0 == currentPage.Append(rec)) {
			cerr << "Error appending record to new page " << currentPagePos << endl;
		}
	}
}

int DBFile::GetNext (Record& rec) {
	if (0 == currentPage.GetFirst(rec)) {
		// move to the next page in File
		if (currentPagePos + 1 < file.GetLength()) {
			if (-1 == file.GetPage(currentPage, ++currentPagePos)) {
				cerr << "No next page (" << currentPagePos << ") to get from " << fileName << endl;
				return -1;
			}
			currentRecordOffset = 0;
			if (0 == currentPage.GetFirst(rec)) {
				cerr << "No record from " << fileName << " page " << currentPagePos << endl;
				return -1;
			}
		}
		else {
			return -1;
		}
		currentRecordOffset = 0;
	} else {
		currentRecordOffset++;
	}

	return 1;
}

int DBFile::GetPage(Page& p, int pageNum) {
	return file.GetPage(p, pageNum); // underlying File object
}
