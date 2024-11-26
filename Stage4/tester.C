#include "error.h"
#include "heapfile.h"
#include <iostream>

extern const Status createHeapFile(const string fileName);
extern const Status destroyHeapFile(const string fileName);

DB db;
BufMgr *bufMgr;

typedef struct {
  int id;
  float value;
  char str[64];
} TestRecord;

// helper function to create a test record
void createTestRecord(TestRecord &rec, int id, float value, const char *str) {
  rec.id = id;
  rec.value = value;
  strncpy(rec.str, str, sizeof(rec.str) - 1);
  rec.str[sizeof(rec.str) - 1] = '\0';
}

// test create and destroy heap file
Status testCreateDestroy() {
  cout << "\n=== Testing File Creation and Destruction ===" << endl;

  Status status;

  // cleanup existing test files
  destroyHeapFile("test1.dat");

  // test 1: Create new file
  cout << "Creating new heap file..." << endl;
  status = createHeapFile("test1.dat");
  if (status != OK) {
    cerr << "Failed to create heap file" << endl;
    return status;
  }

  // test 2: Try to create same file again (should fail)
  cout << "Attempting to create duplicate file..." << endl;
  status = createHeapFile("test1.dat");
  if (status != FILEEXISTS) {
    cerr << "Expected FILEEXISTS error for duplicate creation" << endl;
    return status;
  }

  // test 3: Destroy file
  cout << "Destroying heap file..." << endl;
  status = destroyHeapFile("test1.dat");
  if (status != OK) {
    cerr << "Failed to destroy heap file" << endl;
    return status;
  }

  // test 4: Try to destroy non-existent file
  cout << "Attempting to destroy non-existent file..." << endl;
  status = destroyHeapFile("nonexistent.dat");
  if (status == OK) {
    cerr << "Expected error for destroying non-existent file" << endl;
    return BADFILE;
  }

  cout << "Create/Destroy tests passed!" << endl;
  return OK;
}

Status testInsertAndRetrieve(int numRecords) {
  cout << "\n=== Testing Record Insertion and Retrieval ===" << endl;

  Status status;

  // cleanup any existing test files
  destroyHeapFile("test2.dat");

  // create test file
  status = createHeapFile("test2.dat");
  if (status != OK)
    return status;

  // open file for insertion
  InsertFileScan *iScan;
  iScan = new InsertFileScan("test2.dat", status);
  if (status != OK)
    return status;

  // insert records
  vector<RID> rids;
  TestRecord testRec;
  Record rec;

  cout << "Inserting " << numRecords << " records..." << endl;
  for (int i = 0; i < numRecords; i++) {
    createTestRecord(testRec, i, float(i) * 1.5,
                     ("Record-" + to_string(i)).c_str());
    rec.data = &testRec;
    rec.length = sizeof(TestRecord);

    RID rid;
    status = iScan->insertRecord(rec, rid);
    if (status != OK) {
      cerr << "Failed to insert record " << i << endl;
      return status;
    }
    rids.push_back(rid);
  }
  delete iScan;

  // verify record count
  HeapFile *file = new HeapFile("test2.dat", status);
  if (status != OK)
    return status;

  if (file->getRecCnt() != numRecords) {
    cerr << "Record count mismatch. Expected: " << numRecords
         << " Got: " << file->getRecCnt() << endl;
    return BADFILE;
  }

  // retrieve and verify records
  cout << "Verifying retrieved records..." << endl;
  for (int i = 0; i < numRecords; i++) {
    Record retrievedRec;
    status = file->getRecord(rids[i], retrievedRec);
    if (status != OK) {
      cerr << "Failed to retrieve record " << i << endl;
      return status;
    }

    TestRecord *retrieved = (TestRecord *)retrievedRec.data;
    if (retrieved->id != i || abs(retrieved->value - float(i) * 1.5) > 0.001 ||
        strcmp(retrieved->str, ("Record-" + to_string(i)).c_str()) != 0) {
      cerr << "Record " << i << " data mismatch" << endl;
      return BADRECPTR;
    }
  }

  delete file;
  destroyHeapFile("test2.dat");

  cout << "Insert/Retrieve tests passed!" << endl;
  return OK;
}

// test scanning functionality
Status testScanning(int numRecords) {
  cout << "\n=== Testing Scanning Functionality ===" << endl;

  Status status;
  Error error;

  // cleanup any existing test file first
  destroyHeapFile("test3.dat");

  // create and populate test file
  cout << "Creating test file..." << endl;
  status = createHeapFile("test3.dat");
  if (status != OK && status != FILEEXISTS) {
    cout << "Failed to create heap file with status: ";
    error.print(status);
    return status;
  }

  // insert records
  cout << "Opening file for insertion..." << endl;
  InsertFileScan *iScan = new InsertFileScan("test3.dat", status);
  if (status != OK) {
    cout << "Failed to create InsertFileScan with status: ";
    error.print(status);
    return status;
  }

  cout << "Inserting " << numRecords << " records for scan test..." << endl;
  TestRecord testRec;
  Record rec;
  int insertedCount = 0;
  for (int i = 0; i < numRecords; i++) {
    createTestRecord(testRec, i, float(i), ("Record-" + to_string(i)).c_str());
    rec.data = &testRec;
    rec.length = sizeof(TestRecord);
    RID rid;
    status = iScan->insertRecord(rec, rid);
    if (status != OK) {
      cout << "Failed to insert record " << i << " with status: ";
      error.print(status);
      return status;
    }
    insertedCount++;
    if (i % 10 == 0) {
      cout << "Inserted " << i << " records..." << endl;
    }
  }
  cout << "Successfully inserted " << insertedCount << " records" << endl;
  delete iScan;

  // verify record count after insertion
  cout << "Opening file to verify record count..." << endl;
  HeapFile *file = new HeapFile("test3.dat", status);
  if (status != OK) {
    cout << "Failed to open file for count check with status: ";
    error.print(status);
    return status;
  }
  int recCount = file->getRecCnt();
  cout << "File contains " << recCount << " records after insertion" << endl;
  if (recCount != numRecords) {
    cout << "Record count mismatch after insertion. Expected: " << numRecords
         << " Got: " << recCount << endl;
    delete file;
    return BADFILE;
  }
  delete file;

  // test 1: full scan
  cout << "\nStarting full scan test..." << endl;
  HeapFileScan *scan = new HeapFileScan("test3.dat", status);
  if (status != OK) {
    cout << "Failed to create HeapFileScan with status: ";
    error.print(status);
    return status;
  }

  cout << "Initializing scan..." << endl;
  status = scan->startScan(0, 0, STRING, NULL, EQ);
  if (status != OK) {
    cout << "Failed to start scan with status: ";
    error.print(status);
    return status;
  }

  int count = 0;
  RID rid;
  cout << "Starting to scan records..." << endl;
  while ((status = scan->scanNext(rid)) == OK) {
    count++;
    if (count % 10 == 0) {
      cout << "Scanned " << count << " records..." << endl;
    }
  }

  if (status != FILEEOF) {
    cout << "Scan ended with unexpected status: ";
    error.print(status);
    return status;
  }

  cout << "Full scan completed. Found " << count << " records" << endl;
  if (count != numRecords) {
    cout << "Full scan count mismatch. Expected: " << numRecords
         << " Got: " << count << endl;
    return BADFILE;
  }
  delete scan;

  // test 2: filtered scan (id > numRecords/2)
  cout << "Testing filtered scan..." << endl;
  scan = new HeapFileScan("test3.dat", status);
  if (status != OK)
    return status;

  int filterValue = numRecords / 2;
  // note: the offset should be 0 since id is the first field in TestRecord
  status = scan->startScan(0, sizeof(int), INTEGER, (char *)&filterValue, GT);
  if (status != OK)
    return status;

  count = 0;
  while ((status = scan->scanNext(rid)) == OK) {
    Record rec;
    status = scan->getRecord(rec);
    if (status != OK)
      return status;

    TestRecord *tr = (TestRecord *)rec.data;
    cout << "Found record with id: " << tr->id << endl;
    if (tr->id <= filterValue) {
      cerr << "Filter condition violated. Found record with id " << tr->id
           << " which is <= " << filterValue << endl;
      return BADFILE;
    }
    count++;
  }
  if (status != FILEEOF)
    return status;

  cout << "Filtered scan found " << count << " records" << endl;
  int expectedCount = (numRecords - filterValue - 1);
  if (count != expectedCount) {
    cerr << "Filtered scan count mismatch. Expected: " << expectedCount
         << " Got: " << count << endl;
    return BADFILE;
  }
  delete scan;

  // test 3: Mark/Reset scan
  cout << "Testing mark/reset functionality..." << endl;
  scan = new HeapFileScan("test3.dat", status);
  status = scan->startScan(0, 0, STRING, NULL, EQ);
  if (status != OK)
    return status;

  // scan to middle
  for (int i = 0; i < numRecords / 2; i++) {
    status = scan->scanNext(rid);
    if (status != OK)
      return status;
  }

  // mark position
  status = scan->markScan();
  if (status != OK)
    return status;

  // get current record
  Record markRec;
  status = scan->getRecord(markRec);
  if (status != OK)
    return status;
  TestRecord *markTr = (TestRecord *)markRec.data;
  int markId = markTr->id;
  cout << "Marked at record with id: " << markId << endl;

  // scan a few more
  for (int i = 0; i < 5; i++) {
    status = scan->scanNext(rid);
    if (status != OK)
      return status;
    Record tmpRec;
    status = scan->getRecord(tmpRec);
    if (status != OK)
      return status;
    TestRecord *tmpTr = (TestRecord *)tmpRec.data;
    cout << "Scanned past record with id: " << tmpTr->id << endl;
  }

  // reset to marked position
  cout << "Resetting to marked position..." << endl;
  status = scan->resetScan();
  if (status != OK)
    return status;

  status = scan->scanNext(rid);
  if (status != OK)
    return status;
  Record resetRec;
  status = scan->getRecord(resetRec);
  if (status != OK)
    return status;
  TestRecord *resetTr = (TestRecord *)resetRec.data;
  cout << "After reset, next record has id: " << resetTr->id << endl;
  if (resetTr->id != markId + 1) {
    cerr << "Mark/Reset position mismatch. Expected id: " << (markId + 1)
         << " Got: " << resetTr->id << endl;
    return BADFILE;
  }

  delete scan;
  destroyHeapFile("test3.dat");

  cout << "Scan tests passed!" << endl;
  return OK;
}

// test deletion functionality
Status testDeletion(int numRecords) {
  cout << "\n=== Testing Deletion Functionality ===" << endl;

  Status status;

  // cleanup any existing test files
  destroyHeapFile("test4.dat");

  // create and populate test file
  status = createHeapFile("test4.dat");
  if (status != OK)
    return status;

  InsertFileScan *iScan = new InsertFileScan("test4.dat", status);
  if (status != OK)
    return status;

  TestRecord testRec;
  Record rec;
  for (int i = 0; i < numRecords; i++) {
    createTestRecord(testRec, i, float(i), ("Record-" + to_string(i)).c_str());
    rec.data = &testRec;
    rec.length = sizeof(TestRecord);
    RID rid;
    status = iScan->insertRecord(rec, rid);
    if (status != OK)
      return status;
  }
  delete iScan;

  // delete every other record
  HeapFileScan *scan = new HeapFileScan("test4.dat", status);
  if (status != OK)
    return status;

  status = scan->startScan(0, 0, STRING, NULL, EQ);
  if (status != OK)
    return status;

  int deleteCount = 0;
  RID rid;
  bool deleteThis = true;
  while ((status = scan->scanNext(rid)) == OK) {
    if (deleteThis) {
      status = scan->deleteRecord();
      if (status != OK)
        return status;
      deleteCount++;
    }
    deleteThis = !deleteThis;
  }
  if (status != FILEEOF)
    return status;
  delete scan;

  // verify remaining records
  scan = new HeapFileScan("test4.dat", status);
  status = scan->startScan(0, 0, STRING, NULL, EQ);
  if (status != OK)
    return status;

  int remainingCount = 0;
  while ((status = scan->scanNext(rid)) == OK) {
    Record rec;
    status = scan->getRecord(rec);
    if (status != OK)
      return status;
    remainingCount++;
  }
  if (status != FILEEOF)
    return status;

  if (remainingCount != numRecords - deleteCount) {
    cerr << "Deletion count mismatch" << endl;
    return BADFILE;
  }

  delete scan;
  destroyHeapFile("test4.dat");

  cout << "Deletion tests passed!" << endl;
  return OK;
}

int main() {
  Status status;

  // initialize buffer manager
  bufMgr = new BufMgr(100);

  // run all tests!!!!!!!!!!!!!!
  status = testCreateDestroy();
  if (status != OK) {
    cerr << "Create/Destroy tests failed" << endl;
    delete bufMgr;
    return 1;
  }

  status = testInsertAndRetrieve(100);
  if (status != OK) {
    cerr << "Insert/Retrieve tests failed" << endl;
    delete bufMgr;
    return 1;
  }

  status = testScanning(100);
  if (status != OK) {
    cerr << "Scanning tests failed" << endl;
    delete bufMgr;
    return 1;
  }

  status = testDeletion(100);
  if (status != OK) {
    cerr << "Deletion tests failed" << endl;
    delete bufMgr;
    return 1;
  }

  // Cleanup, Cleanup -- Everybody, Everywhere (thank god we're done)
  delete bufMgr;

  destroyHeapFile("test1.dat");
  destroyHeapFile("test2.dat");
  destroyHeapFile("test3.dat");
  destroyHeapFile("test4.dat");

  cout << "\nAll tests completed successfully!" << endl;
  return 0;
}
