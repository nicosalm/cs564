#include "heapfile.h"
#include "error.h"
#include <cstring> 

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // file doesn't exist. First create it and allocate
        // an empty header page and data page.
        status = db.createFile(fileName);
        if (status != OK) {
            return status;
        }

        // open newly created file
        status = db.openFile(fileName, file);
        if (status != OK) {
            return status;
        }

        // allocate newly created and opened file
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK) {
            return status;
        }

        // cast Page* to FileHdrPage*
        hdrPage = (FileHdrPage *)&newPage;

        // use hdrPage to initialize values in the header page
        // copy filename in because char[] is immutable, cast to char*
        strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE - 1);

        // assuming these should start at 0
        hdrPage->pageCnt = 0;	  // number of pages
        hdrPage->recCnt = 0;		// record count

        // second allocPage() call -> this will be first data page in file, this will go into the Page*
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) {
            return status;
        }

        // init page contents using Page*
        newPage->init(newPageNo);

        // store newPageNo in firstPage and lastPage of the FileHdrPage
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;

        // unpin both pages and mark them as dirty
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK) {
            return status;
        }

        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK) {
            return status;
        }

        status = db.closeFile(file);
        if (status != OK) {
            return status;
        }
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
    return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        
        // get page number by file->getFirstPage() (see I/O layer)
        status = filePtr->getFirstPage(headerPageNo);
        if (status != OK) {
            returnStatus = status;
            return;
        }

        // read and pin the header page for the file in bufPool
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if (status != OK) {
            returnStatus = status;
            return;
        }

        // init headerPage & hdrDirtyFlag
        headerPage = (FileHdrPage *)&pagePtr;
        hdrDirtyFlag = false;

        // read and pin first page of file into buf pool
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            returnStatus = status;
            return;
        }
        // init curDirtyFlag
        curDirtyFlag = false;
    
        // set curRec to NULLRID
        curRec = NULLRID;

        returnStatus = OK;
        return;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK) cerr << "error in unpin of date page\n";
    }
    
     // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
    
    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file. 
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;

    // !! HINT: retrieve arbitraray record given the RID of the record !!

    // curPage & curPageNo are used to keep track of curr data page pinned in buffer pool
    // if the current page is not null or isn't the requested one
    if (curPage == NULL || curPageNo != rid.pageNo) {
        if (curPage != NULL) {
            // unpin the currently pinned page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) {
                return status;
            }
        }
         // if yes, read the page (with the requested record on it) into the buffer
         // aka bookkeep: set the curPage, curPageNo, curDirtyFlad, & curRec properly


        // read page containing  desired record into buffer pool
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK) {
            return status;
        }

        // bookkeeping
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        curRec = NULLRID; // No specific record set yet
    }

    // get record from currently pinned page
    status = curPage->getRecord(rid, rec);
    if (status != OK) {
        return status;
    }

    // put the rid as curRec so we know which one we have
    curRec = rid;
    return OK;
}

HeapFileScan::HeapFileScan(const string & name,
               Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                     const int length_,
                     const Datatype type_, 
                     const char* filter_,
                     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID &outRid) {
  Status status;
  RID nextRid;
  Record rec;

  // loop until we find a matching record or reach end of file
  while (true) {
    // if no page is currently pinned, start with first page
    if (curPage == NULL) {
      curPageNo = headerPage->firstPage;

      if (curPageNo == -1) {
        return FILEEOF;
      }

      status = bufMgr->readPage(filePtr, curPageNo, curPage);
      if (status != OK) {
        return status;
      }

      curDirtyFlag = false;
      curRec = NULLRID;

      status = curPage->firstRecord(nextRid);
      if (status != OK && status != NORECORDS) {
        return status;
      }

      if (status == NORECORDS) {
        continue;
      }
    } else {
      // continue scan on current page
      status = curPage->nextRecord(curRec, nextRid);

      if (status == ENDOFPAGE) {
        // Unpin current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
          return status;
        }

        // Get next page number
        int nextPageNo;
        status = curPage->getNextPage(nextPageNo);
        if (status != OK) {
          return status;
        }

        if (nextPageNo == -1) {
          curPage = NULL;
          return FILEEOF;
        }

        // Read next page
        curPageNo = nextPageNo;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
          return status;
        }
        curDirtyFlag = false;

        status = curPage->firstRecord(nextRid);
        if (status != OK && status != NORECORDS) {
          return status;
        }

        if (status == NORECORDS) {
          continue;
        }
      } else if (status != OK) {
        return status;
      }
    }

    status = curPage->getRecord(nextRid, rec);
    if (status != OK) {
      return status;
    }

    if (matchRec(rec)) {
      outRid = nextRid;
      curRec = nextRid;
      return OK;
    }

    curRec = nextRid;
  }

  return FILEEOF;
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 
const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
    return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // check if curPage is NULL
    if (curPage == NULL) {
        // if yes, make last page now the cur page and read into the buffer
        curPageNo = headerPage->lastPage;
        curDirtyFlag = false;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            return status;
        }
    }

    // call curPage->insertRecord
    status = curPage->insertRecord(rec, outRid);
    if (status != OK) {
        // if fails

        // unpin cur page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
            return status;
        }
        
        // create new page
        curDirtyFlag = false;
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) {
            return status;
        }

        // init properly
        newPage->init(newPageNo);
        
        // modify header page
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;

        // link up new page
        status = curPage->setNextPage(newPageNo);

        // make cur page to be newly allocated page
        curPage = newPage;
        curPageNo = newPageNo;

        // try to insert record
        status = curPage->insertRecord(rec, rid);
        if (status != OK) {
            return status;
        }

        // bookkeeping!
        headerPage->recCnt++;
        curDirtyFlag = true;
        hdrDirtyFlag = true;
    }
    else {
        // if successful, bookkeep: update the recCnt, hdrDirtyFlag, curDirtyFlag, etc properly
        headerPage->recCnt++;
        curDirtyFlag = true;
        hdrDirtyFlag = true;
    }

    // return the RID of the inserted outRid record
    outRid = rid;
    return OK;
}


