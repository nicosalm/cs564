#include "heapfile.h"
#include "error.h"

// creates a new heap file on disk
const Status createHeapFile(const string fileName)
{
    File *filePtr;
    Status status;
    // first check if file exists
    status = db.openFile(fileName, filePtr);

    if (status == OK)
    {
        return FILEEXISTS;
    }

    // if we get here, file doesn't exist so create it
    status = db.createFile(fileName);
    if (status != OK)
        return status;

    // open the newly created file
    status = db.openFile(fileName, filePtr);
    if (status != OK)
        return status;

    // allocate and setup header page
    int headerPgNum;
    Page *headerPg;
    status = bufMgr->allocPage(filePtr, headerPgNum, headerPg);
    if (status != OK)
        return status;

    // initialize header page contents
    FileHdrPage *hdrPg = (FileHdrPage *)headerPg;
    strncpy(hdrPg->fileName, fileName.c_str(), MAXNAMESIZE - 1);
    hdrPg->fileName[MAXNAMESIZE - 1] = '\0';

    // create initial data page
    int dataPgNum;
    Page *dataPg;
    status = bufMgr->allocPage(filePtr, dataPgNum, dataPg);
    if (status != OK)
        return status;

    // initialize data page
    dataPg->init(dataPgNum);

    // update header page pointers
    hdrPg->firstPage = dataPgNum;
    hdrPg->lastPage = dataPgNum;

    // unpin pages and mark -> dirty
    status = bufMgr->unPinPage(filePtr, headerPgNum, true);
    if (status != OK)
        return status;

    status = bufMgr->unPinPage(filePtr, dataPgNum, true);
    if (status != OK)
        return status;

    // cleanup and close file
    return db.closeFile(filePtr);
}

// removes heap file from disk
const Status destroyHeapFile(const string fileName)
{
    return db.destroyFile(fileName);
}

// constructor for heap file object
HeapFile::HeapFile(const string &fileName, Status &status)
{
    // cout << "attempting to open heap file: " << fileName << endl;
    status = db.openFile(fileName, filePtr);
    if (status != OK)
    {
        cout << "failed to open heap file" << endl;
        return;
    }

    // load header page
    status = filePtr->getFirstPage(headerPageNo);
    if (status != OK)
        return;

    Page *page;
    status = bufMgr->readPage(filePtr, headerPageNo, page);
    if (status != OK)
        return;

    headerPage = (FileHdrPage *)page;
    hdrDirtyFlag = false;

    // load first data page
    curPageNo = headerPage->firstPage;
    status = bufMgr->readPage(filePtr, curPageNo, curPage);
    if (status != OK)
        return;

    // initialize tracking variables
    curDirtyFlag = false;
    curRec = NULLRID;
}

// heap file destructor implementation
HeapFile::~HeapFile()
{
    // clean up any pinned pages
    if (curPage)
    {
        // cout << "unpinning current page in destructor" << endl;
        Status s = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (s != OK)
        {
            cout << "error unpinning data page" << endl;
        }
        curPage = nullptr;
        curPageNo = 0;
        curDirtyFlag = false;
    }

    // unpin header page
    Status s = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (s != OK)
    {
        cout << "error unpinning header page" << endl;
    }

    // close the file
    s = db.closeFile(filePtr);
    if (s != OK)
    {
        Error e;
        e.print(s);
    }
}

// returns total number of records
const int HeapFile::getRecCnt() const
{
    return headerPage->recCnt;
}

// fetches a specific record by rid
const Status HeapFile::getRecord(const RID &rid, Record &rec)
{
    // check if we need to load a different page
    if (curPage == nullptr || rid.pageNo != curPageNo)
    {
        // unpin current if it exists
        if (curPage)
        {
            Status s = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (s != OK)
                return s;
        }

        // load requested page
        curPageNo = rid.pageNo;
        Status s = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (s != OK)
            return s;
        curDirtyFlag = false;
    }

    // fetch the record
    Status s = curPage->getRecord(rid, rec);
    if (s == OK)
    {
        curRec = rid;
    }
    return s;
}

// scan implementation
HeapFileScan::HeapFileScan(const string &name, Status &status)
    : HeapFile(name, status)
{
    filter = nullptr;
}

const Status HeapFileScan::startScan(const int offset_,
                                     const int length_,
                                     const Datatype type_,
                                     const char *filter_,
                                     const Operator op_)
{
    // handle case with no filter
    if (!filter_)
    {
        filter = nullptr;
        return OK;
    }

    // ye olde conditional block -- validates scan parameters
    if (offset_ < 0 || length_ < 1)
        return BADSCANPARM;
    if (type_ != STRING && type_ != INTEGER && type_ != FLOAT)
        return BADSCANPARM;
    if ((type_ == INTEGER && length_ != sizeof(int)) ||
        (type_ == FLOAT && length_ != sizeof(float)))
        return BADSCANPARM;
    if (op_ != LT && op_ != LTE && op_ != EQ &&
        op_ != GTE && op_ != GT && op_ != NE)
        return BADSCANPARM;

    // store scan parameters
    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

// ends the current scan
const Status HeapFileScan::endScan()
{
    if (!curPage)
        return OK;

    Status s = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag); // unpin current page (last page)
    curPage = nullptr;
    curPageNo = 0;
    curDirtyFlag = false;
    return s;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

// saves current scan position for later retrieval
const Status HeapFileScan::markScan()
{
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

// restores saved scan position
const Status HeapFileScan::resetScan()
{
    if (markedPageNo == curPageNo)
    {
        curRec = markedRec; // because we are on the same page
        return OK;
    }

    // need to load different page for reset
    if (curPage)
    {
        // unpin current page
        Status s = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (s != OK)
            return s;
    }

    curPageNo = markedPageNo;
    Status s = bufMgr->readPage(filePtr, curPageNo, curPage);
    if (s != OK)
        return s;

    curRec = markedRec; // reset to last marked record
    curDirtyFlag = false;
    return OK;
}

// advances to next matching record
const Status HeapFileScan::scanNext(RID &outRid)
{
    // cout << "scanning for next matching record" << endl;
    Status status;
    RID nextRid;

    while (true)
    {
        // handle case with no current page
        if (!curPage)
        {
            status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
            if (status != OK)
                return status;
            curPageNo = headerPage->firstPage;
            curDirtyFlag = false;

            status = curPage->firstRecord(nextRid);
        }
        else
        {
            status = curPage->nextRecord(curRec, nextRid);
        }

        // handle end of page
        if (status == ENDOFPAGE)
        {
            int nextPg;
            status = curPage->getNextPage(nextPg);

            // check for end of file condition
            if (status != OK || nextPg == -1)
                return FILEEOF;

            // move to next page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;

            curPageNo = nextPg;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK)
                return status;

            curDirtyFlag = false;
            status = curPage->firstRecord(nextRid);
            if (status != OK)
                continue;
        }

        // check if record matches criteria
        curRec = nextRid;
        Record rec; // SO --> we need to get the record to check if it matches the criteria
        status = getRecord(rec);
        if (status != OK)
            return status;

        if (matchRec(rec)) // if it matches, return the rid
        {
            outRid = curRec;
            return OK;
        }
    }
}

// retrieves current record
const Status HeapFileScan::getRecord(Record &rec)
{
    return curPage->getRecord(curRec, rec);
}

// removes current record
const Status HeapFileScan::deleteRecord()
{
    Status s = curPage->deleteRecord(curRec);
    if (s == OK)
    {
        headerPage->recCnt--;
        curDirtyFlag = true;
        hdrDirtyFlag = true;
    }
    return s;
}

// marks current page as modified
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

// checks if record matches scan criteria
const bool HeapFileScan::matchRec(const Record &rec) const
{
    if (!filter)
        return true;
    if (offset + length - 1 >= rec.length)
        return false;

    float diffr = 0;

    switch (type)
    {
    case INTEGER:
    {
        int recordVal, filterVal;
        memcpy(&recordVal, (char *)rec.data + offset, length);
        memcpy(&filterVal, filter, length);
        diffr = recordVal - filterVal;
        break;
    }
    case FLOAT:
    {
        float recordVal, filterVal;
        memcpy(&recordVal, (char *)rec.data + offset, length);
        memcpy(&filterVal, filter, length);
        diffr = recordVal - filterVal;
        break;
    }
    case STRING:
        diffr = strncmp((char *)rec.data + offset, filter, length);
        break;
    }

    switch (op)
    {
    case LT:
        return diffr < 0;
    case LTE:
        return diffr <= 0;
    case EQ:
        return diffr == 0;
    case GTE:
        return diffr >= 0;
    case GT:
        return diffr > 0;
    case NE:
        return diffr != 0;
    default:
        return false;
    }
}

InsertFileScan::InsertFileScan(const string &name, Status &status)
    : HeapFile(name, status)
{
}

InsertFileScan::~InsertFileScan()
{
    if (curPage)
    {
        // cout << "cleaning up insert scan" << endl;
        Status s = bufMgr->unPinPage(filePtr, curPageNo, true);
        if (s != OK)
        {
            cout << "error unpinning page during cleanup" << endl;
        }
        curPage = nullptr;
        curPageNo = 0;
    }
}

// adds new record to file
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid)
{
    // verify record size
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED)
    {
        return INVALIDRECLEN;
    }

    Status status;

    // load last page if no current page
    if (!curPage)
    {
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false;
    }

    // attempt insertion (may fail if page is full)
    status = curPage->insertRecord(rec, outRid);

    // SO --> handle full page case; we need to create a new page
    if (status == NOSPACE)
    {
        // unpin current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
            return status;

        // create new page
        Page *newPg;
        int newPgNum;
        status = bufMgr->allocPage(filePtr, newPgNum, newPg);
        if (status != OK)
            return status;

        // initialize new page
        newPg->init(newPgNum);

        // link pages
        status = curPage->setNextPage(newPgNum);
        if (status != OK)
            return status;

        // update header info
        headerPage->lastPage = newPgNum;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        // update current page
        curPage = newPg;
        curPageNo = newPgNum;
        curDirtyFlag = false;

        // retry insertion now that we have a new page
        status = curPage->insertRecord(rec, outRid);
        if (status != OK)
            return status;
    }

    // update counts and flags
    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curDirtyFlag = true;

    return OK;
}