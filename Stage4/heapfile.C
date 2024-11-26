#include "error.h"
#include "heapfile.h"
#include <cstring>

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File *file;
    Status status;
    FileHdrPage *hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page *newPage;

    // try to open the file; this should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // file doesn't exist. First create it and allocate
        // an empty header page and data page.
        status = db.createFile(fileName);
        if (status != OK)
        {
            return status;
        }

        status = db.openFile(fileName, file);
        if (status != OK)
        {
            return status;
        }

        // allocate header page
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK)
        {
            return status;
        }

        // cast Page* to FileHdrPage*
        hdrPage = (FileHdrPage *)newPage;

        // initialize header page fields
        strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE - 1);
        hdrPage->pageCnt = 0;
        hdrPage->recCnt = 0;

        // allocate first data page
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK)
        {
            return status;
        }

        // initialize first data page
        newPage->init(newPageNo);

        // update header page
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;

        // unpin both pages and mark them dirty
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK)
        {
            return status;
        }
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK)
        {
            return status;
        }

        status = db.closeFile(file);
        if (status != OK) {
            return status;
        }
        return status;
    }

    // file already exists
    status = db.closeFile(file);
    if (status != OK) {
        return status;
    }
    return FILEEXISTS;
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
    return db.destroyFile(fileName);
}

// constructor opens the underlying file
HeapFile::HeapFile(const string &fileName, Status &returnStatus)
{
    Status status;
    Page *pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // get header page number
        status = filePtr->getFirstPage(headerPageNo);
        if (status != OK)
        {
            returnStatus = status;
            return;
        }

        // read and pin the header page
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if (status != OK)
        {
            returnStatus = status;
            return;
        }

        // initialize header page related fields
        headerPage = (FileHdrPage *)pagePtr;
        hdrDirtyFlag = false;

        // read and pin first data page
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            returnStatus = status;
            return;
        }
        curDirtyFlag = false;

        // initialize curRec
        curRec = NULLRID;

        returnStatus = OK;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// destructor
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName
         << endl;

    // unpin data page if any
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK)
            cerr << "error in unpin of data page\n";
    }

    // unpin header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK)
        cerr << "error in unpin of header page\n";

    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

// return number of records in file
const int HeapFile::getRecCnt() const { return headerPage->recCnt; }

// retrieve an arbitrary record from a file
const Status HeapFile::getRecord(const RID &rid, Record &rec)
{
    Status status;

    // check if requested page is current
    if (curPage == NULL || curPageNo != rid.pageNo)
    {
        if (curPage != NULL)
        {
            // unpin current page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
            {
                return status;
            }
        }

        // read requested page
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK)
        {
            return status;
        }

        // update tracking
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        curRec = NULLRID;
    }

    // get record from page
    status = curPage->getRecord(rid, rec);
    if (status != OK)
    {
        return status;
    }

    // update current record
    curRec = rid;

    return OK;
}

HeapFileScan::HeapFileScan(const string &name, Status &status)
    : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_, const int length_,
                                     const Datatype type_, const char *filter_,
                                     const Operator op_)
{
    if (!filter_)
    {
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int) ||
         type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT &&
         op_ != NE))
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

const bool HeapFileScan::matchRec(const Record &rec) const
{
    // no filtering requested
    if (!filter)
        return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length - 1) >= rec.length)
        return false;

    float diff = 0; // < 0 if attr < fltr
    switch (type)
    {

    case INTEGER:
        int iattr, ifltr; // word-alignment problem possible
        memcpy(&iattr, (char *)rec.data + offset, length);
        memcpy(&ifltr, filter, length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr; // word-alignment problem possible
        memcpy(&fattr, (char *)rec.data + offset, length);
        memcpy(&ffltr, filter, length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset, filter, length);
        break;
    }

    switch (op)
    {
    case LT:
        if (diff < 0.0)
            return true;
        break;
    case LTE:
        if (diff <= 0.0)
            return true;
        break;
    case EQ:
        if (diff == 0.0)
            return true;
        break;
    case GTE:
        if (diff >= 0.0)
            return true;
        break;
    case GT:
        if (diff > 0.0)
            return true;
        break;
    case NE:
        if (diff != 0.0)
            return true;
        break;
    }

    return false;
}

// returns outRid if RID of the next record satisfies the scan predicate
const Status HeapFileScan::scanNext(RID &outRid)
{
    Status status;
    RID nextRid;
    Record rec;

    // loop until we find a matching record or reach end of file
    while (true)
    {
        // if no page is currently pinned, start with first page
        if (curPage == NULL)
        {
            curPageNo = headerPage->firstPage;

            if (curPageNo == -1)
            {
                return FILEEOF;
            }

            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK)
            {
                return status;
            }

            curDirtyFlag = false;
            curRec = NULLRID;

            status = curPage->firstRecord(nextRid);
            if (status != OK && status != NORECORDS)
            {
                return status;
            }

            if (status == NORECORDS)
            {
                continue;
            }
        }
        else
        {
            // continue scan on current page
            status = curPage->nextRecord(curRec, nextRid);

            if (status == ENDOFPAGE)
            {
                // unpin current page
                status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                if (status != OK)
                {
                    return status;
                }

                // get next page number
                int nextPageNo;
                status = curPage->getNextPage(nextPageNo);
                if (status != OK)
                {
                    return status;
                }

                if (nextPageNo == -1)
                {
                    curPage = NULL;
                    return FILEEOF;
                }

                // read in next page
                curPageNo = nextPageNo;
                status = bufMgr->readPage(filePtr, curPageNo, curPage);
                if (status != OK)
                {
                    return status;
                }
                curDirtyFlag = false;

                status = curPage->firstRecord(nextRid);
                if (status != OK && status != NORECORDS)
                {
                    return status;
                }

                if (status == NORECORDS)
                {
                    continue;
                }
            }
            else if (status != OK)
            {
                return status;
            }
        }

        status = curPage->getRecord(nextRid, rec);
        if (status != OK)
        {
            return status;
        }

        if (matchRec(rec))
        {
            outRid = nextRid;
            curRec = nextRid;
            return OK;
        }

        curRec = nextRid;
    }

    return FILEEOF;
}

const Status HeapFileScan::endScan()
{
    Status status;
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

HeapFileScan::~HeapFileScan() { endScan(); }

const Status HeapFileScan::markScan()
{
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
            if (status != OK)
                return status;
        }
        curPageNo = markedPageNo;
        curRec = markedRec;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false;
    }
    else
        curRec = markedRec;
    return OK;
}

const Status HeapFileScan::getRecord(Record &rec)
{
    return curPage->getRecord(curRec, rec);
}

const Status HeapFileScan::deleteRecord()
{
    Status status;
    status = curPage->deleteRecord(curRec);
    if (status != OK)
        return status;

    curDirtyFlag = true;
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return OK;
}

const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

InsertFileScan::InsertFileScan(const string &name, Status &status)
    : HeapFile(name, status)
{
    // heapfile constructor already read in header page and first data page -- no
    // need to do it again
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK)
            cerr << "error in unpin of data page\n";
    }
}

const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid)
{
    Page *newPage;
    int newPageNo;
    Status status;

    // check if record is too large for a page
    // (record length must be less than PAGESIZE - DPFIXED)
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED)
    {
        return INVALIDRECLEN;
    }

    // If no current page, we need to get the last page
    if (curPage == NULL)
    {
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
        {
            return status;
        }
        curDirtyFlag = false;
    }

    // try to insert record into current page
    status = curPage->insertRecord(rec, outRid);
    if (status != OK)
    {
        // current page is full, need to allocate a new page

        // first unpin current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
        {
            return status;
        }

        // allocate new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK)
        {
            return status;
        }

        // and initialize it
        newPage->init(newPageNo);

        // link new page to end of file
        status = curPage->setNextPage(newPageNo);
        if (status != OK)
        {
            return status;
        }

        // update header page
        headerPage->pageCnt++;
        headerPage->lastPage = newPageNo;
        hdrDirtyFlag = true;

        // update current page to new page
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = true;

        // & try insert again on new page
        status = curPage->insertRecord(rec, outRid);
        if (status != OK)
        {
            return status;
        }
    }

    // update record count
    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curDirtyFlag = true;

    return OK;
}
