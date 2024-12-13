#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6

/** This function will delete all tuples in relation satisfying the predicate specified 
 * by attrName, op, and the constant attrValue. type denotes the type of the attribute. 
 * You can locate all the qualifying tuples using a filtered HeapFileScan. */

cout << "Doing QU_Delete " << endl;

Status status;
AttrDesc attrDesc;

// no specific record to delete then delete all
if (attrName.empty())
{
	HeapFileScan scan(relation, status);
	if (status != OK)
	{
		return status;
	}

	// no filter scan
	status = scan.startScan(0, 0, STRING, NULL, EQ);
	if (status != OK)
	{
		return status;
	}

	// delete records
	RID rid;
	while (scan.scanNext(rid) == OK)
	{
		status = scan.deleteRecord();
		if (status != OK)
		{
			scan.endScan();
			return status;
		}
	}

	return scan.endScan();
}

// get attr info
status = attrCat->getInfo(relation, attrName, attrDesc);
if (status != OK)
{
	return status;
}

// check types
if (type != attrDesc.attrType)
{
	return ATTRTYPEMISMATCH;
}

// create filter
char* filter;

if (attrValue) {
	switch (type)
	{
		case STRING:
		{
			filter = (char*)attrValue;
			break;
		}
		case INTEGER:
		{
			filter = (char*)malloc(sizeof(int));
			*(int*)filter = atoi(attrValue);
			break;
		}

		case FLOAT:
		{
			filter = (char*)malloc(sizeof(float));
			*(float*)filter = atof(attrValue);
			break;
		}

		default:
			return ATTRTYPEMISMATCH;
	}
}
else
{
	filter = NULL;
}

HeapFileScan scan(relation, status);
if (status != OK)
{
	if (type != STRING && filter)
	{
		free(filter);
	}
	return status;
}

status = scan.startScan(attrDesc.attrOffset,
						attrDesc.attrLen,
						type, 
						filter,
						op);
if (status != OK)
{
	if (type != STRING && filter)
	{
		free(filter);
	}
	return status;
}

RID rid;
int deleteCnt = 0;
while (scan.scanNext(rid) == OK)
{
	status = scan.deleteRecord();
	if (status != OK)
	{
		scan.endScan();
		if (type != STRING && filter)
		{
			free(filter);
		}
		return status;
	}
	deleteCnt++;
}

if (type != STRING && filter)
{
	free(filter);
}
status = scan.endScan();

return status;



}


