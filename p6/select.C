#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
	/* A selection is implemented using a filtered HeapFileScan.
	The result of the selection is stored in the result relation called result 
	(a heapfile with this name will be created by the parser before QU_Select() is called).
	The project list is defined by the parameters projCnt and projNames.
	Projection should be done on the fly as each result tuple is being appended to the result table.
	A final note: the search value is always supplied as the character string attrValue.
	You should convert it to the proper type based on the type of attr.
	You can use the atoi() function to convert a char* to an integer and atof() to convert it to a float.
	If attr is NULL, an unconditional scan of the input table should be performed.*/

	// Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	
	char* filter;
	if (attrValue) 
	{
		switch (attr->attrType)
		{
			case STRING:
				filter = (char*)attrValue;
				break;

			case INTEGER:
				filter = (char*)malloc(sizeof(int));
				*(int*)filter = atoi(attrValue);
				break;

			case FLOAT:
				filter = (char*)malloc(sizeof(float));
				*(float*)filter = atof(attrValue);
				break;
		}
	}
	
	Status status;
	AttrDesc attrDescArray[projCnt];

	for (int i = 0; i < projCnt; i++)
	{
		status = attrCat->getInfo(projNames[i].relName,
									projNames[i].attrName,
									attrDescArray[i]);

		if (status != OK)
		{
			return status;
		}
	}

	AttrDesc attrDesc;
	AttrDesc* attrDescPtr = NULL;

	if (attr)
	{
		status = attrCat->getInfo(attr->relName,
									attr->attrName,
									attrDesc);

		if (status != OK)
		{
			return status;
		}

		attrDescPtr = &attrDesc;
	}

	int reclen = 0;
	for (int i = 0; i < projCnt; i++)
	{
		reclen += attrDescArray[i].attrLen;
	}

	status = ScanSelect(result, projCnt, attrDescArray, attrDescPtr, op, filter, reclen);
	return status;

}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
	// startScan()

	// scanNext()

	// endScan()
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;


}
