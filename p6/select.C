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
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;


}
