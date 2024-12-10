#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6

/** Insert a tuple with the given attribute values (in attrList) in relation.
 * The value of the attribute is supplied in the attrValue member of the attrInfo structure.
 * Since the order of the attributes in attrList[] may not be the same as in the relation, 
 * you might have to rearrange them before insertion.
 * If no value is specified for an attribute, you should reject the insertion 
 * as Minirel does not implement NULLs. */

return OK;

}

