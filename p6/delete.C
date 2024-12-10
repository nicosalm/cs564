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

return OK;



}


