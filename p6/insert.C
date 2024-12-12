#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(
  const string & relation, 
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

  Status status;
  RID outRID;
  
  // check args
  if (relation.empty() || attrCnt <= 0 || attrList == nullptr) {
      return ATTRTYPEMISMATCH;
  }

  // start searching!
  InsertFileScan inserter(relation, status);
  if (status != OK)
  {
    return status;
  }

  // Retrieve attributes of the relation
  AttrDesc *attrs;
  int relAttrCnt;
  status = attrCat->getRelInfo(relation, relAttrCnt, attrs);
  if (status != OK) 
  {
    return status;
  }

  // make the record (get its size, assign a ptr, )
  int size = 0;
  for (int i = 0; i < attrCnt; i++) {
    size += attrs[i].attrLen;
  }
  char data[size];
  Record outRec;
  outRec.data = (void *) data;
  outRec.length = size;

  // # we need to look through all of the attrs and find matches so that
  // the attrs are in the correct order (sometimes they arent!)
  // https://cplusplus.com/forum/general/211593/ (for finding matches between 2 arrays)
  bool matchFound = false;
  for (int i = 0; i < attrCnt; i++) {    
    for (int j = 0; j < relAttrCnt; j++) {

      // check to see if the keys match; if they do then we want this value
      if (strcmp(attrList[i].attrName, attrs[j].attrName) == 0) {
        // match found! now we can get the value from this key if it's not null
        matchFound = true;
        if (attrList[i].attrValue == NULL) {
          return ATTRTYPEMISMATCH;
        }
        
        // get the value and cast it depending on what type it is
        char* value = nullptr;
        switch (attrList[i].attrType) {
            case INTEGER: {
                int tempInt = atoi(static_cast<char*>(attrList[i].attrValue));
                value = reinterpret_cast<char*>(&tempInt);
                break;
            }
            case FLOAT: {
                float tempFloat = atof(static_cast<char*>(attrList[i].attrValue));
                value = reinterpret_cast<char*>(&tempFloat);
                break;
            }
            case STRING: {
                value = static_cast<char*>(attrList[i].attrValue);
                break;
            }
            default:
                return ATTRTYPEMISMATCH;
        }

        // copy the data into record
        memcpy(data + attrs[j].attrOffset, value, attrs[j].attrLen);
      }

      // if we get here, the keys didn't match. onto the next
    }
  }
  
  // the attrs didn't match for any of them
  if (!matchFound) {
    return ATTRNOTFOUND;
  }

  // # make the insert
  status = inserter.insertRecord(outRec, outRID);
  if(status != OK) {
        return status;
  }

  return OK;
}



