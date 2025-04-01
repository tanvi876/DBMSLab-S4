#include "Algebra.h"
#include <cstring>
#include <iostream>
using namespace std;


bool isNumber(char *str) {
  int len;
  float ignore;
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}

// int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE],
//                     char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
//   int srcRelId = OpenRelTable::getRelId(srcRel); 
//   if (srcRelId == E_RELNOTOPEN) {
//     return E_RELNOTOPEN;
//   }

//   AttrCatEntry attrCatEntry;
//   if (AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry) ==
//       E_ATTRNOTEXIST) {
//     return E_ATTRNOTEXIST;
//   }

//   int type = attrCatEntry.attrType;
//   Attribute attrVal;
//   if (type == NUMBER) {
//     if (isNumber(strVal)) { 
//       attrVal.nVal = atof(strVal);
//     } else {
//       return E_ATTRTYPEMISMATCH;
//     }
//   } else if (type == STRING) {
//     strcpy(attrVal.sVal, strVal);
//   }

//   RelCacheTable::resetSearchIndex(srcRelId);

//   RelCatEntry relCatEntry;
//   RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

//   printf("|");
//   for (int i = 0; i < relCatEntry.numAttrs; ++i) {
//     AttrCatEntry attrCatEntry;
//     AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
//     printf(" %s |", attrCatEntry.attrName);
//   }
//   printf("\n");

//   while (true) {
//     RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

//     if (searchRes.block != -1 && searchRes.slot != -1) {

//       AttrCatEntry attrCatEntry;
//       RecBuffer BlockBuffer(searchRes.block);
//       HeadInfo blockHeader;
//       BlockBuffer.getHeader(&blockHeader);

//       Attribute recordBuffer[blockHeader.numAttrs];
//       BlockBuffer.getRecord(recordBuffer, searchRes.slot);

//       for (int i = 0; i < relCatEntry.numAttrs; ++i) {
//         AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
//         if (attrCatEntry.attrType == NUMBER)
//           printf(" %d |", (int)recordBuffer[i].nVal);
//         else
//           printf(" %s |", recordBuffer[i].sVal);
//       }
//       printf("\n");

//     } else {
//       break;
//     }
//   }

//   return SUCCESS;
// }

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], 
  char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) 
{
// get the srcRel's rel-id (let it be srcRelid), using OpenRelTable::getRelId()
// if srcRel is not open in open relation table, return E_RELNOTOPEN

int srcRelId = OpenRelTable::getRelId(srcRel); // we'll implement this later
if (srcRelId == E_RELNOTOPEN) return E_RELNOTOPEN;

// get the attr-cat entry for attr, using AttrCacheTable::getAttrCatEntry()
// if getAttrcatEntry() call fails return E_ATTRNOTEXIST
AttrCatEntry attrCatEntry;
int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);

if (ret == E_ATTRNOTEXIST) return E_ATTRNOTEXIST;

/*** Convert strVal to an attribute of data type NUMBER or STRING ***/

// TODO: Convert strVal (string) to an attribute of data type NUMBER or STRING 
int type = attrCatEntry.attrType;
Attribute attrVal;
if (type == NUMBER)
{
if (isNumber(strVal)) // the isNumber() function is implemented below
attrVal.nVal = atof(strVal);
else
return E_ATTRTYPEMISMATCH;
}
else if (type == STRING)
strcpy(attrVal.sVal, strVal);

/*** Creating and opening the target relation ***/
// Prepare arguments for createRel() in the following way:
// get RelcatEntry of srcRel using RelCacheTable::getRelCatEntry()
RelCatEntry relCatEntryBuffer;
RelCacheTable::getRelCatEntry(srcRelId, &relCatEntryBuffer);

int srcNoAttrs =  relCatEntryBuffer.numAttrs;

/* let attr_names[src_nAttrs][ATTR_SIZE] be a 2D array of type char
(will store the attribute names of rel). */
char srcAttrNames [srcNoAttrs][ATTR_SIZE];

// let attr_types[src_nAttrs] be an array of type int
int srcAttrTypes [srcNoAttrs];

/*iterate through 0 to src_nAttrs-1 :
get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
fill the attr_names, attr_types arrays that we declared with the entries
of corresponding attributes
*/
for (int attrIndex = 0; attrIndex < srcNoAttrs; attrIndex++) {
AttrCatEntry attrCatEntryBuffer;
AttrCacheTable::getAttrCatEntry(srcRelId, attrIndex, &attrCatEntryBuffer);

strcpy (srcAttrNames[attrIndex], attrCatEntryBuffer.attrName);
srcAttrTypes[attrIndex] = attrCatEntryBuffer.attrType;
}

/* Create the relation for target relation by calling Schema::createRel()
by providing appropriate arguments */
// if the createRel returns an error code, then return that value.

ret = Schema::createRel(targetRel, srcNoAttrs, srcAttrNames, srcAttrTypes);
if (ret != SUCCESS) return ret;

/* Open the newly created target relation by calling OpenRelTable::openRel()
method and store the target relid */
/* If opening fails, delete the target relation by calling Schema::deleteRel()
and return the error value returned from openRel() */
int targetRelId = OpenRelTable::openRel(targetRel);
if (targetRelId < 0 || targetRelId >= MAX_OPEN) return targetRelId;

/*** Selecting and inserting records into the target relation ***/
/* Before calling the search function, reset the search to start from the
first using RelCacheTable::resetSearchIndex() */
// RelCacheTable::resetSearchIndex(srcRelId);

Attribute record[srcNoAttrs];

/*
The BlockAccess::search() function can either do a linearSearch or
a B+ tree search. Hence, reset the search index of the relation in the
relation cache using RelCacheTable::resetSearchIndex().
Also, reset the search index in the attribute cache for the select
condition attribute with name given by the argument `attr`. Use
AttrCacheTable::resetSearchIndex().
Both these calls are necessary to ensure that search begins from the
first record.
*/

RelCacheTable::resetSearchIndex(srcRelId);
AttrCacheTable::resetSearchIndex(srcRelId, attr);

// read every record that satisfies the condition by repeatedly calling
// BlockAccess::search() until there are no more records to be read

while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS) 
{
ret = BlockAccess::insert(targetRelId, record);

// if (insert fails) {
//     close the targetrel(by calling Schema::closeRel(targetrel))
//     delete targetrel (by calling Schema::deleteRel(targetrel))
//     return ret;
// }

if (ret != SUCCESS) 
{
Schema::closeRel(targetRel);
Schema::deleteRel(targetRel);
return ret;
}
}

// Close the targetRel by calling closeRel() method of schema layer
Schema::closeRel(targetRel);

return SUCCESS;
}

// will return if a string can be parsed as a floating point number

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
  if((strcmp(relName,(char*)RELCAT_RELNAME))==0||
      strcmp(relName,(char*)ATTRCAT_RELNAME) ==0){
        return E_NOTPERMITTED;
      }
   int relId = OpenRelTable::getRelId(relName);

   if(relId == E_RELNOTOPEN) return E_RELNOTOPEN;
   
   RelCatEntry relCatEntry;
   RelCacheTable::getRelCatEntry(relId, &relCatEntry);
   if(relCatEntry.numAttrs!=nAttrs){
    return E_NATTRMISMATCH;
   }
   Attribute recordValues[nAttrs];
   for(int i=0;i<nAttrs;i++){
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId,i,&attrCatEntry);
    int type = attrCatEntry.attrType;

    if (type == NUMBER){
     if(isNumber(record[i])){
               recordValues[i].nVal = atof(recordValues[i].sVal);
            }
            else{
                return E_ATTRTYPEMISMATCH;
            }
        }
     else if (type == STRING){
            strcpy(recordValues[i].sVal, record[i]);
        }
   }
   return BlockAccess::insert(relId, recordValues);
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId < 0 or srcRelId >= MAX_OPEN) {
    return E_RELNOTOPEN;
  }
  RelCatEntry srcRelCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);

  int numAttrs = srcRelCatEntry.numAttrs;

  char attrNames[numAttrs][ATTR_SIZE];
  int attrTypes[numAttrs];
  for (int i = 0; i < numAttrs; i++) {
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
    strcpy(attrNames[i], attrCatEntry.attrName);
    attrTypes[i] = attrCatEntry.attrType;
  }

  /*** Creating and opening the target relation ***/
  int ret = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
  if (ret != SUCCESS)
    return ret;

  int targetrelId = OpenRelTable::openRel(targetRel);
  if (targetrelId < 0 or targetrelId >= MAX_OPEN) {
    Schema::deleteRel(targetRel);
    return targetrelId;
  }

  /***Inserting projected records into the target relation ***/
  RelCacheTable::resetSearchIndex(srcRelId);
  Attribute record[numAttrs];
  while (BlockAccess::project(srcRelId, record) == SUCCESS) {
    ret = BlockAccess::insert(targetrelId, record);
    if (ret != SUCCESS) {
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      return ret;
    }
  }
  Schema::closeRel(targetRel);
  return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);

  if (srcRelId == E_RELNOTOPEN)
      return srcRelId;

  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

  int src_nAttrs = relCatEntry.numAttrs;

  int attrOffsets[tar_nAttrs];
  int attrTypes[tar_nAttrs];

  for (int i = 0; i < tar_nAttrs; i++) {
      AttrCatEntry attrCatEntry;
      int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrCatEntry);

      if (ret != SUCCESS)
          return ret;

      attrOffsets[i] = attrCatEntry.offset;
      attrTypes[i] = attrCatEntry.attrType;
  }

  int ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attrTypes);
  if (ret != SUCCESS)
      return ret;

  int tarRelId = OpenRelTable::openRel(targetRel);

  if (tarRelId < 0 || tarRelId >= MAX_OPEN)
      return tarRelId;

  Attribute record[src_nAttrs];


  RelCacheTable::resetSearchIndex(srcRelId);
  while(BlockAccess::project(srcRelId, record) == SUCCESS) {

      Attribute projRecord[tar_nAttrs];

      for (int i = 0; i < tar_nAttrs; i++)
          projRecord[i] = record[attrOffsets[i]];

      int ret = BlockAccess::insert(tarRelId, projRecord);

      if (ret != SUCCESS) {
          OpenRelTable::closeRel(tarRelId);
          Schema::deleteRel(targetRel);
          return ret;
      }
  }

  Schema::closeRel(targetRel);

  return SUCCESS;
}