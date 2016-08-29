/******************************************************************************/
/*                                                                            */
/*    Copyright (c) 1990-2016, KAIST                                          */
/*    All rights reserved.                                                    */
/*                                                                            */
/*    Redistribution and use in source and binary forms, with or without      */
/*    modification, are permitted provided that the following conditions      */
/*    are met:                                                                */
/*                                                                            */
/*    1. Redistributions of source code must retain the above copyright       */
/*       notice, this list of conditions and the following disclaimer.        */
/*                                                                            */
/*    2. Redistributions in binary form must reproduce the above copyright    */
/*       notice, this list of conditions and the following disclaimer in      */
/*       the documentation and/or other materials provided with the           */
/*       distribution.                                                        */
/*                                                                            */
/*    3. Neither the name of the copyright holder nor the names of its        */
/*       contributors may be used to endorse or promote products derived      */
/*       from this software without specific prior written permission.        */
/*                                                                            */
/*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS     */
/*    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT       */
/*    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS       */
/*    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE          */
/*    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    */
/*    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;        */
/*    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER        */
/*    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT      */
/*    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN       */
/*    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE         */
/*    POSSIBILITY OF SUCH DAMAGE.                                             */
/*                                                                            */
/******************************************************************************/
/******************************************************************************/
/*                                                                            */
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
/*                                                                            */
/*    Developed by Professor Kyu-Young Whang et al.                           */
/*                                                                            */
/*    Advanced Information Technology Research Center (AITrc)                 */
/*    Korea Advanced Institute of Science and Technology (KAIST)              */
/*                                                                            */
/*    e-mail: odysseus.oosql@gmail.com                                        */
/*                                                                            */
/*    Bibliography:                                                           */
/*    [1] Whang, K., Lee, J., Lee, M., Han, W., Kim, M., and Kim, J., "DB-IR  */
/*        Integration Using Tight-Coupling in the Odysseus DBMS," World Wide  */
/*        Web, Vol. 18, No. 3, pp. 491-520, May 2015.                         */
/*    [2] Whang, K., Lee, M., Lee, J., Kim, M., and Han, W., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with IR Features," In Proc. */
/*        IEEE 21st Int'l Conf. on Data Engineering (ICDE), pp. 1104-1105     */
/*        (demo), Tokyo, Japan, April 5-8, 2005. This paper received the Best */
/*        Demonstration Award.                                                */
/*    [3] Whang, K., Park, B., Han, W., and Lee, Y., "An Inverted Index       */
/*        Storage Structure Using Subindexes and Large Objects for Tight      */
/*        Coupling of Information Retrieval with Database Management          */
/*        Systems," U.S. Patent No.6,349,308 (2002) (Appl. No. 09/250,487     */
/*        (1999)).                                                            */
/*    [4] Whang, K., Lee, J., Kim, M., Lee, M., Lee, K., Han, W., and Kim,    */
/*        J., "Tightly-Coupled Spatial Database Features in the               */
/*        Odysseus/OpenGIS DBMS for High-Performance," GeoInformatica,        */
/*        Vol. 14, No. 4, pp. 425-446, Oct. 2010.                             */
/*    [5] Whang, K., Lee, J., Kim, M., Lee, M., and Lee, K., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with Spatial Database       */
/*        Features," In Proc. 23rd IEEE Int'l Conf. on Data Engineering       */
/*        (ICDE), pp. 1493-1494 (demo), Istanbul, Turkey, Apr. 16-20, 2007.   */
/*                                                                            */
/******************************************************************************/

/*
 * Module: LOM_CreateClass.c
 *
 * Description:
 *  Create a Relationship.
 *
 * Imports:
 *
 * Exports:
 *  Four LOM_Relationship_Create( Four ,char *, Four , Two, Four, Two, One)
 *
 * Returns:
 *  Error code
 */


#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog_Internal.h"
#include "Catalog.h"
#include <string.h>	

static Four lom_TraverseSubclassTree(LOM_Handle* handle, Four volId, Four orn, Four classInfo, Four* index, Four startIndex, Four endIndex, Four* subClasses);

Four LOM_Relationship_Create(
    LOM_Handle* handle,     /* IN LOM system handle */
    Four volId,             /* IN volume ID */
    char *relationshipName, /* IN relationship name */
    Four fromClassId,       /* IN from class ID */
    Two  fromAttrNum,       /* IN from attribute number */
    Four toClassId,         /* IN to calss ID */
    Two  toAttrNum,         /* IN to attribute number */
    One  cardinality,       /* IN cardinality */
    One  direction,         /* IN uni or bi-directional */
    Four *relationshipId    /* OUT return relationship id */
)
{
    Two  keyLen;            /* should be Two to store key length */
    Four e;                 /* error number */
    Four i, j;              /* index variable */
    Four v;                 /* index on LRDS mount table */
    Four mv;                /* mount volume index used in in-memory catalog */
    Four catScanId;         /* scan id for a catalog table access */
    Boolean found;          /* SM_TRUE if the relation is already defined */
    BoundCond bound;        /* boundary condition */
    LockParameter lockup;   /* lock parameter */
    Four orn;               /* open relation number */
    Two complexType;        /* complex type */
    Two type;               /* attribute type */
    lrds_RelTableEntry* relTableEntry; 
    Four nFromSubClasses;
    Four iFromSubClass;
    Four nToSubClasses;
    Four iToSubClass;
    Four fromSubClassIdsBuf[100];
    Two  fromSubClassAttrNosBuf[100];
    Four toSubClassIdsBuf[100];
    Two  toSubClassAttrNosBuf[100];
    Four attrInfo;
    One  reverseCardinality;
    
    /* check parameters */
    if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

    for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
        if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) break;

    if (v == CATALOG_MAXNUMOFVOLS) LOM_ERROR(handle, eVOLUMENOTMOUNTED_LOM);

    if (relationshipName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* check parameters */
    if (cardinality < LOM_RELATIONSHIP_ONE_TO_ONE ||
        cardinality > LOM_RELATIONSHIP_MANY_TO_MANY )
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (direction != LOM_RELATIONSHIP_UNIDIRECTIONAL
        && direction != LOM_RELATIONSHIP_BIDIRECTIONAL )
        LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* check if the relationship given already exists */
    /* scan SYSRELATIONSHIP table */
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSRELATIONSHIP_CLASSNAME);
    if(orn < 0) LOM_ERROR(handle, orn);

    bound.op = SM_EQ;
    keyLen = strlen(relationshipName);
    bound.key.len = sizeof(Two) + keyLen;
    memcpy(&(bound.key.val[0]), &keyLen, sizeof(Two));
    memcpy(&(bound.key.val[sizeof(Two)]), relationshipName, keyLen);

    /* set lock up parameters */
    /* just for reading */
    lockup.mode = L_IS;
    lockup.duration = L_COMMIT;

    relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);
    catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn,&((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[2].iid),
                                   &bound, &bound, 0, (BoolExp*)NULL, &lockup);
    if (catScanId < 0) LOM_ERROR(handle, catScanId);

    /* Initialize a flag meaning the existence of data file search. */
    found = SM_FALSE;

    e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL);
    if (e < 0) LOM_ERROR(handle, e);

    if (e != EOS) found = SM_TRUE;

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
    if (e < 0) LOM_ERROR(handle, e);
    
    if (found) LOM_ERROR(handle, eRELATIONDUPLICATED_LOM);

    /* convert user colNo to system colNo */
    fromAttrNum = GET_SYSTEMLEVEL_COLNO(fromAttrNum);
    toAttrNum   = GET_SYSTEMLEVEL_COLNO(toAttrNum);

    /*
     *  check if parameter is right.
     */  
    /* check from attribute type */
    e = Catalog_GetMountTableInfo(handle, volId, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    
    e = Catalog_GetAttrInfo(handle, volId, fromClassId, fromAttrNum, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    complexType = CATALOG_GET_ATTRCOMPLEXTYPE(&(CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]));
    type = CATALOG_GET_ATTRTYPE(&(CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]));
    
    switch(cardinality)
    {
      case LOM_RELATIONSHIP_ONE_TO_ONE:
      case LOM_RELATIONSHIP_MANY_TO_ONE:
        if(!(complexType == LOM_COMPLEXTYPE_BASIC && 
            (type == LOM_OID || type == LOM_REF || type == LOM_LINK))) 
            return eBADATTRRELATIONSHIP_LOM;
        break;
      case LOM_RELATIONSHIP_ONE_TO_MANY:
      case LOM_RELATIONSHIP_MANY_TO_MANY:
        if(!((complexType == LOM_COMPLEXTYPE_SET ||
              complexType == LOM_COMPLEXTYPE_COLLECTIONSET ||
              complexType == LOM_COMPLEXTYPE_COLLECTIONBAG ||
              complexType == LOM_COMPLEXTYPE_COLLECTIONLIST ||
              complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET ||
              complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG ||
              complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST ||
              complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY) && 
             (type == LOM_OID || type == LOM_REF || type == LOM_LINK))) 
            return eBADATTRRELATIONSHIP_LOM;
        break;
    }

    /* check to attribute type */
    e = Catalog_GetAttrInfo(handle, volId, toClassId, toAttrNum, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    complexType = CATALOG_GET_ATTRCOMPLEXTYPE(&(CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]));
    type = CATALOG_GET_ATTRTYPE(&(CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]));
    
    switch(cardinality)
    {
      case LOM_RELATIONSHIP_ONE_TO_MANY:
      case LOM_RELATIONSHIP_ONE_TO_ONE:
        if(!(complexType == LOM_COMPLEXTYPE_BASIC && 
            (type == LOM_OID || type == LOM_REF || type == LOM_LINK))) 
            return eBADATTRRELATIONSHIP_LOM;
        break;
      case LOM_RELATIONSHIP_MANY_TO_ONE:
      case LOM_RELATIONSHIP_MANY_TO_MANY:
        if(!((complexType == LOM_COMPLEXTYPE_SET ||
              complexType == LOM_COMPLEXTYPE_COLLECTIONSET ||
              complexType == LOM_COMPLEXTYPE_COLLECTIONBAG ||
              complexType == LOM_COMPLEXTYPE_COLLECTIONLIST ||
              complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET ||
              complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG ||
              complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST ||
              complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY) && 
             (type == LOM_OID || type == LOM_REF || type == LOM_LINK))) 
            return eBADATTRRELATIONSHIP_LOM;
        break;
    }
    
    /*  
     * get relationship ID 
     */
    *relationshipId = lom_GetAndIncrementLastRelationshipId(handle, volId);
    if(*relationshipId < 0) LOM_ERROR(handle, *relationshipId);
    
    /*
     * update SYSRELATIONSHIP table
     */
    lockup.mode = L_IX;
    lockup.duration = L_COMMIT;
    
    catScanId = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn, FORWARD, 0, NULL, &lockup);
    if(catScanId < 0) LOM_ERROR(handle, catScanId);

    /* generate reverse cardinality */
    switch(cardinality)
    {
      case LOM_RELATIONSHIP_ONE_TO_ONE:
      case LOM_RELATIONSHIP_MANY_TO_MANY:
        reverseCardinality = cardinality;
        break;
      case LOM_RELATIONSHIP_MANY_TO_ONE:
        reverseCardinality = LOM_RELATIONSHIP_ONE_TO_MANY;
        break;
      case LOM_RELATIONSHIP_ONE_TO_MANY:
        reverseCardinality = LOM_RELATIONSHIP_MANY_TO_ONE;
        break;
    }
    
    /* create relationship between (fromClass, fromSubClasses) and (toClass, toSubClasses) */
    iFromSubClass = 0;
    while(1)
    {
        if(iFromSubClass == -1)
            break;

        if(iFromSubClass == 0)
        {
            nFromSubClasses = lom_GetSubClasses(handle, volId, fromClassId,
                                                iFromSubClass,
                                                sizeof(fromSubClassIdsBuf) / sizeof(Four),
                                                &fromSubClassIdsBuf[1]);
            if(nFromSubClasses < 0) LOM_ERROR(handle, nFromSubClasses);
            if(nFromSubClasses == 0) 
                iFromSubClass = -1;     /* in next loop, exit loop */

            iFromSubClass   += nFromSubClasses;
            nFromSubClasses ++;

            /* adjust fromattrno */
            for(i = 0; i < (nFromSubClasses - 1); i++)
            {
                /* adjust attr-num caused by multiple inheritance */
                e = lom_AdjustAttrNum(handle, volId, fromClassId, fromAttrNum, 
                                      fromSubClassIdsBuf[i + 1], &fromSubClassAttrNosBuf[i + 1]);
                if(e < 0) LOM_ERROR(handle, e);
            }

            fromSubClassIdsBuf[0]     = fromClassId;
            fromSubClassAttrNosBuf[0] = fromAttrNum;
        }
        else
        {
            nFromSubClasses = lom_GetSubClasses(handle, volId, fromClassId,
                                                iFromSubClass,
                                                sizeof(fromSubClassIdsBuf) / sizeof(Four),
                                                fromSubClassIdsBuf);
            if(nFromSubClasses < 0) LOM_ERROR(handle, nFromSubClasses);
            if(nFromSubClasses == 0)
                break;

            iFromSubClass += nFromSubClasses;

            /* adjust fromattrno */
            for(i = 0; i < nFromSubClasses; i++)
            {
                /* adjust attr-num caused by multiple inheritance */
                e = lom_AdjustAttrNum(handle, volId, fromClassId, fromAttrNum, 
                                      fromSubClassIdsBuf[i], &fromSubClassAttrNosBuf[i]);
                if(e < 0) LOM_ERROR(handle, e);
            }
        }
        
        iToSubClass = 0;
        while(1)
        {
            if(iToSubClass == -1)
                break;
            if(iToSubClass == 0)
            {
                nToSubClasses = lom_GetSubClasses(handle, volId, toClassId,
                                                  iToSubClass,
                                                  sizeof(toSubClassIdsBuf) / sizeof(Four),
                                                  &toSubClassIdsBuf[1]);
                if(nToSubClasses < 0) LOM_ERROR(handle, nToSubClasses);
                if(nToSubClasses == 0) 
                    iToSubClass = -1;       /* in next loop, exit loop */

                iToSubClass += nToSubClasses;
                nToSubClasses ++;

                /* adjust fromattrno */
                for(i = 0; i < (nToSubClasses - 1); i++)
                {
                    /* adjust attr-num caused by multiple inheritance */
                    e = lom_AdjustAttrNum(handle, volId, toClassId, toAttrNum, 
                                          toSubClassIdsBuf[i + 1], &toSubClassAttrNosBuf[i + 1]);
                    if(e < 0) LOM_ERROR(handle, e);
                }

                toSubClassIdsBuf[0]     = toClassId;
                toSubClassAttrNosBuf[0] = toAttrNum;
            }
            else
            {
                nToSubClasses = lom_GetSubClasses(handle, volId, toClassId,
                                                  iToSubClass,
                                                  sizeof(toSubClassIdsBuf) / sizeof(Four),
                                                  toSubClassIdsBuf);
                if(nToSubClasses < 0) LOM_ERROR(handle, nToSubClasses);
                if(nToSubClasses == 0)
                    break;

                iToSubClass += nToSubClasses;

                /* adjust toattrno */
                for(i = 0; i < nToSubClasses; i++)
                {
                    /* adjust attr-num caused by multiple inheritance */
                    e = lom_AdjustAttrNum(handle, volId, toClassId, toAttrNum, 
                                          toSubClassIdsBuf[i], &toSubClassAttrNosBuf[i]);
                    if(e < 0) LOM_ERROR(handle, e);
                }
            }

            for(i = 0; i < nFromSubClasses; i++)
            {
                for(j = 0; j < nToSubClasses; j++)
                {
                    e = lom_AddRelationship(handle, catScanId, SM_TRUE,
                                            fromSubClassIdsBuf[i],
                                            fromSubClassAttrNosBuf[i],
                                            toSubClassIdsBuf[j],
                                            toSubClassAttrNosBuf[j],
                                            direction, cardinality, 
                                            *relationshipId,
                                            relationshipName);
                    if(e < 0) LOM_ERROR(handle, e);

                    if(direction == LOM_RELATIONSHIP_BIDIRECTIONAL)
                    {
                        /* create reverse direction relationship */
                        e = lom_AddRelationship(handle, catScanId, SM_TRUE,
                                                toSubClassIdsBuf[j],
                                                toSubClassAttrNosBuf[j],
                                                fromSubClassIdsBuf[i],
                                                fromSubClassAttrNosBuf[i],
                                                direction, reverseCardinality, 
                                                *relationshipId,
                                                relationshipName);
                        if(e < 0) LOM_ERROR(handle, e);
                    }
                }
            }
        }
    }

    /* update in-memory catalog */
    e = Catalog_Relationship_CreateRelationship(handle, volId, relationshipName, 
					        fromClassId, fromAttrNum, toClassId, toAttrNum, 
					        cardinality, direction, *relationshipId);
    if(e < 0) LOM_ERROR(handle, e);

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
    if(e < 0) LOM_ERROR(handle, e);
    
    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if(e < 0) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four lom_AddRelationship(
    LOM_Handle*     handle,              /* IN LOM system handle */
    Four            ocnOrScanId,
    Boolean         useScanFlag,
    Four            fromClassId,         /* IN */
    Two             fromAttrNum,         /* IN */
    Four            toClassId,           /* IN */ 
    Two             toAttrNum,           /* IN */
    One             direction,           /* IN */
    One             cardinality,         /* IN */
    Four            relationshipId,      /* IN */
    char*           relationshipName)    /* IN */
{
    Four          e;
    ColListStruct clist[LOM_SYSRELATIONSHIP_NUM_COLS];

    clist[0].colNo = LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO;
    clist[0].start = ALL_VALUE;
    clist[0].dataLength = LOM_LONG_SIZE_VAR;
    ASSIGN_VALUE_TO_COL_LIST(clist[0], fromClassId, sizeof(Four));
	clist[0].nullFlag = SM_FALSE;
   
    clist[1].colNo = LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO;
    clist[1].start = ALL_VALUE;
    clist[1].dataLength = LOM_SHORT_SIZE_VAR;
    ASSIGN_VALUE_TO_COL_LIST(clist[1], fromAttrNum, sizeof(Two));
	clist[1].nullFlag = SM_FALSE;
   
    clist[2].colNo = LOM_SYSRELATIONSHIP_TOCLASSID_COLNO;
    clist[2].start = ALL_VALUE;
    clist[2].dataLength = LOM_LONG_SIZE_VAR;
    ASSIGN_VALUE_TO_COL_LIST(clist[2], toClassId, sizeof(Four));
	clist[2].nullFlag = SM_FALSE;

    clist[3].colNo = LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO;
    clist[3].start = ALL_VALUE;
    clist[3].dataLength = LOM_SHORT_SIZE_VAR;
    ASSIGN_VALUE_TO_COL_LIST(clist[3], toAttrNum, sizeof(Two));
	clist[3].nullFlag = SM_FALSE;

    clist[4].colNo = LOM_SYSRELATIONSHIP_CARDINALITY_COLNO;
    clist[4].start = ALL_VALUE;
    clist[4].dataLength = sizeof(One);
    clist[4].data.ptr = &cardinality;
	clist[4].nullFlag = SM_FALSE;

    clist[5].colNo = LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO;
    clist[5].start = ALL_VALUE;
    clist[5].dataLength = strlen(relationshipName);
    clist[5].data.ptr = relationshipName;
		clist[5].nullFlag = SM_FALSE;

    clist[6].colNo = LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO;
    clist[6].start = ALL_VALUE;
    clist[6].dataLength = sizeof(Four);
    ASSIGN_VALUE_TO_COL_LIST(clist[6], relationshipId, sizeof(Four));
	clist[6].nullFlag = SM_FALSE;

    clist[7].colNo = LOM_SYSRELATIONSHIP_DIRECTION_COLNO;
    clist[7].start = ALL_VALUE;
    clist[7].dataLength = sizeof(One);
    clist[7].data.ptr = &direction;
		clist[7].nullFlag = SM_FALSE;

    e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, LOM_SYSRELATIONSHIP_NUM_COLS, clist, NULL);
    if(e < 0) LOM_ERROR(handle, e);
    
    return eNOERROR;
}

Four lom_GetSubClasses(
    LOM_Handle*     handle,             /* IN LOM system handle */
    Four            volId,              /* mount volume index */
    Four            classId,            /* IN  : class */
    Four            fromNthSubClass,    /* IN  : from what subclass is read */
    Four            sizeOfSubClasses,   /* IN  : size of array which will
                                           contain subclasses of the class */
    Four*           subClasses          /* OUT : array of sub classes */
)
{
    Four          index;
    Four          e;
    Four          orn;
    
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_INHERITANCE_CLASSNAME);
    if(orn < 0) LOM_ERROR(handle, orn);
    
    index = 0;
    e = lom_TraverseSubclassTree(handle, volId, orn, classId, &index, fromNthSubClass, 
                                 fromNthSubClass + sizeOfSubClasses - 1,
                                 subClasses);
    if(e < 0) LOM_ERROR(handle, e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if (e < 0) LOM_ERROR(handle, e);
    
    return index;
}
    
static Four lom_TraverseSubclassTree(
    LOM_Handle*     handle,             /* IN LOM system handle */
    Four            volId,              /* volume id */
    Four            orn,                /* open relation number for LOM_INHERITANCE_CLASSNAME */
    Four            classId,            /* root class id */
    Four*           index,              /* sub class index */
    Four            startIndex,         /* start index */
    Four            endIndex,           /* end index */
    Four*           subClasses          /* subclass array */
)
{
    Four          i;    
    Four          e;                    /* error code */
    ColListStruct clist[1];
    LockParameter lockup;
    TupleID       tid;
    BoundCond     bound;
    Four          catScanId;
    lrds_RelTableEntry* relTableEntry;

    relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

    /* construct bound condition */
    bound.op = SM_EQ;
    bound.key.len = sizeof(Four);
    memcpy(&(bound.key.val[0]), &classId, sizeof(Four));

    /* set lock up parameters */
    /* just for reading */
    lockup.mode = L_IS;
    lockup.duration = L_COMMIT;
    catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn,
                                   &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid,
                                   &bound, &bound,
                                   0, (BoolExp*)NULL, &lockup);
    if (catScanId < 0) LOM_ERROR(handle, catScanId);

    while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL))!= EOS)
    {
        if (e < 0) LOM_ERROR(handle, e);

        clist[0].colNo = LOM_INHERITANCE_CLASSID_COLNO;
        clist[0].start = ALL_VALUE;
        clist[0].dataLength = LOM_LONG_SIZE_VAR;
        clist[0].length = LOM_LONG_SIZE_VAR;

        e =  LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, &tid, 1, &clist[0]);
        if (e < 0) LOM_ERROR(handle, e);

        /* if index is between start index and end index, then add to array */
        if(startIndex <= *index && *index <= endIndex)
        {
			subClasses[*index - startIndex] = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(subClasses[*index - startIndex]));
            (*index) ++;
        }

        e = lom_TraverseSubclassTree(handle, volId, orn, clist[0].data.l, index,
                                     startIndex, endIndex, subClasses);
        if(e < 0) LOM_ERROR(handle, e);
    }

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
    if (e < 0) LOM_ERROR(handle, e);

        
    return eNOERROR;
}


Four lom_AdjustAttrNum(
    LOM_Handle* handle,    
    Four        volId,                  /* mount volume index */
    Four        classId,                /* IN  : class */
    Two         attrNum,                /* IN  : class's attr num */
    Four        subClassId,             /* IN  : sub class id */
    Two*       subClassAttrNum)        /* OUT : adjusted subclass's attr num */
{
    Four e;
    Four mv;
    Four attrInfo;
    Four subClassAttrInfo;

    /* check from attribute type */
    e = Catalog_GetMountTableInfo(handle, volId, &mv);
    if(e < 0) LOM_ERROR(handle, e);
    
    e = Catalog_GetAttrInfo(handle, volId, classId, attrNum, &attrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    e = Catalog_GetAttrInfoByName(handle, volId, subClassId, 
                                  CATALOG_GET_ATTRNAME(&(CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo])), 
                                  &subClassAttrInfo);
    if(e < 0) LOM_ERROR(handle, e);

    *subClassAttrNum = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(handle, mv)[subClassAttrInfo]));

    return eNOERROR;
}



