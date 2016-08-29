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
 *  Four LOM_Relationship_Destroy( Four ,char *)
 *
 * Returns:
 *  Error code
 */


#include <string.h>	
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog_Internal.h"
#include "Catalog.h"


Four LOM_Relationship_Destroy(
    LOM_Handle* handle,             /* IN LOM system handle */
    Four volId,
    char *relationshipName)
{
    Two  keyLen;            /* should be Two to store key length */
    Four e;                 /* error number */
    Four i;                 /* index variable */
    Four v;                 /* index on LRDS mount table */
    Four catScanId;         /* scan id for a catalog table access */
    Boolean found;          /* TRUE if the relation is already defined */
    BoundCond bound;        /* boundary condition */
    LockParameter lockup;
    Four orn;               /* open relation number */
    TupleID tid;
    Four nFromSubClasses;
    Four iFromSubClass;
    Four nToSubClasses;
    Four iToSubClass;
    Four fromSubClassIdsBuf[100];
    Four toSubClassIdsBuf[100];
    ColListStruct clist[2];
    Four fromClassId, toClassId;
    lrds_RelTableEntry *relTableEntry; 
    
    /* check parameters */
    if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
    
    for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
        if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) break;
    
    if (v == CATALOG_MAXNUMOFVOLS) LOM_ERROR(handle, eVOLUMENOTMOUNTED_LOM);
    
    if (relationshipName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
    
    /*
     * update SYSRELATIONSHIP table
     */
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSRELATIONSHIP_CLASSNAME);
    if(orn < 0) LOM_ERROR(handle, orn);
    
    bound.op = SM_EQ;
    keyLen = strlen(relationshipName);
    bound.key.len = sizeof(Two) + keyLen;
    memcpy(&(bound.key.val[0]), &keyLen, sizeof(Two));
    memcpy(&(bound.key.val[sizeof(Two)]), relationshipName, keyLen);
    
    /* set lock up parameters */
    /* just for reading */
    lockup.mode = L_IX;
    lockup.duration = L_COMMIT;
    
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);
    catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[2].iid),
                                   &bound, &bound, 0, (BoolExp*)NULL, &lockup);
    if (catScanId < 0) LOM_ERROR(handle, catScanId);
    
    /* Initialize a flag meaning the existence of data file search. */
    found = SM_FALSE;

    clist[0].colNo = LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO;
    clist[0].start = ALL_VALUE;
    clist[0].dataLength = LOM_LONG_SIZE_VAR;

    clist[1].colNo = LOM_SYSRELATIONSHIP_TOCLASSID_COLNO;
    clist[1].start = ALL_VALUE;
    clist[1].dataLength = LOM_LONG_SIZE_VAR;

    fromClassId = -1; toClassId = -1;
    while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL)) != EOS)
    {
        if (e < 0) LOM_ERROR(handle, e);

        e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, &tid, 2, clist);
        if(e < 0) LOM_ERROR(handle, e);

        /* determine fromClassId, toClassId */
        if(fromClassId == -1)
        {
            fromClassId = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(fromClassId));
            toClassId = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(toClassId));
        }
        else
        {
            if(fromClassId > GET_VALUE_FROM_COL_LIST(clist[0], sizeof(fromClassId)))
            {
                fromClassId = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(fromClassId));
                toClassId = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(toClassId));
            }
        }
        
        e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, &tid);
        if(e < 0) LOM_ERROR(handle, e);
        
        found = SM_TRUE;
    }
    
    if (!found) LOM_ERROR(handle, eRELATIONNOTFOUND_LOM);
    
    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
    if (e < 0) LOM_ERROR(handle, e);
    
    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if(e < 0) LOM_ERROR(handle, e);

    e = Catalog_Relationship_DestroyRelationship(handle, volId, relationshipName);
    if(e < 0) LOM_ERROR(handle, e);

    return eNOERROR;
}
