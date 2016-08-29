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
 * Module: LOM_Relationship_GetId()
 *
 * Description:
 *
 * Imports:
 *
 * Exports:
 *
 * Returns:
 *  Error code
 *     eBADPARAMETER_LRDS
 *     eCOLUMNVALUEEXPECTED_LRDS
 *     eWRONGCOLUMNVALUE_LRDS
 *     some errors caused by function calls
 *
 * Assumption:
 */

#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog_Internal.h"
#include "Catalog.h"
#include <string.h>	

Four LOM_Relationship_GetId(
    LOM_Handle* handle,             /* IN LOM system handle */
    Four        volId,              /* IN volumn where the relationship was placed */
    char*       relationshipName,   /* IN relationship name */
    Four*       relationshipId)     /* OUT relationship Id */
{
    LockParameter lockup;   /* IN lock mode & duration */
    Four catScanId;         /* catalog scan Id */
    TupleID tid;            /* tuple id */
    ColListStruct clist[1]; /* column struct */
    Four orn;               /* open relationship nuber */
    Four e;                 /* error code */
    BoundCond bound;
    Two keyLen;             /* should be Two to store key length */
    lrds_RelTableEntry *relTableEntry; 

    /* check parameters */

    if (relationshipName == NULL) LOM_ERROR(handle, eBADPARAMETER);
    
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSRELATIONSHIP_CLASSNAME);
    if( orn < 0 ) LOM_ERROR(handle, orn);

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
    catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn,
                    &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[2].iid),
                    &bound,&bound,0,(BoolExp*)NULL,&lockup);
    if (catScanId < 0) LOM_ERROR(handle, catScanId);

    e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL);
    if (e == EOS) LOM_ERROR(handle, eNOSUCHRELATIONSHIP_LOM);
    if (e < 0) LOM_ERROR(handle, e);

    clist[0].colNo = LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO;
    clist[0].start = ALL_VALUE;

    e =  LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, &tid, 1 , &clist[0]);
    if (e < 0) LOM_ERROR(handle, e);

    e =  LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
    if (e < 0) LOM_ERROR(handle, e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if (e < 0) LOM_ERROR(handle, e);

    /* here we need codes to check if relationship id is correct */
    *relationshipId = (Four)GET_VALUE_FROM_COL_LIST(clist[0], sizeof(*relationshipId));

    return eNOERROR;
}

