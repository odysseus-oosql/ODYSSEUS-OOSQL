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
/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System --    */
/*    Coarse-Granule Locking (Volume Lock) Version                            */
/*    Version 3.0                                                             */
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
 * Module: SM_GetKeyOIDSortStream.c
 *
 * Description:
 *  Get key,oid sort stream for index bulk load.
 *
 * Exports:
 *  Four SM_GetKeyOIDSortStream(ObjectID*, Two, SortKeyDesc*, omGetKeyAttrsFuncPtr_T, void*, Four*)
 *
 */

#include <string.h>

#include "common.h"
#include "param.h"
#include "bl_param.h"
#include "RDsM_Internal.h"	/* change RDsM.h to RDsM_Internal.h */
#include "BfM.h"           
#include "OM_Internal.h"
#include "BL_OM_Internal.h"
#include "SM_Internal.h"
#include "Util_Sort.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@========================================
 *  SM_GetKeyOIDSortStream()
 * =======================================*/

/*
 * Function : Four SM_GetKeyOIDSortStream()
 *
 * Description :
 *  Get key,oid sort stream for index bulk load.
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 *  0)
 *
 */

Four _SM_GetKeyOIDSortStream(
    Four handle,
    VolID       tmpVolId,                  /* IN  temporary volume in which sort stream is created */ 
    FileID      *fid,                      /* IN  file that get key oid sort stream */
    Two         numIndex,                  /* IN  number of index */
    SortKeyDesc *kdesc,                    /* IN  sort key description */
    GetKeyAttrsFuncPtr_T getKeyAttrs,      /* IN  object analysis function */	
    void        *schema,                   /* IN  schema for analysis function */
    PageID      *firstPageID,              /* IN  first page ID */
    Four        *keyOIDSortStreamID)       /* OUT key-oid sort stream ID */
{

    Four        e;                         /* error number */
    Four        v;                         /* array index on scan manager mount table */
    ObjectID    catObjForDataFile;         /* catalog object id for data file bulk load */


    /* Check parameters */
    if (fid == NULL) 
        ERR(handle, eBADPARAMETER_SM);
    if (kdesc == NULL || getKeyAttrs == NULL ) 
        ERR(handle, eBADPARAMETER_SM); 

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == fid->volNo) break; 

    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    e = sm_GetCatalogEntryFromDataFileId(handle, v, fid, &catObjForDataFile);
    if(e < 0) ERR(handle, e);

    /* call OM level get key,oid function */
    e = OM_GetKeyOIDSortStream(handle, tmpVolId, &catObjForDataFile, numIndex, kdesc,(omGetKeyAttrsFuncPtr_T)getKeyAttrs, schema, firstPageID, keyOIDSortStreamID);
    if(e < 0) ERR(handle, e);

    return(eNOERROR);

} /* SM_GetKeyOIDSortStream() */
