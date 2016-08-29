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
/******************************************************************************/
/*                                                                            */
/*    This module has been implemented based on "The Multilevel Grid File     */
/*    (MLGF) Version 4.0," which can be downloaded at                         */
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".                */
/*                                                                            */
/******************************************************************************/

/*
 * Module: MLGF_CreateIndex.c
 *
 * Description:
 *  Create an MLGF index.
 *
 * Exports:
 *  Four MLGF_CreateIndex(ObjectID*, MLGF_KeyDesc*, IndexID*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "OM_Internal.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four MLGF_CreateIndex(
    Four handle,
    ObjectID *catObjForFile,	/* IN catalog object of index file */
    MLGF_KeyDesc *kdesc,	/* IN key descriptor of the new MLGF index */
    PageID *rootPid)		/* OUT root page of the newly created MLGF */
{
    Four e;			/* error code */
    Boolean isTmp;
    SlottedPage *catPage;	/* buffer page containing the catalog object */
    sm_CatOverlayForBtree *catEntry; /* pointer to Btree file catalog information */
    PhysicalFileID pFid;	/* physical file ID */
    mlgf_DirectoryPage *apage;  /* root page of the created index */


    TR_PRINT(TR_MLGF, TR1, ("MLGF_CreateIndex(handle)"));


    /* check parameters */
    if (catObjForFile == NULL || kdesc == NULL || rootPid == NULL) ERR(handle, eBADPARAMETER); 


    /* Get the index file's FileID from the catalog object */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    GET_PTR_TO_CATENTRY_FOR_INDEX(catObjForFile, catPage, catEntry);
    
    MAKE_PHYSICALFILEID(pFid, catEntry->fid.volNo, catEntry->firstPage); 
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);


    /* Allocate a new btree page for the root of a btree. */
    e = mlgf_AllocPage(handle, catObjForFile, (PageID *)&pFid, rootPid); 
    if (e < 0)  ERR(handle, e);

    /* check this MLGF is temporary */
    e = mlgf_IsTemporary(handle, catObjForFile, &isTmp);
    if (e < 0)  ERR(handle, e);

    
    /*
    ** Construct a root page.
    */

    /* Read the root page into the buffer. */
    e = BfM_GetNewTrain(handle, rootPid, (char**)&apage, PAGE_BUF); 
    if (e < 0) ERR(handle, e);

    MLGF_INIT_DIRECTORY_PAGE(apage, isTmp, *rootPid, 1, TRUE, kdesc->nKeys); 
    
    e = BfM_SetDirty(handle, rootPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    e = BfM_FreeTrain(handle, rootPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* MLGF_CreateIndex() */
