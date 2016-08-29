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
 * Module: BtM_InsertObject.c
 *
 * Description :
 *  Insert an ObjectID 'oid' into a Btree whose key value is 'kval'. 
 *
 * Exports:
 *  Four BtM_InsertObject(ObjectID*, PageID*, KeyDesc*, KeyValue*, ObjectID*, Pool*, DeallocListElem*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * BtM_InsertObject() 
 *================================*/
/*
 * Function: Four BtM_InsertObject(ObjectID*, PageID*, KeyDesc*, KeyValue*, ObjectID*, Pool*, DeallocListElem*)
 * 
 * Description :
 *  Insert an ObjectID 'oid' into a Btree whose key value is 'kval'. 
 *  If an overflow page is created as the result of the insert, it may occur
 *  merging or redistibuting two leaves and this may affect the root.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_BTM
 *    some errors caused by function calls
 */
Four BtM_InsertObject(
    Four handle,
    ObjectID *catObjForFile,	/* IN catalog object of B+ tree file */
    PageID   *root,				/* IN the root of Btree */
    KeyDesc  *kdesc,			/* IN key descriptor */
    KeyValue *kval,				/* IN key value */
    ObjectID *oid,				/* IN ObjectID which will be inserted */
    Pool     *dlPool,			/* INOUT pool of dealloc list */
    DeallocListElem *dlHead)	/* INOUT head of the dealloc list */
{
    Four 					e;			/* error number */
    Boolean 				lh;			/* for spliting */
    Boolean 				lf;			/* for merging */
    InternalItem 			item;		/* Internal Item */
    SlottedPage 			*catPage;	/* buffer page containing the catalog object */
    sm_CatOverlayForBtree 	*catEntry; 	/* pointer to Btree file catalog information */
    PhysicalFileID 			pFid;	 	/* B+-tree file's FileID */ 

    
    TR_PRINT(TR_BTM, TR1,
             ("BtM_InsertObject(handle, catObjForFile=%P, root=%P, kdesc=%P, kval=%P, oid=%P, dlPool=%P, dlHead=%P)",
	      catObjForFile, root, kdesc, kval, oid, dlPool, dlHead));

    /*@ check parameters */
    
    if (catObjForFile == NULL) ERR(handle, eBADPARAMETER_BTM);
    
    if (root == NULL) ERR(handle, eBADPARAMETER_BTM);

    if (kdesc == NULL) ERR(handle, eBADPARAMETER_BTM);

    if (kval == NULL) ERR(handle, eBADPARAMETER_BTM);

    if (oid == NULL) ERR(handle, eBADPARAMETER_BTM);    

    
    /* Get the B+ tree file's FileID from the catalog object */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    GET_PTR_TO_CATENTRY_FOR_BTREE(catObjForFile, catPage, catEntry);
    
    MAKE_PHYSICALFILEID(pFid, catEntry->fid.volNo, catEntry->firstPage); 
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /*@ insert the object */
    e = btm_Insert(handle, catObjForFile, root, kdesc, kval, oid, &lf, &lh, &item, dlPool, dlHead);
    if (e < 0) ERR(handle, e);
    
    if (lh) {	/* the root was splitted */
	e = btm_root_insert(handle, catObjForFile, root, &item);
	if (e < 0) ERR(handle, e);
	
    } else if (lf) {  /* the root was merged */
	e = btm_root_delete(handle, &pFid, root, dlPool, dlHead);
	if (e < 0) ERR(handle, e);
    }
    
    return(eNOERROR);
    
}   /* BtM_InsertObject() */
