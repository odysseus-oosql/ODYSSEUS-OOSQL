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
/*    Fine-Granule Locking Version                                            */
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
 *  Four BtM_InsertObject(Four, PageID*, LATCH_TYPE*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER_BTM
 *    some errors caused by function calls
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four BtM_InsertObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo *iinfo,      /* IN B tree index information */ 
    FileID     *fid,            /* IN FileID */ 
    KeyDesc  *kdesc,		/* IN key descriptor */
    KeyValue *kval,		/* IN key value */
    ObjectID *oid,		/* IN ObjectID which will be inserted */
    LockParameter *lockup,	/* IN request lock or not */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    btm_TraversePath path;	/* Btree traverse path stack */
    LATCH_TYPE *treeLatchPtr;
    PageID root;


    TR_PRINT(handle, TR_BTM, TR1,
	     ("BtM_InsertObject(handle, iinfo=%P, kdesc=%P, kval=%P, oid=%P, lockup=%P)",
	      iinfo, kdesc, kval, oid, lockup));


    /* check parameters */

    if (iinfo == NULL) ERR(handle, eBADPARAMETER);

    if (kdesc == NULL) ERR(handle, eBADPARAMETER);

    if (kval == NULL) ERR(handle, eBADPARAMETER);

    if (oid == NULL) ERR(handle, eBADPARAMETER);


    /* Get root's page ID of the current index */
    e = btm_GetRootPid(handle, xactEntry, iinfo, &root, lockup);
    if (e < eNOERROR) ERR(handle, e);

    /* Get TreeLatchPtr of the current index */
    e = BtM_GetTreeLatchPtrFromIndexId(handle, &iinfo->iid, &treeLatchPtr); 
    if (e < eNOERROR) ERR(handle, e);

    /* Initialize the traverse path stack. */
    btm_InitPath(handle, &path, treeLatchPtr);

    for (;;) {

	/* Get the traverse path. */
	e = btm_Search(handle, &iinfo->iid, &root, kdesc, kval, BTM_INSERT, 0, &path); 
	if (e < eNOERROR) ERRPATH(handle, e, &path);

	/* Insert the given ObjectID into the Btree if there is enough space */
	/* in the leaf page where the ObjectID is to be placed. */
	e = btm_InsertLeaf(handle, xactEntry, iinfo, fid, &path, kdesc, kval, oid, lockup, logParam); 
	if (e < eNOERROR) ERRPATH(handle, e, &path);

	/* The ObjectID was inserted successfully.*/
	if (e == eNOERROR) break;

	if (e == BTM_NOSPACE) {
	    /* btm_Insert() have failed since there was no enough space. */
	    /* Split the page on the top of 'path'. */
	    e = btm_Split(handle, xactEntry, &path, iinfo, &root, kdesc, kval, logParam); 
	    if (e < eNOERROR) ERRPATH(handle, e, &path);
	}
    }

    /* Finalize the traverse path stack. */
    e = btm_FinalPath(handle, &path);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

}   /* BtM_InsertObject() */
