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
 * Module: OM_GetObjectHdr
 *
 * Description:
 *  Return the object header of the given object.
 *  Object header consists of an object's properties and  data length.
 *
 * Exports:
 *  Four OM_GetObjectHdr(Four, DataFileInfo*, ObjectID *, ObjectHdr *, LockParameter*)
 *
 * Return value:
 *  Error code
 *    eBADOBJECTID_OM
 *    eLOCKREQUESTFAIL_OM
 *    some errors caused by function calls
 *
 * Side effect:
 *  None
 */

#include "common.h"
#include "error.h"
#include "trace.h"
#include "BfM.h"
#include "OM.h"
#include "LM.h"
#include "TM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four OM_GetObjectHdr(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    ObjectID *oid,		/* IN object whose header to get */
    ObjectHdr *objHdr,		/* OUT object header to return */
    LockParameter *lockup)
{
    Four e;			/* error number */
    PageID pid;                 /* page identifier */
    SlottedPage *apage;		/* pointer to buffer holding the slotted page */
    Buffer_ACC_CB *aPage_BCBP;	/* buffer access control block containing data */
    Object *obj;		/* pointer to the object in the slotted page */
    LockReply lockReply;	/* lock reply */
    LockMode oldMode;


    TR_PRINT(handle, TR_OM, TR1, ("OM_GetObjectHdr(finfo=%P, oid=%P, objHdr=%P, lockup=%P)",
			  finfo, oid, objHdr, lockup));


    /* get pid from oid */
    pid = *((PageID *)oid);

#ifdef CCPL
    if(lockup){
	/* lock the page */
	e = LM_getPageLock(handle, &xactEntry->xactId, &pid, &finfo->fid,
			   lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK){
	    ERR(handle, eDEADLOCK);	    /* deadlock */
	}
    }
#endif /* CCPL */

#ifdef CCPL
    e = BfM_getAndFixBuffer(handle, (TrainID *)oid, M_FREE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
    e = BfM_getAndFixBuffer(handle, (TrainID *)oid, M_SHARED, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

    if(!IS_VALID_OBJECTID(oid, apage)) {
#ifdef CCPL
	ERRB1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	ERRBL1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

#ifdef CCRL
    if (lockup) {

	e = LM_getObjectLock(handle, &xactEntry->xactId, oid, &apage->header.fid,
			     lockup->mode, lockup->duration, L_CONDITIONAL, &lockReply, &oldMode);
	if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

	if ( lockReply == LR_NOTOK ) {
	    /* release latch to avoid deadlock situation */
	    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
	    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

	    e = LM_getObjectLock(handle, &xactEntry->xactId, oid, &apage->header.fid,
				 lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	    if ( e < eNOERROR ) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

	    if ( lockReply == LR_DEADLOCK ) ERRB1(handle, eDEADLOCK, aPage_BCBP, PAGE_BUF);

	    /* relatch the page */
	    e = SHM_getLatch(handle, aPage_BCBP->latchPtr, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
	    if ( e < eNOERROR ) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

	    /* re-validation the objectID */
	    if(!IS_VALID_OBJECTID(oid, apage)) ERRBL1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
	}
    }
#endif /* CCRL */

    obj = (Object *)&(apage->data[apage->slot[-(oid->slotNo)].offset]);

    *objHdr = obj->header;

#ifdef CCRL
    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* OM_GetObjectHdr() */
