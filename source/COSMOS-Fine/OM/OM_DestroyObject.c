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
 * Module : OM_DestroyObject.c
 *
 * Description :
 *  OM_DestroyObject() destroys the specified object. The specified object
 *  will be removed from the slotted page. The freed space is not merged
 *  to make the contiguous space; it is done when it is needed.
 *  If the destroyed object is the only object in the page, then deallocate
 *  the page.
 *
 * Exports:
 *  Four OM_DestroyObject(Four, DataFileInfo*, ObjectID*, LocalPool*, DeallocListElem*, LockParameter*)
 *
 * Return Values :
 *  Error Code
 *    eBADFILEID_OM
 *    some errors caused by function calls
 *
 * Side Effects :
 *
 */

#include "common.h"
#include "error.h"
#include "trace.h"		/* for tracing : TR_PRINT(handle, ) macro */
#include "Util.h"		/* to get Pool */
#include "TM.h"
#include "LM.h"
#include "LOG.h"
#include "LOT.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four OM_DestroyObject(
    Four 	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    ObjectID *oid,		/* IN object to destroy */
    LockParameter *lockup,      /* IN request lock or not */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four        e;		/* error number */
    Four        i;		/* temporary variable */
    PageID	pid;		/* page on which the object resides */
    SlottedPage *apage;		/* pointer to the buffer holding the page */
    Buffer_ACC_CB *aPage_BCBP;	/* buffer access control block containing data */
    Four        offset;		/* start offset of object in data area */
    Object      *obj;		/* points to the object in data area */
    Four        alignedLen;	/* aligned length of object data area */
    Boolean     last;		/* indicates the object is the last one */
    LockReply lockReply;	/* lock reply */
    LockMode oldMode;
    Lsn_T lsn;		/* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_OM_ObjectInPage_T objInfo; /* specify an object in a slotted page */
    Boolean pageUpdateFlag;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_OM, TR1, ("OM_DestroyObject()"));


    /* Check parameters. */
    if (finfo == NULL) ERR(handle, eBADCATOBJ);

    if (oid == NULL) ERR(handle, eBADOBJECTID_OM);


    /* get the PageID of page on which the object resides */
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

#ifdef CCRL
    /* Request lock as soon as possible */
    if (lockup) {
	e = LM_getObjectLock(handle, &xactEntry->xactId, oid, &finfo->fid,
			     lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	if ( e < eNOERROR ) ERR(handle, e);

	if ( lockReply == LR_DEADLOCK ) ERR(handle, eDEADLOCK);
    }
#endif /* CCRL */


    /* Read the slotted page into the buffer */
#ifdef CCPL
    e = BfM_getAndFixBuffer(handle, &pid, M_FREE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
    e = BfM_getAndFixBuffer(handle, &pid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

    /* check the given file contains the 'oid' object */
    if (!EQUAL_FILEID(finfo->fid, apage->header.fid)) {
#ifdef CCPL
	ERRB1(handle, eBADFILEID, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	ERRBL1(handle, eBADFILEID, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    /* check that 'oid' is valid */
    if (!IS_VALID_OBJECTID(oid, apage)) {
#ifdef CCPL
	ERRB1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	ERRBL1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    /* obj points to the object */
    offset = apage->slot[-(oid->slotNo)].offset;
    obj = (Object *)&(apage->data[offset]);


    if (obj->header.properties & P_LRGOBJ) {
	/* This is the large object. */

        alignedLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(LOT_GetSize(handle, obj->data, IS_LRGOBJ_ROOTWITHHDR(obj->header.properties))));
	e = LOT_DestroyObject(handle, xactEntry, finfo, obj->data, IS_LRGOBJ_ROOTWITHHDR(obj->header.properties), logParam);
	if (e < 0) {
#ifdef CCPL
            ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
            ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
        }

    } else {			/* small object */

	if (obj->header.properties & P_MOVED) {
	    /* This object is the moved object */
	    /* recursively call the OM_DestroyObject */

#ifdef CCPL
	    e = OM_DestroyObject(handle, xactEntry, finfo, (ObjectID *)(obj->data), lockup, logParam);
	    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
            /* lockup == NULL :: no lock on forwarded object */
	    e = OM_DestroyObject(handle, xactEntry, finfo, (ObjectID *)(obj->data), NULL, logParam);
	    if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

	    /* the length of data area */
	    alignedLen = sizeof(ObjectID);

	} else {
	    /* Normal Object */

	    /* the length of data area */
	    alignedLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(obj->header.length));
	}
    }

#ifdef CCRL
    e = om_ReleaseSpace(handle, &xactEntry->xactId, apage, sizeof(ObjectHdr)+alignedLen, &pageUpdateFlag);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    /* free the space alocated */
    if (offset+sizeof(ObjectHdr)+alignedLen == apage->header.free) {
        /* this is the last object in the page. */
        apage->header.free -= sizeof(ObjectHdr) + alignedLen; 

    } else {
        apage->header.unused += sizeof(ObjectHdr) + alignedLen; 
    }

    /* set the slot to EMPTYSLOT */
    apage->slot[-(oid->slotNo)].offset = EMPTYSLOT;


    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
        Boolean deleteSlotFlag = FALSE;

        objInfo.slotNo = oid->slotNo;
        objInfo.unique = oid->unique;

        LOG_FILL_LOGRECINFO_3(logRecInfo,  xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_OM_DESTROY_SMALL_OBJECT, LOG_REDO_UNDO,
                              pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_OM_ObjectInPage_T), &objInfo,
                              sizeof(Boolean), &deleteSlotFlag,
                              sizeof(ObjectHdr) + alignedLen, obj);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) {
#ifdef CCPL
            ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
            ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
        }

        /* mark the lsn in the page */
        apage->header.lsn = lsn;
        apage->header.logRecLen = logRecLen;
    }

    aPage_BCBP->dirtyFlag = 1;

#ifdef CCPL
    if ((apage->header.free == apage->header.unused)
	&& !EQUAL_PAGEID(pid, finfo->fid)) {
	/* There is no more tuple in this page. Deallocate it. */

	/* Remove this page from the file. */
	e = om_DeletePageFromFile(handle, xactEntry, finfo, apage, lockup, logParam);
	if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

	/*
	 * Insert the deallocated page into the deallocated page list.
	 */
        e = TM_XT_AddToDeallocPageList(handle, xactEntry, &pid);
	if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
    }
#endif /* CCPL */

#ifdef CCRL
    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* OM_DestroyObject() */
