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
 * Module : OM_WriteObjectRedoOnly.c
 *
 * Description :
 *  OM_WriteObjectRedoOnly() causes the region of bytes from 'start' to
 *  'start'+'length' to be overwritten with the data pointed to by
 *  the parameter 'data'. If 'length' is REMAINDER, the data from 'start'
 *  to end of the object are overwritten.
 *  This routine returns the number of bytes actually overwritten.
 *
 * Exports:
 *  Four OM_WriteObjectRedoOnly(Four, DataFileInfo*, ObjectID*, Four, Four, char*, LockParameter*)
 *
 * Return value:
 *  1) number of bytes actually overwritten (value greater than or equal to 0)
 *  2) Error Code (negative value)
 *    eBADOBJECTID_OM
 *    eBADLENGTH_OM
 *    eBADUSERBUF_OM
 *    eBADSTART_OM
 *    eLOCKREQUESTFAIL_OM
 */

#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"		/* for tracing : TR_PRINT(handle, ) macro */
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "LOG.h"
#include "BfM.h"		/* for the buffer manager call */
#include "LOT.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four OM_WriteObjectRedoOnly(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    ObjectID *oid,		/* IN object to read */
    Four     start,		/* IN starting offset of write */
    Four     length,		/* IN amount of data to write */
    char     *buf,		/* IN user buffer holding data to write */
    LockParameter *lockup,	/* IN request lock or not */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four	e;		/* error code */
    PageID      pid;		/* page containing object specified by 'oid' */
    SlottedPage	*apage;		/* pointer to the buffer of the page  */
    Buffer_ACC_CB *aPage_BCBP;  /* buffer access control block holding data */
    Object	*obj;		/* pointer to the object in the slotted page */
    LockReply   lockReply;      /* lock reply */
    LockMode oldMode;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_OM_ObjDataInPage_T objDataInfo;	/* write position information */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_OM, TR1,
	     ("OM_WriteObjectRedoOnly(handle, finfo=%P, oid=%P, start=%ld, length=%ld, buf=%P, lockup=%P)",
	      finfo, oid, start, length, buf, lockup));


    /* check parameters */

    if (finfo == NULL) ERR(handle, eBADCATOBJ);

    if (oid == NULL) ERR(handle, eBADOBJECTID_OM);

    if (length < 0 && length != REMAINDER) ERR(handle, eBADLENGTH_OM);

    if (buf == NULL) ERR(handle, eBADUSERBUF_OM);


    /* Get the PageID from the ObjectID */
    MAKE_PAGEID(pid, oid->volNo, oid->pageNo);

#ifdef CCPL
    if(lockup){
	/* lock the slotted page */
	e = LM_getPageLock(handle, &xactEntry->xactId, &pid, &finfo->fid,
			   lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK){
	    ERR(handle, eDEADLOCK);	    /* deadlock */
	}
    }
#endif /* CCPL */

    /* read the slotted page into the system buffer */
#ifdef CCPL
    e = BfM_getAndFixBuffer(handle, &pid, M_FREE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
    e = BfM_getAndFixBuffer(handle, &pid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

    /* check the 'oid' is valid. */
    if (!IS_VALID_OBJECTID(oid, apage)) {
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
	    if ( e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

	    e = LM_getObjectLock(handle, &xactEntry->xactId, oid, &apage->header.fid,
				 lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

	    if ( lockReply == LR_DEADLOCK ) ERRB1(handle, eDEADLOCK, aPage_BCBP, PAGE_BUF);

	    /* relatch the page */
	    e = SHM_getLatch(handle, aPage_BCBP->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
	    if ( e < eNOERROR ) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

	    /* check the 'oid' is valid. */
	    if (!IS_VALID_OBJECTID(oid, apage)) ERRBL1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
	}
    }
#endif /* CCRL */

    /* Get the object from the 'oid' */
    obj = (Object *)&apage->data[apage->slot[-(oid->slotNo)].offset];

    if (start < 0 || start >= obj->header.length) {
#ifdef CCPL
	ERRB1(handle, eBADSTART_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	ERRBL1(handle, eBADSTART_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    /* Read all the remained data if the 'length' is REMAINDER. */
    if (length == REMAINDER) length = obj->header.length - start;

    /* if 'length' exceeds object boundary, reduce 'length' upto boundary */
    if (start + length > obj->header.length)
	length = obj->header.length - start;

    TR_PRINT(handle, TR_OM, TR2, ("start=%ld, length=%ld\n", start, length));


    /*
     * Copy the data into the user specified buffer.
     */
    if (obj->header.properties & P_LRGOBJ) {
        /* this operation supports only small object */

#ifdef CCPL
        ERRB1(handle, eBADPARAMETER, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
        ERRBL1(handle, eBADPARAMETER, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

    } else if (obj->header.properties & P_MOVED) {
	/* This is the moved object : recursively call OM_WriteObjectRedoOnly() */
	TR_PRINT(handle, TR_OM, TR2, ("This is the moved object\n"));

#ifdef CCPL
	e = OM_WriteObjectRedoOnly(handle, xactEntry, finfo, (ObjectID *)obj->data, start, length, buf, lockup, logParam);
	if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	/* lockup == NULL :: no lock on forwarded object */
	e = OM_WriteObjectRedoOnly(handle, xactEntry, finfo, (ObjectID *)obj->data, start, length, buf, lockup, logParam);
	if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

    } else {
	/* the unmoved object : normal case */
	TR_PRINT(handle, TR_OM, TR2, ("This is the normal small object.\n"));

	/*
	 * Write log record.
	 */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

	    objDataInfo.slotNo = oid->slotNo;
	    objDataInfo.start = start;
	    objDataInfo.length = length;

            LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_OM_WRITE_SMALL_OBJECT, LOG_REDO_ONLY,
                                  pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_OM_ObjDataInPage_T), &objDataInfo,
                                  length, buf,
                                  length, &(obj->data[start]));

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

	memcpy(&(obj->data[start]), buf, length);

	/* set dirty flag */
	aPage_BCBP->dirtyFlag = 1;
    }

#ifdef CCRL
    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

    /* unfix the buffer page */
    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(length);

} /* OM_WriteObjectRedoOnly( ) */
