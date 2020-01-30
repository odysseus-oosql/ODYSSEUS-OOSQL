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
 * Module: om_ConverToLarge.c
 *
 * Description:
 *  om_ConvertToLarge( ) converts the given small object into the large
 *  object. This function is called when the small object becomes a large
 *  object by inserting or appending some data.
 *
 * Exports:
 *  Four om_ConvertToLarge(Four, DataFileInfo*, SlottedPage*, Four)
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "LOG.h"
#include "BfM.h"
#include "LOT.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * om_ConvertToLarge( )
 *================================*/
/*
 * Function: Four om_ConvertToLarge(Four, DataFileInfo*, SlottedPage*, Four)
 *
 * Description:
 *  om_ConvertToLarge( ) converts the given small object into the large
 *  object. This function is called when the small object becomes a large
 *  object by inserting or appending some data.
 *
 * Returns:
 *  Error codes
 *    eBADOBJECTID_OM
 *    some errors caused by function calls
 */
Four om_ConvertToLarge(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    SlottedPage *apage,		/* IN pointer to buffer holding the slotted page */
    Four        slotNo,		/* IN slot no of object in slotted page */
    LockParameter *lockup,      /* IN request lock or not */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    ObjectID movedOID;		/* ObjectID of the forwarded object */
    SlottedPage *fpage;		/* pointer to buffer holding page with forwarded object */
    Buffer_ACC_CB *fpage_BCBP;
    Four offset;		/* starting offset of object in slotted page */
    Object *origObj;		/* pointer to object in slotted page (not moved) */
    Object *obj;		/* pointer to object in slotted page */
    Four alignedLen;		/* aligned length of the original object */
    Four newLength;             /* aligned length of large object root node */
    L_O_T_INode anode;		/* buffer holding the root node */
    Boolean rootWithHdr_Flag;   /* TRUE if large object root node is with header */
    Four objLength;
    char tmpData[PAGESIZE];
    LockReply lockReply;	/* lock reply */
    LockMode oldMode;
    Two newProperties;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_OM_ChangeObject_T changeObjInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1, ("om_ConvertToLarge()"));


    /* obj points to the old small object */
    offset = apage->slot[-slotNo].offset;
    origObj = (Object *)&(apage->data[offset]);

    objLength = origObj->header.length;

    if (origObj->header.properties & P_MOVED) {
	movedOID = *((ObjectID *) origObj->data);

#ifdef CCPL
        if (lockup) {
            /* lock the page */
            e = LM_getPageLock(handle, &xactEntry->xactId, (PageID*)&movedOID, &finfo->fid,
                               lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
            if (e < eNOERROR) ERR(handle, e);

            if (lockReply == LR_DEADLOCK){
                ERR(handle, eDEADLOCK);	    /* deadlock */
            }
        }
#endif /* CCPL */

#ifdef CCPL
	e = BfM_getAndFixBuffer(handle, (PageID *)&movedOID, M_FREE, &fpage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	e = BfM_getAndFixBuffer(handle, (PageID *)&movedOID, M_SHARED, &fpage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	fpage = (SlottedPage *)fpage_BCBP->bufPagePtr;

	assert(IS_VALID_OBJECTID(&movedOID, fpage));

	offset = fpage->slot[-movedOID.slotNo].offset;
	obj = (Object *)&(fpage->data[offset]);

	/*@ copy data into the temporary buffer */
	memcpy(tmpData, obj->data, objLength);

#ifdef CCRL
        e = SHM_releaseLatch(handle, fpage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, fpage_BCBP, PAGE_BUF);
#endif /* CCRL */

	e = BfM_unfixBuffer(handle, fpage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	/*
	 * The slotted page must have been locked with x-mode.
	 * So, the lockup parameter is 'NULL'.
	 */
	e = OM_DestroyObject(handle, xactEntry, finfo, &movedOID, NULL, logParam);
	if (e < eNOERROR) ERR(handle, e);

        alignedLen = sizeof(ObjectID);

    } else {
	obj = origObj; 

	/*@ copy data into the temporary buffer */
	memcpy(tmpData, obj->data, objLength);

        alignedLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(objLength));
    }

    e = LOT_CreateObject(handle, xactEntry, finfo, &apage->header.pid, (char*)&anode, &rootWithHdr_Flag,
                         SP_FREE(xactEntry->xactId, apage)+alignedLen, objLength, tmpData, logParam); 
    if (e < eNOERROR) ERR(handle, e);

    newLength = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(LOT_GetSize(handle, (char*)&anode, rootWithHdr_Flag)));
    newProperties = P_LRGOBJ | ((rootWithHdr_Flag) ? P_LRGOBJ_ROOTWITHHDR:0);

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        changeObjInfo.p.type = CHANGE_OBJECT_TYPE_PROPERTIES;
        changeObjInfo.p.slotNo = slotNo;
        changeObjInfo.p.deltaOfDataAreaSize = newLength - alignedLen;
        changeObjInfo.p.propertiesXor = newProperties ^ origObj->header.properties; 

        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_OM_CHANGE_OBJECT, LOG_REDO_UNDO,
                              apage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(changeObjInfo.p), &changeObjInfo.p,
                              newLength, &anode,
                              alignedLen, origObj->data);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        /* mark the lsn in the page */
        apage->header.lsn = lsn;
        apage->header.logRecLen = logRecLen;
    }

    /* prepare the space for holding the root node. */
    e = om_ChangeObjectSize(handle, &xactEntry->xactId, apage, slotNo, alignedLen, newLength, FALSE);
    if (e < eNOERROR) ERR(handle, e);

    /* In om_ChangeObjectSize(), obj may be moved to someplace. */
    obj = (Object *)&(apage->data[apage->slot[-slotNo].offset]);

    obj->header.properties = newProperties;
    memcpy(obj->data, &anode, newLength);

    return(eNOERROR);

} /* om_ConverToLarge() */
