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
 * Module : OM_DeleteFromObject.c
 *
 * Description :
 *  OM_DeleteFromObject() deletes some bytes from the given object.
 *  The delete position can be any byte within the object.
 *
 * Exports:
 *  Four OM_DeleteFromObject(Four, DataFileInfo*, ObjectID*, Four, Four, LockParameter*)
 *
 * Return Values :
 *  Error Code
 *    eBADCATOBJ_OM
 *    eBADLENGTH_OM
 *    eBADOBJECTID_OM
 *    eTOOBIGSTART_OM	- start value is too big so it exceeds obj boundary
 *    eLOCKREQUESTFAIL_OM
 *    some codes caused by function calls
 *
 * Side Effects :
 */

/* The program control flow changed. */

#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"		/* for tracing : TR_PRINT(handle, ) macro */
#include "TM.h"
#include "LM.h"
#include "LOG.h"
#include "BfM.h"		/* for the buffer manager call */
#include "OM.h"
#include "LOT.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four OM_DeleteFromObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    ObjectID *oid,		/* IN object to delete from  */
    Four     start,		/* IN starting offset of delete */
    Four     length,		/* IN amount of data being deleted */
    LockParameter *lockup,      /* IN request lock or not */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four        e;		/* error number */
    PageID	pid;		/* page on which the given object resides */
    SlottedPage *apage;		/* pointer to buffer holding 'pid' page */
    Buffer_ACC_CB *aPage_BCBP;	/* buffer access control block containing data */
    Object	*obj;		/* pointer to the given object itself */
    Four	alignedOrigLen;	/* aligned length of original length */
    Four	newLen;		/* length of the object after delete */
    Four	alignedNewLen;	/* aligned length of newLen */
    Four oldLrgObjRootNodeSize;
    Four oldLrgObjRootNodeSize_alignedLength;
    Four newLrgObjRootNodeSize;
    Boolean rootWithHdr_Flag;
    Two newProperties;
    L_O_T_INode anode;
    LockReply lockReply;	/* lock reply */
    LockMode oldMode;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_OM_ObjDataInPage_T objDataInfo; /* information for updating data of an object */
    LOG_Image_OM_ModifyObjHdrLen_T modifyObjHdrLenInfo; /* object length change information */
    LOG_Image_OM_ChangeObject_T changeObjInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_OM, TR1,
	     ("OM_DeleteFromObject(handle, finfo=%P, oid=%P, start=%ld, length=%ld, lockup=%P)",
	      finfo, oid, start, length, lockup));


    /* check the parameters */
    if (finfo == NULL) ERR(handle, eBADCATOBJ);

    if (oid == NULL) ERR(handle, eBADOBJECTID_OM);

    if (length < 0 && length != REMAINDER) ERR(handle, eBADLENGTH_OM);


    /* get the PageID of page on which the given object resides */
    pid = *((PageID *)oid);

#ifdef CCPL
    if (lockup){
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

    /* read the page into the buffer */
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
    if(!IS_VALID_OBJECTID(oid, apage)) {
#ifdef CCPL
	ERRB1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	ERRBL1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    /* 'obj' points to the object */
    obj = (Object *)&(apage->data[apage->slot[-(oid->slotNo)].offset]);

    /* check that start is valid */
    if (start < 0 || start >= obj->header.length) {
#ifdef CCPL
	ERRB1(handle, eBADSTART_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	ERRBL1(handle, eBADSTART_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    /* if length is REMAINDER, set length to # of bytes after starting offset */
    if (length == REMAINDER) length = obj->header.length - start;

    /* check that 'length' is valid.
       If length is too large, length is reduced to the remains. */
    if (start + length > obj->header.length)
	length = obj->header.length - start;

    if (obj->header.properties & P_LRGOBJ) { /* large object */
	TR_PRINT(handle, TR_OM, TR2, ("This is the large object.\n"));

        rootWithHdr_Flag = IS_LRGOBJ_ROOTWITHHDR(obj->header.properties);
        oldLrgObjRootNodeSize = LOT_GetSize(handle, obj->data, rootWithHdr_Flag);
        oldLrgObjRootNodeSize_alignedLength = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(oldLrgObjRootNodeSize));
        memcpy(&anode, obj->data, oldLrgObjRootNodeSize);

        if (obj->header.length == length) { /* destroy large object */
            e = LOT_DestroyObject(handle, xactEntry, finfo, obj->data, rootWithHdr_Flag, logParam);
            if (e < 0) {
#ifdef CCPL
                ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
                ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
            }

            newLrgObjRootNodeSize = 0;
            if (rootWithHdr_Flag) /* clear flag(s) */
                newProperties = newProperties ^ (P_LRGOBJ | P_LRGOBJ_ROOTWITHHDR);
            else
                newProperties = obj->header.properties ^ P_LRGOBJ;

        } else {
            e = LOT_DeleteFromObject(handle, xactEntry, finfo, (char*)&anode, &rootWithHdr_Flag,
                                     SP_FREE(xactEntry->xactId, apage)+oldLrgObjRootNodeSize_alignedLength,
                                     start, length, logParam);
            if (e < 0) {
#ifdef CCPL
                ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
                ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
            }

            newLrgObjRootNodeSize = LOT_GetSize(handle, (char*)&anode, rootWithHdr_Flag);

            if (rootWithHdr_Flag)
                newProperties = obj->header.properties | P_LRGOBJ_ROOTWITHHDR; /* set the flag */
            else
                newProperties = obj->header.properties & (~P_LRGOBJ_ROOTWITHHDR); /* clear the flag */
        }

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            changeObjInfo.p_l.type = CHANGE_OBJECT_TYPE_PROPERTIES_AND_LENGTH;
            changeObjInfo.p_l.slotNo = oid->slotNo;
            changeObjInfo.p_l.deltaOfDataAreaSize = newLrgObjRootNodeSize - oldLrgObjRootNodeSize;
            changeObjInfo.p_l.propertiesXor = newProperties ^ obj->header.properties;
            changeObjInfo.p_l.deltaInLengthField = -length;

            LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_OM_CHANGE_OBJECT, LOG_REDO_UNDO,
                                  pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(changeObjInfo.p_l), &changeObjInfo.p_l,
                                  newLrgObjRootNodeSize, &anode,
                                  oldLrgObjRootNodeSize, obj->data);

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

        e = om_ChangeObjectSize(handle, &xactEntry->xactId, apage, oid->slotNo, oldLrgObjRootNodeSize_alignedLength,
                                MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(newLrgObjRootNodeSize)), FALSE);
	if (e < 0) {
#ifdef CCPL
            ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
            ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
        }

        /* In om_ChangeObjectSize(), obj may be moved to someplace. */
        obj = (Object *)&(apage->data[apage->slot[-(oid->slotNo)].offset]);

        memcpy(obj->data, &anode, newLrgObjRootNodeSize);
        obj->header.properties = newProperties;
        obj->header.length -= length;

    } else if (obj->header.properties & P_MOVED) {
	/* This is the moved object: recursively call OM_DeleteFormObject() */

	TR_PRINT(handle, TR_OM, TR2, ("This is the moved object.\n"));

#ifdef CCPL
	e = OM_DeleteFromObject(handle, xactEntry, finfo, (ObjectID *)obj->data, start, length, lockup, logParam);
	if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	/* lockup == NULL :: no lock for forwarded object */
	e = OM_DeleteFromObject(handle, xactEntry, finfo, (ObjectID *)obj->data, start, length, NULL, logParam);
	if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */


	/*
	 * Write log record.
	 */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

	    modifyObjHdrLenInfo.slotNo = oid->slotNo;
	    modifyObjHdrLenInfo.length = length;

            LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                   LOG_ACTION_OM_DECREASE_OBJECT_HEADER_LENGTH, LOG_REDO_UNDO,
                                  pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_OM_ModifyObjHdrLen_T), &modifyObjHdrLenInfo);

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

	/* Decrement the object length. */
	obj->header.length -= length;

    } else {
	/* initialize the local variables related with length */
	alignedOrigLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(obj->header.length));
	newLen = obj->header.length - length;
	alignedNewLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(newLen));

	/*
	 * Write log record.
	 */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

	    objDataInfo.slotNo = oid->slotNo;
	    objDataInfo.start = start;
	    objDataInfo.length = length;

            LOG_FILL_LOGRECINFO_2(logRecInfo,  xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_OM_DELETE_FROM_SMALL_OBJECT, LOG_REDO_UNDO,
                                  pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_OM_ObjDataInPage_T), &objDataInfo,
                                  length, &(obj->data[start]));

	    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
	    if (e < eNOERROR) {
#ifdef CCPL
                ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
                ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
            }

	    /* mark the lsn in the page */
	    apage->header.lsn = lsn;
	    apage->header.logRecLen = logRecLen;
	}

	/* delete the bytes by pulling bytes after the deleted bytes */
	memmove(&obj->data[start], &obj->data[start+length], newLen - start);

	e = om_ChangeObjectSize(handle, &xactEntry->xactId, apage, oid->slotNo, alignedOrigLen, alignedNewLen, FALSE);
	if (e < 0) {
#ifdef CCPL
            ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
            ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
        }

	/* Decrement the object length. */
	obj->header.length -= length;
    }

    /* set the dirty flag */
    aPage_BCBP->dirtyFlag = 1;

#ifdef CCRL
    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(length);

} /* OM_DeleteFromObject( ) */
