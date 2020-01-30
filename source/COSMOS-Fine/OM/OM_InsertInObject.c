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
 * Module : OM_InsertInObject.c
 *
 * Description :
 *  OM_InsertInObject() causes 'length' bytes of data to be inserted into
 *  the specified object. The insertion begins at the offset specified by
 *  the parameter 'start'.
 *
 * Exports:
 *  Four OM_InsertInObject(Four, DataFileInfo*, ObjectID*, Four, Four,
 *                         char*, LocalPool*, DeallocListElem*, LockParameter*)
 *
 * Return Values :
 *  Error Code
 *    eBADCATOBJ_OM
 *    eBADOBJECTID_OM
 *    eBADLENGTH_OM
 *    eBADUSERBUF_OM
 *    eBADFILEID_OM
 *    eBADSTART_OM
 *    eLOCKREQUESTFAIL_OM
 *
 * Side Effects :
 *  'data' is inserted into the object.
 *  object size is updated reflecting the insertion.
 *  object may be moved into the other object. (properties bit is updated)
 *
 */

/* The program control flow changed. */

#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"		/* for tracing : TR_PRINT(handle, ) macro */
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "LOG.h"
#include "BfM.h"		/* for the buffer manager call */
#include "OM.h"
#include "LOT.h"                /* for the large object manager call */
#include "perProcessDS.h"
#include "perThreadDS.h"


Four OM_InsertInObject(
    Four 				handle,
    XactTableEntry_T 			*xactEntry, 			/* IN transaction table entry */
    DataFileInfo 			*finfo,				/* IN file information */
    ObjectID 				*oid,				/* IN object to insert in */
    Four     				start,				/* IN starting offset of insert */
    Four     				length,				/* IN amount of data to insert */
    char     				*data,				/* IN data to append */
    LockParameter 			*lockup,      			/* IN request lock or not */
    LogParameter_T 			*logParam) 			/* IN log parameter */
{
    Four 				e;				/* error number */
    PageID 				pid;				/* page holding the object */
    SlottedPage 			*apage;				/* point to buffer holding the page */
    Buffer_ACC_CB 			*aPage_BCBP;			/* buffer access control block containing data */
    Object 				*obj;				/* point to the object in page */
    Four 				origLen;			/* original length */
    Four 				alignedOrigLen;			/* aligned length of original length */
    Four 				totalLen;			/* original length + inserted length */
    Four 				alignedTotalLen;		/* aligned length of totalLen */
    Four 				oldLrgObjRootNodeSize;
    Four 				oldLrgObjRootNodeSize_alignedLength;
    Four 				newLrgObjRootNodeSize;
    Boolean 				rootWithHdr_Flag;
    L_O_T_INode 			anode;
    ObjectID 				movedOid;			/* ObjectID of the moved object */
    ObjectHdr 				objHdr;				/* object header of the moved object */
    char 				totalData[PAGESIZE-SP_FIXED]; 	/* save the data before compaction */
    Boolean 				objDeleteFlag;      		/* destroy this object if TRUE */
    Two 				newProperties;
    LockReply 				lockReply;			/* lock reply */
    LockMode 				oldMode;
    Lsn_T 				lsn;				/* lsn of the newly written log record */
    Four 				logRecLen;             		/* log record length */
    LOG_LogRecInfo_T 			logRecInfo; 			/* log record information */
    LOG_Image_OM_ObjDataInPage_T 	objDataInfo; 			/* information for updating data of an object */
    LOG_Image_OM_ModifyObjHdrLen_T 	modifyObjHdrLenInfo; 		/* object length change information */
    LOG_Image_OM_ChangeObject_T 	changeObjInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_OM, TR1, ("OM_InsertInObejct()"));


    /* check parameters */

    if (finfo == NULL) ERR(handle, eBADCATOBJ);

    if (oid == NULL) ERR(handle, eBADOBJECTID_OM);

    if (length < 0) ERR(handle, eBADLENGTH_OM);

    if (data == NULL) ERR(handle, eBADUSERBUF_OM);


    objDeleteFlag = FALSE;      /* initialize this flag: it is used when forwarded object is moved. */

    /* Get the PageID from the ObjectID */
    MAKE_PAGEID(pid, oid->volNo, oid->pageNo);

#ifdef CCPL
    if (lockup){
	/* lock the page */
	e = LM_getPageLock(handle, &xactEntry->xactId, &pid, &finfo->fid,
			   lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

	if (lockReply == LR_DEADLOCK){
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

    /* check the given file contains the 'oid' object */
    if (!EQUAL_FILEID(finfo->fid, apage->header.fid)) {
#ifdef CCPL
	ERRB1(handle, eBADFILEID, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	ERRBL1(handle, eBADFILEID, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    /* check the 'oid' is valid. */
    if (!IS_VALID_OBJECTID(oid, apage)) {
#ifdef CCPL
	ERRB1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	ERRBL1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    /* Get the object from the 'oid' */
    obj = (Object *)&(apage->data[apage->slot[-(oid->slotNo)].offset]);

    if (start < 0 || start > obj->header.length) {
#ifdef CCPL
	ERRB1(handle, eBADSTART_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	ERRBL1(handle, eBADSTART_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    if (start == obj->header.length) {

#ifdef CCRL
        e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

	e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	/* We already have the lock.
	 * So, we don't need to lock the page again */
	e = OM_AppendToObject(handle, xactEntry, finfo, oid, length, data, NULL, logParam);
	if (e < eNOERROR) ERR(handle, e);

	return(eNOERROR);
    }

    origLen = obj->header.length;
    alignedOrigLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(origLen));
    totalLen = obj->header.length + length;
    alignedTotalLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(totalLen));


    /*
     * The object is converted to the large object when the object is to be
     * the large object as a result from the insert.
     */
    if (!(obj->header.properties & P_LRGOBJ) && ((alignedTotalLen > LRGOBJ_THRESHOLD) ||
	(alignedOrigLen + SP_FREE(xactEntry->xactId, apage) < MIN(alignedTotalLen,MIN_OBJECT_DATA_SIZE)))) {
	/* Assert that this object has not P_FORWARDED property. */
	e = om_ConvertToLarge(handle, xactEntry, finfo, apage, oid->slotNo, lockup, logParam);
	if (e < 0) {
#ifdef CCPL
            ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
            ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
        }

	/* In OM_ConvertToLarge(), obj may be moved to someplace. */
	obj = (Object *)&(apage->data[apage->slot[-(oid->slotNo)].offset]);
    }


    if (obj->header.properties & P_LRGOBJ) { /* large object */
	TR_PRINT(handle, TR_OM, TR2, ("This is the large object.\n"));

        rootWithHdr_Flag = IS_LRGOBJ_ROOTWITHHDR(obj->header.properties);
        oldLrgObjRootNodeSize = LOT_GetSize(handle, obj->data, rootWithHdr_Flag);
        oldLrgObjRootNodeSize_alignedLength = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(oldLrgObjRootNodeSize));
        memcpy(&anode, obj->data, oldLrgObjRootNodeSize);

	e = LOT_InsertInObject(handle, xactEntry, finfo, &apage->header.pid, (char*)&anode, &rootWithHdr_Flag,
                               SP_FREE(xactEntry->xactId, apage)+oldLrgObjRootNodeSize_alignedLength,
                               start, length, data, logParam); 
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

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            changeObjInfo.p_l.type = CHANGE_OBJECT_TYPE_PROPERTIES_AND_LENGTH;
            changeObjInfo.p_l.slotNo = oid->slotNo;
            changeObjInfo.p_l.deltaOfDataAreaSize = newLrgObjRootNodeSize - oldLrgObjRootNodeSize;
            changeObjInfo.p_l.propertiesXor = newProperties ^ obj->header.properties;
            changeObjInfo.p_l.deltaInLengthField = length;

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
        obj->header.length += length;

    } else if (obj->header.properties & P_MOVED) {
	/* This is the moved object: recursively call OM_InsertInObject( ) */
	TR_PRINT(handle, TR_OM, TR2, ("This is the moved object.\n"));

	movedOid = *((ObjectID*)obj->data);
#ifdef CCPL
	e = OM_InsertInObject(handle, xactEntry, finfo, &movedOid,
			      start, length, data, lockup, logParam);
	if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
        /* lockup == NULL :: no lock for forwarded object */
	e = OM_InsertInObject(handle, xactEntry, finfo, &movedOid,
			      start, length, data, NULL, logParam);
	if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

        obj = (Object *)&(apage->data[apage->slot[-(oid->slotNo)].offset]);

	/* The object was moved to another place. */
	if (!EQUAL_OBJECTID(movedOid, *(ObjectID*)obj->data)) {

	    /*
	     * Write log record.
	     */
	    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

		objDataInfo.slotNo = oid->slotNo;
		objDataInfo.start = 0;
		objDataInfo.length = sizeof(ObjectID);

                LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_OM_WRITE_SMALL_OBJECT, LOG_REDO_UNDO,
                                      pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_OM_ObjDataInPage_T), &objDataInfo,
                                      sizeof(ObjectID), &movedOid,
                                      sizeof(ObjectID), obj->data);

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

	    *((ObjectID*)obj->data) = movedOid;
	}


	/*
	 * Write log record.
	 */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

	    modifyObjHdrLenInfo.slotNo = oid->slotNo;
	    modifyObjHdrLenInfo.length = length;

            LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_OM_INCREASE_OBJECT_HEADER_LENGTH, LOG_REDO_UNDO,
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

	obj->header.length += length;

    } else {			/* Normal Object */
	if (alignedTotalLen <= SP_FREE(xactEntry->xactId, apage) + alignedOrigLen) {
	    /* There is the enough space. */

	    /* Prepare the space for appending the data. */
	    e = om_ChangeObjectSize(handle, &xactEntry->xactId, apage, oid->slotNo, alignedOrigLen, alignedTotalLen, FALSE);
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


	    /*
	     * Write log record.
	     */
	    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

		objDataInfo.slotNo = oid->slotNo;
		objDataInfo.start = start;
		objDataInfo.length = length;

                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_OM_INSERT_INTO_SMALL_OBJECT, LOG_REDO_UNDO,
                                      pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_OM_ObjDataInPage_T), &objDataInfo,
                                      length, data);

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

	    /* slide the partial data for inserting the given data */
	    memmove(&(obj->data[start+length]), &obj->data[start], origLen-start);

	    /* insert the data */
	    memcpy(&(obj->data[start]), data, length);

	    /*
	     * increase the length of object data; its log is included in
	     * the above log, LOG_ACTION_OM_INSERT_IN_SMALL_OBJECT.
	     */
	    obj->header.length += length;

	} else {
	    /* There is *no* enough space. */
	    /* We should move the object to the other page. */

	    /* totalData has the original data with new data inserted */
	    memcpy(totalData, obj->data, start);
	    memcpy(&totalData[start], data, length);
	    memcpy(&totalData[start+length], &obj->data[start], origLen-start);

	    /* The created object has P_FORWARDED property. */
	    objHdr.properties = obj->header.properties | P_FORWARDED;
	    objHdr.tag = obj->header.tag;

            /*
             * Before creating the moved object, unfix the buffer.
             * After creating, fix the buffer again.
             */
            aPage_BCBP->dirtyFlag = 1;

#ifdef CCRL
            e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

            e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

#ifdef CCPL
	    e = om_CreateObject(handle, xactEntry, finfo, oid, &objHdr,
				totalLen, totalData, lockup, &movedOid, logParam);
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
            /* lockup == NULL :: no lock on forwarded object */
	    e = om_CreateObject(handle, xactEntry, finfo, oid, &objHdr,
				totalLen, totalData, NULL, &movedOid, logParam);
	    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

            /* read the slotted page into the system buffer */
#ifdef CCPL
            e = BfM_getAndFixBuffer(handle, &pid, M_FREE, &aPage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
            e = BfM_getAndFixBuffer(handle, &pid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

            /* reset the pointers because the address may be changed. */
            apage = (SlottedPage *)aPage_BCBP->bufPagePtr;
            obj = (Object *)&(apage->data[apage->slot[-(oid->slotNo)].offset]);

	    if (obj->header.properties & P_FORWARDED) {
		/* this is the forwarded object */

                objDeleteFlag = TRUE;
	    } else {
		/* This is the normal object. */

		/*
		 * Write log record.
		 */
		if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                    changeObjInfo.p_l.type = CHANGE_OBJECT_TYPE_PROPERTIES_AND_LENGTH;
                    changeObjInfo.p_l.slotNo = oid->slotNo;
                    changeObjInfo.p_l.deltaOfDataAreaSize = sizeof(ObjectID) - origLen;
                    changeObjInfo.p_l.propertiesXor = P_MOVED; 
                    changeObjInfo.p_l.deltaInLengthField = length;

                    LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_OM_CHANGE_OBJECT, LOG_REDO_UNDO,
                                          pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(changeObjInfo.p_l), &changeObjInfo.p_l,
                                          sizeof(ObjectID), &movedOid,
                                          origLen, obj->data);

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

		/* prepare the space for holding the moved ObjectID. */
		e = om_ChangeObjectSize(handle, &xactEntry->xactId, apage, oid->slotNo, alignedOrigLen, sizeof(ObjectID), FALSE);
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

		/* Write the moved ObjectID. */
		*((ObjectID *)obj->data) = movedOid;

		/* Mark that this is the moved object. */
		obj->header.properties |= P_MOVED;

		/* Change the object length. */
		obj->header.length += length;
	    }
	}
    }

    /* set the dirty flag. */
    aPage_BCBP->dirtyFlag = 1;

#ifdef CCRL
    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if (objDeleteFlag) {
        /* delete the old forwarded object */
        /* lock has been acquired before */
        e = OM_DestroyObject(handle, xactEntry, finfo, oid, NULL, logParam);
        if (e < eNOERROR) ERR(handle, e);

        /* change the oid */
        *oid = movedOid;
    }

    return(eNOERROR);

} /* OM_InsertInObject() */
