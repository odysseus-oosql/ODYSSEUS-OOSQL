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
/******************************************************************************/
/*                                                                            */
/*    This module has been implemented based on "The Multilevel Grid File     */
/*    (MLGF) Version 4.0," which can be downloaded at                         */
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".                */
/*                                                                            */
/******************************************************************************/

/*
 * Module: MLGF_DeleteObject.c
 *
 * Description:
 *  Delete an object from the given MLGF index.
 *
 * Exports:
 *  Four MLGF_DeleteObject(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue*,
 *                         ObjectID*, LocalPool*, DeallocListElem*, LockParameter*)
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "RDsM.h"
#include "BfM.h"
#include "MLGF.h"
#include "LM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"

/* Internal Function Prototypes */
Four mlgf_DeleteRecursive(Four, XactTableEntry_T*, MLGFIndexInfo*, PageID*, MLGF_KeyDesc*, 
                          MLGF_HashValue*, ObjectID*, char*, mlgf_DirectoryEntry*,
                          mlgf_DeleteStatus_T*, LockParameter*, LogParameter_T*);


/*
 * Function: Four MLGF_DeleteObject(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue*,
 *                                  ObjectID*, LockParameter*)
 *
 * Description:
 *  Delete an object from the given MLGF index. The parameter `keys' have
 *  hash values of the keys of the deleted objects.
 *
 * Returns:
 *  eNOERROR
 *  MLGF_STATUS_NOTFOUND
 *  Error code
 *   eBADPARAMETER
 *   some errors caused by function calls
 */
Four MLGF_DeleteObject(
    Four                        handle,                 /* IN handle */
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    MLGFIndexInfo 		*iinfo,			/* IN MLGF Index Info */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor for MLGF index */
    MLGF_HashValue 		*keys,			/* IN hash values of keys */
    ObjectID 			*oid,			/* IN object to delete */
    LockParameter 		*lockup,      		/* IN request lock or not */
    LogParameter_T 		*logParam) 		/* IN log parameter */
{
    Four        		e;			/* error code */
    Four        		i;			/* index variable */
    mlgf_DirectoryEntry 	rootEntry; 		/* directory entry for old root page */
    char 			extraData[PAGESIZE]; 	/* extra data of the deleted object */
    PageID 			tmpPid;
    mlgf_DeleteStatus_T 	status;
    LockReply 			lockReply;
    LockMode 			oldMode;
    PageID 			rootPid;		/* PageID of the root page */

    /* pointer for MLGF Data Structure of perThreadTable */
    MLGF_PerThreadDS_T *mlgf_perThreadDSptr = MLGF_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1,
	     ("MLGF_DeleteObject(handle, xactEntry=%P, iinfo=%P, kdesc=%P, keys=%P, oid=%P, lockup=%P, logParam=%P",
	     handle, xactEntry, iinfo, kdesc, keys, oid, lockup, logParam));


    if (iinfo == NULL || kdesc == NULL || keys == NULL || oid == NULL) 
	ERR(handle, eBADPARAMETER);


    /* Get 'rootPid' from MLGF index info */
    e = mlgf_GetRootPid(handle, xactEntry, iinfo, &rootPid, lockup);
    if (e < eNOERROR) ERR(handle, e);

    /* `rootEntry' is the entry for root page. */
    rootEntry.spid = rootPid.pageNo; 
    rootEntry.theta = 0;	/* ignore this field for root page */
    for (i = 0; i < kdesc->nKeys; i++) rootEntry.nValidBits[i] = 0;

    /* call recursion */
    status.allFlags = 0;
    e = mlgf_DeleteRecursive(handle, xactEntry, iinfo, &rootPid, kdesc, keys, oid, extraData, 
                             &rootEntry, &status, lockup, logParam);
    if (e < eNOERROR) ERR(handle, e);


    /* end of nested top action */
    if (status.flags.nestedTopAction) {
        e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
        if (e < eNOERROR) ERR(handle, e);
    }

    /* release all locks on the lock stack */
    for(; !MLGF_LOCKSTACK_IS_EMPTY(mlgf_perThreadDSptr->mlgfLockStack); ){
	MLGF_LOCKSTACK_POP(mlgf_perThreadDSptr->mlgfLockStack, &tmpPid);

	e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, &tmpPid, L_MANUAL);
	if (e < eNOERROR) ERR(handle, e);
    }

    if (status.flags.notFound) ERR(handle, eNOTFOUND);

    return(eNOERROR);

} /* MLGF_DeleteObject( ) */



/*
 * Function: Four mlgf_DeleteRecursive(PageID*, MLGF_KeyDesc*, MLGF_HashValue*,
 *             ObjectID*, char*, mlgf_DirectoryEntry*, Pool*, DeallocListElem*)
 *
 * Description:
 *  This function is an auxiliary function of MLGF_DeleteObject().
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four mlgf_DeleteRecursive(
    Four			handle,
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    MLGFIndexInfo 		*iinfo,        		/* IN MLGF Index Info */
    PageID 			*root,			/* IN root of subtree */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor for MLGF */
    MLGF_HashValue 		*keys,			/* IN hash values of the deleted object */
    ObjectID 			*oid,			/* IN object to delete */
    char 			*data,			/* OUT extra data of the deleted object */
    mlgf_DirectoryEntry 	*entryToRoot, 		/* IN entry for the current node in parent node */
    mlgf_DeleteStatus_T 	*status, 		/* INOUT delete status */
    LockParameter 		*lockup,      		/* IN request lock or not */
    LogParameter_T 		*logParam)   		/* IN log parameter */

{
    Four 			e;			/* error code */
    Four 			entryLen;		/* length of a directory entry */
    Four 			entryNo;		/* index to directory entry */
    PageID 			child;			/* PageID of the child page */
    Boolean 			found;			/* TRUE when we find something to search */
    Boolean 			mbrChangeFlag;		/* TRUE if the extreme hash values are updated */
    mlgf_Page       		*apage;			/* an MLGF page */
    Buffer_ACC_CB   		*root_BCB;		/* buffer control block for root (of subtree) */
    Buffer_ACC_CB   		*child_BCB;		/* buffer control block for the child */
    mlgf_DirectoryPage 		*childPage; 		/* a child page */
    mlgf_DirectoryEntry 	*entryToChild; 		/* entry pointing to the child */
    mlgf_DirectoryEntry 	entryToChild_tmp; 	/* temporary copy for new update */
    MLGF_HashValue 		*extremeHashValues; 	/* points to array of extreme hash values in the given root node */
    MLGF_HashValue 		*childHashValues; 	/* points to array of extreme hash values in the child node */
    MLGF_HashValue 		*hx, *hy;    		/* points to arrary of hash values in an entry */
    MLGF_HashValue 		*entryHashValues; 	/* points to arrary of hash values in an entry */
    Four 			k, i;
    PageID 			tmpPid;
    LockReply 			lockReply;
    LockMode 			oldMode;
    Lsn_T 			lsn;                  	/* lsn of the newly written log record */
    Four 			logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 		logRecInfo; 		/* log record information */
    LockParameter 		kLockup; 

    /* pointer for MLGF Data Structure of perThreadTable */
    MLGF_PerThreadDS_T *mlgf_perThreadDSptr = MLGF_PER_THREAD_DS_PTR(handle);

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_DeleteRecursive()"));

    /* get lock on the root page */
    if(lockup) {
	kLockup.mode = L_IX;
	kLockup.duration = L_MANUAL;

	e = LM_getKeyRangeLock(handle, &xactEntry->xactId, root, kLockup.mode, kLockup.duration,
			       L_UNCONDITIONAL, &lockReply);

	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK){
	    ERR(handle, eDEADLOCK);
	}

	e = LM_getFlatPageLock(handle, &xactEntry->xactId, root, lockup->mode, L_MANUAL,
			       L_UNCONDITIONAL, &lockReply, &oldMode); 

	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK){
	    ERR(handle, eDEADLOCK);
	}
    }

    /* Read the current node into a buffer. */
    e = BfM_getAndFixBuffer(handle, root, M_FREE, &root_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (mlgf_Page*)root_BCB->bufPagePtr;

    if (apage->any.hdr.type & MLGF_LEAFPAGE) { /* leaf page */

	/* delete the given object from the leaf page. */
	e = mlgf_DeleteObjectFromLeaf(handle, xactEntry, iinfo, root, root_BCB, 
				      kdesc, keys, oid, data, 
                                      entryToRoot, status, logParam);
	if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	/* Unfix the buffer. */
	e = BfM_unfixBuffer(handle, root_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	/* release lock on this page */
	if(lockup){
	    e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, root, L_MANUAL);
	    if (e < eNOERROR) ERR(handle, e);

	}

	return(eNOERROR);
    }

    /* From this point the node is the directory node. */

    /* Calculate the length of a directory entry. */
    entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    /* find region to go */
    found = mlgf_FindEntry(handle, &apage->directory, kdesc->nKeys, keys, &entryNo);

    if (!found) {
        status->flags.notFound = 1;

	/* Unfix the buffer. */
	e = BfM_unfixBuffer(handle, root_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	/* release lock on this page */
	if(lockup){
	    e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, root, L_MANUAL);
	    if (e < eNOERROR) ERR(handle, e);

	}

	return(eNOERROR);
    }

    entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, entryNo, entryLen);
    MAKE_PAGEID(child, root->volNo, entryToChild->spid);

    if(lockup){
	if(mlgf_IsSafeDir(handle, root_BCB, kdesc, entryNo)){
	    for(; !MLGF_LOCKSTACK_IS_EMPTY(mlgf_perThreadDSptr->mlgfLockStack); ){
		MLGF_LOCKSTACK_POP(mlgf_perThreadDSptr->mlgfLockStack, &tmpPid);

		e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, &tmpPid, L_MANUAL);
		if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);
	    }
	}
	MLGF_LOCKSTACK_PUSH(handle, mlgf_perThreadDSptr->mlgfLockStack, root);
    }

    /* call recursion */
    memcpy(&entryToChild_tmp, entryToChild, entryLen);
    e = mlgf_DeleteRecursive(handle, xactEntry, iinfo, &child, kdesc, keys, oid, data, 
                             &entryToChild_tmp, status, lockup, logParam);
    if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);


    if (status->flags.thetaUpdated) {
        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            Two tmpEntryNo = entryNo;
            Two thetaXor = entryToChild->theta ^ entryToChild_tmp.theta;

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_MLGF_MODIFY_THETA_VALUE_OF_DIRECTORY_ENTRY, LOG_REDO_UNDO,
                                  apage->any.hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(Two), &tmpEntryNo,
                                  sizeof(Two), &thetaXor);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            apage->any.hdr.lsn = lsn;
            apage->any.hdr.logRecLen = logRecLen;
        }

        entryToChild->theta = entryToChild_tmp.theta;
        root_BCB->dirtyFlag = 1;

        status->flags.thetaUpdated = 0;
    }

    if (status->flags.mbrUpdated) {

        hx = MLGF_DIRENTRY_HASHVALUEPTR(entryToChild, kdesc->nKeys);
        hy = MLGF_DIRENTRY_HASHVALUEPTR(&entryToChild_tmp, kdesc->nKeys);

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            Two tmpEntryNo = entryNo;

            LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_MLGF_MODIFY_HASH_VALUES_OF_DIRECTORY_ENTRY, LOG_REDO_UNDO,
                                  apage->any.hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(Two), &tmpEntryNo,
                                  sizeof(MLGF_HashValue)*kdesc->nKeys, hy,
                                  sizeof(MLGF_HashValue)*kdesc->nKeys, hx);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            apage->any.hdr.lsn = lsn;
            apage->any.hdr.logRecLen = logRecLen;
        }

        for (k = 0; k < kdesc->nKeys; k++) hx[k] = hy[k];
	root_BCB->dirtyFlag = 1;	/* set the dirty flag */

        status->flags.mbrUpdated = 0;

        /*
        ** Update the MBR of the entry 'entryToRoot'
        */
	hx = MLGF_DIRENTRY_HASHVALUEPTR(entryToRoot, kdesc->nKeys);
	for (k = 0; k < kdesc->nKeys; k++) {

	    if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (hx[k] < keys[k])) ||
		(MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (hx[k] > keys[k])))
		continue;

            if (MLGF_KEYDESC_IS_MINTYPE(*kdesc, k))
                hx[k] |= MLGF_HASHVALUE_SET_EXCEPT_UPPER_N_BITS(entryToRoot->nValidBits[k]);
            else
                hx[k] = MLGF_HASHVALUE_MASK_UPPER_N_BITS(hx[k], entryToRoot->nValidBits[k]);

            status->flags.mbrUpdated = 1;

	    hy = MLGF_DIRENTRY_HASHVALUEPTR(
		MLGF_ITH_DIRENTRY(&(apage->directory), 0, entryLen), kdesc->nKeys);
	    for (i = 0; i < apage->directory.hdr.nEntries; i++) {
		if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (hx[k] > hy[k])) ||
		    (MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (hx[k] < hy[k]))) {

		    hx[k] = hy[k];
		}

		hy = (MLGF_HashValue*)((char*)hy + entryLen);
	    }
        }
    }

    if (status->flags.underflow) {
	/* maximize regions in directory page */
	e = mlgf_MaximizeRegions(handle, xactEntry, root_BCB, kdesc, entryToRoot, logParam);
        if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	if (apage->directory.hdr.height == 1) /* merge leaf page */
	    e = mlgf_MergeLeafPage(handle, xactEntry, root_BCB, kdesc, entryNo, lockup, logParam);
	else 	/* merge directory page */
	    e = mlgf_MergeDirectoryPage(handle, xactEntry, root_BCB, kdesc, entryNo, lockup, logParam);
	if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

        status->flags.underflow = 0;

    } else if (status->flags.emptyPage) {
	/* delete entry */
        e = mlgf_DeleteFromDirectory(handle, xactEntry, &apage->directory, kdesc, entryNo, logParam);
        if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	root_BCB->dirtyFlag = 1;

        status->flags.emptyPage = 0;
    }

    if (MLGF_DP_THETA(&apage->directory, entryLen) != entryToRoot->theta) {
        /* update the theta value */
	entryToRoot->theta = MLGF_DP_THETA(&apage->directory, entryLen);
        status->flags.thetaUpdated = 1;
    }

    if (apage->directory.hdr.nEntries == 0)
        status->flags.emptyPage = 1;
    else if (entryToRoot->theta < MLGF_DP_THRESHOLD)
        status->flags.underflow = 1;

    if (status->flags.emptyPage && !(apage->any.hdr.type & MLGF_ROOTPAGE)) { 
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_MLGF_MODIFY_INDEXID_OF_MLGF_PAGE, LOG_REDO_UNDO,
                                  apage->any.hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(IndexID), &(common_perThreadDSptr->nilIid),
                                  sizeof(IndexID), &apage->any.hdr.iid);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

            /* mark the lsn in the page */
            apage->any.hdr.lsn = lsn;
            apage->any.hdr.logRecLen = logRecLen;
        }

        SET_NILINDEXID(apage->any.hdr.iid); /* any more not used in this index */

        e = RDsM_FreeTrain(handle, xactEntry, &apage->any.hdr.pid, PAGESIZE2, FALSE, logParam);
	if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);
    }

    if (apage->any.hdr.type & MLGF_ROOTPAGE) {	/* level down */

	/* if the root page has only one entry, let level down */
	while (apage->directory.hdr.nEntries == 1 && apage->directory.hdr.height > 1) {

	    /* Copy the only one child into the root. */
	    entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, 0, entryLen);
	    MAKE_PAGEID(child, root->volNo, entryToChild->spid);

	    /* get lock on the root page */
	    if(lockup) {
		e = LM_getFlatPageLock(handle, &xactEntry->xactId, &child, L_X, L_MANUAL,
				       L_UNCONDITIONAL, &lockReply, &oldMode);
		if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

		if(lockReply == LR_DEADLOCK){
		    ERRB1(handle, eDEADLOCK, root_BCB, PAGE_BUF);
		}

	    }

	    /* Read the child into the buffer. */
	    e = BfM_getAndFixBuffer(handle, &child, M_FREE, &child_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	    childPage = (mlgf_DirectoryPage*)child_BCB->bufPagePtr;

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                /*
                 * root page
                 */
                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_MLGF_ROOT_PAGE_LEVEL_DOWN, LOG_REDO_UNDO,
                                      apage->any.hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      entryLen*childPage->hdr.nEntries, &childPage->data[0],
                                      entryLen, entryToChild);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERR(handle, e);

                /* mark the lsn in the page */
                apage->any.hdr.lsn = lsn;
                apage->any.hdr.logRecLen = logRecLen;

                /*
                 * child page
                 */
                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_MLGF_MODIFY_INDEXID_OF_MLGF_PAGE, LOG_REDO_UNDO,
                                      childPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(IndexID), &(common_perThreadDSptr->nilIid),
                                      sizeof(IndexID), &childPage->hdr.iid);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERR(handle, e);

                /* mark the lsn in the page */
                childPage->hdr.lsn = lsn;
                childPage->hdr.logRecLen = logRecLen;

            }

            memcpy(&apage->directory.data[0], &childPage->data[0], entryLen*childPage->hdr.nEntries);
            apage->directory.hdr.nEntries = childPage->hdr.nEntries;
            apage->directory.hdr.height --;
	    root_BCB->dirtyFlag = 1;

	    SET_NILINDEXID(childPage->hdr.iid); /* any more not used in this index */
	    child_BCB->dirtyFlag = 1;

	    e = BfM_unfixBuffer(handle, child_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

            e = RDsM_FreeTrain(handle, xactEntry, &child, PAGESIZE2, FALSE, logParam);
            if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	    if(lockup){
		e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, &child, L_MANUAL);
		if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	    }
	}
    }

    /* Unfix the buffer. */
    e = BfM_unfixBuffer(handle, root_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_DeleteRecursive() */

