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
 * Module: MLGF_InsertObject.c
 *
 * Description:
 *  Insert an object and its data into the given MLGF index.
 *
 * Exports:
 *  Four MLGF_InsertObject(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[],
 *                         ObjectID*, char*)
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "RDsM.h"
#include "TM.h"
#include "BfM.h"
#include "MLGF.h"
#include "LM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal Fucntion Prototypes */
Four mlgf_InsertRecursive(Four, XactTableEntry_T*, MLGFIndexInfo*, PageID*, MLGF_KeyDesc*, MLGF_HashValue[], 
                          ObjectID*, char*, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*,
                          mlgf_InsertStatus_T*, LockParameter*, LogParameter_T*);


/*
 * Function: Four MLGF_InsertObject(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[],
 *                                  ObjectID*, char*)
 *
 * Description:
 *  Insert a pair of an ObjectID and its associated data into the given MLGF
 *  index.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four MLGF_InsertObject(
    Four                        handle,                 /* IN handle */
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    MLGFIndexInfo               *iinfo,                 /* IN MLGF Index Info */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor of MLGF */
    MLGF_HashValue 		*keys,			/* IN hash values of keys */
    ObjectID 			*oid,			/* IN Object to insert */
    char 			*data,			/* IN additional data to store */
    LockParameter 		*lockup,      		/* IN request lock or not */
    LogParameter_T 		*logParam) 		/* IN log parameter */
{
    Four        		e;			/* error code */
    Four			i;			/* index */
    Four        		entryLen;		/* length of a directory entry */
    PageID      		newPid;			/* PageID of the newly allocated page */
    Buffer_ACC_CB 		*newPage_BCB;		/* buffer acces control block for new page */
    Buffer_ACC_CB 		*rootPage_BCB; 		/* buffer acces control block for root page */
    mlgf_DirectoryPage 		*newPage;        	/* pointer to buffer for new page  */
    mlgf_DirectoryPage 		*rootPage;		/* pointer to buffer for root page */
    mlgf_DirectoryEntry 	newEntry;		/* directory entry for new page */
    mlgf_DirectoryEntry 	rootEntry;		/* directory entry for old root page */
    mlgf_DirectoryEntry		overflowInRoot;		/* directroy entry which are overflowed */
    PageID 			tmpPid;
    LockReply 			lockReply;
    LockMode 			oldMode;
    mlgf_InsertStatus_T 	status;
    Lsn_T 			lsn;                  	/* lsn of the newly written log record */
    Four 			logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 		logRecInfo; 		/* log record information */
    SegmentID_T 		pageSegmentID;  	/* page segment ID */
    PageID 			rootPid;		/* root page of MLGF */ 

    /* pointer for MLGF Data Structure of perThreadTable */
    MLGF_PerThreadDS_T *mlgf_perThreadDSptr = MLGF_PER_THREAD_DS_PTR(handle);

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1,
	     ("MLGF_InsertObject(handle, xactEntry=%P, iinfo=%P, kdesc=%P, keys=%P, oid=%P, data=%P, lockup=%P, logParam=%P",
	      xactEntry, iinfo, kdesc, keys, oid, data, lockup, logParam));


    /* Check parameters. */
    if (iinfo == NULL || kdesc == NULL || keys == NULL || oid == NULL ||   
	(kdesc->extraDataLen != 0 && data == NULL))
	ERR(handle, eBADPARAMETER);

    /* Get 'rootPid' from MLGF index info */
    e = mlgf_GetRootPid(handle, xactEntry, iinfo, &rootPid, lockup);
    if (e < eNOERROR) ERR(handle, e);

    /* `rootEntry' is the entry for root page. */
    rootEntry.spid = rootPid.pageNo; 
    rootEntry.theta = 0;	/* ignore this field for root page */
    for (i = 0; i < kdesc->nKeys; i++) rootEntry.nValidBits[i] = 0;

    do {
        /* call recursion */
        status.allFlags = 0;
        e = mlgf_InsertRecursive(handle, xactEntry, iinfo, &rootPid, kdesc, keys,  
                                 oid, data, &rootEntry, &overflowInRoot,
                                 &status, lockup, logParam);
        if (e < eNOERROR) ERR(handle, e);

        if (status.flags.overflow) {	/* root page split */
            TR_PRINT(handle, TR_MLGF, TR1, ("Split\n"));

	    e = mlgf_GetSegmentIDFromIndexInfo(handle, xactEntry, iinfo, &pageSegmentID, PAGESIZE2);
	    if (e < eNOERROR) ERR(handle, e);

            e = RDsM_AllocTrains(handle, xactEntry, rootPid.volNo, &pageSegmentID, &rootPid, 1, PAGESIZE2, FALSE, &newPid, logParam);
            if (e < eNOERROR) ERR(handle, e);

            e = BfM_fixNewBuffer(handle, &newPid, M_FREE, &newPage_BCB, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            newPage = (mlgf_DirectoryPage*)newPage_BCB->bufPagePtr;

            /* Read the root page into the buffer. */
            e = BfM_getAndFixBuffer(handle, &rootPid, M_FREE, &rootPage_BCB, PAGE_BUF); 
            if (e < eNOERROR) ERRB1(handle, e, newPage_BCB, PAGE_BUF);

            rootPage = (mlgf_DirectoryPage*)rootPage_BCB->bufPagePtr;

            /* Copy the root page contents to the new page. */
            MLGF_COPY_DIRECTORY_PAGE(newPage, newPid, rootPage, FALSE);

            newPage_BCB->dirtyFlag = 1;

            rootEntry.spid = newPid.pageNo;
            e = mlgf_SplitDirectoryPage(handle, xactEntry, iinfo, &newPid, kdesc, &overflowInRoot, 
                                        &rootEntry, &newEntry, lockup, logParam);
            if (e < eNOERROR) ERRB1(handle, e, rootPage_BCB, PAGE_BUF);

            /* 'entryLen' is the length of a directory entry. */
            entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

            /* Construct the new root page. */
            memcpy(&rootPage->data[0], (char*)&rootEntry, entryLen);
            memcpy(&rootPage->data[entryLen], (char*)&newEntry, entryLen);

            rootPage->hdr.nEntries = 2;
            rootPage->hdr.height ++;

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                /*
                 * new page
                 */
                LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_MLGF_COPY_PAGE, LOG_REDO_ONLY,
                                      newPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(mlgf_DirectoryPage), newPage);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB2(handle, e, newPage_BCB, PAGE_BUF, rootPage_BCB, PAGE_BUF);

                /* mark the lsn in the page */
                newPage->hdr.lsn = lsn;
                newPage->hdr.logRecLen = logRecLen;


                /*
                 * root page
                 */
                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_MLGF_ROOT_PAGE_LEVEL_UP, LOG_REDO_UNDO,
                                      rootPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      entryLen*2, &rootPage->data[0],
                                      entryLen*newPage->hdr.nEntries, &newPage->data[0]);

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERRB2(handle, e, newPage_BCB, PAGE_BUF, rootPage_BCB, PAGE_BUF);

                /* mark the lsn in the page */
                rootPage->hdr.lsn = lsn;
                rootPage->hdr.logRecLen = logRecLen;
            }

            /* Unfix buffer for new page. */
            e = BfM_unfixBuffer(handle, newPage_BCB, PAGE_BUF);
            if (e < eNOERROR) ERRB1(handle, e, rootPage_BCB, PAGE_BUF);

            rootPage_BCB->dirtyFlag = 1;

            e = BfM_unfixBuffer(handle, rootPage_BCB, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);
        }

        /* end of nested top action */
        /* If an SMO has occurred before the object was to be inserted,
           we should complete the SMO before we insert the object again.
           When the object was inserted we should handle the update of theta
           and/or MBR with nested top action.
           => So we should always complete the nested top action. */
        assert(status.flags.objectInserted == 1 || status.flags.nestedTopAction == 1);
        if (status.flags.nestedTopAction) { 
            e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
            if (e < eNOERROR) ERR(handle, e);
        }

        for(; !MLGF_LOCKSTACK_IS_EMPTY(mlgf_perThreadDSptr->mlgfLockStack); ){
            MLGF_LOCKSTACK_POP(mlgf_perThreadDSptr->mlgfLockStack, &tmpPid);
            e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, &tmpPid, L_MANUAL);
            if (e < eNOERROR) ERR(handle, e);
        }

    } while (!status.flags.objectInserted);

    return(eNOERROR);

} /* MLGF_InsertObject() */



/*
 * Module: Four mlgf_InsertRecursive(PageID*, MLGF_KeyDesc*, MLGF_HashValue*,
 *                ObjectID*, char*, MLGF_DirectoryEntry*, MLGF_DirectoryEntry*, LockParameter*)
 *
 * Description:
 *  insert an object into mlgf index, recursively
 *
 * Returns:
 *  Error code
 */
Four mlgf_InsertRecursive(
    Four				handle,
    XactTableEntry_T 			*xactEntry, 		/* IN transaction table entry */
    MLGFIndexInfo 			*iinfo,       		/* IN MLGF index info */
    PageID 				*root,			/* IN root of subtree */
    MLGF_KeyDesc 			*kdesc,			/* IN key descriptor of MLGF */
    MLGF_HashValue 			*keys,			/* IN hash values of keys */
    ObjectID 				*oid,			/* IN object to insert */
    char 				*data,			/* IN data to store */
    mlgf_DirectoryEntry 		*entryToRoot,   	/* INOUT entry which indicates parent */
    mlgf_DirectoryEntry 		*overflowInRoot, 	/* OUT entry which failed to insert */
    mlgf_InsertStatus_T 		*status, 		/* INOUT current status */
    LockParameter 			*lockup,      		/* IN request lock or not */ 
    LogParameter_T 			*logParam) 		/* IN log parameter */
{
    Four          			e;		  	/* error code */
    Four          			entryNo;	  	/* index to some entry */
    Four          			entryLen;	  	/* length of a directory entry */
    Four          			buddyKey;	  	/* key on which buddys can be merged */
    PageID        			child;	  		/* PageID of the child page */
    PageID        			newPid;	  		/* PageID of the newly allocated page */
    Boolean       			found;	  		/* TRUE if we find something */
    Boolean       			insertEntryFlag; 	/* TRUE when we are to insert an entry in root */
    mlgf_Page     			*apage;	  		/* an MLGF page */
    mlgf_Page     			*newPage;	  	/* an MLGF page */
    Buffer_ACC_CB 			*root_BCB;	  	/* buffer cotnrol block for root page */
    Buffer_ACC_CB 			*new_BCB;	  	/* buffer control block for new page */
    mlgf_DirectoryEntry 		*entryToChild; 		/* entry pointing the child page */
    mlgf_DirectoryEntry 		entryToChild_tmp; 	/* temporary copy for new update */
    mlgf_DirectoryEntry 		overflowInChild; 	/* entry overflowed in child page */
    MLGF_HashValue 			*hx, *hy;    		/* points to arrary of hash values in an entry */
    Four 				k;
    LockReply 				lockReply; 
    LockMode 				oldMode;
    PageID    				tmpPid;
    Lsn_T 				lsn;                  	/* lsn of the newly written log record */
    Four 				logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 			logRecInfo; 		/* log record information */
    LOG_Image_MLGF_InitDirectoryPage_T 	initDirPageInfo;
    LOG_Image_MLGF_InitLeafPage_T 	initLeafPageInfo;
    SegmentID_T 			pageSegmentID;  	/* page segment ID */

    /* pointer for MLGF Data Structure of perThreadTable */
    MLGF_PerThreadDS_T *mlgf_perThreadDSptr = MLGF_PER_THREAD_DS_PTR(handle);

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_InsertRecursive(root=%P, kdesc=%P, keys=%P, oid=%P, data=%P, entryToRoot=%P, overflowInRoot=%P, lockup=%P)",
	      root, kdesc, keys, oid, data, entryToRoot, overflowInRoot, lockup));

    TR_PRINT(handle, TR_MLGF, TR1, ("recursive\n"));

    /* get lock on the root page */
    if(lockup) {
	e = LM_getKeyRangeLock(handle, &xactEntry->xactId, root, L_IX, L_MANUAL,
			       L_UNCONDITIONAL, &lockReply);
	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);

	e = LM_getFlatPageLock(handle, &xactEntry->xactId, root, L_X, L_MANUAL,
			       L_UNCONDITIONAL, &lockReply, &oldMode); 
	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK){
	    ERR(handle, eDEADLOCK);
	}

    }

    /* Read the current page into the buffer. */
    e = BfM_getAndFixBuffer(handle, root, M_FREE, &root_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (mlgf_Page*)root_BCB->bufPagePtr;

    if (apage->any.hdr.type & MLGF_LEAFPAGE) { /* leaf page */

	/* insert the new object into the leaf page */
	e = mlgf_InsertIntoLeaf(handle, xactEntry, iinfo, root, root_BCB, kdesc, keys, oid, data, entryToRoot, status, logParam); 
	if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	e = BfM_unfixBuffer(handle, root_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	if(lockup){
	    e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, root, L_MANUAL);
	    if (e < eNOERROR) ERR(handle, e);

	}

	return(eNOERROR);
    }

    /* From this point, the page is a directory page. */

    /* Initialize 'insertEntryFlag' to FALSE. */
    /* It has TRUE when we should insert a new entry into current page. */
    insertEntryFlag = FALSE;

    /* Calculate the length of a directory entry. */
    entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    /* find region to go next time */
    found = mlgf_FindEntry(handle, &apage->directory, kdesc->nKeys, keys, &entryNo);

    /*
     * If found is FALSE, it means that we are inserting
     * record into empty region.
     * So we first find mergable(buddy) region.
     * If we find one, insert record into that region,
     * else allocate new page.
     */
    if (found) {
	entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, entryNo, entryLen);

	MAKE_PAGEID(child, root->volNo, entryToChild->spid);

    } else {

	status->flags.objectInEmptyRegion = 1;

        if (status->flags.nestedTopAction == 0) {
            e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
            if (e < eNOERROR) ERR(handle, e);

            status->flags.nestedTopAction = 1;
        }

	/* find maximum region which buddy entry can contain. */
	mlgf_GetMaxRegion(handle, kdesc, &apage->directory, entryToRoot, keys, overflowInRoot);

	/* find buddy region */
	found = mlgf_FindBuddyEntry(handle, &apage->directory, kdesc->nKeys,
				    overflowInRoot, &entryNo, &buddyKey);

	if (found) {
	    entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, entryNo, entryLen);

            memcpy(overflowInRoot, entryToChild, entryLen);
            overflowInRoot->nValidBits[buddyKey] -- ;

            e = mlgf_DeleteFromDirectory(handle, xactEntry, &apage->directory, kdesc, entryNo, logParam);
            if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	    e = mlgf_InsertIntoDirectory(handle, xactEntry, &apage->directory, kdesc, overflowInRoot, logParam);
            if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

            found = mlgf_FindEntry(handle, &apage->directory, kdesc->nKeys, keys, &entryNo);
            assert(found == TRUE);

            entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, entryNo, entryLen);
	    MAKE_PAGEID(child, root->volNo, entryToChild->spid);

	    root_BCB->dirtyFlag = 1;

	} else {

	    e = mlgf_GetSegmentIDFromIndexInfo(handle, xactEntry, iinfo, &pageSegmentID, PAGESIZE2);
	    if (e < eNOERROR) ERR(handle, e);

	    e = RDsM_AllocTrains(handle, xactEntry, root->volNo, &pageSegmentID, root, 1, PAGESIZE2, FALSE, &newPid, logParam);
	    if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	    /* Read the leaf page into the buffer. */
	    e = BfM_fixNewBuffer(handle, &newPid, M_FREE, &new_BCB, PAGE_BUF);
	    if (e < 0) {
		ERRB1(handle, e, root_BCB, PAGE_BUF);
	    }

	    newPage = (mlgf_Page*)new_BCB->bufPagePtr;

	    if (apage->directory.hdr.height == 1) /* child is the leaf page. */
                MLGF_INIT_LEAF_PAGE(&newPage->leaf, apage->any.hdr.iid, newPid, kdesc->nKeys, kdesc->extraDataLen);
	    else
                MLGF_INIT_DIRECTORY_PAGE(&newPage->directory, apage->any.hdr.iid,
                                         newPid, apage->directory.hdr.height - 1, FALSE, kdesc->nKeys);

            /*
             * Write log record.
             */
            if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
                if (apage->directory.hdr.height == 1) {
                    initLeafPageInfo.iid = newPage->leaf.hdr.iid;
                    initLeafPageInfo.nKeys = kdesc->nKeys;
                    initLeafPageInfo.extraDataLen = kdesc->extraDataLen;

                    LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_MLGF_INIT_LEAF_PAGE, LOG_REDO_ONLY, 
                                          newPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(LOG_Image_MLGF_InitLeafPage_T), &initLeafPageInfo);
                } else {
                    initDirPageInfo.iid = newPage->directory.hdr.iid;
                    initDirPageInfo.height = newPage->directory.hdr.height;
                    initDirPageInfo.rootFlag = FALSE;
                    initDirPageInfo.nKeys = kdesc->nKeys;

                    LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                          LOG_ACTION_MLGF_INIT_DIRECTORY_PAGE, LOG_REDO_ONLY,
                                          newPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                          sizeof(LOG_Image_MLGF_InitDirectoryPage_T), &initDirPageInfo);
                }

                e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                if (e < eNOERROR) ERR(handle, e);

                /* mark the lsn in the page */
                newPage->any.hdr.lsn = lsn;
                newPage->any.hdr.logRecLen = logRecLen;
            }

            new_BCB->dirtyFlag = 1;

	    /* Unfix buffer for leaf page. */
	    e = BfM_unfixBuffer(handle, new_BCB, PAGE_BUF);
	    if (e < 0) {
		ERRB1(handle, e, root_BCB, PAGE_BUF);
	    }

	    overflowInRoot->spid = newPid.pageNo;
	    entryToChild = overflowInRoot;
	    MAKE_PAGEID(child, root->volNo, entryToChild->spid);
	    insertEntryFlag = TRUE;
	    entryNo = MLGF_NOENTRY;
	}
    }

    if(lockup){
	if(mlgf_IsSafeDir(handle, root_BCB, kdesc, entryNo)){
	    for(; !MLGF_LOCKSTACK_IS_EMPTY(mlgf_perThreadDSptr->mlgfLockStack); ){
		MLGF_LOCKSTACK_POP(mlgf_perThreadDSptr->mlgfLockStack, &tmpPid);
		e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, &tmpPid, L_MANUAL);
		if (e < eNOERROR) ERR(handle, e);
	    }
	}
	MLGF_LOCKSTACK_PUSH(handle, mlgf_perThreadDSptr->mlgfLockStack, root);
    }

    /* go to next page */
    memcpy(&entryToChild_tmp, entryToChild, entryLen);
    e = mlgf_InsertRecursive(handle, xactEntry, iinfo, &child, kdesc, keys, oid, data, 
			     &entryToChild_tmp, &overflowInChild, status, lockup, logParam);
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

            if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (hx[k] > hy[k])) ||
                (MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (hx[k] < hy[k]))) {
                hx[k] = hy[k];
                status->flags.mbrUpdated = 1;
            }
        }
    }

    if (insertEntryFlag == TRUE) {

        /*
        ** Update the MBR of the entry 'entryToRoot'
        */
        hx = MLGF_DIRENTRY_HASHVALUEPTR(entryToRoot, kdesc->nKeys);
        hy = MLGF_DIRENTRY_HASHVALUEPTR(overflowInRoot, kdesc->nKeys);
        for (k = 0; k < kdesc->nKeys; k++) {

            if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (hx[k] > hy[k])) ||
                (MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (hx[k] < hy[k]))) {
                hx[k] = hy[k];
                status->flags.mbrUpdated = 1;
            }
        }
    }

    if (status->flags.overflow) {
	/* Split the child page. */
	if (apage->directory.hdr.height == 1) { /* child is the leaf page. */

            assert(status->flags.nestedTopAction == 0);

            e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
            if (e < eNOERROR) ERR(handle, e);

            status->flags.nestedTopAction = 1;

	    e = mlgf_SplitLeafPage(handle, xactEntry, iinfo, &child, kdesc, keys, oid, data, 
				   &entryToChild_tmp, overflowInRoot, lockup, logParam);
	} else
	    e = mlgf_SplitDirectoryPage(handle, xactEntry, iinfo, &child, kdesc, &overflowInChild, 
					&entryToChild_tmp, overflowInRoot, lockup, logParam);

	if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	/* entryToChild was updated in the previous call. */
        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            Two tmpEntryNo = entryNo;

            LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_MLGF_MODIFY_DIRECTORY_ENTRY, LOG_REDO_UNDO,
                                  apage->any.hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(Two), &tmpEntryNo,
                                  entryLen, &entryToChild_tmp,
                                  entryLen, entryToChild);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            /* mark the lsn in the page */
            apage->any.hdr.lsn = lsn;
            apage->any.hdr.logRecLen = logRecLen;
        }

        memcpy(entryToChild, &entryToChild_tmp, entryLen);
	root_BCB->dirtyFlag = 1;	/* set the dirty flag */

        status->flags.overflow = 0;
	insertEntryFlag = TRUE;
    }

    if (insertEntryFlag) {
	if (apage->directory.hdr.nEntries < MLGF_MAX_DIRENTRIES(kdesc->nKeys)) {

	    /* Insert a new entry. */
	    e = mlgf_InsertIntoDirectory(handle, xactEntry, &apage->directory, kdesc, overflowInRoot, logParam);
            if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	    entryToRoot->theta += entryLen;
            status->flags.thetaUpdated = 1;

	    root_BCB->dirtyFlag = 1;	/* set the dirty flag */

	} else
	    status->flags.overflow = 1;
    }

    /* Unfix buffer for current page. */
    e = BfM_unfixBuffer(handle, root_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_InsertRecursive() */
