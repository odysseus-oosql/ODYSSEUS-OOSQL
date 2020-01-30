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
 * Module: btm_InsertLeaf.c
 *
 * Description: Insert the given key into the searched leaf.
 *
 * Exports:
 *  Four btm_InsertLeaf(Four, btm_TraversePath*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*)
 */

#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "LM.h"
#include "LOG.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal function prototypes */
Four btm_CheckLeafSpace(Four, Buffer_ACC_CB*, Four, KeyValue*, Boolean);
Four btm_InsertLeafNoCheck(Four, XactTableEntry_T*, BtreeIndexInfo*, Buffer_ACC_CB*, Four, KeyValue*, ObjectID*, Boolean, PageID*, KeyDesc*, LogParameter_T*);

/*
 * Function: Four btm_InsertLeaf(Four, btm_TraversePath*, KeyDesc*, KeyValue*, ObjectID*, LockParameter*)
 *
 * Description: Insert the given key into the searched leaf
 *              using path information.
 *
 * CAUTION :: Calling routine must acquire the latch(es) of child_BCB
 *            (and parent_BCB, if exists).
 *
 *            When this routine returns ....
 *            CASE eNOERROR : after releasing these latches.
 *            CASE BTM_RETRAVERSE : after release these latches.
 *                                 and with the maybe correct half path.
 *            CASE BTM_NOSPACE : without release the latch of child_BCB
 *                               and with the correct full path.
 *            CASE error : without release any latch or any lock.
 *
 * Returns:
 *  eNOERROR
 *  BTM_NOSPACE
 *  BTM_RETRAVERSE
 *  Error code
 *    some errors caused by function calls
 */
Four btm_InsertLeaf(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo* iinfo,	/* IN index information */ 
    FileID     *fid,            /* IN FileID */ 
    btm_TraversePath *path,	/* IN Btree traverse path stack */
    KeyDesc  *kdesc,		/* IN Btree Key Descriptor */
    KeyValue *kval,		/* IN key value to be inserted */
    ObjectID *oid,		/* IN ObjectID to insert */
    LockParameter *lockup,	/* IN request lock or not */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e, e1;			/* error number */
    Four state;			/* state of the program */
    Four slotNo;		/* slot no */
    Boolean found;		/* search result */
    PageID ovPid;		/* PageID of overflow page */
    BtreeLeaf *apage;		/* pointer to buffer holding a leaf page */
    Lsn_T parent_LSN;		/* log sequence number of parent page */
    Lsn_T leaf_LSN;		/* log sequence number of a leaf page */
    Buffer_ACC_CB *parent_BCB;	/* buffer control block for parent page */
    Buffer_ACC_CB *leaf_BCB;	/* buffer control block for leaf page */


    TR_PRINT(handle, TR_BTM, TR1, ("btm_InsertLeaf()"));


    /* Get the buffer control block for the leaf. */
    e = btm_PopElemFromPath(handle, path, &leaf_BCB, &leaf_LSN);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the buffer control block for the parent. */
    if (btm_IsEmptyPath(handle, path)) {
	parent_BCB = NULL;
    } else {
	e = btm_ReadTopElemFromPath(handle, path, &parent_BCB, &parent_LSN);
  	if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
    }

    apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

    if (apage->hdr.statusBits != 0) { /* SM_BIT == 1 || DELETE_BIT === 1 */
        /* this routine release the parent latch */
	e = btm_ResetStatusBitsLeaf(handle, path, kdesc, kval, leaf_BCB,
				    (parent_BCB == NULL) ? NULL:parent_BCB->latchPtr );
	if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

	if (e == BTM_RETRAVERSE) {
	    /* unfix buffer and restart the search from the parent. */
	    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);

	    return(BTM_RETRAVERSE);
        }
    } else {
	/* Release the parent latch. */
	if (parent_BCB != NULL) {
	    e = SHM_releaseLatch(handle, parent_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
	}
    }

    for (state = 2; state != 5; ) {
        switch (state) {
          case 1:		/* reset the status bits of leaf page */
            if (apage->hdr.statusBits != 0) {
                e = btm_ResetStatusBitsLeaf(handle, path, kdesc, kval, leaf_BCB, NULL);
                if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

                if (e == BTM_RETRAVERSE) {
                    /* Release the holding resources and restart the search from the parent. */
                    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
                    if (e < eNOERROR) ERR(handle, e);

                    return(BTM_RETRAVERSE);
                }
            }

            state = 2;

            break;

          case 2:		/* check unique index constraint */
            if (kdesc->flag & KEYFLAG_UNIQUE ) {
                e = btm_CheckDuplicatedKey(handle, xactEntry, fid, leaf_BCB, kdesc, kval, lockup); 
                if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

                if (e == BTM_RETRAVERSE ) {

                    /* Release the holding resources and restart the search from the parent. */
                    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
                    if (e < eNOERROR) ERR(handle, e);

                    return(BTM_RETRAVERSE);
                }

                if (e == BTM_RELATCHED) state = 1;
                else state = 3;	/* e == eNOERROR */

            } else state = 3;

            break;

          case 3:              
            if (lockup) state = 4;
            else state = 5;

            break;

          case 4:		/* lock the next object */

            /* Get an instant duration X lock on the next object. */
                e = btm_GetNextObjectLock(handle, xactEntry, fid, path, leaf_BCB, kdesc, kval, oid, BTM_INSERT, &ovPid);
                if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

                if (e == BTM_RETRAVERSE || e == BTM_FOUND) {

                    e1 = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
                    if (e1 < 0) ERR(handle, e1);

                    if (e == BTM_RETRAVERSE)
                        return(BTM_RETRAVERSE);
                    else
                        ERR(handle, eDUPLICATEDOBJECTID_BTM);

            }

            if (e == BTM_RELATCHED) state = 1;
            else state = 5;	/* e == eNOERROR */


            break;
        }
    }

    /* Search the leaf to find the insert position. */
    found = btm_BinarySearchLeaf(handle, apage, kdesc, kval, &slotNo);

    if (!found) slotNo++;

    /* Is there enough space in the leaf page? */
    e = btm_CheckLeafSpace(handle, leaf_BCB, slotNo, kval, found);
    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

    if (e == BTM_NOSPACE) {
	e = btm_PushElemIntoPath(handle, path, leaf_BCB, &apage->hdr.lsn);
  	if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);


        /* return without releasing latch of leaf_BCB if there is not enough space */
	return(BTM_NOSPACE);
    }

    /* Insert the ObjectID in the original page. */
    /* If lockup is NULL ovPid is not initialized */
    e = btm_InsertLeafNoCheck(handle, xactEntry, iinfo, leaf_BCB, slotNo, kval, oid, found, 
			      (lockup) ? &ovPid:NULL, kdesc, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* Logs the insert. */
    /* ASSIGN_LSN(apage->lsn,???); */

    /* Release the holding latches. */
    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* Returns with success status. */
    return(eNOERROR);

} /* btm_InsertLeaf( ) */



/*
 * Function: Four btm_CheckLeafSpace(Four, Buffer_ACC_CB*, Four, KeyValue*, ObjectID*, Boolean)
 *
 * Description:
 *  Check if there is enough space needed for inserting the given object.
 *
 * Returns:
 *  eNOERROR : There is enough space.
 *  BTM_NOSPACE : There is not enough space.
 */
Four btm_CheckLeafSpace(
    Four	handle,
    Buffer_ACC_CB *bcb,		  /* IN buffer control block for updated page */
    Four insertSlotNo,		  /* IN slot where the new entry to place */
    KeyValue *kval,		  /* IN key value of the inserted ObjectID */
    Boolean duplicateFlag)	  /* IN TRUE if the same key exists */
{
    Four entryLen;		/* length of the new entry */
    Four neededSpace;		/* amount of space needed to insert the entry */
    BtreeLeaf *apage;		/* a Btree leaf page */
    btm_LeafEntry *entry;	/* a leaf entry */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_CheckLeafSpace(bcb=%P, insertSlotNo=%ld, kval=%P, duplicateFlag=%ld)",
	      bcb, insertSlotNo, kval, duplicateFlag));


    apage = (BtreeLeaf*)bcb->bufPagePtr;


    /* length of the inserted entry */
    if (duplicateFlag) {
	entry = (btm_LeafEntry*)&apage->data[apage->slot[-insertSlotNo]];

	if (entry->nObjects > 0) { /* normal entry */
	    entryLen = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED+entry->klen) +
		(entry->nObjects+1)*OBJECTID_SIZE;

	    if (entryLen <= OVERFLOW_SPLIT && OBJECTID_SIZE > BL_FREE(apage)) {
		return(BTM_NOSPACE); /* split should occur */
	    }
	}

    } else {			/* A new key is inserted. */
	entryLen = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED + kval->len) + OBJECTID_SIZE;
	neededSpace = entryLen + sizeof(Two); /* add slot space */

	if (neededSpace > BL_FREE(apage)) /* no more space */
	    return(BTM_NOSPACE);	  /* split should occur */
    }

    return(eNOERROR);		/* dummy return */

} /* btm_CheckLeafSpace() */



/*
 * Function: Four btm_InsertLeafNoCheck(Four, Buffer_ACC_CB*, Four, KeyValue*, ObjectID*, Boolean, KeyDesc*)
 *
 * Description:
 *  Insert an object into the leaf page. This routine assumes that there is
 *  enough space needed for the inserted object.
 *
 * Returns:
 *  Error code
 *    some errors caused by functin calls
 */
Four btm_InsertLeafNoCheck(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo* iinfo,	/* IN index information */ 
    Buffer_ACC_CB *leaf_BCB,	/* IN buffer control block for updated page */
    Four insertSlotNo,		/* IN slot where the new entry to place */
    KeyValue *kval,		/* IN key value of the inserted ObjectID */
    ObjectID *oid,		/* IN ObjectID to be inserted */
    Boolean duplicateFlag,	/* IN TRUE if the same key exists */
    PageID *oid_ovPid,		/* IN PageID of the overflow page */
    KeyDesc *keyDesc,           /* IN key descriptor for logging */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error code */
    Four offset;		/* starting offset of array of ObjectIDs or first overflow page's PageID */
    Four elemNo;		/* index on array of ObjectIDs  */
    Four entryLen;		/* length of the new entry */
    Four neededSpace;		/* amount of space needed to store entry */
    ObjectID *oidArray;		/* array of ObjectIDs */
    BtreeLeaf *apage;		/* a Btree leaf page */
    btm_LeafEntry *entry;	/* a leaf entry */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* information for a log record */
    LOG_Image_BtM_OidInLeafEntry_T insertOidInfo;
    PageID ovPid;		/* PageID of the overflow page */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_InsertLeafNoCheck(leaf_BCB=%P, insertSlotNo=%ld, kval=%P, oid=%P, duplicateFlag=%P)",
	      leaf_BCB, insertSlotNo, kval, oid, duplicateFlag));


    apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;


    /* length of the inserted entry */
    if (duplicateFlag) {
	entry = (btm_LeafEntry*)&apage->data[apage->slot[-insertSlotNo]];
        offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED+entry->klen) - BTM_LEAFENTRY_FIXED;

	if (entry->nObjects > 0) { /* normal entry */
	    entryLen = BTM_LEAF_ENTRY_LENGTH(entry->klen, entry->nObjects);

	    if (entryLen + OBJECTID_SIZE > OVERFLOW_SPLIT) {

		/* make an overflow chain */
		e = btm_CreateOverflow(handle, xactEntry, iinfo, leaf_BCB, insertSlotNo, logParam); 
		if (e < eNOERROR) ERR(handle, e);

                /* Insert the new object to the overflow page */
                MAKE_PAGEID(ovPid, apage->hdr.pid.volNo, *((ShortPageID*)&entry->kval[offset]));
                e = btm_InsertOverflow(handle, xactEntry, iinfo, &ovPid, keyDesc, kval, oid, logParam); 
                if (e < eNOERROR) ERR(handle, e);

	    } else if (OBJECTID_SIZE <= BL_FREE(apage)) { /* enough space */
		/* Find the correct position which the given object is inserted */
		oidArray = (ObjectID*)&entry->kval[offset];

		e = btm_BinarySearchOidArray(handle, oidArray, oid, entry->nObjects, &elemNo);

		if (e) ERR(handle, eDUPLICATEDOBJECTID_BTM);

                /*
                 * Write log record.
                 */
                if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

                    insertOidInfo.slotNo = insertSlotNo;
                    insertOidInfo.oidArrayElemNo = elemNo + 1;
                    insertOidInfo.oid = *oid;

                    /*
                     *  fill in the fields for log record information
                     */
                    if (logParam->logFlag & LOG_FLAG_UNDO) {
                        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_COMPENSATION,
                                              LOG_ACTION_BTM_INSERT_OID_INTO_LEAF_ENTRY, LOG_REDO_ONLY,
                                              apage->hdr.pid, xactEntry->lastLsn, logParam->undo.undoNextLsn,
                                              sizeof(LOG_Image_BtM_OidInLeafEntry_T), &insertOidInfo);
                    } else {
                        LOG_Image_BtM_IndexInfo_T logImage;

                        /* set logImage */
                        logImage.iid = iinfo->iid;
                        logImage.catEntry = iinfo->catalog.oid;

                        LOG_FILL_LOGRECINFO_4(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                              LOG_ACTION_BTM_INSERT_OID_INTO_LEAF_ENTRY, LOG_REDO_UNDO,
                                              apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                              sizeof(LOG_Image_BtM_OidInLeafEntry_T), &insertOidInfo,
                                              sizeof(LOG_Image_BtM_IndexInfo_T), &logImage,
                                              KEYDESC_USED_SIZE(keyDesc), keyDesc,
                                              entry->klen, entry->kval);
                    }

                    /*
                     *  write the log record into the log file
                     */
                    e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
                    if (e < eNOERROR) ERR(handle, e);

                    apage->hdr.lsn = lsn;
                    apage->hdr.logRecLen = logRecLen;
                } else {
                    INCREASE_LSN_BY_ONE(apage->hdr.lsn);
                }


		if (apage->slot[-insertSlotNo] + entryLen == apage->hdr.free &&
		    BL_CFREE(apage) >= OBJECTID_SIZE) {

		    apage->hdr.free += OBJECTID_SIZE;

		} else if (BL_CFREE(apage) >= entryLen + OBJECTID_SIZE) {

		    apage->slot[-insertSlotNo] = apage->hdr.free;
		    memcpy(&(apage->data[apage->slot[-insertSlotNo]]), entry,
			  entryLen);

		    entry = (btm_LeafEntry*)&apage->data[apage->slot[-insertSlotNo]];
		    oidArray = (ObjectID*)&entry->kval[offset];

		    apage->hdr.free += entryLen + OBJECTID_SIZE;
		    apage->hdr.unused += entryLen;

		} else {
		    btm_CompactLeafPage(handle, apage, insertSlotNo);

		    entry = (btm_LeafEntry*)&(apage->data[apage->slot[-insertSlotNo]]);
		    oidArray = (ObjectID*)&entry->kval[offset];

		    apage->hdr.free += OBJECTID_SIZE;
		}

		/* Reserve space for (elemNo+1)-th element. */
                BTM_INSERT_OIDS_SPACE_INTO_OID_ARRAY(oidArray, entry->nObjects, elemNo+1, 1);
		oidArray[elemNo+1] = *oid;

		entry->nObjects ++;

	    }

	} else {		   /* overflow page */
	    if (oid_ovPid == NULL) {
		offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED+entry->klen) - BTM_LEAFENTRY_FIXED;

		/* Get the PageID of the overflow page. */
		MAKE_PAGEID(ovPid, apage->hdr.pid.volNo, *((ShortPageID*)&entry->kval[offset]));

		oid_ovPid = &ovPid;
	    }

	    e = btm_InsertOverflow(handle, xactEntry, iinfo, oid_ovPid, keyDesc, kval, oid, logParam); 
	    if (e < eNOERROR) ERR(handle, e);

	    /* It is unnecessary to set the dirty flag of the given page. */
	    return(eNOERROR);
	}

    } else {			/* A new key is inserted. */

	entryLen = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED + kval->len) + OBJECTID_SIZE;

	neededSpace = entryLen + sizeof(Two); /* add slot space */

	if (neededSpace > BL_CFREE(apage))
	    btm_CompactLeafPage(handle, apage, NIL);

	/* Move slots for a new slot */
        BTM_INSERT_SLOTS_IN_BTREE_PAGE(apage, insertSlotNo, 1);

	/* Store the new entry */
	apage->slot[-insertSlotNo] = apage->hdr.free;
	entry = (btm_LeafEntry*)&(apage->data[apage->slot[-insertSlotNo]]);

	entry->nObjects = 1;
	entry->klen = kval->len;
	memcpy(&(entry->kval[0]), &(kval->val[0]), entry->klen);
	offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED+entry->klen) - BTM_LEAFENTRY_FIXED;
	*((ObjectID*)&(entry->kval[offset])) = *oid;

	apage->hdr.free += entryLen;	/* slot size is not included */
	apage->hdr.nSlots++;


        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
            Two tmp_insertPosition = insertSlotNo; /* type Two variable */

            if (logParam->logFlag & LOG_FLAG_UNDO) {
                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_COMPENSATION,
                                      LOG_ACTION_BTM_INSERT_OID_WITH_LEAF_ENTRY, LOG_REDO_ONLY,
                                      apage->hdr.pid, xactEntry->lastLsn, logParam->undo.undoNextLsn,
                                      sizeof(Two), &tmp_insertPosition,
                                      entryLen, entry);
            } else {
                LOG_Image_BtM_IndexInfo_T logImage;

                /* set logImage */
                logImage.iid = iinfo->iid;
                logImage.catEntry = iinfo->catalog.oid;

                LOG_FILL_LOGRECINFO_4(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_BTM_INSERT_OID_WITH_LEAF_ENTRY, LOG_REDO_UNDO,
                                      apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(Two), &tmp_insertPosition,
                                      entryLen, entry,
                                      sizeof(LOG_Image_BtM_IndexInfo_T), &logImage,
                                      KEYDESC_USED_SIZE(keyDesc), keyDesc);
            }

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            apage->hdr.lsn = lsn;
            apage->hdr.logRecLen = logRecLen;
        } else {
            INCREASE_LSN_BY_ONE(apage->hdr.lsn); 
        }

    }

    /* Set the dirty flag of the given page. */
    leaf_BCB->dirtyFlag = 1;

    return(eNOERROR);

} /* btm_InsertLeafNoCheck() */






