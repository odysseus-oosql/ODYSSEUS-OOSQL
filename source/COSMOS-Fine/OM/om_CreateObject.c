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
 * Module : om_CreateObject.c
 *
 * Description :
 *  om_CreateObject( ) creates a new object near the specified object; the near
 *  page is the page holding the near object.
 *  If there is no room in the near page and the near object 'nearObj' is not
 *  NULL, a new page is allocated for object creation (In this case, the newly
 *  allocated page is inserted after the near page in the list of pages
 *  consiting in the file).
 *  If there is no room in the near page, the new object will be out into the
 *  newly allocated page(in this case, the newly allocated page is appended at
 *  the tail of the list of pages consisting in the file).
 *  If the near object 'nearObj' is NULL, it trys to create a new object in the
 *  last page. If fail, then the new object will be put into the newly allocated
 *  page(In this case, the newly allocated page is appended at the tail of the
 *  list of pages consisting in the file).
 *
 * Exports:
 *  Four om_CreateObject(Four, DataFileInfo*, ObjectID*,
 *                       ObjectHdr*, Four, char*, LockParameter*, ObjectID*)
 *
 * Return Values :
 *  Error Code
 *    eBADCATOBJ_OM
 *    eBADOBJECTID_OM_OM
 *    eDEADLOCK_OM
 *    some errors caused by fuction calls
 *
 * Side Effects :
 *  An x-lock is placed on the page which the new object has been inserted into.
 */

#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"		/* for tracing : TR_PRINT(handle, ) macro */
#include "latch.h"
#include "LOG.h"
#include "RDsM.h"		/* for the raw disk manager call */
#include "BfM.h"		/* for the buffer manager call */
#include "OM.h"
#include "LM.h"
#include "TM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"

Four om_GetNeighborExtNo(Four, PageID *, Four *, Four *);


Four om_CreateObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    ObjectID 	*nearObj,	/* IN create the new object near this object */
    ObjectHdr	*objHdr,	/* IN from which tag & properties are set */
    Four	length,		/* IN amount of data */
    char	*data,		/* IN the initial data for the object */
    LockParameter *lockup,      /* IN request lock or not  */
    ObjectID	*oid,		/* OUT the object's ObjectID */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four        e;		/* error number */
    Four	neededSpace;	/* space needed to put new object [+ header] */
    SlottedPage *apage;		/* pointer to the slotted page buffer */
    Buffer_ACC_CB *aPage_BCBP;  /* buffer access control block for a data page */
    SlottedPage *newPage;       /* pointer to the newly allocated slotted page buffer */
    Buffer_ACC_CB *newPage_BCBP;/* buffer access control block holding newly allocated data */
    Four        alignedLen;	/* aligned length of initial data */
    PageID      pid;            /* PageID in which new object to be inserted */
    PageID      newPid;		/* PageID of the newly allocated */
    PageID      lastPid;        /* Last PageID in catalog */
    Four        firstExt;	/* first Extent No of the file */
    Object      *obj;		/* point to the newly created object */
    Four        freeSlotNo;	/* Slot No of a free slot */
    Boolean     allocFlag;
    LockReply   lockReply;	/* lock reply */
    LockMode oldMode;
    SlottedPage *catPage;  	/* pointer to buffer containing the catalog */
    sm_CatOverlayForData *catEntry;/* pointer to data file catalog information */
    Buffer_ACC_CB *catPage_BCBP;  /* buffer access control block holding catalog data */
    Boolean     addSlotFlag;	/* add a new slot if TRUE */
    Boolean     checkFlag;	/* TRUE when we should do the check */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_OM_ObjectInPage_T objInfo; /* object creation information */
    Boolean pageUpdateFlag;
    Boolean     needToAllocPage = FALSE;
    PageID      prevPid;
    PageID      nextPid;
    Four        prevExtNo;
    Four        nextExtNo;
    SegmentID_T pageSegmentID;   	/* page segment ID */
    Boolean	isLockAcquired = FALSE;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_OM, TR1,
	     ("om_CreateObject(handle, finfo=%P, nearObj=%P, objHdr=%P, length=%ld, data=%P, lockup=%P, oid=%P)",
	      finfo, nearObj, objHdr, length, data, lockup, oid));

    /* parameter checking */
    if (finfo == NULL) ERR(handle, eBADCATOBJ);

    if (objHdr == NULL) ERR(handle, eBADOBJECTID_OM);

    if (oid == NULL) ERR(handle, eBADPARAMETER);


    /* at first, look up the near page */
    if (nearObj != NULL && !IS_NILOBJECTID(*nearObj)) {	/* check if 'nearObj' is NILOBJECTID. */
	/* create the object in the near page if possible */

	/* get the near page's PageID from nearObj */
	pid = *((PageID *)nearObj);

#ifdef CCPL
	if(lockup){
	    e = LM_getPageLock(handle, &xactEntry->xactId, &pid, &finfo->fid,
			       lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	    if (e < eNOERROR) ERR(handle, e);

	    if(lockReply == LR_DEADLOCK){
		ERR(handle, eDEADLOCK);     /* deadlock */
	    }
	}

	e = BfM_getAndFixBuffer(handle, &pid, M_FREE, &aPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	e = BfM_getAndFixBuffer(handle, &pid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

	/* check the nearPage is included in the file */
	if (!EQUAL_FILEID(finfo->fid, apage->header.fid)) {
#ifdef CCPL
	    ERRB1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
	    ERRBL1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
        }

	/* check if nearObj is the valid ObjectID */
	if (!IS_VALID_OBJECTID(nearObj, apage)) {
#ifdef CCPL
            ERRB1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
            ERRBL1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
        }

    } else {
	/* at first, look up the last page */

	/* use the last page */
	if (finfo->tmpFileFlag) {
	    /* temporary file */
	    MAKE_PAGEID(pid, finfo->fid.volNo, finfo->catalog.entry->data.lastPage);
	} else {
	    /* ordinary file; get the last page from the catalog table */

#ifdef CCPL
	    for (checkFlag = FALSE; ; checkFlag = TRUE) {

		/* Request X lock on the page where the catalog entry resides. */
                if (lockup != NULL) {
                    e = LM_getFlatPageLock(handle, &xactEntry->xactId, (PageID*)&finfo->catalog.oid,
                                           L_S, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode); 
                    if (e < eNOERROR) ERR(handle, e);

                    if (lockReply == LR_DEADLOCK) {
                        ERR(handle, eDEADLOCK); /* deadlock */
                    }
                }

		/* get the last page in catalog */
		e = BfM_getAndFixBuffer(handle, (TrainID*)&finfo->catalog.oid, M_FREE, &catPage_BCBP, PAGE_BUF);
		if (e < eNOERROR) ERR(handle, e);

		catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;
		GET_PTR_TO_CATENTRY_FOR_DATA(finfo->catalog.oid.slotNo, catPage, catEntry);

		/* get the last page */
		MAKE_PAGEID(lastPid, finfo->fid.volNo, catEntry->lastPage);

		e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
		if(e < eNOERROR) ERR(handle, e);

		/* Release the lock on the catalog page. */
                if (lockup != NULL) {
                    e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, (PageID*)&finfo->catalog.oid, L_MANUAL);
                    if (e < eNOERROR) ERR(handle, e);
                }

		if (checkFlag) {
		    /* Be sure we get the correct last page. */
		    /* The confirmation is required because we released the latch. */
		    if(EQUAL_PAGEID(lastPid, pid)) break;

		    if(lockup){
			e = LM_releasePageLock(handle, &xactEntry->xactId, &pid, L_MANUAL);
			if (e < eNOERROR) ERR(handle, e);
		    }
		}

		pid = lastPid;

		if(lockup){
		    e = LM_getPageLock(handle, &xactEntry->xactId, &pid, &finfo->fid,
				       L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
		    if (e < eNOERROR) ERR(handle, e);

		    if(lockReply == LR_DEADLOCK){
			ERR(handle, eDEADLOCK); /* deadlock */
		    }
		}
	    } /* end of for loop */
#endif /* CCPL */

#ifdef CCRL
            /* get the last page in catalog */
            e = BfM_getAndFixBuffer(handle, (TrainID*)&finfo->catalog.oid, M_SHARED, &catPage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;
            GET_PTR_TO_CATENTRY_FOR_DATA(finfo->catalog.oid.slotNo, catPage, catEntry);

            /* get the last page */
            MAKE_PAGEID(pid, finfo->fid.volNo, catEntry->lastPage);

            e = SHM_releaseLatch(handle, catPage_BCBP->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1(handle, e, catPage_BCBP, PAGE_BUF);

            e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
            if(e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

            /* Now, we got the correct last page */
	}

	/* Read the last page into the buffer. */
#ifdef CCPL
	e = BfM_getAndFixBuffer(handle, &pid, M_FREE, &aPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	e = BfM_getAndFixBuffer(handle, &pid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	apage = (SlottedPage *)aPage_BCBP->bufPagePtr;
    }

    alignedLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(length));

    if (objHdr->properties & P_FORWARDED) {
        /* Do not use this page, if this object is forwarded object from this page. */
        allocFlag = FALSE;

    } else {
        /* Find a free slot index. */
#ifdef CCPL
        for (freeSlotNo = 0;
             freeSlotNo < apage->header.nSlots && apage->slot[-freeSlotNo].offset != EMPTYSLOT;
             freeSlotNo++);
#endif /* CCPL */

#ifdef CCRL
        for(freeSlotNo = 0; freeSlotNo < apage->header.nSlots; freeSlotNo++)
            if (apage->slot[-freeSlotNo].offset == EMPTYSLOT) {

                if ( lockup ) {
                    /* check if this slot is involved in uncommmited transaction */
                    MAKE_OBJECTID(*oid, pid.volNo, pid.pageNo, freeSlotNo, apage->slot[-freeSlotNo].unique);
                    e = LM_getObjectLock(handle,  &xactEntry->xactId, oid, &finfo->fid,
                                          lockup->mode, lockup->duration, L_CONDITIONAL, &lockReply, &oldMode);
                    if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

                    if ( lockReply != LR_NOTOK ) {
						isLockAcquired = TRUE;
						break;
				    }

                }
                else
                    break;
            }
#endif /* CCRL */

        /* If there is no empty slot, add a new slot. */
        addSlotFlag = (freeSlotNo == apage->header.nSlots) ? TRUE:FALSE;

        /*
         * calculate the length to be needed in the slotted page.
         * If need to create the large object, the slotted page only contains
         * object header.
         */
        neededSpace = sizeof(ObjectHdr) + alignedLen + ((addSlotFlag) ? sizeof(SlottedPageSlot):0);

#ifdef CCPL
        allocFlag = (neededSpace <= SP_FREE(xactEntry->xactId, apage)) ? TRUE:FALSE;
#endif /* CCPL */

#ifdef CCRL
        e = om_AcquireSpace(handle, &xactEntry->xactId, apage, neededSpace,
                            (addSlotFlag ? sizeof(SlottedPageSlot):0),
                            &allocFlag, &pageUpdateFlag);
        if (pageUpdateFlag == TRUE) aPage_BCBP->dirtyFlag = 1;
        if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    /*
    ** If the page has no enough room, then use a new page
    */
    if (allocFlag) {
	/* There is enough space. */

	/* prepare space for object inserted */
	if (SP_CFREE(apage) < neededSpace)
	    (void) om_CompactPage(handle, apage, NIL);
				
	/* if no empty slot, then add a new slot */
	/* if (addSlotFlag) apage->header.nSlots++;*/ /* increment # of slots */
	if (addSlotFlag) {
	    do{
		/* add an empty slot */
		/* at this point, apage->header.nSlots == freeSlotNo */
		apage->slot[-(apage->header.nSlots)].offset = EMPTYSLOT;
	    	apage->header.nSlots++;

                if ( lockup ) {
                    /* check if this slot is involved in uncommmited transaction */
                    MAKE_OBJECTID(*oid, pid.volNo, pid.pageNo, freeSlotNo, apage->slot[-freeSlotNo].unique);
                    e = LM_getObjectLock(handle,  &xactEntry->xactId, oid, &finfo->fid,
                                          lockup->mode, lockup->duration, L_CONDITIONAL, &lockReply, &oldMode);
                    if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

                    if ( lockReply != LR_NOTOK ) {
		        isLockAcquired = TRUE;
			break;
		    }
                }
                else {
                    break;
		}

		freeSlotNo++;

		/* an empty slot is added, thus we should prepare space for object inserted */
		if(SP_CFREE(apage) < neededSpace){
	    	    (void) om_CompactPage(handle, apage, NIL);
		}

	    } while(SP_CFREE(apage) >= neededSpace); 

	    if( lockup!=NULL && isLockAcquired == FALSE ){
		/* It must not be happened */
		ERRBL1(handle, eINTERNAL, aPage_BCBP, PAGE_BUF);
	    }
	}

    } else {

        /* check whether neighbor pages have enough space */

        /* get 'prevPid' & 'nextPid' */
        MAKE_PAGEID(prevPid, pid.volNo, apage->header.prevPage);
        MAKE_PAGEID(nextPid, pid.volNo, apage->header.nextPage);

#ifdef CCRL
        e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

        /* free buffer page */
        e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        /* Note!! this while loop is executed only once!! */
        while (1) {

            /* first, check next page */
            if( !IS_NILPAGEID(nextPid) ) {

#ifdef CCPL
		if(lockup){
		    e = LM_getPageLock(handle, &xactEntry->xactId, &nextPid, &finfo->fid,
				       L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
		    if (e < eNOERROR) ERR(handle, e);

		    if(lockReply == LR_DEADLOCK){
			ERR(handle, eDEADLOCK); /* deadlock */
		    }
		}

                /* get next page into buffer pool */
	        e = BfM_getAndFixBuffer(handle, &nextPid, M_FREE, &aPage_BCBP, PAGE_BUF);
	        if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
                /* get next page into buffer pool */
	        e = BfM_getAndFixBuffer(handle, &nextPid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF);
	        if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	        apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

#ifdef CCPL
                /* Find a free slot index in next page */
                for (freeSlotNo = 0;
                     freeSlotNo < apage->header.nSlots && apage->slot[-freeSlotNo].offset != EMPTYSLOT;
	             freeSlotNo++);
#endif /* CCPL */

#ifdef CCRL
                for(freeSlotNo = 0; freeSlotNo < apage->header.nSlots; freeSlotNo++)
                    if (apage->slot[-freeSlotNo].offset == EMPTYSLOT) {

                        if ( lockup ) {
                            /* check if this slot is involved in uncommmited transaction */
                            MAKE_OBJECTID(*oid, nextPid.volNo, nextPid.pageNo, freeSlotNo, apage->slot[-freeSlotNo].unique);

                            e = LM_getObjectLock(handle,  &xactEntry->xactId, oid, &finfo->fid,
                                                  lockup->mode, lockup->duration, L_CONDITIONAL, &lockReply, &oldMode);
                            if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

                            /* if ( lockReply != LR_NOTOK ) break; */
                            if ( lockReply != LR_NOTOK ){
				isLockAcquired = TRUE;
 				break;
			    }
                        }
                        else
                            break;
                    }
#endif /* CCRL */

                /* If there is no empty slot in next page, add a new slot. */
                addSlotFlag = (freeSlotNo == apage->header.nSlots) ? TRUE:FALSE;

                /* recalculate 'neededSpace' */
                neededSpace = sizeof(ObjectHdr) + alignedLen + ((addSlotFlag) ? sizeof(SlottedPageSlot):0);

#ifdef CCPL
                allocFlag = (neededSpace <= SP_FREE(xactEntry->xactId, apage)) ? TRUE:FALSE;
#endif /* CCPL */

#ifdef CCRL
                e = om_AcquireSpace(handle, &xactEntry->xactId, apage, neededSpace,
                                    (addSlotFlag ? sizeof(SlottedPageSlot):0),
                                    &allocFlag, &pageUpdateFlag);
                if (pageUpdateFlag == TRUE) aPage_BCBP->dirtyFlag = 1;
                if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

	        /* if next page have enough space */
	        if (allocFlag) {

                    /* update 'pid' to 'nextPid' */
                    pid = nextPid;

	            /* prepare space for object insert */
	            if (SP_CFREE(apage) < neededSpace)
	                (void) om_CompactPage(handle, apage, NIL);

		    /* if no empty slot, then add a new slot */
		    /* if (addSlotFlag) apage->header.nSlots++;*/ /* increment # of slots */
		    if (addSlotFlag) {
	    		do{
			    /* add an empty slot */
			    /* at this point, apage->header.nSlots == freeSlotNo */
			    apage->slot[-(apage->header.nSlots)].offset = EMPTYSLOT;
	    		    apage->header.nSlots++;

                	    if ( lockup ) {
                    		/* check if this slot is involved in uncommmited transaction */
                    		MAKE_OBJECTID(*oid, pid.volNo, pid.pageNo, freeSlotNo, apage->slot[-freeSlotNo].unique);
                    		e = LM_getObjectLock(handle,  &xactEntry->xactId, oid, &finfo->fid,
                                              lockup->mode, lockup->duration, L_CONDITIONAL, &lockReply, &oldMode);
                    		if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

                    		if ( lockReply != LR_NOTOK ) {
		        	    isLockAcquired = TRUE;
				    break;
		    		}
                	    } else {
                    		break;
			    }

			    freeSlotNo++;

			    /* an empty slot is added, thus we should prepare space for object inserted */
			    if(SP_CFREE(apage) < neededSpace){
	    	    		(void) om_CompactPage(handle, apage, NIL);
			    }

	    		} while(SP_CFREE(apage) >= neededSpace); 

	    		if( lockup!=NULL && isLockAcquired == FALSE ){
			    /* It must not be happened */
			    ERRBL1(handle, eINTERNAL, aPage_BCBP, PAGE_BUF);
	    		}
		    } /* if (addSlotFlag) */	

                    break;
	        }

#ifdef CCRL
                e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex); 
                if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

	        /* free buffer for next page */
                e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
                if (e < eNOERROR) ERR(handle, e);

#ifdef CCPL
                if (!IS_NILPAGEID(nextPid) && lockup) {  
                    e = LM_releasePageLock(handle, &xactEntry->xactId, &nextPid, L_MANUAL);
                    if (e < eNOERROR) ERR(handle, e);
                }
#endif /* CCPL */
            }

            /* second, check previous page */
            if( !IS_NILPAGEID(prevPid) ) {

#ifdef CCPL
		if(lockup){
		    e = LM_getPageLock(handle, &xactEntry->xactId, &prevPid, &finfo->fid,
				       L_X, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
		    if (e < eNOERROR) ERR(handle, e);

		    if(lockReply == LR_DEADLOCK){
			ERR(handle, eDEADLOCK); /* deadlock */
		    }
		}

                /* get previous page into buffer pool */
	        e = BfM_getAndFixBuffer(handle, &prevPid, M_FREE, &aPage_BCBP, PAGE_BUF);
	        if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
                /* get next page into buffer pool */
	        e = BfM_getAndFixBuffer(handle, &prevPid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF);
	        if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	        apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

#ifdef CCPL
                /* Find a free slot index in previous page */
                for (freeSlotNo = 0;
                     freeSlotNo < apage->header.nSlots && apage->slot[-freeSlotNo].offset != EMPTYSLOT;
	             freeSlotNo++);
#endif /* CCPL */

#ifdef CCRL
                for(freeSlotNo = 0; freeSlotNo < apage->header.nSlots; freeSlotNo++)
                    if (apage->slot[-freeSlotNo].offset == EMPTYSLOT) {

                        if ( lockup ) {
                            /* check if this slot is involved in uncommmited transaction */
                            MAKE_OBJECTID(*oid, prevPid.volNo, prevPid.pageNo, freeSlotNo, apage->slot[-freeSlotNo].unique);
                            e = LM_getObjectLock(handle,  &xactEntry->xactId, oid, &finfo->fid,
                                                  lockup->mode, lockup->duration, L_CONDITIONAL, &lockReply, &oldMode);
                            if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

                            /* if ( lockReply != LR_NOTOK ) break; */
                            if ( lockReply != LR_NOTOK ) {
				isLockAcquired = TRUE;
				break;
			    }
                        }
                        else
                            break;
                    }
#endif /* CCRL */


                /* If there is no empty slot, add a new slot. */
                addSlotFlag = (freeSlotNo == apage->header.nSlots) ? TRUE:FALSE;

                /* recalculate 'neededSpace' */
                neededSpace = sizeof(ObjectHdr) + alignedLen + ((addSlotFlag) ? sizeof(SlottedPageSlot):0);

#ifdef CCPL
                allocFlag = (neededSpace <= SP_FREE(xactEntry->xactId, apage)) ? TRUE:FALSE;
#endif /* CCPL */

#ifdef CCRL
                e = om_AcquireSpace(handle, &xactEntry->xactId, apage, neededSpace,
                                    (addSlotFlag ? sizeof(SlottedPageSlot):0),
                                    &allocFlag, &pageUpdateFlag);
                if (pageUpdateFlag == TRUE) aPage_BCBP->dirtyFlag = 1;
                if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

	        /* if previous page have enough space */
	        if (allocFlag) {

                    /* update 'pid' to 'nextPid' */
                    pid = prevPid;

	            /* prepare space for object insert */
	            if (SP_CFREE(apage) < neededSpace)
	                (void) om_CompactPage(handle, apage, NIL);

		    /* if no empty slot, then add a new slot */
		    /* if (addSlotFlag) apage->header.nSlots++;*/ /* increment # of slots */
		    if (addSlotFlag) {
	    		do{
			    /* add an empty slot */
			    /* at this point, apage->header.nSlots == freeSlotNo */
			    apage->slot[-(apage->header.nSlots)].offset = EMPTYSLOT;
	    		    apage->header.nSlots++;

                	    if ( lockup ) {
                    		/* check if this slot is involved in uncommmited transaction */
                    		MAKE_OBJECTID(*oid, pid.volNo, pid.pageNo, freeSlotNo, apage->slot[-freeSlotNo].unique);
                    		e = LM_getObjectLock(handle,  &xactEntry->xactId, oid, &finfo->fid,
                                              lockup->mode, lockup->duration, L_CONDITIONAL, &lockReply, &oldMode);
                    		if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

                    		if ( lockReply != LR_NOTOK ) {
		        	    isLockAcquired = TRUE;
				    break;
		    		}
                	    } else {
		    		isLockAcquired = TRUE;
                    		break;
			    }

			    freeSlotNo++;

			    /* an empty slot is added, thus we should prepare space for object inserted */
			    if(SP_CFREE(apage) < neededSpace){
	    	    		(void) om_CompactPage(handle, apage, NIL);
			    }

	    		} while(SP_CFREE(apage) >= neededSpace); 

	    		if( lockup!=NULL && isLockAcquired == FALSE ){
			    /* It must not be happened */
			    ERRBL1(handle, eINTERNAL, aPage_BCBP, PAGE_BUF);
	    		}
		    } /* if (addSlotFlag) */

                    break;
	        }

#ifdef CCRL
                e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex); 
                if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

	        /* free buffer for previous page */
                e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
                if (e < eNOERROR) ERR(handle, e);

#ifdef CCPL
                if (!IS_NILPAGEID(prevPid) && lockup) {  
                    e = LM_releasePageLock(handle, &xactEntry->xactId, &prevPid, L_MANUAL); 
                    if (e < eNOERROR) ERR(handle, e);
                }
#endif /* CCPL */
            }

            /* at this point, there aren't enough space in neighbor pages */
            needToAllocPage = TRUE;
            break;
        }
    }

    if (needToAllocPage) {
	/* There is no enough space; allocate a new page. */

        /* get prevExtNo & nextExtNo */
        e = om_GetNeighborExtNo(handle, (PageID *)&pid, &prevExtNo, &nextExtNo);
        if (e < eNOERROR) ERR(handle, e);

#ifdef CCRL
        e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
        if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

        e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, finfo, &pageSegmentID, PAGESIZE2);
	if (e < eNOERROR) ERR(handle, e);

        e = RDsM_AllocTrains(handle, xactEntry, finfo->fid.volNo, &pageSegmentID, (PageID *)&pid, 1, PAGESIZE2, FALSE, &newPid, logParam);
	if (e < eNOERROR) ERR(handle, e);

#ifdef CCPL
	/*
	 * NOTICE: We should acquire the X lock on the new page if the page
	 *         becomes the new last page of this file.
	 */
	if(lockup){
	    /* place a commit duration x-lock on the new page */
	    e = LM_getPageLock(handle, &xactEntry->xactId, &newPid, &finfo->fid,
			       lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	    if (e < eNOERROR) ERR(handle, e);

	    if(lockReply == LR_DEADLOCK){
		ERR(handle, eDEADLOCK);		/* deadlock */
	    }
	}

	/* Initialize the page */
	e = BfM_fixNewBuffer(handle, &newPid, M_FREE, &newPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
	/* Initialize the page */
	e = BfM_fixNewBuffer(handle, &newPid, M_EXCLUSIVE, &newPage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

	newPage = (SlottedPage *)newPage_BCBP->bufPagePtr;

	/* Initialize the slotted page. */
	e = om_InitSlottedPage(handle, xactEntry, newPage, &(finfo->fid), &newPid, logParam);
	if (e < eNOERROR) {
#ifdef CCPL
            ERRB1(handle, e, newPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
            ERRBL1(handle, e, newPage_BCBP, PAGE_BUF);
#endif /* CCRL */
        }

	/* om_AddPageToFile( ) may change the lastPage field of the catalog */

	e = om_AddPageToFile(handle, xactEntry, finfo, &pid, newPage, lockup, logParam);
	if (e < 0) {
#ifdef CCPL
            ERRB1(handle, e, newPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
            ERRBL1(handle, e, newPage_BCBP, PAGE_BUF);
#endif /* CCRL */
        }

#ifdef CCRL
        e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
        if (e < eNOERROR) ERRBL1(handle, e, newPage_BCBP, PAGE_BUF);
#endif /* CCRL */

	aPage_BCBP = newPage_BCBP;
	apage = newPage;
	pid = newPid;
	freeSlotNo = 0;

        if ( lockup ) {
            MAKE_OBJECTID(*oid, pid.volNo, pid.pageNo, freeSlotNo, apage->slot[-freeSlotNo].unique);
            e = LM_getObjectLock(handle,  &xactEntry->xactId, oid, &finfo->fid,
                                lockup->mode, lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
            if ( e < eNOERROR ) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

            if (lockReply == LR_DEADLOCK) ERRBL1(handle, eDEADLOCK, aPage_BCBP, PAGE_BUF);
	    
 	    isLockAcquired = TRUE;
	}

        addSlotFlag = FALSE;
        neededSpace = sizeof(ObjectHdr) + alignedLen + ((addSlotFlag) ? sizeof(SlottedPageSlot):0);

#ifdef CCRL
        e = om_AcquireSpace(handle, &xactEntry->xactId, apage, neededSpace, 0, &allocFlag, &pageUpdateFlag);
        if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

        assert(allocFlag == TRUE);
#endif /* CCRL */
    }

    /*
     * At this point
     * pid : PageID of the page into which the new object will be placed
     * apage : pointer to the slotted page buffer
     * alignedLen : space for data of the new object
     */

    /* where to put the object[header]? */
    obj = (Object *)&(apage->data[apage->header.free]);

    /* set the object header */
    obj->header = *objHdr;

    /* copy the data into the object */
    memcpy(obj->data, data, length);
    obj->header.length = length;

    apage->slot[-freeSlotNo].offset = apage->header.free;
    e = om_GetUnique(handle, xactEntry, aPage_BCBP, &(apage->slot[-freeSlotNo].unique), logParam);
    if (e < 0) {
#ifdef CCPL
        ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
        ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    /* update the pointer to the start of contiguous free space */
    apage->header.free += sizeof(ObjectHdr) + alignedLen;

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

	objInfo.slotNo = freeSlotNo;
	objInfo.unique = apage->slot[-freeSlotNo].unique;

        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_OM_CREATE_SMALL_OBJECT, LOG_REDO_UNDO,
                              apage->header.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
			      sizeof(LOG_Image_OM_ObjectInPage_T), &objInfo,
                              sizeof(Boolean), &addSlotFlag,
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

	/*
 	* mark the lsn in the page
 	*/
	apage->header.lsn = lsn;
	apage->header.logRecLen = logRecLen;
    }

    /* set dirty bit */
    aPage_BCBP->dirtyFlag = 1;

    /* Construct the ObjectID to be returned */
    MAKE_OBJECTID(*oid, pid.volNo, pid.pageNo, freeSlotNo, apage->slot[-freeSlotNo].unique);

#ifdef CCRL
    if (lockup!=NULL && isLockAcquired ==FALSE) {
	/* this must not be happened */ 
	/* but for safety, if the lock is not acquired, then acquire the lock */
        e = LM_getObjectLock(handle, &xactEntry->xactId, oid, &finfo->fid, lockup->mode,
                             lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
        if (e < eNOERROR) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);

        if (lockReply == LR_DEADLOCK) ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
    }

    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

    /* unfix the buffer page */
    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* om_CreateObject() */


Four om_GetNeighborExtNo(
    Four	handle,
    PageID      *pid,           /* IN */
    Four        *prevExtNo,     /* OUT */
    Four        *nextExtNo)     /* OUT */
{
    Four        e;		/* error number */
    Four        curExtNo;
    Four        tmpExtNo;
    PageID      prevPid;        /* page ID of previous page */
    PageID      nextPid;        /* page ID of next page */
    PageNo      tmpPno;
    SlottedPage *apage;		/* pointer to the slotted page buffer */
    Buffer_ACC_CB *aPage_BCBP;  /* buffer access control block for a data page */


    /* parameter check */
    if (pid == NULL) return eBADPARAMETER;

    /* get the extent number of current page */
    e = RDsM_TrainIdToExtNo(handle, pid, PAGESIZE2, &curExtNo);
    if (e < eNOERROR) ERR(handle, e);


    /* initialize 'prevPid' & 'nextPid' */

#ifdef CCPL
    e = BfM_getAndFixBuffer(handle, pid, M_FREE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
    e = BfM_getAndFixBuffer(handle, pid, M_SHARED, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

    MAKE_PAGEID(prevPid, pid->volNo, apage->header.prevPage);
    MAKE_PAGEID(nextPid, pid->volNo, apage->header.nextPage);

#ifdef CCRL
    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    /* first, get 'prevExtNo' */
    while (1) {

        /* if there is no more page */
        if ( IS_NILPAGEID(prevPid) ) {
            *prevExtNo = NIL;
            break;
        }

        /* get extent number of previous page */
        e = RDsM_TrainIdToExtNo(handle, &prevPid, PAGESIZE2, &tmpExtNo);
        if (e < eNOERROR) ERR(handle, e);

        /* check previous extent is found */
        if (curExtNo != tmpExtNo) {
            *prevExtNo = tmpExtNo;
            break;
        }

        /* get next 'previus page ID' */
#ifdef CCPL
        e = BfM_getAndFixBuffer(handle, &prevPid, M_FREE, &aPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
        e = BfM_getAndFixBuffer(handle, &prevPid, M_SHARED, &aPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

        apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

        tmpPno = apage->header.prevPage;

#ifdef CCRL
        e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

        e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        MAKE_PAGEID(prevPid, pid->volNo, tmpPno);
    }


    /* second, get 'nextExtNo' */
    while (1) {

        /* if there is no more page */
        if ( IS_NILPAGEID(nextPid) ) {
            *nextExtNo = NIL;
            break;
        }

        /* get extent number of next page */
        e = RDsM_TrainIdToExtNo(handle, &nextPid, PAGESIZE2, &tmpExtNo);
        if (e < eNOERROR) ERR(handle, e);

        /* check next extent is found */
        if (curExtNo != tmpExtNo && curExtNo != *prevExtNo) { 
            *nextExtNo = tmpExtNo;
            break;
        }

        /* get next 'next page ID' */
#ifdef CCPL
        e = BfM_getAndFixBuffer(handle, &nextPid, M_FREE, &aPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
        e = BfM_getAndFixBuffer(handle, &nextPid, M_SHARED, &aPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

        apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

        tmpPno = apage->header.nextPage;

#ifdef CCRL
        e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */

        e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        MAKE_PAGEID(nextPid, pid->volNo, tmpPno);
    }


    return eNOERROR;
}
