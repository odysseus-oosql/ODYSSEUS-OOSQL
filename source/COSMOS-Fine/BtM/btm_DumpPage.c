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
 * Module: btm_dumpPage.c
 *
 * Description:
 *   Dump a given Btree Page.
 *
 * Exports:
 *  Four btm_dumpBtreePage(Four, PageID*)
 */


#include <stdio.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal function prototypes */
void btm_dumpInternal(Four, BtreeInternal*, PageID*);
void btm_dumpLeaf(Four, BtreeLeaf*, PageID*);
void btm_dumpOverflow(Four, BtreeOverflow*, PageID*);

/*
 * Function: Four btm_dumpBtreePage(PageID*)
 *
 * Description:
 *  Dump a Btree Page. It looks up the page type and call the corresponding
 *  dump routine.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_dumpBtreePage(
    Four    handle,
    PageID   *pid)		/* IN page to dump */
{
    Four e;			/* error number */
    BtreePage *apage;		/* page to dump */
    Buffer_ACC_CB *bcb;		/* a buffer control block */


    TR_PRINT(handle, TR_BTM, TR1, ("btm_dumpBtreePage(pid=%P)", pid));

    e = BfM_getAndFixBuffer(handle, pid, M_SHARED, &bcb, PAGE_BUF);
    if (e < 0)  ERR(handle, e);

    apage = (BtreePage*)bcb->bufPagePtr;

    if (apage->any.hdr.type & INTERNAL)
	btm_dumpInternal(handle, &(apage->bi), pid);
    else if (apage->any.hdr.type & LEAF)
	btm_dumpLeaf(handle, &(apage->bl), pid);
    else if (apage->bo.hdr.type & OVERFLOW)
	btm_dumpOverflow(handle, &(apage->bo), pid);
    else
	ERR(handle, eBADBTREEPAGE_BTM);

    e = SHM_releaseLatch(handle, bcb->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    e = BfM_unfixBuffer(handle, bcb, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}


/*
 * Function: void btm_dumpInternal(Four, BtreeInternal*, PageID*)
 *
 * Description:
 *  Dump an internal page.
 *
 * Returns:
 *  None
 */
void btm_dumpInternal(
    Four  	   handle,
    BtreeInternal *internal,	/* IN pointer to buffer of internal page */
    PageID        *pid)		/* IN page identifier */
{
    Four i;			/* index variable */
    Four entryOffset;		/* starting offset of an internal entry */
    btm_InternalEntry *entry;	/* IN an internal entry */
    Four j;			/* index variable */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_dumpInternal(internal=%P, pid=%P)", internal, pid));

    printf("\n\t|===========================================================================|\n");
    printf("\t|    PageID = (%4ld,%6ld)   IndexID->serial=%6ld  type = INTERNAL%s    |\n", 
	   pid->volNo, pid->pageNo, internal->hdr.iid.serial, (internal->hdr.type & ROOT) ? "|ROOT":"     "); 
    printf("\t|===========================================================================|\n");
    printf("\t|    LSN=(%4ld,%6ld)   logRecLen=%4ld     SM_BIT=%1ld   height=%4ld    |\n",
	   internal->hdr.lsn.offset, internal->hdr.lsn.wrapCount, internal->hdr.logRecLen,
	   (internal->hdr.statusBits & BTM_SM_BIT)? 1:0, internal->hdr.height);
    printf("\t|          free = %-5ld, unused = %-5ld", internal->hdr.free, internal->hdr.unused);
    printf("  nSlots = %-5ld, p0 = %-6ld       |\n", internal->hdr.nSlots, internal->hdr.p0 );
    printf("\t|---------------------------------------------------------------------------|\n");
    for (i = 0; i < internal->hdr.nSlots; i++) {
	entryOffset = internal->slot[-i];
	entry = (btm_InternalEntry*)&(internal->data[entryOffset]);

	printf("\t| \"");
	printf("%5ld : ", entry->klen);
	for(j = 0; (j<5) && (j<entry->klen); j++) putchar(entry->kval[j]);
	printf("\"");
	printf(" : spid = %5ld |\n", entry->spid);
    }
    printf("\t|---------------------------------------------------------------------------|\n");

}  /* btm_dumpInternal() */



/*
 * Function: void btm_dumpLeaf(Four, BtreeLeaf*, PageID*)
 *
 * Description:
 *  Dump a leaf page.
 *
 * Returns:
 *  None
 */
void btm_dumpLeaf(
    Four      handle,
    BtreeLeaf *leaf,		/* IN pointer to buffer of Leaf page */
    PageID    *pid)		/* IN pointer to leaf PageID */
{
    Four entryOffset;		/* starting offset of a leaf entry */
    btm_LeafEntry *entry;	/* a leaf entry */
    ObjectID *oid;		/* an object identifier */
    Four i;			/* index variable */
    Four j;			/* index variable */
    Four alignedKlen;		/* aligned length of the key length */


    TR_PRINT(handle, TR_BTM, TR1, ("btm_dumpLeaf(leaf=%P, pid=%P)", leaf, pid));

    printf("\n\t|===========================================================================|\n");
    printf("\t|    PageID = (%4ld,%6ld)    IndexID->serial=%6ld   type = LEAF%s      |\n", 
	   pid->volNo, pid->pageNo, leaf->hdr.iid.serial, (leaf->hdr.type & ROOT) ? "|ROOT":"     "); 
    printf("\t|===========================================================================|\n");
    printf("\t|  LSN=(%4ld,%6ld)  logRecLen=%4ld  SM_BIT=%1ld  Delete_BIT=%1ld  height=%3ld  |\n",
	   leaf->hdr.lsn.offset, leaf->hdr.lsn.wrapCount, leaf->hdr.logRecLen,
	   (leaf->hdr.statusBits & BTM_SM_BIT)? 1:0,
	   (leaf->hdr.statusBits & BTM_DELETE_BIT)? 1:0, leaf->hdr.height);
    printf("\t|          free = %-5ld, unused = %-5ld", leaf->hdr.free, leaf->hdr.unused);
    printf("  nSlots = %-5ld                   |\n", leaf->hdr.nSlots);
    printf("\t|                  nextPage = %-5ld, prevPage = %-5ld                        |\n",
	leaf->hdr.nextPage, leaf->hdr.prevPage );
    printf("\t|---------------------------------------------------------------------------|\n");
    for (i = 0; i < leaf->hdr.nSlots; i++) {
	entryOffset = leaf->slot[-i];
	entry = (btm_LeafEntry*)&(leaf->data[entryOffset]);

	printf("\t| \"");
	printf("%5ld : ", entry->klen);
	for(j = 0; (j<5) && (j<entry->klen); j++) putchar(entry->kval[j]);
	printf("\"");

	printf(" : nObjects = %ld : ", entry->nObjects);

	alignedKlen = ALIGNED_LENGTH(entry->klen);
	if (entry->nObjects < 0)
	    printf(" OverPID = %ld ", *((ShortPageID*)&(entry->kval[alignedKlen])));
	else {
	    oid = (ObjectID *)&(entry->kval[alignedKlen]);
	    printf(" ObjectID = (%ld, %ld, %ld) ", oid->volNo, oid->pageNo, oid->slotNo);
	}
	printf("     |\n");
    }
    printf("\t|---------------------------------------------------------------------------|\n");

}  /* btm_dumpLeaf() */



/*
 * Function: Four btm_dumpOverflow(Four, BtreeOverflow*, PageID*)
 *
 * Description:
 *  Dump the overflow page.
 *
 * Returns:
 *  None
 */
void btm_dumpOverflow(
    Four          handle,
    BtreeOverflow *overflow,	/* IN pointer to bugffer of Overflow Page */
    PageID        *pid)		/* IN PageID of overflow page */
{
    Four i;			/* index variable */
    ObjectID oid;		/* an object identifier */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_dumpOverflow(overflow=%P, pid=%P)", overflow, pid));

    printf("\n\t|===========================================================================|\n");
    printf("\t|    PageID = (%4ld,%6ld)   IndexID->serial=%6ld  type = OVERFLOW      |\n",
	   pid->volNo, pid->pageNo, overflow->hdr.iid.serial); 
    printf("\t|===========================================================================|\n");
    printf("\t|    LSN=(%4ld,%6ld)   logRecLen=%4ld      height=%4ld        |\n",
	   overflow->hdr.lsn.offset, overflow->hdr.lsn.wrapCount, overflow->hdr.logRecLen, overflow->hdr.height);
    printf("\t|    nObjects=%2ld    nextPage = %-5ld   prevPage = %-5ld       |\n",
	   overflow->hdr.nObjects, overflow->hdr.nextPage, overflow->hdr.prevPage);
    printf("\t|---------------------------------------------------------------------------|");

    for (i = 0; i < overflow->hdr.nObjects; i++) {
	if(i%3 == 0) printf("\n | ");

	oid = overflow->oid[i];
	printf("  {volNo = %-4ld, pageNo = %-4ld, slotNo = %-4ld} ",
	       oid.volNo, oid.pageNo, oid.slotNo);
    }
    printf("\n\t|---------------------------------------------------------------------------|\n");

}  /* btm_dumpOverflow() */
