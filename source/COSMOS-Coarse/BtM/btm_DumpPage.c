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
/*    Coarse-Granule Locking (Volume Lock) Version                            */
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
 * Module: btm_DumpPage.c
 *
 * Description:
 *   Dump a given Btree Page.
 *
 * Exports:
 *  Four btm_DumpBtreePage(PageID*)
 */


#include <stdio.h>
#include <ctype.h>
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@ Internal function prototypes */
void btm_DumpInternal(Four, BtreeInternal*, PageID*);
void btm_DumpLeaf(Four, BtreeLeaf*, PageID*);
void btm_DumpOverflow(Four, BtreeOverflow*, PageID*);



/*@================================
 * btm_DumpBtreePage()
 *================================*/
/*
 * Function: Four btm_DumpBtreePage(PageID*)
 *
 * Description:
 *  Dump a Btree Page. It looks up the page type and call the corresponding
 *  dump routine.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_DumpBtreePage(
    Four handle,
    PageID   *pid)		/* IN page to dump */
{
    Four e;					/* error number */
    BtreePage *apage;		/* page to dump */


    TR_PRINT(TR_BTM, TR1, ("btm_DumpBtreePage(handle, pid=%P)", pid));
    
    e = BfM_GetTrain(handle, pid, (char **)&apage, PAGE_BUF);
    if (e < 0)  ERR(handle, e);
    
    if (apage->any.hdr.type & INTERNAL)
	btm_DumpInternal(handle, &(apage->bi), pid);
    else if (apage->any.hdr.type & LEAF)
	btm_DumpLeaf(handle, &(apage->bl), pid);
    else if (apage->bo.hdr.type & OVERFLOW)
	btm_DumpOverflow(handle, &(apage->bo), pid);
    else
	ERRB1(handle, eBADBTREEPAGE_BTM, pid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
}



/*@================================
 * btm_DumpInternal()
 *================================*/
/*
 * Function: void btm_DumpInternal(BtreeInternal*, PageID*)
 *
 * Description:
 *  Dump an internal page.
 *
 * Returns:
 *  None
 */
void btm_DumpInternal(
    Four		handle,
    BtreeInternal       *internal,      /* IN pointer to buffer of internal page */
    PageID              *pid)           /* IN page identifier */
{
    Two                 i;              /* index variable */
    Two                 entryOffset;    /* starting offset of an internal entry */
    btm_InternalEntry   *entry;         /* IN an internal entry */
    Two                 j;              /* index variable */



    TR_PRINT(TR_BTM, TR1,
             ("btm_DumpInternal(handle, internal=%P, pid=%P)", internal, pid));

    printf("\n\t|===========================================================================|\n");
    printf("\t|          PageID = (%4ld,%6ld)            type = INTERNAL%s          |\n",
	   pid->volNo, pid->pageNo, (internal->hdr.type & ROOT) ? "|ROOT":"     ");
    printf("\n\t|===========================================================================|\n");
    printf("\t|          free = %-5ld, unused = %-5ld", internal->hdr.free, internal->hdr.unused);
    printf("  nSlots = %-5ld, p0 = %-6ld       |\n", internal->hdr.nSlots, internal->hdr.p0 );
    printf("\t|---------------------------------------------------------------------------|\n");
    for (i = 0; i < internal->hdr.nSlots; i++) {
	entryOffset = internal->slot[-i];
	entry = (btm_InternalEntry*)&(internal->data[entryOffset]);
	
	printf("\t| \"");
        printf("%5ld : ", entry->klen);
	for(j = 0; (j<5) && (j<entry->klen); j++) printf("%c", entry->kval[j]);
	printf("\"");
	printf(" : spid = %5ld |\n", entry->spid);
    }
    printf("\t|---------------------------------------------------------------------------|\n");
    
}  /* btm_DumpInternal() */



/*@================================
 * btm_DumpLeaf()
 *================================*/
/*
 * Function: void btm_DumpLeaf(BtreeLeaf*, PageID*)
 *
 * Description:
 *  Dump a leaf page.
 *
 * Returns:
 *  None
 */
void btm_DumpLeaf(
    Four		handle,
    BtreeLeaf           *leaf,          /* IN pointer to buffer of Leaf page */
    PageID              *pid)           /* IN pointer to leaf PageID */
{
    Two                 entryOffset;    /* starting offset of a leaf entry */
    btm_LeafEntry       *entry;         /* a leaf entry */
    ObjectID            *oid;           /* an object identifier */
    Two                 i;              /* index variable */
    Two                 j;              /* index variable */
    Two                 alignedKlen;    /* aligned length of the key length */


    TR_PRINT(TR_BTM, TR1, ("btm_DumpLeaf(handle, leaf=%P, pid=%P)", leaf, pid));
    
    printf("\n\t|===========================================================================|\n");
    printf("\t|            PageID = (%4ld,%6ld)            type = LEAF%s             |\n",
	   pid->volNo, pid->pageNo, (leaf->hdr.type & ROOT) ? "|ROOT":"     ");
    printf("\t|===========================================================================|\n");
    printf("\t|          free = %-5ld, unused = %-5ld", leaf->hdr.free, leaf->hdr.unused);
    printf("  nSlots = %-5ld                     |\n", leaf->hdr.nSlots);
    printf("\t|                  nextPage = %-5ld, prevPage = %-5ld                       |\n",
	leaf->hdr.nextPage, leaf->hdr.prevPage );
    printf("\t|---------------------------------------------------------------------------|\n");
    for (i = 0; i < leaf->hdr.nSlots; i++) {
	entryOffset = leaf->slot[-i];
	entry = (btm_LeafEntry*)&(leaf->data[entryOffset]);
	
	printf("\t| \"");
        printf("%5ld : ", entry->klen);
	for(j = 0; (j<5) && (j<entry->klen); j++)
	    if (isprint(entry->kval[j])) printf("%c", entry->kval[j]);
	    else printf("~");
	printf("\"");
	
        printf(" : nObjects = %ld : ", entry->nObjects);

	alignedKlen = ALIGNED_LENGTH(entry->klen);
	if (entry->nObjects < 0)
            printf(" OverPID = %ld ", *((ShortPageID*)&(entry->kval[alignedKlen])));
	else {
	    oid = (ObjectID *)&(entry->kval[alignedKlen]);
            printf(" ObjectID = (%ld, %ld, %ld, %ld) ", oid->volNo, oid->pageNo, oid->slotNo, oid->unique);
	}
	printf("                |\n");
    }
    printf("\t|---------------------------------------------------------------------------|\n");
    
}  /* btm_DumpLeaf() */



/*@================================
 * btm_DumpOverflow()
 *================================*/
/*
 * Function: Four btm_DumpOverflow(BtreeOverflow*, PageID*)
 *
 * Description:
 *  Dump the overflow page.
 *
 * Returns:
 *  None
 */
void btm_DumpOverflow(
    Four	  handle,
    BtreeOverflow *overflow,	/* IN pointer to bugffer of Overflow Page */
    PageID        *pid)			/* IN PageID of overflow page */
{
    Two           i;            /* index variable */
    ObjectID 	  oid;			/* an object identifier */


    TR_PRINT(TR_BTM, TR1,
             ("btm_DumpOverflow(handle, overflow=%P, pid=%P)", overflow, pid));
        
    printf("\n\t|===========================================================================|\n");
    printf("\t|          PageID = (%4ld,%6ld)            type = OVERFLOW            |\n",
	   pid->volNo, pid->pageNo);
    printf("\n\t|===========================================================================|\n");
    printf("nextPage = %-5ld, prevPage = %-5ld       |\n",
	   overflow->hdr.nextPage, overflow->hdr.prevPage);
    printf("\t|---------------------------------------------------------------------------|");
    
    for (i = 0; i < overflow->hdr.nObjects; i++) {
	if(i%3 == 0) printf("\n\t| ");
	
	oid = overflow->oid[i];
        printf("  (%-4ld, %-4ld, %-4ld, %-4ld) ",
	       oid.volNo, oid.pageNo, oid.slotNo, oid.unique);
    }
    printf("\n\t|---------------------------------------------------------------------------|\n");
    
}  /* btm_DumpOverflow() */
