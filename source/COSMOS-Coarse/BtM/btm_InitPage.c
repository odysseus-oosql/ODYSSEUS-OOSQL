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
 * Module: btm_InitPage.c
 *
 * Description :
 *  This file has three functions which initialize Btree pages.  A Btree page
 *  should be initialized by one of these functions before it is used.
 *
 * Exports:
 *  Four btm_InitInternal(PageID*, Boolean)
 *  Four btm_InitLeaf(PageID*, Boolean)
 *  Four btm_InitOverflow(PageID*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * btm_InitInternal()
 *================================*/
/*
 * Function: Four btm_InitInternal(PageID*, Boolean)
 *
 * Description:
 *  Initialize as an internal page.  If 'root' is TRUE, this page may be
 *  initialized as a root.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_InitInternal(
    Four handle,
    PageID  *internal,		/* IN the PageID to be initialized */
    Boolean root,			/* IN */
    Boolean isTmp)          /* IN */
{
    Four e;					/* error number */
    BtreeInternal *page;	/* a page pointer */


    TR_PRINT(TR_BTM, TR1,
             ("btm_InitInternal(handle, internal=%P, root=%ld)", internal, root));
    
    e = BfM_GetNewTrain(handle, internal, (char **)&page, PAGE_BUF);
    if (e < 0)  ERR(handle, e);
    
    /* set page type */
    SET_PAGE_TYPE(page, BTREE_PAGE_TYPE);

    /* set temporary flag */
    if( isTmp ) SET_TEMP_PAGE_FLAG(page);
    else        RESET_TEMP_PAGE_FLAG(page);

    /* Set the Btree page type. If 'root' is TRUE, the root 'bit' is also set. */
    if (root)  page->hdr.type = INTERNAL | ROOT;
    else       page->hdr.type = INTERNAL;
    
    /*@ Initialize variables */
    page->hdr.pid = *internal;  
    page->hdr.p0 = (ShortPageID)NIL;
    page->hdr.nSlots = 0;
    page->hdr.free = 0;
    page->hdr.unused = 0;

    /* bluechry test ... */
    if (root) {
	if (page->hdr.pid.volNo == 1000 && page->hdr.pid.pageNo == 165) {
	    printf("### [pID=%d, tID=%d] Set Root & Internal Page: [%d, %d] at %x\n", procIndex, handle, 1000, 165, page);
	    printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__);
	    fflush(stdout);
	}
    }
    else {
	if (page->hdr.pid.volNo == 1000 && page->hdr.pid.pageNo == 165) {
	    printf("### [pID=%d, tID=%d] Set Internal Page: [%d, %d] at %x\n", procIndex, handle, 1000, 165, page);
	    printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__);
	    fflush(stdout);
	}
    }
    /* ... bluechry test */
    
    e = BfM_SetDirty(handle, internal, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, internal, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, internal, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
}  /* btm_InitInternal() */



/*@================================
 * btm_InitLeaf()
 *================================*/
/*
 * Function: Four btm_InitLeaf(PageID*, Boolean)
 *
 * Description:
 *  Initialize as a leaf page. If 'root' is TRUE, this page may be initialize
 *  as a root.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_InitLeaf(
    Four handle,
    PageID *leaf,			/* IN the PageID to be initialized */
    Boolean root,			/* IN */
    Boolean isTmp)          /* IN */
{
    Four e;					/* error number */
    BtreeLeaf *page;		/* a page pointer */


    TR_PRINT(TR_BTM, TR1, ("btm_InitLeaf(handle, leaf=%P, root=%ld)", leaf, root));
    
    e = BfM_GetNewTrain(handle, leaf, (char **)&page, PAGE_BUF);
    if (e < 0)  ERR(handle, e);
    
    /* set page type */
    SET_PAGE_TYPE(page, BTREE_PAGE_TYPE);

    /* set temporary flag */
    if( isTmp ) SET_TEMP_PAGE_FLAG(page);
    else        RESET_TEMP_PAGE_FLAG(page);

    if (root)  page->hdr.type = LEAF | ROOT; /* to be a root */
    else       page->hdr.type = LEAF;	 /* a normal leaf */
    
    /*@ Initialize variables */
    page->hdr.pid = *leaf;          
    page->hdr.nextPage = NIL;
    page->hdr.prevPage = NIL;
    page->hdr.nSlots = 0;
    page->hdr.free = 0;
    page->hdr.unused = 0;
    
    /* bluechry test ... */
    if (root) {
	if (page->hdr.pid.volNo == 1000 && page->hdr.pid.pageNo == 165) {
	    printf("### [pID=%d, tID=%d] Set Root & Leaf Page: [%d, %d] at %x\n", procIndex, handle, 1000, 165, page);
	    printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__);
	    fflush(stdout);
	}
    }
    else {
	if (page->hdr.pid.volNo == 1000 && page->hdr.pid.pageNo == 165) {
	    printf("### [pID=%d, tID=%d] Set Leaf Page: [%d, %d] at %x\n", procIndex, handle, 1000, 165, page);
	    printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__);
	    fflush(stdout);
	}
    }
    /* ... bluechry test */
   
    e = BfM_SetDirty(handle, leaf, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, leaf, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, leaf, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
}  /* btm_InitLeaf() */



/*@================================
 * btm_InitOverflow()
 *================================*/
/*
 * Function: Four btm_InitOverflow(PageID*)
 *
 * Description: 
 *  Initialize as an overflow page.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four btm_InitOverflow(
    Four handle,
    PageID *overflow,		/* IN PageID of overflow page */
    Boolean isTmp)         	/* IN */
{
    Four e;					/* error number */
    BtreeOverflow *apage;	/* overflow page to initialize */


    TR_PRINT(TR_BTM, TR1, ("btm_InitOverflow(handle, overflow=%P)", overflow));
    
    e = BfM_GetNewTrain(handle, overflow, (char **)&apage, PAGE_BUF);
    if (e < 0)  ERR(handle, e);
    
    /* set page type */
    SET_PAGE_TYPE(apage, BTREE_PAGE_TYPE);

    /* set temporary flag */
    if( isTmp ) SET_TEMP_PAGE_FLAG(apage);
    else        RESET_TEMP_PAGE_FLAG(apage);

    apage->hdr.type = OVERFLOW;
    
    /*@ Initialize variables */
    apage->hdr.pid = *overflow; 
    apage->hdr.nObjects = 0;
    apage->hdr.nextPage = (ShortPageID)NIL;
    apage->hdr.prevPage = (ShortPageID)NIL;
    
    /* bluechry test ... */
    if (apage->hdr.pid.volNo == 1000 && apage->hdr.pid.pageNo == 165) {
	printf("### [pID=%d, tID=%d] Set Overflow Page: [%d, %d] at %x\n", procIndex, handle, 1000, 165, apage);
	printf("### [pID=%d, tID=%d] File: %s, Line: %d\n", procIndex, handle, __FILE__, __LINE__);
	fflush(stdout);
    }
    /* ... bluechry test */

    e = BfM_SetDirty(handle, overflow, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, overflow, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, overflow, PAGE_BUF);
    if(e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
}  /* btm_InitOverflow() */
