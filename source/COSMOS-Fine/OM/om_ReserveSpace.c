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
 * Module: om_ReserveSpace.c
 *
 * Description:
 *  When an action has released some space on a data page, that space should
 *  not be consumed by actions of other transactions without compromising
 *  the ability to undo the action which released the space.
 *
 * Exports:
 *  Four om_AcquireSpace(Four, SlottedPage*, Four, Boolean*)
 *  void om_InitReserve(Four)
 *  Four om_ReleaseSpace(Four, SlottedPage*, Four)
 *  Four om_UndoAcquire(Four, SlottedPage*, Four)
 *  Four om_UndoRelease(Four, SlottedPage*, Four)
 *
 */

#ifdef CCRL

#include "common.h"
#include "trace.h"
#include "latch.h"
#include "BfM.h"		/* for the buffer manager call */
#include "OM.h"
#include "TM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/* Internal function prototype */
Four om_CheckReserve(Four, XactID*, SlottedPage*, Boolean*);


/*
 * Function: void om_InitReserve(Four)
 *
 * Description:
 *  Initialize the data structures needed for space reservation.
 *
 * Returns:
 *  None
 */
void om_InitReserve(
    Four    handle)
{
    /* pointer for OM Data Structure of perThreadTable */
    OM_PerThreadDS_T *om_perThreadDSptr = OM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_OM, TR1, ("om_InitReserve()"));

    /* Initialzie the variable 'oldMin'. */
    SET_NIL_XACTID(om_perThreadDSptr->oldMin);

} /* om_InitReserve( ) */


/*
 * Function: void om_CheckReserve(SlottedPage*)
 *
 * Description:
 *  Check whether existing reservation is still valid.
 *  `oldMin' is global variable which caches the minimum active
 *  transaction id.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four om_CheckReserve(
    Four	handle,
    XactID      *xactId, 		/* IN transaction table entry */
    SlottedPage *apage,			/* IN page to check the reserved space */
    Boolean     *pageUpdateFlag)        /* OUT TRUE when page is updated */
{

    /* pointer for COMMON Data Structure of perThreadTable */
    OM_PerThreadDS_T *om_perThreadDSptr = OM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_OM, TR1, ("om_CheckReserve(apage=%P, pageUpageFlag=%P)", apage, pageUpdateFlag));

    *pageUpdateFlag = FALSE;

    if (XACTID_CMP_GT(om_perThreadDSptr->oldMin, apage->header.trans)) {
	/*
	** Youngest transaction to reserve space is complete.
	** Reservation can be released.
	*/
	SET_NIL_XACTID(apage->header.trans);
	apage->header.rsvd = 0;
	apage->header.trsvd = 0;

        *pageUpdateFlag = TRUE;

    } else {
	if (XACTID_CMP_GT(*xactId, apage->header.trans)) {
	    /*
	    ** Recompute minimum active transaction id.
	    **/
	    TM_XT_GetMinXactId(handle, &(om_perThreadDSptr->oldMin));
	    if (XACTID_CMP_GT(om_perThreadDSptr->oldMin, apage->header.trans)) {
		SET_NIL_XACTID(apage->header.trans);
		apage->header.rsvd = 0;
		apage->header.trsvd = 0;

                *pageUpdateFlag = TRUE;
	    }
	}
    }

    return(eNOERROR);

} /* om_CheckReserve() */



/*
 * Function: Four om_ReleaseSpace(Four, Buffer_ACC_CB*, Four)
 *
 * Description:
 *  Release 'amount' of space and reserve that space
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four om_ReleaseSpace(
    Four 	handle,
    XactID 	*xactId, 		/* IN transaction table entry */
    SlottedPage *apage,         	/* INOUT page where the space is freed */
    Four        amount,			/* IN 'amount' of space released */
    Boolean 	*pageUpdateFlag)    	/* OUT TRUE when page is updated */
{

    TR_PRINT(handle, TR_OM, TR1, ("om_ReleaseSpace()"));

    *pageUpdateFlag = FALSE;

    if (amount == 0) return (eNOERROR);	

    om_CheckReserve(handle, xactId, apage, pageUpdateFlag);

    apage->header.totalFreeSpace += amount; /* increment free space */
    apage->header.rsvd += amount;	     /* increment reserved space */

    if (XACTID_CMP_EQ(apage->header.trans, *xactId)) {
	/*
	** I am the current youngest. Increment private reserve.
	*/
	apage->header.trsvd += amount;
    } else {
	if (XACTID_CMP_GT(*xactId, apage->header.trans)) {
	    /* I am the new youngest. */
	    ASSIGN_XACTID(apage->header.trans, *xactId); /* Install me as youngest. */
	    apage->header.trsvd = amount;	     /* Increment private reserve. */
	}
    }

    *pageUpdateFlag = TRUE;

    return(eNOERROR);

} /* om_ReleaseSpace() */



/*
 * Function: Four om_AcquireSpace(Four, Buffer_ACC_CB*, Four, Boolean*)
 *
 * Description:
 *  Acquire 'amount' of space on a page.
 *
 * Returns:
 *  1) TRUE if it is possible to acquire 'amount' of space
 *     FALSE if there is no enough space
 *  2) Error code if the return value is a negative number
 *       some errors caused by function calls
 */
Four om_AcquireSpace(
    Four 	handle,
    XactID 	*xactId, 			/* IN transaction table entry */
    SlottedPage *apage,         		/* INOUT page from which we will allocate space */
    Four        amount,				/* IN 'amount' of space to acquire */
    Four	amountInNonReservedSpace, 	/* IN amount which SHOULD be allocated in non-reserved space */
    Boolean     *allocFlag,			/* OUT TRUE if the allocation is successful */
    Boolean	*pageUpdateFlag) 		/* OUT TRUE when page is updated */
{
    Four 	avail;				/* available space */
    Four 	amountInReservedSpace; 		/* amount of space which CAN be allocated in reserved space */


    TR_PRINT(handle, TR_OM, TR1, ("om_AcquireSpace()"));

    *pageUpdateFlag = FALSE;

    if (amount == 0) {
	*allocFlag = TRUE;
	return (eNOERROR);
    }

    om_CheckReserve(handle, xactId, apage, pageUpdateFlag);

    avail = apage->header.totalFreeSpace - apage->header.rsvd;
    if (amountInNonReservedSpace > avail) {
	/* insufficient space */
	*allocFlag = FALSE;
	return(eNOERROR);
    }

    if (EQUAL_XACTID(apage->header.trans, *xactId)) {
	/* can use private reserve. */
	avail += apage->header.trsvd;
        *pageUpdateFlag = TRUE;
    }

    if (amount > avail) {
	/* insufficient space */
	*allocFlag = FALSE;
	return(eNOERROR);
    } else {
	apage->header.totalFreeSpace -= amount;
	if (EQUAL_XACTID(apage->header.trans, *xactId)) {
	    /*
	    ** Use space from private reserve &
	    ** reduce the global reservation.
	    ** Assert:
	    **   apage->header.trans >= xactId
	    **   until I have completed.
	    */
            amountInReservedSpace = amount - amountInNonReservedSpace;
	    if (amountInReservedSpace > apage->header.trsvd) {
		apage->header.rsvd -= apage->header.trsvd;
		apage->header.trsvd = 0;
	    } else {
		apage->header.rsvd -= amountInReservedSpace;
		apage->header.trsvd -= amountInReservedSpace;
	    }
	}

        *pageUpdateFlag = TRUE;
    }

    /* enough space available */
    *allocFlag = TRUE;
    return(eNOERROR);

} /* om_AcquireSpace() */


/*
 * Function: Four om_UndoRelease(Four, Buffer_ACC_CB*, Four)
 *
 * Description:
 *  Undo the release of space on a page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four om_UndoRelease(
    Four 	handle,
    XactID 	*xactId, 		/* IN transaction table entry */
    SlottedPage *apage,         	/* INOUT page where the space has released */
    Four        amount,			/* IN 'amount' of space having released */
    Boolean	*pageUpdateFlag) 	/* OUT TRUE when page is updated */
{
    TR_PRINT(handle, TR_OM, TR1, ("om_UndoRelease()"));

    *pageUpdateFlag = FALSE;

    if (amount == 0) return(eNOERROR);

    *pageUpdateFlag = TRUE;

    /* Decrement free space. */
    apage->header.totalFreeSpace -= amount;

    /* Decrement reserved space. */
    apage->header.rsvd -= amount;

    if (EQUAL_XACTID(apage->header.trans, *xactId)) {
	/* Decrement private reserve. */
	apage->header.trsvd -= amount;
    }

    return(eNOERROR);

} /* om_UndoRelease() */


/*
 * Function: Four om_UndoAcquire(Four, Buffer_ACC_CB*, Four)
 *
 * Description:
 *  Undo acquisition of space on page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four om_UndoAcquire(
    Four 		handle,
    XactID 		*xactId, 		/* IN transaction table entry */
    SlottedPage 	*apage,         	/* INOUT page where the sapce has acquired */
    Four        	amount,			/* IN 'amount' of space having acquired */
    Boolean		*pageUpdateFlag) 	/* OUT TRUE when page is updated */
{
    TR_PRINT(handle, TR_OM, TR1, ("om_UndoAcquire()"));

    *pageUpdateFlag = FALSE;

    if (amount == 0) return(eNOERROR);

    *pageUpdateFlag = TRUE;

    /* Increase free space. */
    apage->header.totalFreeSpace += amount;

    if (EQUAL_XACTID(apage->header.trans, *xactId) ||
	XACTID_CMP_GT(apage->header.trans, *xactId)) {
	/* I may have used reserved space when the space was acquired. */
	apage->header.rsvd += amount;

	if (EQUAL_XACTID(apage->header.trans, *xactId))
	    apage->header.trsvd += amount;
    }

    return(eNOERROR);

} /* om_UndoAcquire() */


/*
 * Function: om_isEmptyPage()
 *
 * Description:
 *  Check if the given slotted page is an empty page.
 *  A page is an empty page if it has no reserved space and
 *  there is no object stored.
 *
 * Returns:
 *   TRUE if the page is an empty page
 *   FALSE otherwise
 */

Boolean om_isEmptyPage(
    Four	handle,
    SlottedPage *apage)         /* IN page to check if it is an empty page */

{
    XactID minXID;		/* oldest transaction identifier */

    TR_PRINT(handle, TR_OM, TR1, ("om_isEmptyPage()"));

    /* There is no free space in this slotted page. */
    TM_XT_GetMinXactId(handle, &minXID);
    if (apage->header.rsvd != 0 && !XACTID_CMP_GE(minXID, apage->header.trans)) return(FALSE);

    /* Is there any object? */
    if (apage->header.unused != apage->header.free) return(FALSE);

    return(TRUE);

} /* om_isEmptyPage() */

#endif /* CCRL */
