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
 * Module :	OM_DropFile.c
 *
 * Description :
 *  Drop the given data file.
 *  If an deallocated list is given by the caller, then the data pages
 *  consisting of the file are added into the list and the deallocation of the
 *  pages are deffered; the caller is responsible for deallcating the pages.
 *  If the dallcated list is not given, then the index pages are deallocated
 *  immediately.
 *
 * Assume :
 *  The given file might be locked before calling this function.
 *  So, we don't need to lock the page.
 *
 * Exports:
 *  Four OM_DropFile(Four, FileID*, LocalPool*, DeallocListElem*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "RDsM.h"
#include "BfM.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal Function Prototypes */
Four OM_CollectDLforDroppedFile(PageID*, LocalPool*, DeallocListElem*);


/*
 * Function: Four OM_DropFile(Four, FileID*, LocalPool*, DeallocListElem*)
 *
 * Description:
 *  Refer to the module description.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 *
 * Side Effects:
 *  1) parameter dlHead
 *     The nodes for the pages of the dropped file are inserted into the list.
 */
Four OM_DropFile(
    Four                        handle,                 /* IN handle */
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    PhysicalFileID 		*pFid,			/* IN File ID for the file to be deleted */ 
    SegmentID_T			*pageSegmentID,		/* IN page segment ID concerned with the file */
    SegmentID_T			*trainSegmentID,	/* IN train segment ID concerned with the file */
    Boolean 			immediateFlag,      	/* IN TRUE if drop immediately */
    LogParameter_T 		*logParam) 		/* IN log parameter */
{
    Four 			e;			/* for the error number */
    PageID			pid;			/* current page's PageID */
    PageID 			nextPid;		/* next page's PageID */
    SlottedPage 		*apage;			/* pointer to buffer holding slotted page */
    DeallocListElem 		*dlTemp;    		/* current element of the list */
    Buffer_ACC_CB 		*aPage_BCBP;		/* buffer access control block holding data */


    TR_PRINT(handle, TR_OM, TR1,
	     ("OM_DropFile(xactEntry=%P, pFid=%P, pageSegmentID=%P, trainSegmentID=%P, immediateFlag=%lD, logParam=%P)",
	     xactEntry, pFid, pageSegmentID, trainSegmentID, immediateFlag, logParam));


    /* Check parameters. */
    if (pFid == NULL) ERR(handle, eBADPARAMETER);

    if (immediateFlag == TRUE) {
        /* Drop the segment concerned with the file */
        if (IS_NIL_SEGMENT_ID(pageSegmentID) != TRUE) {
	    e = RDsM_DropSegment(handle, xactEntry, pFid->volNo, pageSegmentID, pageSegmentID->sizeOfTrain, immediateFlag, logParam);
	    if (e < eNOERROR) ERR(handle, e);
        }
        if (IS_NIL_SEGMENT_ID(trainSegmentID) != TRUE) {
	    e = RDsM_DropSegment(handle, xactEntry, pFid->volNo, trainSegmentID, trainSegmentID->sizeOfTrain, immediateFlag, logParam);
	    if (e < eNOERROR) ERR(handle, e);
        }
    }
    else {
	/* Add the segment identification to dealloc list */
	e = TM_XT_AddToDeallocList(handle, xactEntry, pFid, pageSegmentID, trainSegmentID, DL_FILE);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* OM_DropFile() */
