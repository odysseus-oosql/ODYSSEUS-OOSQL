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
 * Module:	OM_CreateFile.c
 *
 * Description : 
 *  Create a new data file. 
 *
 * Exports:
 *  Four OM_CreateFile(FileID*, Two, sm_CatOverlayForData*, Boolean)
 */


#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * OM_CreateFile()
 *================================*/
/* 
 * Function: Four OM_CreateFile(FileID*, Two, sm_CatOverlayForData*, Boolean)
 *
 * Description : 
 *  Create a new data file. The FileID - file identifier - is the same as
 *  the first page's PageID of the new segment. 
 *  We reserve the first page. As doing so, we do not have to change
 *  the first page's PageID which is needed for the sequential access.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Side effects:
 *  0) A new data file is created.
 *  1) parameter catEntry
 *      catEntry is set to the information for the newly create file.
 */
Four OM_CreateFile(
    Four handle,
    FileID*  				fid,		/* IN allocated file ID */ 
    Two      				eff,		/* IN Extent fill factor */
    sm_CatOverlayForData 	*catEntry, 	/* OUT File Information for the newly created file */
    Boolean  				tmpFileFlag
)
{
    Four        	e;			/* for the error number */
    Four        	firstExtNo;	/* first extent number of the new segment */
    PageID      	pageID;		/* first PageID of the new segment */
    SlottedPage 	*apage;		/* point to the data page */
    PageID      	nearPid;        

    
    TR_PRINT(TR_OM, TR1,
             ("OM_CreateFile(handle, fid=%ld, eff=%ld, catEntry=%P)",
	      fid, eff, catEntry));

    
    /* Create the segment for the data file */
    e = RDsM_CreateSegment(handle, fid->volNo, &firstExtNo);
    if (e < 0) ERR(handle, e);
    
    /* allocate a page : first page should be reserved.
       As doing so, we do not have to update the first page's PageID
       which is needed for the sequential access.
       From this decision, we also use the first PageID as the FileID. */

    e = RDsM_ExtNoToPageId(handle, fid->volNo, firstExtNo, &nearPid); 
    if (e < 0) ERR(handle, e);

    e = RDsM_AllocTrains(handle, fid->volNo, firstExtNo, (PageID *)&nearPid,
			 eff, 1, PAGESIZE2, &pageID);
    if (e < 0) ERR(handle, e);
    
    /* construct the catalog data structure */
    catEntry->fid = *fid;                   
    catEntry->eff = eff;
    catEntry->firstPage = pageID.pageNo;    
    catEntry->lastPage = pageID.pageNo;
    catEntry->availSpaceList10 = NIL;
    catEntry->availSpaceList20 = NIL;
    catEntry->availSpaceList30 = NIL;
    catEntry->availSpaceList40 = NIL;
    catEntry->availSpaceList50 = pageID.pageNo;

    e = BfM_GetNewTrain(handle, &pageID, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /*@ Initialize Slotted Page */
    apage->header.fid = catEntry->fid;
    apage->header.nSlots = 1;
    apage->header.free = 0;
    apage->header.unused = 0;
    apage->header.prevPage = NIL;
    apage->header.nextPage = NIL;
    apage->header.spaceListPrev = NIL;
    apage->header.spaceListNext = NIL;
    apage->header.unique = 0;
    apage->header.uniqueLimit = 0;
    apage->header.pid = pageID; 
    
    /* set page type */
    SET_PAGE_TYPE(apage, SLOTTED_PAGE_TYPE);

    /* set temporary flag */
    if( tmpFileFlag ) SET_TEMP_PAGE_FLAG(apage);
    else              RESET_TEMP_PAGE_FLAG(apage);

    /* initialize */
    apage->slot[0].offset = EMPTYSLOT;
        
    e = BfM_SetDirty(handle, &pageID, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &pageID, PAGE_BUF);

    e = BfM_FreeTrain(handle, &pageID, PAGE_BUF);
    if (e < 0) ERR(handle, e);
        
    return(eNOERROR);
    
} /* OM_CreateFile() */
