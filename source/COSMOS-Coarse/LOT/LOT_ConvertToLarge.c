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
 * Module: LOT_ConverToLarge.c
 *
 * Description:
 *  LOT_ConvertToLarge() converts the given small object into the large
 *  object.
 *
 * Exports:
 *  Four LOT_ConvertToLarge(ObjectID*, SlottedPage*, Two, Pool*, DeallocListElem*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h" 
#include "BfM.h"
#include "OM_Internal.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * LOT_ConvertToLarge()
 *================================*/
/*
 * Function: Four LOT_ConvertToLarge(ObjectID*, SlottedPage*, Two, Pool*, DeallocListElem*)
 *
 * Description:
 *  LOT_ConvertToLarge() converts the given small object into the large
 *  object. This function is called when the small object becomes a large
 *  object by inserting or appending some data.
 *
 * Returns:
 *  error code
 *    eBADOBJECTID_LOT
 *    some errors caused by function calls
 */
Four LOT_ConvertToLarge(
    Four handle,
    ObjectID                    *catObjForFile,         /* IN file containing the object */
    SlottedPage                 *apage,                 /* IN pointer to buffer holding the slotted page */
    Two                         slotNo,                 /* IN slot no of object in slotted page */
    Pool                        *dlPool,                /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead)                /* INOUT head of dealloc list */
{
    Four                        e;                      /* error number */
    FileID                      fid;                    /* ID of file containing the LOT */
    Two                         eff;                    /* data file's extent fill factor */
    Four                        firstExt;               /* first extent no of file */
    ObjectID                    movedOID;               /* ObjectID of the forwarded object */
    SlottedPage                 *fpage;                 /* pointer to buffer holding page with forwarded object */
    Two                         offset;                 /* starting offset of object in slotted page */
    Object                      *origObj;               /* pointer to object in slotted page (not moved) */
    Object                      *obj;                   /* pointer to object in slotted page */
    Two                         alignedLen;             /* aligned length of the original object */
    Two                         neededSpace;            /* needed space to store the internal node */
    PageID                      root;                   /* new root page */
    PageID                      leafPid;                /* the newly created leaf node */
    L_O_T_INode                 *anode;                 /* pointer to buffer holding the root node */
    L_O_T_LNode                 *leafNode;              /* pointer to buffer holding the leaf node */
    Boolean                     last;                   /* Is the object at the end of data area? */
    SlottedPage                 *catPage;               /* buffer page containing the catalog object */
    sm_CatOverlayForData        *catEntry;              /* overay structure for catalog object access */
    Boolean                     isTmp;                  

    
    TR_PRINT(TR_LOT, TR1,
             ("LOT_ConvertToLarge(handle, catObjForFile=%P, apage=%P, slotNo=%ld)",
	      catObjForFile, apage, slotNo));
    
    /* Get the file ID & extent fill factor from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    fid = catEntry->fid;
    eff = catEntry->eff;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /* check this large object is temporary or not */
    e = lot_IsTemporary(handle, &fid, &isTmp);
    if (e < 0) ERR(handle, e);

    /* get the first extent number */
    e = RDsM_PageIdToExtNo(handle, (PageID *)&fid, &firstExt);
    if (e < 0) ERR(handle, e);

    /*@ allocate the new data page */
    e = RDsM_AllocTrains(handle, fid.volNo, firstExt, &apage->header.pid,
			 eff, 1, TRAINSIZE2, &leafPid);
    if (e < 0) ERR(handle, e);

    /*@ Read the new page into the buffer. */
    e = BfM_GetNewTrain(handle, &leafPid, (char **)&leafNode, LOT_LEAF_BUF);
    if (e < 0) ERR(handle, e);

    /* initialize the leaf node */
    leafNode->hdr.pid = leafPid;
    
    /* set page type */
    SET_PAGE_TYPE(leafNode, LOT_L_NODE_TYPE);

    /* set temporary flag */
    if( isTmp ) SET_TEMP_PAGE_FLAG(leafNode);
    else        RESET_TEMP_PAGE_FLAG(leafNode);

    /* obj points to the old small object */
    offset = apage->slot[-slotNo].offset;
    origObj = (Object *)&(apage->data[offset]);

    if (origObj->header.properties & P_MOVED) {
	movedOID = *((ObjectID *) origObj->data);

	e = BfM_GetTrain(handle, (PageID *)&movedOID, (char **)&fpage, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &leafPid, LOT_LEAF_BUF);

	if (!IS_VALID_OBJECTID(&movedOID, fpage))
	    ERRB2(handle, eBADOBJECTID_LOT, (PageID *)&movedOID, PAGE_BUF, &leafPid, LOT_LEAF_BUF);

/* 'offset' is used for original object. So 'offset' can't be used for moved object */
        obj = (Object *)&(fpage->data[fpage->slot[-movedOID.slotNo].offset]);
	
	/*@ copy old data into the new train */
	memcpy(leafNode->data, obj->data, obj->header.length);

	e = OM_DestroyObject(handle, catObjForFile, &movedOID, dlPool, dlHead);
	if (e < 0) ERRB2(handle, e, (TrainID*)&movedOID, PAGE_BUF, &leafPid, LOT_LEAF_BUF);

	e = BfM_FreeTrain(handle, (PageID *)&movedOID, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &leafPid, LOT_LEAF_BUF);

    } else {
	obj = origObj;
	
	/* copy old data into the new train */
	memcpy(leafNode->data, obj->data, obj->header.length);
    }

    e = BfM_SetDirty(handle, &leafPid, LOT_LEAF_BUF);
    if (e < 0) ERRB1(handle, e, &leafPid, LOT_LEAF_BUF);
    
    e = BfM_FreeTrain(handle, &leafPid, LOT_LEAF_BUF);
    if (e < 0) ERR(handle, e);

    /* make the root node */
    if (origObj->header.properties & P_MOVED)
	alignedLen = sizeof(ObjectID);
    else     
	alignedLen = MAX(sizeof(ShortPageID), ALIGNED_LENGTH(origObj->header.length));
    neededSpace = sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry);

    /* Is the old object at the end in the page? */
    if (apage->header.free == offset + sizeof(ObjectHdr) + alignedLen)
	last = TRUE;
    else
	last = FALSE;
    
    if (alignedLen >= neededSpace ||
	(last && neededSpace <= (SP_CFREE(apage) + alignedLen))) {
	
	/* mark the root node is with the header. */
	origObj->header.properties = P_LRGOBJ | P_LRGOBJ_ROOTWITHHDR;

	anode = (L_O_T_INode *)origObj->data;

	if (last)
	    apage->header.free -= alignedLen - neededSpace;
	else
	    apage->header.unused += alignedLen - neededSpace;
	
    } else if (SP_CFREE(apage) >= sizeof(ObjectHdr) + neededSpace) {
	/* assert that last is FALSE */

	/* move the old object to the end of the data area */
	offset = apage->header.free;

	/*@ copy the object header */
	memcpy(&(apage->data[offset]), (char*)&(origObj->header), sizeof(ObjectHdr));

	/* now, obj points to the moved object */
	origObj = (Object *)&(apage->data[offset]);
	anode = (L_O_T_INode *)origObj->data;
	
	/* mark the root node is with the header. */
	origObj->header.properties = P_LRGOBJ | P_LRGOBJ_ROOTWITHHDR;

	/* change the offset of slot */
	apage->slot[-slotNo].offset = offset;
	
	apage->header.unused += sizeof(ObjectHdr) + alignedLen;
	apage->header.free += sizeof(ObjectHdr) + neededSpace;
	
    } else if (SP_FREE(apage)+alignedLen >= neededSpace) {

	OM_CompactPage(handle, apage, slotNo);

	offset = apage->slot[-slotNo].offset;
	origObj = (Object *)&(apage->data[offset]);
	anode = (L_O_T_INode *)origObj->data;

	/* mark the root node is with the header. */
	origObj->header.properties = P_LRGOBJ | P_LRGOBJ_ROOTWITHHDR;
	
	apage->header.free -= alignedLen - neededSpace;
	
    } else {
	/* create the root page */
	e = RDsM_AllocTrains(handle, fid.volNo, firstExt, &apage->header.pid,
			     eff, 1, PAGESIZE2, &root); 
	if (e < 0) ERR(handle, e);

	/*@ store the root PageNo */
	*((ShortPageID *)origObj->data) = root.pageNo;

	origObj->header.properties = P_LRGOBJ;

	if (last)
	    apage->header.free -= alignedLen - sizeof(ShortPageID);
	else
	    apage->header.unused += alignedLen - sizeof(ShortPageID);
	
	/* initialize the root node */
	e = BfM_GetNewTrain(handle, &root, (char **)&anode, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }
    
    /*@ set the node values */
    anode->header.pid = root;   
    anode->header.height = 1;
    anode->header.nEntries = 1;
    anode->entry[0].spid = leafPid.pageNo;
    anode->entry[0].count = origObj->header.length;

    /* set page type */
    SET_PAGE_TYPE(anode, LOT_I_NODE_TYPE);

    /* set temporary flag */
    if( isTmp ) SET_TEMP_PAGE_FLAG(anode);
    else        RESET_TEMP_PAGE_FLAG(anode);

    if (!(origObj->header.properties & P_LRGOBJ_ROOTWITHHDR)) {
	/* root node is in the separated page. */
	
	e = BfM_SetDirty(handle, &root, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &root, PAGE_BUF);
	
	e = BfM_FreeTrain(handle, &root, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }

    return(eNOERROR);
    
} /* LOT_ConverToLarge() */
