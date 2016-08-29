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
 * Module: LOT_DeleteFromObject.c
 *
 * Description:
 *  Delete the given bytes from the large object tree.
 *
 * Exports:
 *  Four LOT_DeleteFromObject(ObjectID*, PageID*, Two, Four, Four, Pool*, DeallocListElem*)
 */


#include "common.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * LOT_DeleteFromObject()
 *================================*/
/*
 * Function: Four LOT_DeleteFromObject(ObjectID*, PageID*, Two, Four, Four)
 *
 * Description:
 *  Delete the given bytes from the large object tree.
 *
 * Returns:
 *  error code
 *    eBADCATALOGOBJECT_LOT
 *    eBADPAGEID_LOT
 *    eBADOFFSET_LOT
 *    eBADLENGTH_LOT
 *    eBADSLOTNO_LOT
 */
Four LOT_DeleteFromObject(
    Four handle,
    ObjectID            *catObjForFile, /* IN information for file holding LOT */
    PageID              *pid,           /* IN slotted page holding the object header */
    Two                 slotNo,         /* IN slot no of the given object */
    Four                start,          /* IN starting offset of delete */
    Four                length,         /* IN amount of data to delete */
    Pool                *dlPool,        /* INOUT pool of deallocated list elements */
    DeallocListElem     *dlHead)        /* INOUT head of deallocated list */
{
    Four                e;              /* Error Number */
    Four                firstExt;       /* first Extent No of file */
    SlottedPage         *apage;         /* pointer to buffer holding slotted page */
    Two                 offset;         /* starting offset of object in page */
    Object              *obj;           /* pointer to the object header */
    L_O_T_ItemList      list;           /* infomation for root node */
    Boolean             uf;             /* underflow flag */
    Boolean             mf;             /* merge flag */
    PageID              root;           /* root of large object tree */
    PageID              leafPid;        /* PageID of newly allocated leaf node */
    Two                 rootSlotNo;     /* slot No of object, NIL if root is separated page */
    L_O_T_Path          path;           /* cut path */
    Boolean             f;              /* indicates the change of root PageID */
    Two                 c_which;
    DeallocListElem     *dlElem;        /* an element of the dealloc list */
    L_O_T_INodeEntry    tmpEntries[2];  /* temporary entries */ 
    
    TR_PRINT(TR_LOT, TR1,
             ("LOT_DeleteFromObject(handle, catObjForFile=%P, pid=%P, slotNo=%ld, start=%ld, length=%ld)",
	      catObjForFile, pid, slotNo, start, length));

    
    /*@ checking the parameters */
    if (catObjForFile == NULL)	/* catObjForFile unexpected NULL */
	ERR(handle, eBADCATALOGOBJECT_LOT);

    if (pid == NULL)		/* pid unexpected NULL */
	ERR(handle, eBADPAGEID_LOT);
    
    if (start < 0)		/* bad starting offset of insert*/
	ERR(handle, eBADOFFSET_LOT);
    
    if (length < 0)		/* bad length (< 0) of insert */
	ERR(handle, eBADLENGTH_LOT);

    if (length == 0) return(eNOERROR);
    

    /*@ Read the slotted page into the buffer */
    e = BfM_GetTrain(handle, pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (slotNo < 0 || slotNo >= apage->header.nSlots)
	/* slotNo invalid */
	ERRB1(handle, eBADSLOTNO_LOT, pid, PAGE_BUF);

    offset = apage->slot[-slotNo].offset;
    obj = (Object *)&(apage->data[offset]);

    if (obj->header.properties & P_LRGOBJ_ROOTWITHHDR) {
	/* root node is with the object header */

	root = *pid;
	rootSlotNo = slotNo;
	
    } else {

	/*@ get root PageID */
	MAKE_PAGEID(root, pid->volNo, *((ShortPageID *)obj->data));
	rootSlotNo = NIL;

    }
	    
    list.nEntries = 2;
    list.entry = tmpEntries; 
    list.entry[0].spid = root.pageNo;
    list.entry[0].count = obj->header.length;
    list.entry[1].spid = NIL;
    list.entry[1].count = 0;

    /*@ Initialize the path */
    lot_InitPath(handle, &path);
    
    e = lot_DeleteFromObject(handle, catObjForFile, &list, rootSlotNo, start,
			     start+length-1, &c_which, &uf, &mf, &path, dlPool, dlHead);
    if (e < 0) {
	lot_FinalPath(handle, &path);
	ERRB1(handle, e, pid, PAGE_BUF);
    }

    if (list.entry[0].count == 0) {
	/* special case : all bytes are deleted */

	if (obj->header.properties & P_LRGOBJ_ROOTWITHHDR) {

	    apage->header.unused += sizeof(L_O_T_INodeHdr) - sizeof(ShortPageID);
	    
	    /* we free the root because the root node wasn't freed in above call */
	    e = BfM_SetDirty(handle, &root, PAGE_BUF);
	    if (e < 0) ERRB2(handle, e, &root, PAGE_BUF, pid, PAGE_BUF);
	    
	    e = BfM_FreeTrain(handle, &root, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	    
	} else {
	    /* free the root page */
	    
	    e = BfM_FreeTrain(handle, &root, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

            /* remove page from buffer pool */
            e = BfM_RemoveTrain(handle, &root, PAGE_BUF, FALSE);
            if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

	    e = Util_getElementFromPool(handle, dlPool, &dlElem);
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

            dlElem->type = DL_PAGE;
	    dlElem->elem.pid = root; /* save the PageID. */
	    dlElem->next = dlHead->next; /* insert to the list */
	    dlHead->next = dlElem;       /* new first element of the list */

	}
	
	obj->header.properties = P_CLEAR;

	e = BfM_SetDirty(handle, pid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

	
    } else {

	e = lot_RebalanceTree(handle, catObjForFile, &path, &root, &uf, &f, dlPool, dlHead);
	if (e < 0) {
	    lot_FinalPath(handle, &path);
	    ERRB1(handle, e, pid, PAGE_BUF);
	}

	if (f) {
	    if (obj->header.properties & P_LRGOBJ_ROOTWITHHDR)
		obj->header.properties ^= P_LRGOBJ_ROOTWITHHDR;
	    
	    *((ShortPageID *)obj->data) = root.pageNo;
	    
	    e = BfM_SetDirty(handle, pid, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

	}

	if (!(obj->header.properties & P_LRGOBJ_ROOTWITHHDR)) {
	    e = lot_MakeRootWithHdr(handle, pid, apage, slotNo, &root, dlPool, dlHead);
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	}
    }
	
    /* free(list.entry); */ 
    
    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* LOT_DeleteFromObject() */
