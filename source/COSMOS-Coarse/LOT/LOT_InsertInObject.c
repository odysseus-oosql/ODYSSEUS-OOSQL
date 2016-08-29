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
 * Module :	LOT_InsertInObject.c
 *
 * Description :
 *  Insert data to the large object tree.
 *
 * Exports :
 *  Four LOT_InsertInObject(ObjectID*, PageID*, Two, Four, Four, char*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * LOT_InsertInObject()
 *================================*/
/*
 * Function: Four LOT_InsertInObject(ObjectID*, PageID*, Two, Four, Four, char*)
 *
 * Description :
 *  Insert data to the large object tree.
 *
 * Returns:
 *  Error codes
 *    eBADCATALOGOBJECT_LOT
 *    eBADPAGEID_LOT
 *    eBADOFFSET_LOT
 *    eBADLENGTH_LOT
 *    eBADPARAMETER_LOT
 *    eBADSLOTNO_LOT
 *    some errors caused by function calls
 */
Four LOT_InsertInObject(
    Four handle,
    ObjectID       *catObjForFile,      /* IN file info to hold the L_O_T */
    PageID         *pid,                /* IN slotted page holding the object header */
    Two            slotNo,              /* IN slot No of the given object */
    Four           start,               /* IN starting offset to insert */
    Four           length,              /* IN amount of data to insert */
    char           *data)               /* IN user buffer holding the data */
{
    Four           e;                   /* error number */
    Four           height;              /* height of root node */
    SlottedPage    *apage;              /* pointer to buffer holding slotted page */
    Two            offset;              /* starting offset of object in slotted page */
    Object         *obj;                /* pointer to object header in slotted page */
    PageID         root;                /* PID of subtree root page */
    Two            rootSlotNo;          /* slot no of slot at which object is located if root */
                                        /* node is in slotted page; otherwise NIL */
    L_O_T_INode    *anode;              /* subtree's root node */
    L_O_T_ItemList childItems;          /* storage to hold slots overflowed from child node */
    Boolean        f;                   /* indicates the change of root PageID */

    
    TR_PRINT(TR_LOT, TR1,
             ("LOT_InsertInObject(handle, catObjForFile=%P, pid=%P, slotNo=%ld, start=%ld, length=%ld, data=%P)",
	      catObjForFile, pid, slotNo, start, length, data));

    /*@ checking the parameters */
    if (catObjForFile == NULL)	/* catObjForFile unexpected NULL */
	ERR(handle, eBADCATALOGOBJECT_LOT);

    if (pid == NULL)		/* pid unexpected NULL */
	ERR(handle, eBADPAGEID_LOT);

    if (start < 0)		/* bad starting offset of insert */
	ERR(handle, eBADOFFSET_LOT);

    if (length < 0)		/* bad length (< 0) of insert */
	ERR(handle, eBADLENGTH_LOT);

    /* just return */
    if (length == 0) return(eNOERROR);
    
    if (data == NULL)		/* data buffer unexpected NULL */
	ERR(handle, eBADPARAMETER_LOT);

    
    /*@ Read the slotted page into the buffer */
    e = BfM_GetTrain(handle, pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (slotNo < 0 || slotNo >= apage->header.nSlots) /* slotNo invalid */
	ERRB1(handle, eBADSLOTNO_LOT, pid, PAGE_BUF);
    
    offset = apage->slot[-slotNo].offset;
    obj = (Object *)&(apage->data[offset]);

    if (start > obj->header.length) /* large starting offset of insert */
	ERRB1(handle, eBADOFFSET_LOT, pid, PAGE_BUF);

    if (obj->header.properties & P_LRGOBJ_ROOTWITHHDR) {
	/* root node is in the slotted page. */
	root = *pid;
	rootSlotNo = slotNo;
	
	anode = (L_O_T_INode *)obj->data;
	height = anode->header.height;

    } else {

	/*@ get root PageID */
	MAKE_PAGEID(root, pid->volNo, *((ShortPageID *)obj->data));
	rootSlotNo = NIL;
	
	/* Read the node from the disk into the buffer */
	e = BfM_GetTrain(handle, &root, (char **)&anode, PAGE_BUF);
	if(e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	
	height = anode->header.height;
	
	e = BfM_FreeTrain(handle, &root, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
    }
    
    if (start == obj->header.length)
	/* append the data to the large object tree */
	e = lot_AppendToObject(handle, catObjForFile, &root, rootSlotNo,
			       length, data, &childItems, &f);
    else
	/* insert the data into the large object tree */
	e = lot_InsertInObject(handle, catObjForFile, &root, rootSlotNo, start,
			       length, data, &childItems, &f);
    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
    
    /* overflow handling */
    if (f) {	    
	e = lot_MakeRoot(handle, catObjForFile, pid, slotNo, height+1, &childItems);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

    }
    
    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return (eNOERROR);

} /* LOT_InsertInObject() */
