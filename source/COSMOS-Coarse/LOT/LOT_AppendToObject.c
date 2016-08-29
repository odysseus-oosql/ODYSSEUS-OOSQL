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
 * Module:	LOT_AppendToObject.c
 *
 * Description:
 *  Append data to the large object. 
 *
 * Exports:
 *  Four LOT_AppendToObject(ObjectID*, PageID*, Two, Four, char*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * LOT_AppendToObject()
 *================================*/
/*
 * Function: Four LOT_AppendToObject(ObjectID*, PageID*, Two, Four, char*)
 *
 * Description:
 *  Append data to the large object. 
 *  It is sufficient to handle the large object with LOT_InsertToObject().
 * But this append function is designed specially for efficient storage
 * utilization. User can make very large object with high store utilization
 * by successive call of append function.
 *
 * Returns:
 *  error code
 *    eBADCATALOGOBJECT_LOT
 *    eBADPAGEID_LOT
 *    eBADLENGTH_LOT
 *    eBADPARAMETER_LOT
 *    eBADSLOTNO_LOT
 *    some errors caused by function calls
 */
Four LOT_AppendToObject(
    Four handle,
    ObjectID            *catObjForFile, /* IN file containing the L_O_T */
    PageID              *pid,           /* IN slotted page holding the object header */
    Two                 slotNo,         /* IN slot No of the given object */
    Four                length,         /* IN amount of data to append */
    char                *data)          /* IN user buffer holding the data */
{
    Four                e;              /* error number */
    Four                height;         /* height of root node */
    SlottedPage         *apage;         /* pointer to buffer holding slotted page */
    Two                 offset;         /* starting offset of object in page */
    Object              *obj;           /* pointer to the object header */
    PageID              root;           /* root of the large object tree */
    L_O_T_INode         *anode;         /* subtree's root node */
    L_O_T_ItemList      childItems;     /* storage to hold slots overflowed from child node */
    Boolean             f;              /* indicates the change of root */

    
    TR_PRINT(TR_LOT, TR1,
             ("LOT_AppendToObject(handle, catObjForFile=%P, root=%P, length=%ld, data=%P, f=%P)",
	      catObjForFile, root, length, data, f));

    /*@ checking the parameters */
    if (catObjForFile == NULL)	/* catObjForFile unexpected NULL */
	ERR(handle, eBADCATALOGOBJECT_LOT);

    if (pid == NULL)		/* pid unexpected NULL */
	ERR(handle, eBADPAGEID_LOT);
    
    if (length < 0)			/* bad length (< 0) of insert */
	ERR(handle, eBADLENGTH_LOT);

    if (length == 0) return(eNOERROR);
    
    if (data == NULL)		/* data buffer unexpected NULL */
	ERR(handle, eBADPARAMETER_LOT);

    
    /*@ Read the slotted page into the buffer */
    e = BfM_GetTrain(handle, pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (slotNo < 0 || slotNo >= apage->header.nSlots)
	/* slotNo invalid */
	ERRB1(handle, eBADSLOTNO_LOT, pid, PAGE_BUF);

    offset = apage->slot[-slotNo].offset;
    obj = (Object *)&(apage->data[offset]);

    if (obj->header.properties & P_LRGOBJ_ROOTWITHHDR) {
	/* root node is in the slotted page. */

	anode = (L_O_T_INode *)obj->data;
	height = anode->header.height;

	/*@ append the data to the large object tree */
	e = lot_AppendToObject(handle, catObjForFile, pid, slotNo,
			       length, data, &childItems, &f);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	
    } else {

	/*@ get root PageID */
	MAKE_PAGEID(root, pid->volNo, *((ShortPageID *)obj->data));
	
	/*@ Read the node from the disk into the buffer */
	e = BfM_GetTrain(handle, &root, (char **)&anode, PAGE_BUF);
	if(e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	
	height = anode->header.height;
	
	e = BfM_FreeTrain(handle, &root, PAGE_BUF);
	if(e < 0) ERRB1(handle, e, pid, PAGE_BUF);

	/* append the data to the large object tree */
	e = lot_AppendToObject(handle, catObjForFile, &root, NIL,
			       length, data, &childItems, &f);
	if(e < 0) ERRB1(handle, e, pid, PAGE_BUF);
    }

    /* overflow handling */
    if (f) {
	e = lot_MakeRoot(handle, catObjForFile, pid, slotNo, height+1, &childItems);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	
    }

    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);

} /* LOT_AppendToObject() */
