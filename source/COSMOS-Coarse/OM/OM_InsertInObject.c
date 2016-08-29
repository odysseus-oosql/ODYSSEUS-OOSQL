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
 * Module : OM_InsertInObject.c
 * 
 * Description : 
 *  OM_InsertInObject() causes 'length' bytes of data to be inserted into 
 *  the specified object.
 *
 * Exports:
 *  Four OM_InsertInObject(ObjectID*, ObjectID*, Four, Four, char*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"		/* for tracing : TR_PRINT() macro */
#include "BfM.h"		/* for the buffer manager call */
#include "LOT.h"		/* for the large object manager call */
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * OM_InsertInObject()
 *================================*/
/*
 * Function: Four OM_InsertInObject(ObjectID*, ObjectID*, Four, Four, char*)
 * 
 * Description : 
 *  (1) What to do?
 *  OM_InsertInObject() causes 'length' bytes of data to be inserted into 
 *  the specified object. The insertion begins at the offset specified by
 *  the parameter 'start'.
 *
 *  (2) How to do?
 *  a. Read in the slotted page
 *  b. See the object header
 *  c. IF moved object THEN
 *         call this routine recursively for the forwarded object
 *     ELSE
 *         IF large object THEN
 *             call the large object manager's LOT_InsertInObject()
 *         ELSE
 *             insert the data into the buffer
 *         ENDIF
 *     ENDIF
 *  d. Free the buffer page
 *  e. Return
 *
 * Returns:
 *  error code
 *    eBADCATALOGOBJECT_OM
 *    eBADOBJECTID_OM
 *    eBADLENGTH_OM
 *    eBADUSERBUF_OM
 *    eBADFILEID_OM
 *    eBADSTART_OM
 *
 * Side Effects :
 *  'data' is inserted into the object.
 *  object size is updated reflecting the insertion.
 *  object may be moved into the other object. (properties bit is updated)
 */
Four OM_InsertInObject(
    Four handle,
    ObjectID 		*catObjForFile,	/* IN file containing the object */
    ObjectID 		*oid,			/* IN object to insert in */
    Four     		start,			/* IN starting offset of insert */
    Four     		length,			/* IN amount of data to insert */
    char     		*data,			/* IN data to append */
    Pool     		*dlPool,		/* IN pool of dealloc list elements */
    DeallocListElem *dlHead) 		/* INOUT head of dealloc list */
{
    Four 					e;					/* error number */
    FileID 					fid;				/* ID of file where the object was placed */
    PageID 					pid;				/* page holding the object */
    SlottedPage 			*apage;				/* point to buffer holding the page */
    Object 					*obj;				/* point to the object in page */
    Four 					offset;				/* offset in data area of the object */
    Four 					origLen;			/* length of the original object */
    Four 					alignedOrigLen;		/* aligned length of original length */
    Four 					totalLen;			/* original length + inserted length */
    Four 					alignedTotalLen;	/* aligned length of totalLen */
    ObjectID 				movedOid;			/* ObjectID of the moved object */
    ObjectHdr 				objHdr;				/* object header of the moved object */
    SlottedPage 			*catPage;			/* buffer page containing the catalog object */
    sm_CatOverlayForData 	*catEntry; 			/* overlay structure for catalog object access */
    char totalData[PAGESIZE-SP_FIXED]; 			/* save the data before compaction */
    
    
    TR_PRINT(TR_OM, TR1,
             ("OM_InsertInObejct(catObjForFile=%P, oid=%P, start=%ld, length=%ld, data=%P, dlPool=%P, dlHead=%P)",
	       catObjForFile, oid, start, length, data, dlPool, dlHead));

    /*@ check parameters */
    
    if (catObjForFile == NULL) ERR(handle, eBADCATALOGOBJECT_OM);

    if (oid == NULL) ERR(handle, eBADOBJECTID_OM);

    if (length < 0) ERR(handle, eBADLENGTH_OM);

    if (data == NULL) ERR(handle, eBADUSERBUF_OM);

    /* Get the data file's identifier from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    fid = catEntry->fid;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /*@ Get the PageID from the ObjectID */
    pid = *((PageID *)oid);
    
    /*@ read the slotted page into the system buffer */
    e = BfM_GetTrain(handle, &pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* check the given file contains the 'oid' object */
    if (!EQUAL_FILEID(fid, apage->header.fid))
	ERRB1(handle, eBADFILEID_OM, &pid, PAGE_BUF);
    
    /* check the 'oid' is valid. */
    if (!IS_VALID_OBJECTID(oid, apage)) 
	ERRB1(handle, eBADOBJECTID_OM, &pid, PAGE_BUF);

    /*@ Get the object from the 'oid' */
    offset = apage->slot[-(oid->slotNo)].offset;
    obj = (Object *)&apage->data[offset];

    if (start < 0 || start > obj->header.length)
	ERRB1(handle, eBADSTART_OM, &pid, PAGE_BUF);

    if (start == obj->header.length) {
	
	e = OM_AppendToObject(handle, catObjForFile, oid, length, data, dlPool, dlHead);
	if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
	
	e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	return(eNOERROR);
    }
    
    /* If the page is in the available space list, remove it from the list */
    e = om_RemoveFromAvailSpaceList(handle, catObjForFile, &pid, apage);
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
    
    origLen = obj->header.length;
    alignedOrigLen = MAX(sizeof(ShortPageID), ALIGNED_LENGTH(origLen));
    totalLen = obj->header.length + length;
    alignedTotalLen = MAX(sizeof(ShortPageID), ALIGNED_LENGTH(totalLen));
    
    if (obj->header.properties & P_MOVED) {
	/*@ This is the moved object: recursively call OM_InsertInObject() */
	TR_PRINT(TR_OM, TR2, ("This is the moved object.\n"));

	if (alignedTotalLen > LRGOBJ_THRESHOLD) {
	    /* we should convert the object to the large object. */
	    
	    e = LOT_ConvertToLarge(handle, catObjForFile, apage, oid->slotNo, dlPool, dlHead);
	    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
	    
	    e = LOT_InsertInObject(handle, catObjForFile, &pid, oid->slotNo, start, length, data);
	    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

	    /* In LOT_InsertInObject(), obj may be moved to someplace. */
	    offset = apage->slot[-(oid->slotNo)].offset;
	    obj = (Object *)&(apage->data[offset]);

	} else {

	    /* Notice: If another move is to be occured, the new moved ObjectID
	       is set automatically in the called routine */
	    e = OM_InsertInObject(handle, catObjForFile, (ObjectID *)obj->data,
				  start, length, data, dlPool, dlHead);
	    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
	}
	
	obj->header.length += length;
    
    } else {			/*@ Normal Object */
	if (obj->header.properties & P_LRGOBJ) {
	    /*@ large object */
	    TR_PRINT(TR_OM, TR2, ("This is the large object.\n"));

	    e = LOT_InsertInObject(handle, catObjForFile, &pid,
				   oid->slotNo, start, length, data);
	    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

	    /* In LOT_InsertInObject(), obj may be moved to someplace. */
	    offset = apage->slot[-(oid->slotNo)].offset;
	    obj = (Object *)&(apage->data[offset]);
	    obj->header.length += length;

	} else {
	    if (alignedTotalLen > SP_FREE(apage) + alignedOrigLen) {
		/* There is *no* enough space. */
		/* We should move the object to the other page. */

		if (!(obj->header.properties & P_FORWARDED) &&
		    (alignedTotalLen > LRGOBJ_THRESHOLD ||
		    alignedOrigLen + SP_FREE(apage) < sizeof(ObjectID))) {
		    /* we should convert the object to the large object. */

		    e = LOT_ConvertToLarge(handle, catObjForFile, apage, oid->slotNo, dlPool, dlHead);
		    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

		    e = LOT_InsertInObject(handle, catObjForFile, &pid, oid->slotNo,
					   start, length, data);
		    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
		    
		    /* In LOT_InsertInObject(), obj may be moved to someplace. */
		    offset = apage->slot[-(oid->slotNo)].offset;
		    obj = (Object *)&(apage->data[offset]);
		    obj->header.length += length;

		} else {
		    /* We put in the page the ObjectID of the moved object */
		    
		    /* totalData has the original data with new data inserted */
		    memcpy(totalData, obj->data, start);
		    memcpy(&totalData[start], data, length);
		    memcpy(&totalData[start+length], &obj->data[start], origLen-start);
		    
		    /* The created object has P_FORWARDED property. */
		    objHdr.properties = obj->header.properties | P_FORWARDED;
		    objHdr.tag = obj->header.tag;
		    e = om_CreateObject(handle, catObjForFile, oid, &objHdr,
					totalLen, totalData, &movedOid);
		    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
		    
		    if (obj->header.properties & P_FORWARDED) {
			/* this is the forwarded object */
			
			/* delete the old forwarded object */
			e = OM_DestroyObject(handle, catObjForFile, oid, dlPool, dlHead);
			if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
			
			/* It is possible that OM_DestroyObject() insert the page into the available space list */
			/* If the page is in the available space list, remove it from the list */
			e = om_RemoveFromAvailSpaceList(handle, catObjForFile, &pid, apage);
			if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

			/* change the oid */
			*oid = movedOid;
			
		    } else {
			
			if (apage->header.free == offset+sizeof(ObjectHdr)+alignedOrigLen &&
			    sizeof(ObjectID) <= SP_CFREE(apage)+alignedOrigLen) {
			    /* This object is the last object in the page and
			     * ObjectID can be stored in place. */
			    
			    *((ObjectID *)obj->data) = movedOid;
			    
			    apage->header.free += sizeof(ObjectID) - alignedOrigLen;
			    
			} else if (alignedOrigLen >= sizeof(ObjectID)) {
			    /* write the moved ObjectID in the data area */
			    
			    *((ObjectID *)obj->data) = movedOid;
			    apage->header.unused += alignedOrigLen - sizeof(ObjectID);
			    
			} else if (sizeof(ObjectID) + sizeof(ObjectHdr) <= SP_CFREE(apage)) {
			    /* The contiguous free space can accomadate full object */
			    
			    /* copy the original object's header to the last data space */
			    *((ObjectHdr *)&apage->data[apage->header.free]) = obj->header;
			    
			    /* Now obj points to the new object */
			    obj = (Object *)(&(apage->data[apage->header.free]));
			    
			    /* store the Moved ObjectID */
			    *((ObjectID *)obj->data) = movedOid;
			    
			    apage->slot[-(oid->slotNo)].offset = apage->header.free;
			    apage->header.free += sizeof(ObjectHdr) + sizeof(ObjectID);
			    apage->header.unused += alignedOrigLen + sizeof(ObjectHdr);
			    
			} else {
			    /* compact the slotted page & write moved ObjectID */
			    
			    e = OM_CompactPage(handle, apage, oid->slotNo);
			    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
			    
			    /* Now the original object is the last one in page */
			    offset = apage->slot[-(oid->slotNo)].offset;
			    obj = (Object *)&(apage->data[offset]);
			    *((ObjectID *)obj->data) = movedOid;
			    apage->header.free += sizeof(ObjectID) - alignedOrigLen;
			}
			
			obj->header.properties |= P_MOVED;
			obj->header.length += length;
			
		    }		    
		}
		
	    } else {
		/* There is the enough space. */
				
		if (alignedOrigLen == alignedTotalLen) {
		    /* Just a few bytes are inserted. */

		    memmove(&(obj->data[start+length]), &(obj->data[start]), origLen-start);
		    memcpy(&(obj->data[start]), data, length);
		    		    
		} else if (apage->header.free == offset+sizeof(ObjectHdr)+alignedOrigLen &&
			   alignedTotalLen <= SP_CFREE(apage)+alignedOrigLen) {
		    /* This object is the last object in the page. */
		    /* move the bytes after the insertion point */
		    
		    memmove(&(obj->data[start+length]), &(obj->data[start]), origLen-start);
		    
		    /* insert the data */
		    memcpy(&(obj->data[start]), data, length);
		    
		    apage->header.free += alignedTotalLen - alignedOrigLen;
		    
		} else if (alignedTotalLen + sizeof(ObjectHdr) <= SP_CFREE(apage)) {
		    /* The contiguous free space can accomadate full object */
		    
		    /* copy the original object to the last data space */
		    memcpy(&(apage->data[apage->header.free]), (char*)obj, sizeof(ObjectHdr) + start);
		    memcpy(&(apage->data[apage->header.free+sizeof(ObjectHdr)+start+length]),
			   &(obj->data[start]), origLen-start);
		    
		    /* Now obj points to the new object */
		    obj = (Object *)(&(apage->data[apage->header.free]));
		    
		    /* insert the new data */
		    memcpy(&(obj->data[start]), data, length);
		    
		    apage->slot[-(oid->slotNo)].offset = apage->header.free;
		    apage->header.free += sizeof(ObjectHdr) + alignedTotalLen;
		    apage->header.unused += sizeof(ObjectHdr) + alignedOrigLen;
		    
		} else {
		    /* Complex Case: Compact the data page and insert it */
		    e = OM_CompactPage(handle, apage, oid->slotNo);
		    if (e < 0) ERRB1(handle, e, (PageID *)oid, PAGE_BUF);
		    
		    /* Now obj points to the new object */
		    offset = apage->slot[-(oid->slotNo)].offset;
		    obj = (Object *)(&(apage->data[offset]));
		    
		    /* move the bytes after the insertion point */
		    memmove(&(obj->data[start+length]), &(obj->data[start]), origLen-start);
		    
		    /*@ insert the new data */
		    memcpy(&(obj->data[start]), data, length);
		    
		    apage->header.free += alignedTotalLen - alignedOrigLen;
		}
		
		obj->header.length += length;
		
	    }
	}
	
    }

    e = om_PutInAvailSpaceList(handle, catObjForFile, &pid, apage);
    if (e < 0) ERR(handle, e);
	
    e = BfM_SetDirty(handle, &pid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

    e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
    
} /* OM_InsertInObject() */
