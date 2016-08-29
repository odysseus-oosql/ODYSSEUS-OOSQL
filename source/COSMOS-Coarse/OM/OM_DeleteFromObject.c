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
 * Module : OM_DeleteFromObject.c
 * 
 * Description : 
 *  OM_DeleteFromObject() deletes some bytes from the given object.
 *
 * Exports:
 *  Four OM_DeleteFromObject(ObjectID*, ObjectID*, Four, Four)
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
 * OM_DeleteFromObject()
 *================================*/
/*
 * Function: Four OM_DeleteFromObject(ObjectID*, ObjectID*, Four, Four)
 * 
 * Description : 
 *  (1) What to do?
 *  OM_DeleteFromObject() deletes some bytes from the given object.
 *  The delete position can be any byte within the object.
 *
 * (2) How to do?
 *  a. Read in the slotted page
 *  b. See the object header
 *  c. IF moved object THEN
 *	   call this function recursively
 *     ELSE 
 *         IF large object THEN 
 *             call the large object manager's lom_ReadObject()
 *	   ELSE 
 *	       delete the data from the buffer page
 *	   ENDIF
 *     ENDIF
 *  d. Free the buffer page
 *  e. Return
 *
 * Returns:
 *  error code
 *    eBADCATALOGOBJECT_OM
 *    eBADLENGTH_OM
 *    eBADOBJECTID_OM
 *    eTOOBIGSTART_OM	- start value is too big so it exceeds obj boundary
 *    some codes caused by function calls
 */
Four OM_DeleteFromObject(
    Four handle,
    ObjectID 		*catObjForFile,	/* IN file containing the object */
    ObjectID 		*oid,			/* IN object to delete from  */
    Four     		start,			/* IN starting offset of delete */
    Four     		length,			/* IN amount of data being deleted */
    Pool    		*dlPool,		/* INOUT pool of dealloc list elements */
    DeallocListElem *dlHead) 		/* INOUT head of the dealloc list */
{
    Four        			e;				/* error number */
    FileID      			fid;			/* ID of file which have the given object */
    PageID					pid;			/* page on which the given object resides */
    SlottedPage 			*apage;			/* pointer to buffer holding 'pid' page */
    Object					*obj;			/* pointer to the given object itself */
    Four					offset;			/* starting offset of object in data area */
    Four					origLen;		/* length of the original object */
    Four					alignedOrigLen;	/* aligned length of length */
    Four					newLen;			/* length of the object after delete */
    Four					alignedNewLen;	/* aligned length of newLen */
    SlottedPage 			*catPage;		/* buffer page containing the catalog object */
    sm_CatOverlayForData 	*catEntry; 		/* overlay structure for catalog object access */

    
    TR_PRINT(TR_OM, TR1,
             ("OM_DeleteFromObject(handle, catObjForFile=%P, oid=%P, start=%ld, length=%ld)",
	      catObjForFile, oid, start, length));

    
    /*@ check the parameters */
    if (catObjForFile == NULL) ERR(handle, eBADCATALOGOBJECT_OM);

    if (oid == NULL) ERR(handle, eBADOBJECTID_OM);

    if (length < 0 && length != REMAINDER) ERR(handle, eBADLENGTH_OM);


    /* Get the data file's ID from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    fid = catEntry->fid;

    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /*@ get the PageID of page on which the given object resides */
    pid = *((PageID *)oid);

    /*@ read the page into the buffer */
    e = BfM_GetTrain(handle, &pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* check the given file contains the 'oid' object */
    if (!EQUAL_FILEID(fid, apage->header.fid))
	ERRB1(handle, eBADFILEID_OM, &pid, PAGE_BUF);
    
    /* check that 'oid' is valid */
    if(!IS_VALID_OBJECTID(oid, apage))
	ERRB1(handle, eBADOBJECTID_OM, &pid, PAGE_BUF);

    /*@ 'obj' points to the object */
    offset = apage->slot[-(oid->slotNo)].offset;
    obj = (Object *)&(apage->data[offset]);

    /* check that start is valid */
    if (start < 0 || start >= obj->header.length)
	ERRB1(handle, eBADSTART_OM, &pid, PAGE_BUF);

    /* if length is REMAINDER, set length to # of bytes after starting offset */
    if (length == REMAINDER) length = obj->header.length - start;
    
    /* check that 'length' is valid.
       If length is too large, length is reduced to the remains. */
    if (start + length > obj->header.length)
	length = obj->header.length - start;

    /* If the page is in the available space list, remove it from the list. */
    e = om_RemoveFromAvailSpaceList(handle, catObjForFile, &pid, apage);
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

    if (obj->header.properties & P_MOVED) {
	/*@ This is the moved object: recursively call OM_DeleteFormObject() */

	TR_PRINT(TR_OM, TR2, ("This is the moved object.\n"));

	e = OM_DeleteFromObject(handle, catObjForFile, (ObjectID *)obj->data, start, length, dlPool, dlHead);
	if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
	
	obj->header.length -= length;
	
    } else {
	if (obj->header.properties & P_LRGOBJ) {
	    /*@ large object */
	    TR_PRINT(TR_OM, TR2, ("This is the large object.\n"));

	    e = LOT_DeleteFromObject(handle, catObjForFile, &pid, oid->slotNo, start, length, dlPool, dlHead);
	    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

	    /* In LOT_DeleteFromObject(), obj may be moved to someplace. */
	    offset = apage->slot[-(oid->slotNo)].offset;
	    obj = (Object *)&(apage->data[offset]);
	    obj->header.length -= length;
	    
	} else {
	    /* initialize the local variables related with length */
	    origLen = obj->header.length;
	    alignedOrigLen = MAX(sizeof(ShortPageID), ALIGNED_LENGTH(origLen));
	    newLen = origLen - length;
	    alignedNewLen = MAX(sizeof(ShortPageID), ALIGNED_LENGTH(newLen));
	    
	    /* delete the bytes by pulling bytes after the deleted bytes */
	    memmove(&obj->data[start], &obj->data[start+length],
		    obj->header.length-start-length);
	    obj->header.length -= length;
	    
	    /* Is the object at the end of data area? */
	    if (apage->header.free == offset+sizeof(ObjectHdr)+alignedOrigLen) {
		/* we decrement free instead of incrementing unused */
		
		apage->header.free -= alignedOrigLen - alignedNewLen;
		
	    } else {
		/* increment unused */
		
		apage->header.unused += alignedOrigLen - alignedNewLen;
	    }
	}
    }
    
    e = om_PutInAvailSpaceList(handle, catObjForFile, &pid, apage);
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
    
    e = BfM_SetDirty(handle, &pid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(length);

} /* OM_DeleteFromObject() */
