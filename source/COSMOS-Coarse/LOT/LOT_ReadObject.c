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
 * Module : 	LOT_ReadObject.c
 *
 * Description :
 *  Read the large object data from disk into the user supplied buffer.
 * The function calls itself recursively. The basis is the lot_ReadDataPage()
 * function call at the leaf node.
 *
 * Exports :
 *  Four LOT_ReadObject(PageID*, Two, Four, Four, char*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * LOT_ReadObject()
 *================================*/
/*
 * Function: Four LOT_ReadObject(PageID*, Two, Four, Four, char*)
 *
 * Description :
 *  Read the large object data from disk into the user supplied buffer.
 * The function calls itself recursively. The basis is the lot_ReadDataPage()
 * function call at the leaf node.
 *
 * Returns:
 *  Error codes
 *    eBADPAGEID_LOT
 *    eBADOFFSET_LOT
 *    eBADLENGTH_LOT
 *    eBADPARAMETER_LOT
 *    eBADSLOT_LOT
 *    some errors caused by function calls
 *
 * NOTE:
 *  The parameters are not checked. The caller should pass the correct
 * parameters. For example, root should not be NIL, start & length must be
 * less than the object size, and buf may not be NULL.
 */
Four LOT_ReadObject(
    Four handle,
    PageID              *pid,           /* IN page containing the object */
    Two                 slotNo,         /* IN slot no of object */
    Four                start,          /* IN starting offset of read */
    Four                length,         /* IN amount of data to read */
    char                *data)          /* OUT user buffer holding the data */
{
    Four                e;              /* error code */
    SlottedPage         *apage;         /* pointer to buffer holding slotted page */
    Two                 offset;         /* starting offset of object in slotted page */
    Object              *obj;           /* pointer to object in slotted page */
    PageID              root;           /* root page of the large object tree */
    
    TR_PRINT(TR_LOT, TR1,
            ("LOT_ReadObject(handle, pid=%P, slotNo=%ld, start=%ld, length=%ld, data=%P)\n",
	     pid, slotNo, start, length, data));

    /*@ checking the parameters */
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

    /*@ read the slotted page into the buffer */
    e = BfM_GetTrain(handle, pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (slotNo < 0 || slotNo >= apage->header.nSlots) /* slotNo invalid */
	ERRB1(handle, eBADSLOTNO_LOT, pid, PAGE_BUF);

    offset = apage->slot[-slotNo].offset;
    obj = (Object *)&(apage->data[offset]);

    if (obj->header.properties & P_LRGOBJ_ROOTWITHHDR) {
	e = lot_ReadObject(handle, pid, slotNo, start, length, data);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	
    } else {
	/* get the root PageID */
	MAKE_PAGEID(root, pid->volNo, *((ShortPageID *)obj->data));
	
	e = lot_ReadObject(handle, &root, NIL, start, length, data);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
    }
    
    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if(e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* LOT_ReadObject() */
