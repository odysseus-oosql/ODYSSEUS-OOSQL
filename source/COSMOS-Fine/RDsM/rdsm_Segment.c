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
 * Module: rdsm_Segment.c
 *
 * Description:
 *  Manipulation the segment
 *
 * Exports:
 *  Four rdsm_InsertExtentToSegment(XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, LogParameter_T*)
 *  Four rdsm_RemoveExtentFromSegment(XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, LogParameter_T*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "RDsM.h"
#include "BfM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four rdsm_InsertExtentToSegment(XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, LogParameter_T*)
 *
 * Description:
 *  Insert new extent pointed by 'newExtent' to the segment
 *         between the extent pointed by 'midExtent' and the extent pointed by 'nextExtent'
 *
 * Returns:
 *  Error code
 */
Four rdsm_InsertExtentToSegment(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T		*xactEntry, 		/* IN transaction table entry */
    RDsM_VolumeInfo_T		*volInfo, 		/* IN volume information */
    AllocAndFreeExtentInfo_T	*midExtent, 		/* IN pointer of middle extent */
    AllocAndFreeExtentInfo_T	*nextExtent, 		/* IN pointer of next extnet */
    AllocAndFreeExtentInfo_T	*newExtent, 		/* IN pointer of new extent */
    LogParameter_T		*logParam		/* IN log parameter */
)
{
    Four			e;			/* returned error value */


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_InsertExtentToSegment(xactEntry=%P, volInfo=%P, midExtent=%P, nextExtent=%P, newExtent=%P, logParam=%P)", xactEntry, volInfo, midExtent, nextExtent, newExtent, logParam));


    /* update extent map about middle extent */
    e = rdsm_SetExtentMapInfo(handle, xactEntry, midExtent, NO_OP, newExtent->extentNo, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* update extent map about new extent */
    e = rdsm_SetExtentMapInfo(handle, xactEntry, newExtent, midExtent->extentNo, nextExtent->extentNo, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* update extent map about next extent */
    if (nextExtent->extentNo != NIL) {
    	e = rdsm_SetExtentMapInfo(handle, xactEntry, nextExtent, newExtent->extentNo, NO_OP, logParam);
    	if (e < eNOERROR) ERR(handle, e);
    }


    return (eNOERROR);
}


/*
 * Function: Four rdsm_RemoveExtentFromSegment(XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, LogParameter_T*)
 *
 * Description:
 *  Delete the extent pointed by 'midExtent'
 *         between the extent pointed by 'prevExtent' and the extent pointed by 'nextExtent'
 *
 * Returns:
 *  Error code
 */
Four rdsm_RemoveExtentFromSegment(
    Four                        handle,                 /* IN    handle */
    XactTableEntry_T		*xactEntry, 		/* IN  transaction table entry */
    RDsM_VolumeInfo_T		*volInfo, 		/* IN  volume information */
    AllocAndFreeExtentInfo_T	*prevExtent, 		/* IN  pointer of previous extent */
    AllocAndFreeExtentInfo_T	*midExtent, 		/* IN  pointer of middle extent */
    AllocAndFreeExtentInfo_T	*nextExtent, 		/* IN  pointer of next extent */
    LogParameter_T		*logParam		/* IN  log parameter */
)
{
    Four			e;			/* returned error value */


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_RemoveExtentFromSegment(xactEntry=%P, volInfo=%P, prevExtent=%P, midExtent=%P, nextExtent=%P, logParam=%P)", xactEntry, volInfo, prevExtent, midExtent, nextExtent, logParam));


    /* case: previous extent and next extent exist */
    if (prevExtent->extentNo != NIL && nextExtent->extentNo != NIL) {

	e = rdsm_SetExtentMapInfo(handle, xactEntry, prevExtent, NO_OP, nextExtent->extentNo, logParam);
	if (e < eNOERROR) ERR(handle, e);

	e = rdsm_SetExtentMapInfo(handle, xactEntry, nextExtent, prevExtent->extentNo, NO_OP, logParam);
	if (e < eNOERROR) ERR(handle, e);
    }
    /* case: previous extent doesn't exist */
    else if (nextExtent->extentNo != NIL) {

	e = rdsm_SetExtentMapInfo(handle, xactEntry, nextExtent, NIL, NO_OP, logParam);
	if (e < eNOERROR) ERR(handle, e);
    }
    /* case: next extent doesn't exist */
    else if (prevExtent->extentNo != NIL) {

	e = rdsm_SetExtentMapInfo(handle, xactEntry, prevExtent, NO_OP, NIL, logParam);
	if (e < eNOERROR) ERR(handle, e);
    }
    else {

	return (eBADPARAMETER);
    }

    return (eNOERROR);
}
