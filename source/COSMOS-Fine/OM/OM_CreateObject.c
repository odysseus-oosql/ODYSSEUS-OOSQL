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
 * Module : OM_CreateObject.c
 *
 * Description :
 * om_CreateObject( ) creates a new object near the specified object.
 * If there is no room in the page holding the specified object,
 * it trys to insert into the page in the available space list. If fail, then
 * the new object will be put into the newly allocated page.
 *
 * Exports:
 *  Four OM_CreateObject(Four, DataFileInfo*, ObjectID*, ObjectHdr*, Four, char*, ObjectID*, LockParameter*)
 *
 * Return Values :
 *  Error Code
 *    eBADCATOBJ_OM
 *    eBADLENGTH_OM
 *    eBADUSERBUF_OM
 *    some error codes from the lower level
 *
 * Side Effects :
 *  0) A new object is created.
 *  1) parameter oid
 *     'oid' is set to the ObjectID of the newly created object.
 */

#include "common.h"
#include "error.h"
#include "trace.h"		/* for tracing : TR_PRINT(handle, ) macro */
#include "latch.h"
#include "LOG.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"

Four OM_CreateObject(
    Four 		handle,
    XactTableEntry_T 	*xactEntry, /* IN transaction table entry */
    DataFileInfo 	*finfo,	/* IN file information */
    ObjectID  		*nearObj,		/* IN create the new object near this object */
    ObjectHdr 		*objHdr,		/* IN from which tag is to be set */
    Four      		length,		/* IN amount of data */
    char      		*data,		/* IN the initial data for the object */
    ObjectID  		*oid,		/* OUT the object's ObjectID */
    LockParameter 	*lockup,      /* IN request lock or not */
    LogParameter_T 	*logParam) /* IN log parameter */
{
    Four        	e;		/* error number */
    ObjectHdr   	objectHdr;	/* ObjectHdr with tag set from parameter */


    TR_PRINT(handle, TR_OM, TR1,
	     ("OM_CreateObject(handle, finfo=%P, nearObj=%P, objHdr=%P, length=%ld, data=%P, oid=%P",
	      finfo, nearObj, objHdr, length, data, oid));


    /* parameter checking */

    if (finfo == NULL) ERR(handle, eBADCATOBJ);

    if (length < 0) ERR(handle, eBADLENGTH_OM);

    if (length > 0 && data == NULL) return(eBADUSERBUF_OM);


    /* initialize ObjectHdr */
    objectHdr.properties = P_CLEAR;
    objectHdr.tag = 0;
    objectHdr.length = 0;
    if (objHdr != NULL)
	objectHdr.tag = objHdr->tag;

    if (ALIGNED_LENGTH(length) > LRGOBJ_THRESHOLD) {
	e = om_CreateObject(handle, xactEntry, finfo, nearObj, &objectHdr, 0, NULL, lockup, oid, logParam);
	if (e < eNOERROR) ERR(handle, e);

	e = OM_AppendToObject(handle, xactEntry, finfo, oid, length, data, lockup, logParam);
	if (e < eNOERROR) ERR(handle, e);

    } else {
	e = om_CreateObject(handle, xactEntry, finfo, nearObj, &objectHdr, length, data, lockup, oid, logParam);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* OM_CreateObject() */
