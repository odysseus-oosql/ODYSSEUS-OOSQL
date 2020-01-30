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
 * Module: SM_FetchObject.c
 *
 * Description:
 *  Read the given object's contents. If the given object is NULL, then read
 *  the current cursor's object.
 *
 * Exports:
 *  Four SM_FetchObject(Four, Four, ObjectID*, Four, Four, char*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "OM.h"
#include "BtM.h"
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * SM_FetchObject( )
 *================================*/
/*
 * Function: Four SM_FetchObject(Four, Four, ObjectID*, Four, Four, char*)
 *
 * Description:
 *  Read the given object's contents. If the given object is NULL, then read
 *  the current cursor's object.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eBADCURSOR
 *    some errors caused by function calls
 */
Four SM_FetchObject(
    Four handle,
    Four scanId,		/* IN scan to use */
    ObjectID *oid,		/* IN object to read */
    Four start,			/* IN starting offset of data to read */
    Four length,		/* IN amount of data to read */
    char *data,			/* OUT space to return the read data */
    LockParameter *lockup)      /* IN request lock or not */
{
    Four e, e1;			/* error number */ 
    ObjectID *readOid;		/* object to read */
    LockParameter *realLockup;


    TR_PRINT(handle, TR_SM, TR1,
	     ("SM_FetchObject(handle, scanId=%ld, oid=%P, start=%ld, length=%ld, data=%P, lockup=%P)",
	      scanId, oid, start, length, data, lockup));

    /*@ check parameters. */
    if (!VALID_SCANID(handle, scanId)) ERR(handle, eBADPARAMETER);

    if (length < 0 && length != REMAINDER) ERR(handle, eBADPARAMETER);

    if (length > 0 && data == NULL) ERR(handle, eBADPARAMETER);


    if(SM_NEED_AUTO_ACTION(handle)) {
        e = LM_beginAction(handle, &MY_XACTID(handle), AUTO_ACTION);
        if(e < eNOERROR) ERR(handle, e);
    }

    /*@ oid is equal to NULL? */
    /* If the 'oid' is NULL, then use the current cursor's object. */
    if (oid == NULL) {
	if (SM_SCANTABLE(handle)[scanId].cursor.any.flag != CURSOR_ON)
	    ERR(handle, eBADCURSOR);

	readOid = &(SM_SCANTABLE(handle)[scanId].cursor.any.oid);
    } else
	readOid = oid;

    /* check the lockup parameter */
    realLockup = NULL;

    if(lockup){
	if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
	if((lockup->mode != L_X) && (lockup->mode != L_S))
	    ERR(handle, eBADLOCKMODE_SM);
	switch ( SM_SCANTABLE(handle)[scanId].acquiredFileLock ) {
	  case L_IS :
	  case L_IX : realLockup = lockup; break;
	  case L_S  :
	  case L_SIX:
	  case L_X  : realLockup = NULL; break; /* already enough lock  */
          /*
           * This error can detected by LM_getObjectLock call
           * default   : ERR(handle, eINVALIDHIERARCHICALLOCK_SM);
           */
	}
    }

    if(length != 0){
	/* Read the object. */
	e = OM_ReadObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SCANTABLE(handle)[scanId].finfo.fid), readOid, start,
			  length, data, realLockup); 
	if (e < eNOERROR) ERR(handle, e);
    }
    else e = 0;

    if(ACTION_ON(handle)){  
	e1 = LM_endAction(handle, &MY_XACTID(handle), AUTO_ACTION); 
        if(e1 < eNOERROR) ERR(handle, e1);
    }

    return(e);

} /* SM_FetchObject() */


