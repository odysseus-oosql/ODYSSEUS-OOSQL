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
 * Module: SM_CreateObject.c
 *
 * Description:
 *  Create an object into the opened file.
 *
 * Exports:
 *  Four SM_CreateObject(Four, Four, ObjectID*, ObjectHdr*, Four, void*, ObjectID*)
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
 * SM_CreateObject( )
 *================================*/
/*
 * Function: Four SM_CreateObject(Four, Four, ObjectID*, ObjectHdr*, Four, void*, ObjectID*)
 *
 * Description:
 *  Create an object into the opened file. The file is specified by 'scanId'.
 *  The new object is inserted near 'nearObj' object. The new object's ObjectID
 *  is returned via 'oid'.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 */
Four SM_CreateObject(
    Four      handle,
    Four      scanId,		/* IN scan to use; specify the data file */
    ObjectID  *nearObj,		/* IN create the new object near this object */
    ObjectHdr *objHdr,		/* IN tag value of the new object's header*/
    Four      length,		/* IN amount of data */
    char      *data,		/* IN the initial data of the new object */
    ObjectID  *oid,		/* OUT the created object's ObjectID */
    LockParameter *lockup)      /* IN request lock or not */
{
    Four e;			/* error number */
    LockParameter *realLockup;
    LogParameter_T logParam;


    TR_PRINT(handle, TR_SM, TR1,
	     ("SM_CreateObject(handle, scanId=%ld, nearObj=%P, objHdr=%P, length=%ld, data=%P, oid=%P, lockup=%P)",
	      scanId, nearObj, objHdr, length, data, oid, lockup));


    /*@ check parameters */

    if (!VALID_SCANID(handle, scanId)) ERR(handle, eBADPARAMETER);

    if (oid == NULL) ERR(handle, eBADPARAMETER);

    if (length < 0) ERR(handle, eBADPARAMETER);

    if (length > 0 && data == NULL) ERR(handle, eBADPARAMETER);

    if(SM_NEED_AUTO_ACTION(handle)) {
        e = LM_beginAction(handle, &MY_XACTID(handle), AUTO_ACTION);
        if(e < eNOERROR) ERR(handle, e);
    }

    /* check the lockup parameter */
    realLockup = NULL;

    if(lockup){
	if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
	if(lockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        switch ( SM_SCANTABLE(handle)[scanId].acquiredFileLock ) {
	  case L_IS :
	  case L_S  : ERR(handle, eINVALIDHIERARCHICALLOCK_SM);
	  case L_X  : realLockup = NULL; break; /* already enough lock  */
	  case L_SIX:
	  case L_IX : realLockup = lockup; break;
/*
** This error can detected by LM_getObjectLock call
		default   : ERR(handle, eINVALIDHIERARCHICALLOCK_SM);
*/
	}
    }

    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, SM_SCANTABLE(handle)[scanId].finfo.tmpFileFlag);

    /*@ create the object */
    /* Create an object into the data file. */
    e = OM_CreateObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SCANTABLE(handle)[scanId].finfo),
			nearObj, objHdr, length, data, oid, realLockup, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    if(ACTION_ON(handle)){  
	e = LM_endAction(handle, &MY_XACTID(handle), AUTO_ACTION); 
        if(e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* SM_CreateObject() */


