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
 * Module: SM_NextObject.c
 *
 * Description:
 *  Return the next object.
 *
 * Exports:
 *  Four SM_NextObject(Four, Four, ObjectID*, ObjectHdr*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "OM.h"
#include "BtM.h"
#include "MLGF.h"
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * SM_NextObject( )
 *================================*/
/*
 * Function: Four SM_NextObject(Four, Four, ObjectID*, ObjectHdr*)
 *
 * Description:
 *  Return the next object. The word 'next' in the next object does not mean
 *  the physical order but the scan order. So if the scan direction is
 *  reversed, the word 'next' in the next object means the previous object
 *  in the physical order.
 *
 * Returns:
 *  1) Error code - negative value
 *        some errors caused by function calls
 *  2) return EOS if the scan reaches the end of the scan.
 */
Four SM_NextObject(
    Four handle,
    Four scanId,		/* IN scan to use */
    ObjectID *oid,		/* OUT the next object */
    ObjectHdr *objHdr,		/* OUT object header of the next object */
    char *extraData,            /* OUT extra data available via index */
    Cursor **cursor,            /* OUT pointer to the cursor */
    LockParameter *lockup)      /* IN request lock or not */
{
    Four e;			/* error code */
    ObjectID    nextOid;	/* the next object's ObjectID */
    Cursor nextCursor;		/* the B+ tree cursor of the next object */
    LockParameter *realLockup;


    TR_PRINT(handle, TR_SM, TR1,
	     ("SM_NextObject(handle, scanId=%ld, oid=%P, objHdr=%P, lockup=%P)",
	      scanId, oid, objHdr, lockup));


    /*@ check parameters */

    if (!VALID_SCANID(handle, scanId)) ERR(handle, eBADPARAMETER);

    if(SM_NEED_AUTO_ACTION(handle)) {
        e = LM_beginAction(handle, &MY_XACTID(handle), AUTO_ACTION);
        if(e < eNOERROR) ERR(handle, e);
    }

    realLockup = NULL;

    if (lockup) {
	switch (SM_SCANTABLE(handle)[scanId].scanType) {
	  case SEQUENTIAL:
	    /* check the lock mode of the data file */
	    /* SEQUENTIAL scan need lock on the data file by S or X or SIX mode */
	    switch(SM_SCANTABLE(handle)[scanId].acquiredFileLock ) {
	      /* we here permit IS or IX lock mode.
		   * Some certification should be needed
	      case L_SIX:
	      case L_S  :
	      case L_X  : realLockup = NULL; break;  * already enough lock *
	      case L_IS :
	      case L_IX : ERR(handle, eINVALIDHIERARCHICALLOCK_SM);
	      */
	      case L_SIX:
	      case L_S  :
	      case L_X  :
	      case L_IS :
	      case L_IX : realLockup = NULL; break; /* already enough lock  */
              /*
               * This error can detected by LM_getObjectLock call
               * default   : ERR(handle, eINVALIDHIERARCHICALLOCK_SM);
               */
	    }
            break;

          case BTREEINDEX:
	  case MLGFINDEX:
	    /* check the lockup parameter */
	    if(lockup){
		if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
		if((lockup->mode != L_X) && (lockup->mode != L_S)) ERR(handle, eBADLOCKMODE_SM);
		switch ( SM_SCANTABLE(handle)[scanId].acquiredFileLock ) {
		  case L_S  :
		  case L_X  : realLockup = NULL; break; /* already enough lock  */
		  case L_IS :
		  case L_SIX:
		  case L_IX : realLockup = lockup; break;
                  /*
                   * This error can detected by LM_getObjectLock call
                   * default   : ERR(handle, eINVALIDHIERARCHICALLOCK_SM);
                   */
		}

	    }
            break;

          default:
            ERR(handle, eINTERNAL);

        } /* case  */
    } /* if lockup */

    switch (SM_SCANTABLE(handle)[scanId].cursor.any.flag) {

      case CURSOR_INVALID:	/* invalid current cursor position */
	ERR(handle, eBADCURSOR);

      case CURSOR_BOS:		/* Begin of the scan */
        switch (SM_SCANTABLE(handle)[scanId].scanType) {
	  case SEQUENTIAL:
            /* Initialy set the cursor to invalid. */
            SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_INVALID;

            if (SM_SCANTABLE(handle)[scanId].scanInfo.seq.direction == FORWARD) {
                e = OM_NextObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SCANTABLE(handle)[scanId].finfo),
                                  NULL,
                                  &(SM_SCANTABLE(handle)[scanId].cursor.seq.oid),
                                  objHdr,
                                  NULL);
            } else {		/* BACKWARD */
                e = OM_PrevObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SCANTABLE(handle)[scanId].finfo),
                                  NULL,
                                  &(SM_SCANTABLE(handle)[scanId].cursor.seq.oid),
                                  objHdr,
                                  NULL);
            }

            if (e != EOS && e < 0) ERR(handle, e);

            if (e == EOS) {
                /* Set the cursor to EOS(end of scan). */
                SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_EOS;
            } else {
                /* Set the current cursor to valid. */
                SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_ON;
            }
            break;

          case BTREEINDEX:
	    e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SCANTABLE(handle)[scanId].scanInfo.btree.iinfo), 
			  &(SM_SCANTABLE(handle)[scanId].finfo.fid), 
			  &(SM_SCANTABLE(handle)[scanId].scanInfo.btree.kdesc),
			  &(SM_SCANTABLE(handle)[scanId].scanInfo.btree.startCond.key),
			  SM_SCANTABLE(handle)[scanId].scanInfo.btree.startCond.op,
			  &(SM_SCANTABLE(handle)[scanId].scanInfo.btree.stopCond.key),
			  SM_SCANTABLE(handle)[scanId].scanInfo.btree.stopCond.op,
			  &(SM_SCANTABLE(handle)[scanId].cursor.btree),
			  NULL, realLockup);
	    if (e < 0) {
		SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_INVALID;
		ERR(handle, e);
	    }
	    break;

	  case MLGFINDEX:
	    e = MLGF_Fetch(handle, MY_XACT_TABLE_ENTRY(handle),
                           &(SM_SCANTABLE(handle)[scanId].scanInfo.mlgf.iinfo), 
			   &(SM_SCANTABLE(handle)[scanId].scanInfo.mlgf.kdesc),
			   SM_SCANTABLE(handle)[scanId].scanInfo.mlgf.lowerBound,
			   SM_SCANTABLE(handle)[scanId].scanInfo.mlgf.upperBound,
			   &(SM_SCANTABLE(handle)[scanId].cursor.mlgf),
			   extraData, realLockup);
	    if (e < 0) {
		SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_INVALID;
		ERR(handle, e);
	    }

	    break;

	  default:
	    ERR(handle, eINTERNAL);
	} /* end of switch */
	break;

      case CURSOR_EOS:		/* End of the scan */
	break;

      case CURSOR_ON:
	switch (SM_SCANTABLE(handle)[scanId].scanType) {
	  case SEQUENTIAL:
	    /* check the lock mode of the data file */

	    SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_INVALID;

	    if (SM_SCANTABLE(handle)[scanId].scanInfo.seq.direction == FORWARD) {
		e = OM_NextObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SCANTABLE(handle)[scanId].finfo),
				  &(SM_SCANTABLE(handle)[scanId].cursor.seq.oid),
				  &nextOid,
				  objHdr,
                                  NULL);
	    } else {		/* BACKWARD */
		e = OM_PrevObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SCANTABLE(handle)[scanId].finfo),
				  &(SM_SCANTABLE(handle)[scanId].cursor.seq.oid),
				  &nextOid,
				  objHdr,
                                  NULL);
	    }

	    if (e != EOS && e < 0) ERR(handle, e);

	    if (e == EOS) {
		/* We have reached the end of scan. */
		SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_EOS;
	    } else {
		/* We found the next object. */
		SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_ON;
		SM_SCANTABLE(handle)[scanId].cursor.seq.oid = nextOid;
	    }
	    break;

	  case BTREEINDEX:

	    e = BtM_FetchNext(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SCANTABLE(handle)[scanId].scanInfo.btree.iinfo), 
			      &(SM_SCANTABLE(handle)[scanId].finfo.fid), 
			      &(SM_SCANTABLE(handle)[scanId].scanInfo.btree.kdesc),
			      &(SM_SCANTABLE(handle)[scanId].scanInfo.btree.stopCond.key),
			      SM_SCANTABLE(handle)[scanId].scanInfo.btree.stopCond.op,
			      &(SM_SCANTABLE(handle)[scanId].cursor.btree),
			      &nextCursor.btree, realLockup);
	    if (e < 0) {
		SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_INVALID;
		ERR(handle, e);
	    }

	    SM_SCANTABLE(handle)[scanId].cursor.btree = nextCursor.btree;
	    break;

	  case MLGFINDEX:

	    e = MLGF_FetchNext(handle, MY_XACT_TABLE_ENTRY(handle),
                               &(SM_SCANTABLE(handle)[scanId].scanInfo.mlgf.iinfo), 
			       &(SM_SCANTABLE(handle)[scanId].scanInfo.mlgf.kdesc),
			       SM_SCANTABLE(handle)[scanId].scanInfo.mlgf.lowerBound,
			       SM_SCANTABLE(handle)[scanId].scanInfo.mlgf.upperBound,
			       &(SM_SCANTABLE(handle)[scanId].cursor.mlgf),
			       extraData, realLockup);
	    if (e < 0) {
		SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_INVALID;
		ERR(handle, e);
	    }

	    break;

	  default:
	    ERR(handle, eINTERNAL);
	} /* end of switch */
	break;

      default:
	ERR(handle, eBADCURSOR);
    }

    if (cursor != NULL) *cursor = (Cursor*)&SM_SCANTABLE(handle)[scanId].cursor;

    /*@ end of scan? */
    /* Return if scan reaches the end. */
    if (SM_SCANTABLE(handle)[scanId].cursor.any.flag == CURSOR_EOS) {

    	if(ACTION_ON(handle)){  
       	    e = LM_endAction(handle, &MY_XACTID(handle), AUTO_ACTION); 
            if(e < eNOERROR) ERR(handle, e);
    	}
	return(EOS);
    }
    /*@ oid is not equal to NULL? */
    /* Return the current cursor's ObjectID. */
    if (oid != NULL)
	*oid = SM_SCANTABLE(handle)[scanId].cursor.any.oid;

    /* Return the current cursor's object header. */
    /* When the scan type is SEQUENTIAL we have already got the object header. */
    if (objHdr != NULL && SM_SCANTABLE(handle)[scanId].scanType != SEQUENTIAL) { 
	e = OM_GetObjectHdr(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_SCANTABLE(handle)[scanId].finfo),
			    &(SM_SCANTABLE(handle)[scanId].cursor.any.oid), objHdr, realLockup);
	if (e < 0) {
	    SM_SCANTABLE(handle)[scanId].cursor.any.flag = CURSOR_INVALID;
	    ERR(handle, e);
	}
    }

    if(ACTION_ON(handle)){  
	e = LM_endAction(handle, &MY_XACTID(handle), AUTO_ACTION); 
        if(e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* SM_NextObject( ) */
