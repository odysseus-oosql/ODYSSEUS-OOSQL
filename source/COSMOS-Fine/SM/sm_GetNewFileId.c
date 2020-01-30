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
 * Module: sm_GetNewFileId.c
 *
 * Description:
 *  Allocate new file ID
 *
 * Exports:
 *  Four sm_GetNewFileId(Four, Four, FileID*, LockParameter*)
 */

#include <assert.h>
#include <string.h>
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



/*
 * Function: Four sm_GetNewFileId(Four, Four, FileID*, LockParameter*)
 *
 * Description:
 *  Allocate new file ID
 *
 * Returns:
 *  error code
 */
Four sm_GetNewFileId(
    Four           handle,
    Four           v,                  		/* IN  index for the used volume on the mount table */
    FileID*        newFid)             		/* OUT allocated file id */
{
    Four           e;                  		/* error code */
    KeyValue       kval;               		/* key value for index on SYSCOUNTERS */
    BtreeCursor    cursor;             		/* a B+ tree cursor */
    Serial         serialForFid;       		/* serial from sysSerialForFileCounter */
    CounterID      cntrIdForFid;       		/* ID of counter for file ID */
    Serial	   tmpSerialForFid = NIL;    	/* temporary serial */
    Boolean        breakFlag = FALSE;  		/* loop break flag */


    TR_PRINT(handle, TR_LRDS, TR1, ("sm_GetNewFileId()"));


    /*
     *  Check parameters
     */
    if (newFid == NULL) ERR(handle, eBADPARAMETER);


    /*
     *  Get ID of counter for file ID
     */
    e = SM_GetCounterId(handle, SM_MOUNTTABLE[v].volId, "smSysSerialForFileCounter", &cntrIdForFid);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Find unallocated file ID
     */

    while(1) {

        /* get serial from counter */
        e = SM_GetCounterValues(handle, SM_MOUNTTABLE[v].volId, &cntrIdForFid, 1, &serialForFid);
        if (e < eNOERROR) ERR(handle, e);

        /* set kval for checking */
        kval.len = sizeof(Two) + sizeof(Four);
        memcpy(&(kval.val[0]), (char*)&SM_MOUNTTABLE[v].volId, sizeof(Two));
        memcpy(&(kval.val[sizeof(Two)]), (char*)&serialForFid, sizeof(Four));

        /* check allocated serial for file ID is already allocated */
#ifdef CCPL
        indexLockup.mode = L_S;
        indexLockup.duration = L_COMMIT;

        e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysTablesDataFileIdIndexInfo), 
                      &(SM_MOUNTTABLE[v].sysTablesInfo.fid), 
                      &SM_SYSTBL_DFILEIDIDX_KEYDESC,
                      &kval, SM_EQ, &kval, SM_EQ, &cursor, NULL, &indexLockup);
        if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
        e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysTablesDataFileIdIndexInfo), 
                      &(SM_MOUNTTABLE[v].sysTablesInfo.fid), 
                      &SM_SYSTBL_DFILEIDIDX_KEYDESC,
                      &kval, SM_EQ, &kval, SM_EQ, &cursor, NULL, NULL); /* no lock request */
        if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

        /* if not allocated yet, break */
        if (cursor.flag == CURSOR_EOS) break;
        else if (tmpSerialForFid == NIL) tmpSerialForFid = serialForFid;
        else if (serialForFid == tmpSerialForFid) {
	    if (breakFlag = TRUE) ERR(handle, eFILEIDFULL_SM);
	    else breakFlag = TRUE;
	}
    }

    /* set newFid */
    MAKE_FILEID(*newFid, SM_MOUNTTABLE[v].volId, serialForFid);


    return(eNOERROR);

} /* sm_GetNewFileId() */
