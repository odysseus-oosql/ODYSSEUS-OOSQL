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
 * Module: SM_CreateFile.c
 *
 * Description:
 *  Create a file in the specified volume, 'volId'. 
 *
 * Exports:
 *  Four SM_CreateFile(Four, Two, Two, FileID*, Boolean)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "OM_Internal.h"
#include "BtM.h"
#include "SM_Internal.h"
#include "Util.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * SM_CreateFile()
 *================================*/
/*
 * Function: Four SM_CreateFile(Four, Two, Two, FileID*, Boolean)
 *
 * Description:
 *  Create a file in the specified volume, 'volId'. Users should give
 *  'dEff' and 'bEff', which are data file extent fill factor and
 *  B+-tree file extent fill factor, respectively. SM_CreateFile() returns
 *  FileID 'fid' with which the user can identify the file from the others.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_SM
 *    eNOTMOUNTEDVOLUME_SM
 *    some errors caused by function calls
 *
 * Side effects:
 *  1) parameter fid
 *     'fid' is filled with the newly created file's identifier.
 */
Four _SM_CreateFile(
    Four handle,
    Four   			volId,			/* IN on which volume to put the file */
    Two                         dEff,                   /* IN data extent fill factor */
    Two                         bEff,                   /* IN B+-tree extent fill factor */
    FileID                      *fid,                   /* OUT newly created file's FileID */
    Boolean                     tmpFileFlag)            /* IN flag which indicates newly created file is temporary or not */
{
    Four                        e;                      /* error code */
    Four                        i, j;                   /* array index on temporary file ID table */
    Four                        v;                      /* array index on scan manager mount table */
    KeyValue                    kval;                   /* a key value */
    ObjectID                    oid;                    /* an ObjectID */
    FileID                      indexFileId;            
    sm_CatOverlayForSysTables   sysTablesOverlay;       /* entry of SM_SYSTABLES */

    
    TR_PRINT(TR_SM, TR1,
             ("SM_CreateFile(handle, volId=%ld, dEff=%ld, bEff=%ld, fid=%P)",
	      volId, dEff, bEff, fid));

    /*@
    ** check parameters
    */
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == volId) break; /* found */ 
    
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);
    
    if (dEff < 0 || dEff > 100)	ERR(handle, eBADPARAMETER_SM);

    if (bEff < 0 || bEff > 100)	ERR(handle, eBADPARAMETER_SM);

    if (fid == NULL) ERR(handle, eBADPARAMETER_SM);


    /* Set the extent fill factor to the default value if its value is 0. */
    if (dEff == 0) dEff = DEFF;

    if (bEff == 0) bEff = BEFF;

    /* Is there an active transactin? */
    if (!SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eNOACTIVETRANSACTION_SM);
    
    /* allocate data file ID */
    e = sm_GetNewFileId(handle, v, fid);
    if (e < 0) ERR(handle, e);

    /* Create a data file in the given volume.*/
    e = OM_CreateFile(handle, fid, dEff, &(sysTablesOverlay.data), tmpFileFlag);
    if (e < 0) ERR(handle, e);

    /* allocate B+tree file ID */
    e = sm_GetNewFileId(handle, v, &indexFileId);
    if (e < 0) ERR(handle, e);

    /* Create a B+-tree file in the given volume. */
    /*
     * We share this file to store both Btree and MLGF indexes,
     * though the sysTablesOverlay.btree may mislead that it stores only Btree indexes.
     */
    e = BtM_CreateFile(handle, &indexFileId, bEff, &(sysTablesOverlay.btree));
    if (e < 0) ERR(handle, e);

    
    /*
    ** Register the files in the catalog table SM_SYSTABLES.
    */    
    e = OM_CreateObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry), 
			NULL, NULL, sizeof(sm_CatOverlayForSysTables),
			&sysTablesOverlay, &oid);
    if (e < 0) ERR(handle, e);

    /* Insert the new ObjectID into B+ tree on data FileID of SM_SYSTABLES. */
    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(sysTablesOverlay.data.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(sysTablesOverlay.data.fid.serial), sizeof(Serial));

    e = BtM_InsertObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry),
			 &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesDataFileIdIndex),
			 &SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc,
			 &kval, &oid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);

    /* Insert the new ObjectID into B+ tree on Btree FileID of SM_SYSTABLES. */
    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(sysTablesOverlay.btree.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(sysTablesOverlay.btree.fid.serial), sizeof(Serial)); 

    e = BtM_InsertObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry),
			 &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesBtreeFileIdIndex),
			 &SM_PER_THREAD_DS(handle).smSysTablesBtreeFileIdIndexKdesc,
			 &kval, &oid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);

    if( tmpFileFlag ) {

        /* find the empty temporary file ID table entry */
        for (i = 0; i < SM_PER_THREAD_DS(handle).smTmpFileIdTable.nEntries; i++)
            if (IS_NILFILEID(SM_TMPFILEIDTABLE(handle)[i])) break;

        /* There is no empty entry. */
        if (i == SM_PER_THREAD_DS(handle).smTmpFileIdTable.nEntries) {

            /* doubling it */
            e = Util_doublesizeVarArray(handle, &SM_PER_THREAD_DS(handle).smTmpFileIdTable, sizeof(FileID));
            if (e < 0) ERR(handle, e);

            /* Initialize the newly allocated entries. */
            for (j=i; j < SM_PER_THREAD_DS(handle).smScanTable.nEntries; j++)
                SET_NILFILEID(SM_TMPFILEIDTABLE(handle)[j]);
        }

        /* assign file ID */
        SM_TMPFILEIDTABLE(handle)[i] = *fid;
    }

    return(eNOERROR);

} /* SM_CreateFile() */
