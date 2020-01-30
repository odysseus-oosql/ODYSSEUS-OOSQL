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
 * Module: SM_InitDS.c
 *
 * Description:
 *  Initialize data structures used in the scan manager.
 *  Also, includes a routine which initialize all managers.
 *
 * Exports:
 *  Four SM_InitAllSharedDS(Four)
 *  Four SM_InitAllLocalDS(Four)
 *  Four SM_InitSharedDS(Four)
 *  Four SM_InitLocalDS(Four)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "RM.h"
#include "RDsM.h"
#include "BfM.h"
#include "OM.h"
#include "BtM.h"
#include "MLGF.h"
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"

CfgParams_T sm_cfgParams = { "" };




/*@================================
 * SM_InitSharedDS( )
 *================================*/
Four SM_InitSharedDS(
    Four handle)
{
    Four i;			/* array index variable */


    TR_PRINT(handle, TR_SM, TR1, ("SM_InitSharedDS()"));

    /*
    ** Initailze the main memory data structure used in Scan Manager.
    */
    /* Initialize the latch for mount/dismout operation */
    SHM_initLatch(handle, &SM_LATCH_MOUNTTABLE);

    /*@ for each entry */
    /* Initialize the entries in the Mount Table. */
    for (i = 0; i < MAXNUMOFVOLS; i++)
	SM_MOUNTTABLE[i].volId = NIL;

    /*
    ** Construct key descriptors of the indexes on the catalog tables.
    */
    SM_SYSTBL_DFILEIDIDX_KEYDESC.flag = 0;
    SM_SYSTBL_DFILEIDIDX_KEYDESC.nparts = 2;
    SM_SYSTBL_DFILEIDIDX_KEYDESC.kpart[0].type = SM_VOLNO;
    SM_SYSTBL_DFILEIDIDX_KEYDESC.kpart[0].offset = 0;
    SM_SYSTBL_DFILEIDIDX_KEYDESC.kpart[0].length = SM_VOLNO_SIZE;
    SM_SYSTBL_DFILEIDIDX_KEYDESC.kpart[1].type = SM_SERIAL;
    SM_SYSTBL_DFILEIDIDX_KEYDESC.kpart[1].offset = SM_VOLNO_SIZE;
    SM_SYSTBL_DFILEIDIDX_KEYDESC.kpart[1].length = SM_SERIAL_SIZE;

    /* The following line is removed. */
    /* SM_SYSTBL_BTREEFILEID_KEYDESC = SM_SYSTBL_DFILEIDIDX_KEYDESC; */
    SM_SYSIDX_DATAFILEID_KEYDESC = SM_SYSTBL_DFILEIDIDX_KEYDESC; /* SM_SYSIDX_BTREEFILEID_KEYDESC => SM_SYSIDX_DATAFILEID_KEYDESC */
    SM_SYSIDX_INDEXID_KEYDESC = SM_SYSTBL_DFILEIDIDX_KEYDESC;

    SM_SYSCNTR_CNTRNAME_KEYDESC.flag = 0;
    SM_SYSCNTR_CNTRNAME_KEYDESC.nparts = 1;
    SM_SYSCNTR_CNTRNAME_KEYDESC.kpart[0].type = SM_VARSTRING;
    SM_SYSCNTR_CNTRNAME_KEYDESC.kpart[0].offset = 0;
    SM_SYSCNTR_CNTRNAME_KEYDESC.kpart[0].length = SM_COUNTER_NAME_MAX_LEN;


    return(eNOERROR);

} /* SM_InitSharedDS( ) */



/*@================================
 * SM_InitLocalDS( )
 *================================*/
Four SM_InitLocalDS(
    Four handle)
{
    Four e;                     /* error number */
    Four i;                     /* array index variable */

    /* pointer for SM Data Structure of perThreadTable */
    SM_PerThreadDS_T *sm_perThreadDSptr = SM_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_SM, TR1, ("SM_InitLocalDS()"));

    /*
    ** sm_sysTablesForTmpFiles
    */
    /* Allocate some entries in the sm_sysTablesForTmpFiles. */
    e = Util_initVarArray(handle, &(sm_perThreadDSptr->sm_sysTablesForTmpFiles),
			  sizeof(sm_CatOverlayForSysTables), INIT_SM_ST_FOR_TMP_FILES);
    if (e < eNOERROR) ERR(handle, e);

    /*@ for each entry */
    /* Initialize the allocated entries in the sm_sysTablesForTmpFiles. */
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); i++) 
	SM_SET_TO_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i]);

    /*
    ** sm_sysIndexesForTmpFiles
    */
    /* Allocate some entries in the sm_sysIndexesForTmpFiles. */
    e = Util_initVarArray(handle, &(sm_perThreadDSptr->sm_sysIndexesForTmpFiles), sizeof(sm_CatOverlayForSysIndexes), INIT_SM_SI_FOR_TMP_FILES); 
    if (e < eNOERROR) ERR(handle, e);

    /*@ for each entry */
    /* Initialize the allocated entries in the sm_sysIndexesForTmpFiles. */
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_SI_FOR_TMP_FILES(handle); i++) 
	SM_SET_TO_UNUSED_ENTRY_OF_SI_FOR_TMP_FILES(SM_SI_FOR_TMP_FILES(handle)[i]);

    /* Allocate some entries in the Scan Table */
    e = Util_initVarArray(handle, &(sm_perThreadDSptr->smScanTable), sizeof(sm_ScanTableEntry), INITSCAN);
    if (e < eNOERROR) ERR(handle, e);

    /*@ for each entry */
    /* Initialize the allocated entries in the Scan Table */
    for (i = 0; i < sm_perThreadDSptr->smScanTable.nEntries; i++){
	SM_SCANTABLE(handle)[i].scanType = NIL;
	SM_SCANTABLE(handle)[i].acquiredFileLock = L_NL;
    }

    return(eNOERROR);

} /* SM_InitLocalDS() */

