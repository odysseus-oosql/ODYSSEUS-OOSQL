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
 * Module: SM_Mount.c
 *
 * Description:
 *  Mount a given volume using a RDsM function call, RDsM_Mount( ).
 *
 * Exports:
 *  Four SM_Mount(Four, char*, Four*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "TM.h"
#include "LM.h"
#include "RDsM.h"
#include "OM.h"
#include "BtM.h"
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * SM_Mount( )
 *================================*/
/*
 * Function: Four SM_Mount(Four, char*, Four*)
 *
 * Description:
 *  Mount a given volume using a RDsM function call, RDsM_Mount( ).
 *  If the mount is successful, returns its volume id.
 *  A volume id is given to the volume when the volume is formated.
 *
 * Returns:
 *  Error code (when negative value is returned)
 *    eBADPARAMETER
 *    eTOOMANYVOLUMES
 *    some errors caused by function calls
 *
 * Side effects:
 *  1) parameter volId
 *     returns the mounted volume's volume id
 *     If mount is failed, the value is undefined.
 */
Four SM_Mount(
    Four handle,
    Four numDevices,            /* IN # of devices in the volume to be mounted */
    char **devNames,            /* IN devices' name in the volume to be mounted */
    Four *volId)                /* OUT mounted volume's volume id */
{
    Four    e;			/* error number */
    Four    v;			/* array index on the mount table */
    XactID xactId;		/* transaction ID */


    TR_PRINT(handle, TR_SM, TR1, ("SM_Mount(numDevices=%lD, devNames=%P, volId=%P)", numDevices, devNames, volId));


    /*@ Check parameter */
    if (numDevices < 1) ERR(handle, eBADPARAMETER);
    if (devNames == NULL) ERR(handle, eBADPARAMETER);
    if (volId == NULL) ERR(handle, eBADPARAMETER);


    /*
    ** Mount/Dismount or Mount/Mount must be serialized.
    */
    ERROR_PASS(handle, SHM_getLatch(handle, &SM_LATCH_MOUNTTABLE,
			    procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));

    /*@
    ** Mount the volume.
    */
    e = RDsM_Mount(handle, numDevices, devNames, volId, common_shmPtr->recoveryFlag);
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    /*
    ** Check whether the volume is already mounted.
    */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_MOUNTTABLE[v].volId == *volId){

	    /* increase the number of mount */
	    SM_MOUNTTABLE[v].nMount++;

	    ERROR_PASS(handle, SHM_releaseLatch(handle, &SM_LATCH_MOUNTTABLE, procIndex));
	    return(eNOERROR);	/* already mounted */
	}


    /*
    ** Fill the SM_MOUNTTABLE entry
    */
    /*@ for each entry */
    /* Find an empty slot in SM_MOUNTTABLE */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_MOUNTTABLE[v].volId == NIL) break; /* empty slot */

    if (v == MAXNUMOFVOLS) ERRL1(handle, eTOOMANYVOLUMES, &SM_LATCH_MOUNTTABLE);


    /*@
     * save the volume information
     */

    /*
    ** Fixed Information for the index on SM_SYSTABLES & SM_SYSINDEXES
    ** Note!! these information must be got first because they are used
    **        in sm_GetCatalogEntryFromDataFileId() & sm_GetCatalogEntryFromIndexId()
    */

    /* get index ID of index on SM_SYSTABLES */
    e = RDsM_GetMetaDictEntry(handle, MY_XACT_TABLE_ENTRY(handle), *volId, "smSysTablesDataFileIdIndexId",
			(char *)&(SM_MOUNTTABLE[v].sysTablesDataFileIdIndexInfo.iid), sizeof(IndexID));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    /* set tmpFileFlag of sysTablesDataFileIdIndexInfo */
    SM_MOUNTTABLE[v].sysTablesDataFileIdIndexInfo.tmpIndexFlag = FALSE;

    /* Get ObjectID for the index on SM_SYSTABLES entry in SM_SYSINDEXES. */
    e = RDsM_GetMetaDictEntry(handle, MY_XACT_TABLE_ENTRY(handle), *volId, "smSysTablesDataFileIdIndexEntry",
			(char *)&(SM_MOUNTTABLE[v].sysTablesDataFileIdIndexInfo.catalog.oid), sizeof(ObjectID));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);


    /* get index ID of index on SM_SYSINDEXES */
    e = RDsM_GetMetaDictEntry(handle, MY_XACT_TABLE_ENTRY(handle), *volId, "smSysIndexesDataFileIdIndexId",
			(char *)&(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo.iid), sizeof(IndexID));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    /* set tmpFileFlag of smSysIndexesDataFileIdIndexInfo */
    SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo.tmpIndexFlag = FALSE;

    /* Get ObjectID for the index on SM_SYSINDEXES entry in SM_SYSINDEXES. */
    e = RDsM_GetMetaDictEntry(handle, MY_XACT_TABLE_ENTRY(handle), *volId, "smSysIndexesDataFileIdIndexEntry",
			(char *)&(SM_MOUNTTABLE[v].sysIndexesDataFileIdIndexInfo.catalog.oid), sizeof(ObjectID));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);


    /* get index ID of index on SM_SYSINDEXES */
    e = RDsM_GetMetaDictEntry(handle, MY_XACT_TABLE_ENTRY(handle), *volId, "smSysIndexesIndexIdIndexId",
			(char *)&(SM_MOUNTTABLE[v].sysIndexesIndexIdIndexInfo.iid), sizeof(IndexID));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    /* set tmpFileFlag of sysIndexesIndexIdIndexInfo */
    SM_MOUNTTABLE[v].sysIndexesIndexIdIndexInfo.tmpIndexFlag = FALSE;

    /* Get ObjectID for the index on SM_SYSINDEXES entry in SM_SYSINDEXES. */
    e = RDsM_GetMetaDictEntry(handle, MY_XACT_TABLE_ENTRY(handle), *volId, "smSysIndexesIndexIdIndexEntry",
			(char *)&(SM_MOUNTTABLE[v].sysIndexesIndexIdIndexInfo.catalog.oid), sizeof(ObjectID));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);


    /*
    ** Fixed Information for the catalog table SM_SYSTABLES.
    */

    /* get file ID of SM_SYSTABLES */
    e = RDsM_GetMetaDictEntry(handle, MY_XACT_TABLE_ENTRY(handle), *volId, "smSysTablesDataFileId",
			(char *)&(SM_MOUNTTABLE[v].sysTablesInfo.fid), sizeof(FileID));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    /* set tmpFileFlag of sysTablesInfo */
    SM_MOUNTTABLE[v].sysTablesInfo.tmpFileFlag = FALSE;

    /* Find the ObjectID for the SM_SYSTABLES entry in SM_SYSTABLES. */
    e = sm_GetCatalogEntryFromDataFileId(handle, v, &(SM_MOUNTTABLE[v].sysTablesInfo.fid),
					 &(SM_MOUNTTABLE[v].sysTablesInfo.catalog.oid));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);


    /*
    ** Fixed information for the catalog table SM_SYSINDEXES.
    */

    /* get file ID of SM_SYSINDEXES */
    e = RDsM_GetMetaDictEntry(handle, MY_XACT_TABLE_ENTRY(handle), *volId, "smSysIndexesDataFileId",
			(char *)&(SM_MOUNTTABLE[v].sysIndexesInfo.fid), sizeof(FileID));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    /* set tmpFileFlag of sysIndexesInfo */
    SM_MOUNTTABLE[v].sysIndexesInfo.tmpFileFlag = FALSE;

    /* Find the ObjectID for the SM_SYSINDEXES entry in SM_SYSTABLES. */
    e = sm_GetCatalogEntryFromDataFileId(handle, v, &(SM_MOUNTTABLE[v].sysIndexesInfo.fid),
					 &(SM_MOUNTTABLE[v].sysIndexesInfo.catalog.oid));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);


    /*
    **Fixed information for the catalog table SM_SYSCOUNTERS.
    */

    /* get file ID of SM_SYSCOUNTERS */
    e = RDsM_GetMetaDictEntry(handle, MY_XACT_TABLE_ENTRY(handle), *volId, "smSysCountersDataFileId",
			(char *)&(SM_MOUNTTABLE[v].sysCountersInfo.fid), sizeof(FileID));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    /* set tmpFileFlag of sysCountersInfo */
    SM_MOUNTTABLE[v].sysCountersInfo.tmpFileFlag = FALSE;

    /* Find the ObjectID for the SM_SYSCOUNTERS entry in SM_SYSTABLES. */
    e = sm_GetCatalogEntryFromDataFileId(handle, v, &(SM_MOUNTTABLE[v].sysCountersInfo.fid),
					 &(SM_MOUNTTABLE[v].sysCountersInfo.catalog.oid));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);


    /* get index ID of index on SM_SYSCOUNTERS */
    e = RDsM_GetMetaDictEntry(handle, MY_XACT_TABLE_ENTRY(handle), *volId, "smSysCountersCounterNameIndexId",
			(char *)&(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo.iid), sizeof(IndexID));
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    /* set tmpFileFlag of sysCountersCounterNameIndexInfo */
    SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo.tmpIndexFlag = FALSE;

    /* Find the ObjectID for the index on SM_SYSCOUNTERS entry in SM_SYSINDEXES. */
    e = sm_GetCatalogEntryFromIndexId(handle, v, &(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo.iid),
				   &(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo.catalog.oid), NULL);
    if (e < eNOERROR) ERRL1(handle, e, &SM_LATCH_MOUNTTABLE);

    /* Set the volume identifier. */
    SM_MOUNTTABLE[v].volId = *volId;
    SM_MOUNTTABLE[v].nMount = 1;

    ERROR_PASS(handle, SHM_releaseLatch(handle,  &SM_LATCH_MOUNTTABLE, procIndex));

    return(eNOERROR);

} /* SM_Mount( ) */
