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
 * Module: SM_SortFile.c
 *
 * Description:
 *  Sort the given data file.
 *
 * Exports:
 *  Four SM_SortFile()
 */


#include <string.h> /* for memcpy */
#include <stdio.h>  /* for ERR */

#include "common.h"
#include "BfM.h"
#include "Util.h"
#include "OM_Internal.h"
#include "BtM_Internal.h"
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four sm_CreateCatalogEntriesForFile(Four, Four, sm_CatOverlayForSysTables*, Boolean);
Four sm_UpdateCatalogEntriesForFile(Four, Four, ObjectID*, sm_CatOverlayForData *, Boolean);


/*
 * Function: Four SM_SortFile(Fi)
 *
 * Description:
 *  Sort the given data file.
 *
 * Returns:
 *  error code
 */ 
Four _SM_SortFile(
    Four handle,
    VolID  			tmpVolId,            	/* IN temporary volume in which sort stream is created */ 
    FileID 			*inFid,              	/* IN file to sort */
    SortKeyDesc 		*kdesc,          	/* IN sort key description */
    omGetKeyAttrsFuncPtr_T 	getKeyAttrsFn, 		/* IN tuple analysis function */
    void 			*schema,               	/* IN schema for analysis function */
    Boolean 			newFileFlag,        	/* IN whether we make new file for sort result */
    Boolean 			tmpFileFlag,        	/* IN new file is a temporary file? */
    FileID 			*outFid)             	/* OUT new file storing sort result */
{
    Four      			e;
    Four       			v;
    KeyValue 			kval;
    ObjectID 			catObjForSortFile;
    sm_CatOverlayForSysTables 	sysTablesForOutFile;
    Four 			i,j;
    FileID 			localOutFid; 		
    FileID 			indexFid;    	


    /*
     * Check parameters
     */
    if (inFid == NULL || kdesc == NULL || getKeyAttrsFn == NULL)
        ERR(handle, eBADPARAMETER_SM);

    if(newFileFlag && outFid == NULL) ERR(handle, eBADPARAMETER_SM);
        
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
	if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == inFid->volNo) break; /* found */
    
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);


    e = sm_GetCatalogEntryFromDataFileId(handle, v, inFid, &catObjForSortFile);
    if(e < 0) ERR(handle, e);


    /* get 'localOutFid' */
    if (newFileFlag) {

        /* allocate new data file ID */
        e = sm_GetNewFileId(handle, v, &localOutFid);
        if (e < 0) ERR(handle, e);
    }
    else {

        /* same as input data file ID */
        localOutFid = *inFid;
    }

    /* sort!! */
    e = OM_SortInto(handle, tmpVolId, &catObjForSortFile, &localOutFid, &sysTablesForOutFile.data, 
                    kdesc, getKeyAttrsFn, schema, newFileFlag, tmpFileFlag, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if(e < eNOERROR) ERR(handle, e);

    /* register output file in system catalog */
    if (newFileFlag) {        

        /* allocate new data file ID */
        e = sm_GetNewFileId(handle, v, &indexFid);
        if (e < 0) ERR(handle, e);

        /* Create a B+-tree file in the given volume. */
        e = BtM_CreateFile(handle, &indexFid, BEFF, &(sysTablesForOutFile.btree));
        if (e < 0) ERR(handle, e);

        e = sm_CreateCatalogEntriesForFile(handle, v, &sysTablesForOutFile, tmpFileFlag);
        if (e < eNOERROR) ERR(handle, e);

    	if(tmpFileFlag) {

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
            SM_TMPFILEIDTABLE(handle)[i] = localOutFid;
        }
    }
    else {
        e = sm_UpdateCatalogEntriesForFile(handle, v, &catObjForSortFile, &sysTablesForOutFile.data, tmpFileFlag);
        if (e < eNOERROR) ERR(handle, e);
    }

    /* set output parameter */
    if(outFid != NULL) *outFid = localOutFid;
    

    return(eNOERROR);
    
} /* SM_SortFile() */



/*@===================================
 * sm_CreateCatalogEntriesForFile()
 *===================================*/

Four sm_CreateCatalogEntriesForFile(
    Four handle,
    Four   v,			/* IN array index on scan manager mount table */
    sm_CatOverlayForSysTables *sysTablesOverlay, /* IN entry of SM_SYSTABLES */
    Boolean tmpFileFlag)	/* IN TRUE if the file is a temporary file */
{
    Four   e;			/* error code */
    KeyValue kval;		/* a key value */
    ObjectID oid;		/* an ObjectID */


    
    /*
    ** Register the catalog entry in the catalog table SM_SYSTABLES.
    */    
    e = OM_CreateObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry),
                        NULL, NULL, sizeof(sm_CatOverlayForSysTables),
                        (char *)sysTablesOverlay, &oid);
    if (e < 0) ERR(handle, e);


    /*
    ** Insert the new ObjectID into B+ tree on data FileID of SM_SYSTABLES.
    */

    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(sysTablesOverlay->data.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(sysTablesOverlay->data.fid.serial), sizeof(Serial)); 

    e = BtM_InsertObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry),
                         &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesDataFileIdIndex),
                         &SM_PER_THREAD_DS(handle).smSysTablesDataFileIdIndexKdesc,
                         &kval, &oid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);


    /*
    ** Insert the new ObjectID into B+ tree on Btree FileID of SM_SYSTABLES.
    */

    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(sysTablesOverlay->btree.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(sysTablesOverlay->btree.fid.serial), sizeof(Serial)); 

    e = BtM_InsertObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry),
                         &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesBtreeFileIdIndex),
                         &SM_PER_THREAD_DS(handle).smSysTablesBtreeFileIdIndexKdesc,
                         &kval, &oid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);
    

    return(eNOERROR);

} /* sm_CreateCatalogEntriesForFile() */



/*@================================
 * sm_UpdateCatalogEntriesForFile()
 *================================*/

Four sm_UpdateCatalogEntriesForFile(
    Four handle,
    Four   v,			/* IN array index on scan manager mount table */
    ObjectID *catObjForSortFile,/* IN file in which object is to be placed */
    sm_CatOverlayForData *newEntry, /* IN data catalog entry of SM_SYSTABLES */
    Boolean tmpFileFlag)	/* IN TRUE if the file is a temporary file */
{
    Four   e;			/* error code */
    KeyValue kval;		/* a key value */
    ObjectID oid;		/* an ObjectID */


    /*
    ** Update the data part of catalog entry in the catalog table SM_SYSTABLES.
    */    
    e = OM_WriteObject(handle, catObjForSortFile, 0, sizeof(sm_CatOverlayForData), (char*)newEntry);
    if (e < 0) ERR(handle, e);


    return(eNOERROR);

} /* sm_CreateCatalogEntriesForFile() */
