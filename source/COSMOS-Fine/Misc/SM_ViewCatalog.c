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
 * Module: SM_ViewCatalog.c
 *
 * Description:
 *  Print the rows of the catalog tables of the SM.
 *
 * Usage:
 *  SM_ViewCatalog volume_name
 *
 */


#include <stdio.h>
#include <stdlib.h>
#include "perProcessDS.h"
#include "perThreadDS.h"

#include "common.h"
#include "SHM.h"
#include "TM.h"
#include "OM.h"
#include "SM.h"


#define MAXDEVICESINVOLUME 20 
#define ERROR(_e) printf("<FILE:%s LINE:%ld> %ld::%s\n", __FILE__, __LINE__, _e, SM_Err(handle, _e))

#define ERROR_AND_EXIT(handle, _e) \
 	BEGIN_MACRO \
 	ERROR(_e); \
 	(Four) SM_AbortTransaction(handle, &xactId); \
 	(Four) THM_FreeHandle(handle); \
 	(Four) SM_Final(); \
 	exit(1); \
 	END_MACRO

Four procIndex;

/* Internal Function Prototypes */
static char *typeName(Four);


int main(int argc, char *argv[])
{
    Four 			e;				/* error code */
    Four 			volId;				/* volume identifier */
    Four 			scanId;				/* scan id */
    ObjectID 			oid;				/* ObjectID of the objects in SM_SYSINDEXES */
    XactID 			xactId;				/* transaction Id */
    FileID 			sysTablesDataFileId;		/* Data File ID of SM_SYSTABLES */
    FileID 			sysIndexesDataFileId; 		/* Data File ID of SM_SYSINDEXES */
    IndexID 			sysTablesDataFileIdIndex; 	/* B+ tree on DataFileId of SM_SYSTABLES */
    IndexID 			sysIndexesIndexIdIndex;    	/* B+ tree on IndexId of SM_SYSINDEXES */
    IndexID 			sysIndexesDataFileIdIndex; 	/* B+ tree on DataFileId of SM_SYSINDEXES */
    KeyDesc 			sysTablesDataFileIdIndexKdesc; 	/* key descriptor for B+ tree on DataFileId of SM_SYSTABLES */
    KeyDesc 			sysIndexesIndexIdIndexKdesc;    /* key descriptor for B+ tree on IndexID of SM_SYSINDEXES */
    KeyDesc 			sysIndexesDataFileIdIndexKdesc; /* key descriptor for B+ tree on DataFileId of SM_SYSINDEXES */
    sm_CatOverlayForSysTables  	catOverlayForSysTables;		/* an entry in SM_SYSTABLES*/
    sm_CatOverlayForSysIndexes 	catOverlayForSysIndexes;	/* an entry in SM_SYSINDEXES */
    BoundCond 			startBound, stopBound; 		/* boundary condition of range scan */
    KeyDesc 			btreeKdesc;			/* temporary key descriptor for Btree */
    MLGF_KeyDesc 		mlgfKdesc;			/* temprorary key descriptor for MLGF */
    Four 			i;
    Four 			count;
    char 			*devNames[MAXDEVICESINVOLUME];

    Four			handle = 0; 			/* handle */ 

    if (argc < 2) {
	printf("%s volume_name\n", argv[0]);
	exit(1);
    }


    /* get device names */
    for (i = 1; i < argc; i++) devNames[i-1] = argv[i];


    /*
    ** Construct key descriptors of the indexes on the catalog tables.
    */
    sysTablesDataFileIdIndexKdesc.flag = 0;
    sysTablesDataFileIdIndexKdesc.nparts = 2;
    sysTablesDataFileIdIndexKdesc.kpart[0].type = SM_VOLNO;
    sysTablesDataFileIdIndexKdesc.kpart[0].offset = 0;
    sysTablesDataFileIdIndexKdesc.kpart[0].length = SM_VOLNO_SIZE;
    sysTablesDataFileIdIndexKdesc.kpart[1].type = SM_SERIAL;
    sysTablesDataFileIdIndexKdesc.kpart[1].offset = SM_VOLNO_SIZE;
    sysTablesDataFileIdIndexKdesc.kpart[1].length = SM_SERIAL_SIZE;

    sysIndexesDataFileIdIndexKdesc = sysTablesDataFileIdIndexKdesc;
    sysIndexesIndexIdIndexKdesc = sysTablesDataFileIdIndexKdesc;


    /* Initialzie the storage system. */
    e = SM_Init();
    if (e < eNOERROR) {
        ERROR(e);
        printf("storage system initialization failed\n");
        exit(1);
    }

    e = SM_AllocHandle(&handle);
    if (e < eNOERROR) {
        ERROR(e);
        exit(1);
    }


    /* Mount the given volume. */
    e = SM_Mount(handle, argc-1, devNames, &volId); 
    if (e < eNOERROR) {
        ERROR(e);
	(Four) SM_FreeHandle(handle);
        (Four) SM_Final();
        exit(1);
    }


    e = SM_BeginTransaction(handle, &xactId, X_CS_BROWSE); 
    if (e < eNOERROR) {
        ERROR(e);
	(Four) SM_FreeHandle(handle);
        (Four) SM_Final();
        exit(1);
    }


    /*
    ** Fixed Information for the catalog table SM_SYSTABLES.
    */
    e = SM_GetMetaDictEntry(handle, volId, "smSysTablesDataFileId",
			    &sysTablesDataFileId, sizeof(FileID));
    if (e < eNOERROR) ERROR(e);

    e = SM_GetMetaDictEntry(handle, volId, "smSysTablesDataFileIdIndexId", &sysTablesDataFileIdIndex, sizeof(IndexID));
    if (e < eNOERROR) ERROR(e);


    /*
    **Fixed information for the catalog table SM_SYSINDEXES.
    */
    e = SM_GetMetaDictEntry(handle, volId, "smSysIndexesDataFileId",
			    &sysIndexesDataFileId, sizeof(FileID));
    if (e < eNOERROR) ERROR(e);




    e = SM_GetMetaDictEntry(handle, volId, "smSysIndexesDataFileId", &sysIndexesDataFileIdIndex, sizeof(IndexID));
    if (e < eNOERROR) ERROR(e);

    e = SM_GetMetaDictEntry(handle, volId, "smSysIndexesDataFileIdIndexId", &sysIndexesDataFileIdIndex, sizeof(IndexID));
    if (e < eNOERROR) ERROR(e);

    e = SM_GetMetaDictEntry(handle, volId, "smSysIndexesIndexIdIndexId", &sysIndexesIndexIdIndex, sizeof(IndexID));
    if (e < eNOERROR) ERROR(e);


    /*
    ** Print Contents of the SM_SYSTABLES
    */
    scanId = SM_OpenSeqScan(handle, &sysTablesDataFileId, FORWARD, NULL);
    if (scanId < 0) ERROR_AND_EXIT(handle, scanId);

    /* print header */
    printf("################\n");
    printf("# SM_SYSTABLES #\n");
    printf("################\n\n");
    printf("  NO  DATAFILEID    LASTPAGE\n");
    printf("----------------------------\n");

    count = 0;
    while ((e = SM_NextObject(handle, scanId, &oid, NULL, NULL, NULL, NULL)) != EOS) {
	if (e < eNOERROR) ERROR_AND_EXIT(handle, e);

	/* Read an entry of sm_CatOverlayForSysTables. */
	e = SM_FetchObject(handle, scanId, NULL, 0, sizeof(sm_CatOverlayForSysTables),
			   (char*)&catOverlayForSysTables, NULL);
	if (e < eNOERROR) ERROR_AND_EXIT(handle, e);

	printf("[%3ld]", ++count);

	/* sm_CatOverlayForData::fid */
	printf(" (%3ld,%6ld) |", catOverlayForSysTables.data.fid.volNo, catOverlayForSysTables.data.fid.serial); 

	/* sm_CatOverlayForData::lastPage */
	printf(" %6ld", catOverlayForSysTables.data.lastPage);

	printf(" [OID:(%3ld,%6ld,%3ld,%8ld)]", oid.volNo, oid.pageNo, oid.slotNo, oid.unique);
	printf("\n");
    }

    e = SM_CloseScan(handle, scanId);
    if (e < eNOERROR) ERROR_AND_EXIT(handle, e);


    startBound.op = SM_BOF;
    stopBound.op = SM_EOF;
    scanId = SM_OpenIndexScan(handle, &sysTablesDataFileId, &sysTablesDataFileIdIndex,
			      &sysTablesDataFileIdIndexKdesc, &startBound,
			      &stopBound, NULL);
    if (scanId < 0) ERROR_AND_EXIT(handle, scanId);

    printf("<ObjectIDs via B+ tree index>\n");
    while ((e = SM_NextObject(handle, scanId, &oid, NULL, NULL, NULL, NULL)) != EOS) {
	if (e < eNOERROR) ERROR_AND_EXIT(handle, e);

	printf("(%3ld, %6ld, %3ld, %8ld)\n", oid.volNo, oid.pageNo, oid.slotNo, oid.unique);
    }

    e = SM_CloseScan(handle, scanId);
    if (e < eNOERROR) ERROR_AND_EXIT(handle, e);


    /*
    ** Print contnets of the SM_SYSINDEXES
    */
    scanId = SM_OpenSeqScan(handle, &sysIndexesDataFileId, FORWARD, NULL);
    if (scanId < 0) ERROR_AND_EXIT(handle, scanId);

    /* print header */
    printf("\n#################\n");
    printf("# SM_SYSINDEXES #\n");
    printf("#################\n\n");
    printf("  NO  DATAFILEID     INDEXID        INDEXTYPE KEYDESC\n");
    printf("----------------------------------------------------------------------\n");

    count = 0;
    while ((e = SM_NextObject(handle, scanId, &oid, NULL, NULL, NULL, NULL)) != EOS) {
	if (e < eNOERROR) ERROR_AND_EXIT(handle, e);

	/* Read an entry of the sm_CatOverlayForSysIndexes. */
	e = SM_FetchObject(handle, scanId, NULL, 0, sizeof(sm_CatOverlayForSysIndexes),
			   (char*)&catOverlayForSysIndexes, NULL);
	if (e < eNOERROR) ERROR_AND_EXIT(handle, e);

	printf("[%3ld]", ++count);

	/* DATAFILEID */
	printf(" (%3ld,%6ld) |", catOverlayForSysIndexes.dataFid.volNo, catOverlayForSysIndexes.dataFid.serial); 

	/* INDEXID */
	printf(" (%3ld,%6ld) |", catOverlayForSysIndexes.iid.volNo, catOverlayForSysIndexes.iid.serial); 

	/* INDEXTYPE, KEYDESC */
	switch( catOverlayForSysIndexes.indexType ) {
	  case SM_INDEXTYPE_BTREE:
	    printf(" BTREE    NOT_STORED");

	    break;

	  case SM_INDEXTYPE_MLGF:
	    printf(" MLGF     ");
	    mlgfKdesc = catOverlayForSysIndexes.kdesc.mlgf;
	    printf("(%0p,%2ld,%0P,%3ld)", mlgfKdesc.flag, mlgfKdesc.nKeys, mlgfKdesc.minMaxTypeVector, mlgfKdesc.extraDataLen);
	    break;

	  default:
	    printf(" INVALID  ");
	}

	printf(" [OID:(%3ld,%6ld,%3ld,%8ld)]", oid.volNo, oid.pageNo, oid.slotNo, oid.unique);

	printf("\n");
    }

    e = SM_CloseScan(handle, scanId);
    if (e < eNOERROR) ERROR_AND_EXIT(handle, e);

    /* Btree sysIndexesDataFileIdIndex */
    startBound.op = SM_BOF;
    stopBound.op = SM_EOF;
    scanId = SM_OpenIndexScan(handle, &sysIndexesDataFileId, &sysIndexesDataFileIdIndex,
			      &sysIndexesDataFileIdIndexKdesc, &startBound,
			      &stopBound, NULL);
    if (scanId < 0) ERROR_AND_EXIT(handle, scanId);

    printf("<ObjectIDs via B+ tree index on DATAFILEID field>\n");
    while ((e = SM_NextObject(handle, scanId, &oid, NULL, NULL, NULL, NULL)) != EOS) {
	if (e < eNOERROR) ERROR_AND_EXIT(handle, e);

	printf("(%3ld, %6ld, %3ld, %8ld)\n", oid.volNo, oid.pageNo, oid.slotNo, oid.unique);
    }

    e = SM_CloseScan(handle, scanId);
    if (e < eNOERROR) ERROR_AND_EXIT(handle, e);


    /* Btree sysIndexesIndexIdIndex */
    startBound.op = SM_BOF;
    stopBound.op = SM_EOF;
    scanId = SM_OpenIndexScan(handle, &sysIndexesDataFileId, &sysIndexesIndexIdIndex,
			      &sysIndexesIndexIdIndexKdesc, &startBound,
			      &stopBound, NULL);
    if (scanId < 0) ERROR_AND_EXIT(handle, scanId);

    printf("<ObjectIDs via B+ tree index on INDEXID field>\n");
    while ((e = SM_NextObject(handle, scanId, &oid, NULL, NULL, NULL, NULL)) != EOS) {
	if (e < eNOERROR) ERROR_AND_EXIT(handle, e);

	printf("(%3ld, %6ld, %3ld, %8ld)\n", oid.volNo, oid.pageNo, oid.slotNo, oid.unique);
    }

    e = SM_CloseScan(handle, scanId);
    if (e < eNOERROR) ERROR_AND_EXIT(handle, e);


    /* Commit the transaction. */
    e = SM_CommitTransaction(handle, &xactId);
    if (e < eNOERROR) ERROR_AND_EXIT(handle, e);


    /* Dismount the volume. */
    e = SM_Dismount(handle, volId);
    if (e < eNOERROR) {
        ERROR(e);
	(Four) SM_FreeHandle(handle);
        (Four) SM_Final();
        exit(1);
    }


    /* Finalize the LRDS. */
    e = SM_FreeHandle(handle);
    if (e < eNOERROR) {
        ERROR(e);
        exit(1);
    }

    e = SM_Final();
    if (e < eNOERROR) {
        ERROR(e);
        exit(1);
    }

    return(0);

} /* main() */



/*
 * Function: static char *typeName(Four)
 *
 * Description:
 *  Return type name corresponding to the given type number
 *
 * Returns:
 *  type name
 */
static char *typeName(Four type)
{
    switch(type) {
      case SM_SHORT:
	return("SM_SHORT");

      case SM_INT:
	return("SM_INT");

      case SM_LONG:
	return("SM_LONG");

      case SM_LONG_LONG:
	return("SM_LONG_LONG");

      case SM_FLOAT:
	return("SM_FLOAT");

      case SM_DOUBLE:
	return("SM_DOUBLE");

      case SM_STRING:
	return("SM_STRING");

      case SM_VARSTRING:
	return("SM_VARSTRING");

      case SM_PAGEID:
	return("SM_PAGEID");

      case SM_FILEID:
	return("SM_FILEID");

      case SM_OID:
	return("SM_OID");
    }

    return("NOT_KNOWN");

} /* typeName() */
