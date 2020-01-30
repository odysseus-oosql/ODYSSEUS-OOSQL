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
 * Module: LRDS_FormatDataVolume.c
 *
 * Description:
 *  Format a file or a raw device so that it can be used as the volume of our
 *  storage system. This format is for LRDS Manager level.
 *
 * Exports:
 *  Four LRDS_FormatDataVolume(Four, char*, char*, Four, Four, Four, Four)
 */


#include <assert.h>
#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SM.h"
#include "LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*@================================
 * LRDS_FormatDataVolume( )
 *================================*/
/*
 * Function: Four LRDS_FormatDataVolume(Four, Four, char*, char*, Four, Four, Four, Four)
 *
 * Description:
 *  Format a file or a raw device so that it can be used as the volume of our
 *  storage system. This format is for LRDS Manager level.
 *
 * Returns:
 *  exit status
 */
Four LRDS_FormatDataVolume(
    Four handle,                /* IN handle */
    Four numDevices,            /* IN number of devices in formated volume */
    char **devNames,            /* IN array of device name */
    char *title,                /* IN volume title */
    Four volId,                 /* IN volume number */
    Four extSize,               /* IN number of pages in an extent */
    Four *numPagesInDevice,     /* IN array of extents' number */
    Four segmentSize)           /* IN # of pages in an segment */
{
    Four e;			/* error number */


    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_FormatDataVolume(handle=%lD, numDevices=%lD, devNames=%P, title=%P, volId=%ld, extSize=%ld, numPagesInDevice=%P, segmentSize=%ld)", handle, numDevices, devNames, title, volId, extSize, numPagesInDevice, segmentSize));


    /* parameter check */
    if (volId < 0 || volId > MAX_VOLUME_NUMBER) ERR(handle, eBADPARAMETER);
    if (extSize < 1) ERR(handle, eBADPARAMETER);
    if (segmentSize < extSize) ERR(handle, eBADPARAMETER);

    /* format the volume for the sublayer */
    e = SM_FormatDataVolume(handle, numDevices, devNames, title, volId, extSize, numPagesInDevice, segmentSize);
    if (e < eNOERROR) ERR(handle, e);

    /* format the volume for the lrds layer */
    e = lrds_FormatDataVolume(handle, numDevices, devNames, title, volId, extSize, numPagesInDevice, segmentSize);
    if (e < eNOERROR) ERR(handle, e);


    return (eNOERROR);

} /* LRDS_FormatDataVolume() */



/*@================================
 * lrds_FormatDataVolume()
 *================================*/
/*
 * Function: Four lrds_FormatDataVolume(Four, char*, char*, Four, Four, Four, Four)
 *
 * Description:
 *  Format a file or a raw device so that it can be used as the volume of our
 *  storage system. This format is for LRDS Manager level.
 *
 * Returns:
 *  exit status
 */
Four lrds_FormatDataVolume(
    Four handle,                /* IN handle */
    Four numDevices,            /* IN number of devices in formated volume */
    char **devNames,            /* IN array of device name */
    char *title,                /* IN volume title */
    Four volId,                 /* IN volume number */
    Four extSize,               /* IN number of pages in an extent */
    Four *numPagesInDevice,     /* IN array of extents' number */
    Four segmentSize)           /* IN # of pages in an segment */
{
    Four e;			/* error number */
    Four i;			/* a temporary variable */
    Four scanId;		/* a scan identifier */
    Four len;			/* a length variable */
    Four fixedStart;		/* starting offset of the first fixed-length column */
    Four keyLen;		/* key length */
    unsigned char *nullVector;	/* bit array of null flags */
    TupleHdr tupHdr;		/* a tuple header */
    KeyValue kval;		/* key value */
    ObjectID oid;		/* temporary ObjectID */
    ColInfo cinfo[10];		/* column descriptor */
				/* The number of elements should be the largest */
				/* of numbers of columns of each catalog table */
    char data[200];		/* tuple buffer: The buffer should be large */
				/* enough to store all type of catalog tuples. */

    FileID sysTablesDataFileId;	            /* FileID of the data file of LRDS_SYSTABLES */
    FileID sysColumnsDataFileId;            /* FileID of the data file of LRDS_SYSCOLUMNS*/
    FileID sysIndexesDataFileId;            /* FileID of the data file of LRDS_SYSINDEXES */
    IndexID sysTablesRelNameIndex;          /* B+ tree index on RelName of LRDS_SYSTABLES */
    IndexID sysColumnsDataFileIdIndex;      /* B+ tree index on DataFileId of LRDS_SYSCOLUMNS */
    IndexID sysIndexesDataFileIdIndex;      /* B+ tree index on DataFileId of LRDS_SYSINDEXES */
    KeyDesc sysTablesRelNameIndexKdesc;	    /* key descriptor of B+ tree on RelName of LRDS_SYSTABLES */
    KeyDesc sysColumnsDataFileIdIndexKdesc; /* key descriptor of B+ tree on DataFileIdIndex */
    KeyDesc sysIndexesDataFileIdIndexKdesc; /* key descriptor of B+ tree on DataFileId of LRDS_SYSINDEXES */

    XactID  xactId;		/* transaction identifier for format operation */
    LRDS_IndexDesc idesc;	/* index descriptor */
    Four dummyVolId;


    TR_PRINT(handle, TR_LRDS, TR1, ("lrds_FormatDataVolume(handle=%lD, numDevices=%lD, devNames=%P, title=%P, volId=%ld, extSize=%ld, numPagesInDevice=%P, segmentSize=%ld)",handle, numDevices, devNames, title, volId, extSize, numPagesInDevice, segmentSize));


    /* Mount the given volume. */
    e = SM_Mount(handle, numDevices, devNames, &dummyVolId);
    if (e < eNOERROR) ERR(handle, e);

    /* because of temporary volume, volume id can be changed!! */
    volId = dummyVolId;

    /* start Format transaction */
    e = SM_BeginTransaction(handle, &xactId, X_RR_RR);
    if (e < eNOERROR) ERR(handle, e);

    /* Construct the key descriptors. */
    sysTablesRelNameIndexKdesc.flag = KEYFLAG_CLUSTERING;
    sysTablesRelNameIndexKdesc.nparts = 1;
    sysTablesRelNameIndexKdesc.kpart[0].type = SM_VARSTRING;
    sysTablesRelNameIndexKdesc.kpart[0].length = MAXRELNAME;

    sysColumnsDataFileIdIndexKdesc.flag = KEYFLAG_CLUSTERING;
    sysColumnsDataFileIdIndexKdesc.nparts = 1;
    sysColumnsDataFileIdIndexKdesc.kpart[0].type = SM_FILEID;
    sysColumnsDataFileIdIndexKdesc.kpart[0].length = SM_FILEID_SIZE;

    sysIndexesDataFileIdIndexKdesc.flag = KEYFLAG_CLUSTERING;
    sysIndexesDataFileIdIndexKdesc.nparts = 1;
    sysIndexesDataFileIdIndexKdesc.kpart[0].type = SM_FILEID;
    sysIndexesDataFileIdIndexKdesc.kpart[0].length = SM_FILEID_SIZE;


    /*
    ** Do the LRDS level format.
    */


    /* Create a data file of relation LRDS_SYSTABLES. */
    e = SM_CreateFile(handle, volId, &sysTablesDataFileId, FALSE, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Create a B+ tree index on of LRDS_SYSTABLES. */
    e = SM_AddIndex(handle, &sysTablesDataFileId, &sysTablesRelNameIndex, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /* Create a data file of relation LRDS_SYSCOLUMNS. */
    e = SM_CreateFile(handle, volId, &sysColumnsDataFileId, FALSE, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Create a B+ tree index on of LRDS_SYSCOLUMNS. */
    e = SM_AddIndex(handle, &sysColumnsDataFileId, &sysColumnsDataFileIdIndex, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /* Create a data file of relation LRDS_SYSINDEXES. */
    e = SM_CreateFile(handle, volId, &sysIndexesDataFileId, FALSE, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Create a B+ tree index on of LRDS_SYSINDEXES. */
    e = SM_AddIndex(handle, &sysIndexesDataFileId, &sysIndexesDataFileIdIndex, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /******************************************
     * Insert tuples into the LRDS_SYSTABLES. *
     ******************************************/

    /* Open a scan on LRDS_SYSTABLES. */
    scanId = SM_OpenSeqScan(handle, &sysTablesDataFileId, FORWARD, NULL);
    if (scanId < 0) ERR(handle, scanId);

    tupHdr.nFixedCols = LRDS_SYSTABLES_NUM_FIXEDCOLS;
    tupHdr.nVarCols = LRDS_SYSTABLES_NUM_VARCOLS;

    fixedStart = TUPLE_HEADER_SIZE(LRDS_SYSTABLES_NUM_COLS, LRDS_SYSTABLES_NUM_VARCOLS);

    tupHdr.firstVarColOffset = fixedStart + LRDS_SYSTABLES_MAXTUPLELEN - MAXRELNAME;
    tupHdr.varColOffset[0] = tupHdr.firstVarColOffset + strlen(LRDS_SYSTABLES_RELNAME);

    nullVector = (unsigned char*)&(tupHdr.varColOffset[tupHdr.nVarCols]);

    /* Reset all the bits of the null vector. */
    for (i = 0; i < (LRDS_SYSTABLES_NUM_COLS+7)/8 ; i++) nullVector[i] = 0;

    /*
    ** entry for LRDS_SYSTABLES
    */
    e = SM_CreateObject(handle, scanId, (ObjectID*)NULL, (ObjectHdr*)NULL,
			fixedStart, (char*)&tupHdr, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);

    len = 0;
    memcpy(&data[len], (char*)&sysTablesDataFileId, SM_FILEID_SIZE); len += SM_FILEID_SIZE;

    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_NUMOFCATALOGTABLES, &data[len], Four); len += sizeof(Four);
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSTABLES_MAXTUPLELEN, &data[len], Four); len += sizeof(Four);
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSTABLES_NUM_COLS, &data[len], Two); len += sizeof(Two);
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSTABLES_NUM_INDEXES, &data[len], Two); len += sizeof(Two);
    memcpy(&data[len], LRDS_SYSTABLES_RELNAME, strlen(LRDS_SYSTABLES_RELNAME)); len += strlen(LRDS_SYSTABLES_RELNAME);

    e = SM_UpdateObject(handle, scanId, &oid, END, 0, data, len, NULL);
    if (e < eNOERROR) ERR(handle, e);

    keyLen = strlen(LRDS_SYSTABLES_RELNAME);
    kval.len = sizeof(Two) + keyLen;
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, keyLen, &kval.val[0], Two);
    memcpy(&kval.val[sizeof(Two)], LRDS_SYSTABLES_RELNAME, keyLen);

    e = SM_InsertIndexEntry(handle, &sysTablesRelNameIndex,
			    &sysTablesRelNameIndexKdesc,
			    &kval, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** entry for LRDS_SYSCOLUMNS
    */
    tupHdr.varColOffset[0] = tupHdr.firstVarColOffset + strlen(LRDS_SYSCOLUMNS_RELNAME);
    e = SM_CreateObject(handle, scanId, (ObjectID*)NULL, (ObjectHdr*)NULL,
			fixedStart, (char*)&tupHdr, &oid, NULL );
    if (e < eNOERROR) ERR(handle, e);

    len = 0;
    memcpy(&data[len], (char*)&sysColumnsDataFileId, SM_FILEID_SIZE); len += SM_FILEID_SIZE;
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSTABLES_NUM_COLS+LRDS_SYSCOLUMNS_NUM_COLS+LRDS_SYSINDEXES_NUM_COLS, &data[len], Four); len += sizeof(Four);
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSCOLUMNS_MAXTUPLELEN, &data[len], Four); len += sizeof(Four);
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSCOLUMNS_NUM_COLS, &data[len], Two); len += sizeof(Two);
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSCOLUMNS_NUM_INDEXES, &data[len], Two); len += sizeof(Two);
    memcpy(&data[len], LRDS_SYSCOLUMNS_RELNAME, strlen(LRDS_SYSCOLUMNS_RELNAME)); len += strlen(LRDS_SYSCOLUMNS_RELNAME);

    e = SM_UpdateObject(handle, scanId, &oid, END, 0, data, len, NULL);
    if (e < eNOERROR) ERR(handle, e);

    keyLen = strlen(LRDS_SYSCOLUMNS_RELNAME);
    kval.len = sizeof(Two) + keyLen;
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, keyLen, &kval.val[0], Two);
    memcpy(&kval.val[sizeof(Two)], LRDS_SYSCOLUMNS_RELNAME, keyLen);

    e = SM_InsertIndexEntry(handle, &sysTablesRelNameIndex,
			    &sysTablesRelNameIndexKdesc,
			    &kval, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** entry for LRDS_SYSINDEXES
    */
    tupHdr.varColOffset[0] = tupHdr.firstVarColOffset + strlen(LRDS_SYSINDEXES_RELNAME);
    e = SM_CreateObject(handle, scanId, (ObjectID*)NULL, (ObjectHdr*)NULL,
			fixedStart, (char*)&tupHdr, &oid, NULL );
    if (e < eNOERROR) ERR(handle, e);

    len = 0;
    memcpy(&data[len], (char*)&sysIndexesDataFileId, SM_FILEID_SIZE); len += SM_FILEID_SIZE;
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSTABLES_NUM_INDEXES+LRDS_SYSCOLUMNS_NUM_INDEXES+LRDS_SYSINDEXES_NUM_INDEXES, &data[len], Four); len += sizeof(Four);
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSINDEXES_MAXTUPLELEN, &data[len], Four); len += sizeof(Four);
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSINDEXES_NUM_COLS, &data[len], Two); len += sizeof(Two);
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, LRDS_SYSINDEXES_NUM_INDEXES, &data[len], Two); len += sizeof(Two);
    memcpy(&data[len], LRDS_SYSINDEXES_RELNAME, strlen(LRDS_SYSINDEXES_RELNAME)); len += strlen(LRDS_SYSINDEXES_RELNAME);

    e = SM_UpdateObject(handle, scanId, &oid, END, 0, data, len, NULL );
    if (e < eNOERROR) ERR(handle, e);

    keyLen = strlen(LRDS_SYSINDEXES_RELNAME);
    kval.len = sizeof(Two) + keyLen;
    ST_TWO_FOUR_EIGHT_INVARIABLE(handle, keyLen, &kval.val[0], Two);
    memcpy(&kval.val[sizeof(Two)], LRDS_SYSINDEXES_RELNAME, keyLen);

    e = SM_InsertIndexEntry(handle, &sysTablesRelNameIndex,
			    &sysTablesRelNameIndexKdesc,
			    &kval, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /* Close the scan on LRDS_SYSTABLES. */
    e = SM_CloseScan(handle, scanId);
    if (e < eNOERROR) ERR(handle, e);


    /******************************************
     * Insert tuples into the LRDS_SYSCOLUMNS. *
     ******************************************/

    /* Open scan on LRDS_SYSCOLUMNS. */
    scanId = SM_OpenSeqScan(handle, &sysColumnsDataFileId, FORWARD, NULL );
    if (scanId < 0) ERR(handle, scanId);

    tupHdr.nFixedCols = LRDS_SYSCOLUMNS_NUM_FIXEDCOLS;
    tupHdr.nVarCols = LRDS_SYSCOLUMNS_NUM_VARCOLS;

    fixedStart = TUPLE_HEADER_SIZE(LRDS_SYSCOLUMNS_NUM_COLS, LRDS_SYSCOLUMNS_NUM_VARCOLS);

    tupHdr.firstVarColOffset = fixedStart + LRDS_SYSCOLUMNS_MAXTUPLELEN - sizeof(AuxColInfo_T);
    tupHdr.varColOffset[0] = tupHdr.firstVarColOffset; /* AUXCOLINFO is NULL for all catalog table columns */

    nullVector = (unsigned char*)&(tupHdr.varColOffset[tupHdr.nVarCols]);

    /* Reset all the bits of the null vector. */
    for (i = 0; i < (LRDS_SYSCOLUMNS_NUM_COLS+7)/8 ; i++) nullVector[i] = 0;

    BITSET(nullVector, LRDS_SYSCOLUMNS_AUXCOLINFO_COLNO);

    /*
    ** entry for LRDS_SYSTABLES
    */
    /* DATAFILEID */
    cinfo[0].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[0].type = SM_FILEID;
    cinfo[0].length = SM_FILEID_SIZE;

    /* NTUPLES */
    cinfo[1].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[1].type = CONVERT_TO_SM_TYPE(Four);
    cinfo[1].length = sizeof(Four);

    /* MAXTUPLELEN */
    cinfo[2].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[2].type = CONVERT_TO_SM_TYPE(Four);
    cinfo[2].length = sizeof(Four);

    /* NCOLUMNS */
    cinfo[3].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[3].type = CONVERT_TO_SM_TYPE(Two);
    cinfo[3].length = sizeof(Two);

    /* NINDEXES */
    cinfo[4].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[4].type = CONVERT_TO_SM_TYPE(Two);
    cinfo[4].length = sizeof(Two);

    /* RELNAME */
    cinfo[5].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[5].type = SM_VARSTRING;
    cinfo[5].length = MAXRELNAME;

    /* Construct a key of the B+ tree. */
    kval.len = SM_FILEID_SIZE;
    memcpy(&kval.val[0], (char*)&sysTablesDataFileId, kval.len);

    for (i = 0; i < LRDS_SYSTABLES_NUM_COLS; i++) {
	e = SM_CreateObject(handle, scanId, (ObjectID*)NULL, (ObjectHdr*)NULL,
			    fixedStart, (char*)&tupHdr, &oid, NULL);
	if (e < eNOERROR) ERR(handle, e);

	len = 0;
	memcpy(&data[len], (char*)&sysTablesDataFileId, SM_FILEID_SIZE); len += SM_FILEID_SIZE;
	ST_TWO_FOUR_EIGHT_INVARIABLE(handle, i, &data[len], Two); len += sizeof(Two);
	memcpy(&data[len], (char*)&cinfo[i], sizeof(ColInfo)); len += sizeof(ColInfo);

	e = SM_UpdateObject(handle, scanId, &oid, END, 0, data, len, NULL);
	if (e < eNOERROR) ERR(handle, e);

	e = SM_InsertIndexEntry(handle, &sysColumnsDataFileIdIndex,
				&sysColumnsDataFileIdIndexKdesc,
				&kval, &oid, NULL);
	if (e < eNOERROR) ERR(handle, e);
    }


    /*
    ** entry for LRDS_SYSCOLUMNS
    */
    /* DATAFILEID */
    cinfo[0].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[0].type = SM_FILEID_SIZE;
    cinfo[0].length = SM_FILEID_SIZE;

    /* COLNO */
    cinfo[1].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[1].type = CONVERT_TO_SM_TYPE(Two);
    cinfo[1].length = sizeof(Two);

    /* COLINFO */
    cinfo[2].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[2].type = SM_STRING;
    cinfo[2].length = sizeof(ColInfo);

    /* ordered set */
    /* AUXCOLINFO */
    cinfo[3].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[3].type = SM_VARSTRING;
    cinfo[3].length = sizeof(AuxColInfo_T);

    /* Construct a key of the B+ tree. */
    kval.len = SM_FILEID_SIZE;
    memcpy(&kval.val[0], (char*)&sysColumnsDataFileId, kval.len);

    for (i = 0; i < LRDS_SYSCOLUMNS_NUM_COLS; i++) {
	e = SM_CreateObject(handle, scanId, (ObjectID*)NULL, (ObjectHdr*)NULL,
			    fixedStart, (char*)&tupHdr, &oid, NULL);
	if (e < eNOERROR) ERR(handle, e);

	len = 0;
	memcpy(&data[len], (char*)&sysColumnsDataFileId, SM_FILEID_SIZE); len += SM_FILEID_SIZE;
	ST_TWO_FOUR_EIGHT_INVARIABLE(handle, i, &data[len], Two); len += sizeof(Two);
	memcpy(&data[len], (char*)&cinfo[i], sizeof(ColInfo)); len += sizeof(ColInfo);

	e = SM_UpdateObject(handle, scanId, &oid, END, 0, data, len, NULL);
	if (e < eNOERROR) ERR(handle, e);

	e = SM_InsertIndexEntry(handle, &sysColumnsDataFileIdIndex,
				&sysColumnsDataFileIdIndexKdesc,
				&kval, &oid, NULL);
	if (e < eNOERROR) ERR(handle, e);
    }

    /*
    ** entry for LRDS_SYSINDEXES
    */
    /* DATAFILEID */
    cinfo[0].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[0].type = SM_FILEID;
    cinfo[0].length = SM_FILEID_SIZE;

    /* INDEXID */
    cinfo[1].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[1].type = SM_INDEXID;
    cinfo[1].length = SM_INDEXID_SIZE;

    /* LRDS_IndexDesc */
    cinfo[2].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[2].type = SM_STRING;
    cinfo[2].length = sizeof(LRDS_IndexDesc);

    /* Construct a key of the B+ tree. */
    kval.len = SM_FILEID_SIZE;
    memcpy(&kval.val[0], (char*)&sysIndexesDataFileId, kval.len);

    for (i = 0; i < LRDS_SYSINDEXES_NUM_COLS; i++) {
	e = SM_CreateObject(handle, scanId, (ObjectID*)NULL, (ObjectHdr*)NULL,
			    fixedStart, (char*)&tupHdr, &oid, NULL);
	if (e < eNOERROR) ERR(handle, e);

	len = 0;
	memcpy(&data[len], (char*)&sysIndexesDataFileId, SM_FILEID_SIZE); len += SM_FILEID_SIZE;
	ST_TWO_FOUR_EIGHT_INVARIABLE(handle, i, &data[len], Two); len += sizeof(Two);
	memcpy(&data[len], (char*)&cinfo[i], sizeof(ColInfo)); len += sizeof(ColInfo);

	e = SM_UpdateObject(handle, scanId, &oid, END, 0, data, len, NULL);
	if (e < eNOERROR) ERR(handle, e);

	e = SM_InsertIndexEntry(handle, &sysColumnsDataFileIdIndex,
				&sysColumnsDataFileIdIndexKdesc,
				&kval, &oid, NULL);
	if (e < eNOERROR) ERR(handle, e);
    }

    /* Close scan on LRDS_SYSCOLUMS. */
    e = SM_CloseScan(handle, scanId);
    if (e < eNOERROR) ERR(handle, e);


    /*******************************************
     * Insert tuples into the LRDS_SYSINDEXES. *
     *******************************************/

    /* Open scan on LRDS_SYSINDEXES. */
    scanId = SM_OpenSeqScan(handle, &sysIndexesDataFileId, FORWARD, NULL);
    if (scanId < 0) ERR(handle, scanId);

    tupHdr.nFixedCols = LRDS_SYSINDEXES_NUM_FIXEDCOLS;
    tupHdr.nVarCols = LRDS_SYSINDEXES_NUM_VARCOLS;

    fixedStart = TUPLE_HEADER_SIZE(LRDS_SYSINDEXES_NUM_COLS, LRDS_SYSINDEXES_NUM_VARCOLS);

    tupHdr.firstVarColOffset = fixedStart + LRDS_SYSINDEXES_MAXTUPLELEN;
    /* LRDS_SYSINDEXES_NUM_VARCOLS is 0. So the following line is useless.*/
    nullVector = (unsigned char*)&(tupHdr.varColOffset[tupHdr.nVarCols]);

    /* Reset all the bits of the null vector. */
    for (i = 0; i < (LRDS_SYSINDEXES_NUM_COLS+7)/8 ; i++) nullVector[i] = 0;

    /*
    ** entry for LRDS_SYSTABLES
    */
    idesc.indexType = SM_INDEXTYPE_BTREE;
    idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING|KEYFLAG_UNIQUE;
    idesc.kinfo.btree.nColumns = 1;
    idesc.kinfo.btree.columns[0].colNo = LRDS_SYSTABLES_RELNAME_COLNO;
    idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

    e = SM_CreateObject(handle, scanId, (ObjectID*)NULL, (ObjectHdr*)NULL,
			fixedStart, (char*)&tupHdr, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);

    len = 0;
    memcpy(&data[len], (char*)&sysTablesDataFileId, SM_FILEID_SIZE); len += SM_FILEID_SIZE;
    memcpy(&data[len], (char*)&sysTablesRelNameIndex, SM_INDEXID_SIZE); len += SM_INDEXID_SIZE;
    memcpy(&data[len], (char*)&idesc, sizeof(LRDS_IndexDesc)); len += sizeof(LRDS_IndexDesc);

    e = SM_UpdateObject(handle, scanId, &oid, END, 0, data, len, NULL);
    if (e < eNOERROR) ERR(handle, e);

    kval.len = SM_FILEID_SIZE;
    memcpy(&kval.val[0], (char*)&sysTablesDataFileId, kval.len);

    e = SM_InsertIndexEntry(handle, &sysIndexesDataFileIdIndex,
			    &sysIndexesDataFileIdIndexKdesc,
			    &kval, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** entry for LRDS_SYSCOLUMNS
    */
    idesc.indexType = SM_INDEXTYPE_BTREE;
    idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
    idesc.kinfo.btree.nColumns = 1;
    idesc.kinfo.btree.columns[0].colNo = LRDS_SYSCOLUMNS_DATAFILEID_COLNO;
    idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

    e = SM_CreateObject(handle, scanId, (ObjectID*)NULL, (ObjectHdr*)NULL,
			fixedStart, (char*)&tupHdr, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);

    len = 0;
    memcpy(&data[len], (char*)&sysColumnsDataFileId, SM_FILEID_SIZE); len += SM_FILEID_SIZE;
    memcpy(&data[len], (char*)&sysColumnsDataFileIdIndex, SM_INDEXID_SIZE); len += SM_INDEXID_SIZE;
    memcpy(&data[len], (char*)&idesc, sizeof(LRDS_IndexDesc)); len += sizeof(LRDS_IndexDesc);

    e = SM_UpdateObject(handle, scanId, &oid, END, 0, data, len, NULL);
    if (e < eNOERROR) ERR(handle, e);

    kval.len = SM_FILEID_SIZE;
    memcpy(&kval.val[0], (char*)&sysColumnsDataFileId, kval.len);

    e = SM_InsertIndexEntry(handle, &sysIndexesDataFileIdIndex,
			    &sysIndexesDataFileIdIndexKdesc,
			    &kval, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** entry for LRDS_SYSINDEXES
    */
    idesc.indexType = SM_INDEXTYPE_BTREE;
    idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
    idesc.kinfo.btree.nColumns = 1;
    idesc.kinfo.btree.columns[0].colNo = LRDS_SYSINDEXES_DATAFILEID_COLNO;
    idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

    e = SM_CreateObject(handle, scanId, (ObjectID*)NULL, (ObjectHdr*)NULL,
			fixedStart, (char*)&tupHdr, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);

    len = 0;
    memcpy(&data[len], (char*)&sysIndexesDataFileId, SM_FILEID_SIZE); len += SM_FILEID_SIZE;
    memcpy(&data[len], (char*)&sysIndexesDataFileIdIndex, SM_INDEXID_SIZE); len += SM_INDEXID_SIZE;
    memcpy(&data[len], (char*)&idesc, sizeof(LRDS_IndexDesc)); len += sizeof(LRDS_IndexDesc);

    e = SM_UpdateObject(handle, scanId, &oid, END, 0, data, len, NULL);
    if (e < eNOERROR) ERR(handle, e);

    kval.len = SM_FILEID_SIZE;
    memcpy(&kval.val[0], (char*)&sysIndexesDataFileId, kval.len);

    e = SM_InsertIndexEntry(handle, &sysIndexesDataFileIdIndex,
			    &sysIndexesDataFileIdIndexKdesc,
			    &kval, &oid, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /* Close scan on LRDS_SYSCOLUMS. */
    e = SM_CloseScan(handle, scanId);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** Store the meta dictionary entries.
    */
    e = SM_InsertMetaDictEntry(handle, volId, "lrdsSysTablesDataFileId",
			    &sysTablesDataFileId, sizeof(FileID));
    if (e < eNOERROR) ERR(handle, e);

    e = SM_InsertMetaDictEntry(handle, volId, "lrdsSysTablesRelNameIndex",
			    &sysTablesRelNameIndex, sizeof(IndexID));
    if (e < eNOERROR) ERR(handle, e);

    e = SM_InsertMetaDictEntry(handle, volId, "lrdsSysColumnsDataFileIdIndex",
			    &sysColumnsDataFileIdIndex, sizeof(IndexID));
    if (e < eNOERROR) ERR(handle, e);

    e = SM_InsertMetaDictEntry(handle, volId, "lrdsSysIndexesDataFileIdIndex",
			    &sysIndexesDataFileIdIndex, sizeof(IndexID));
    if (e < eNOERROR) ERR(handle, e);

    /* start Format transaction */
    e = SM_CommitTransaction(handle, &xactId);
    if (e < eNOERROR) ERR(handle, e);

    /* Dismount the volume. */
    e = SM_Dismount(handle, volId);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* lrds_FormatDataVolume() */
