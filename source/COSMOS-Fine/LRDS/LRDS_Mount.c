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
 * Module: LRDS_Mount.c
 *
 * Description:
 *  Mount a given volume. The volume is specified using its name.
 *  It may be a filename or a device name. This function returns the volume
 *  identifier stored in the volume. We save the hot used values into the
 *  main memory for caching.
 *
 * Exports:
 *  Four LRDS_Mount(Four, char*, Four*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eTOOMANYVOLUMES
 *    eNOTFOUNDCATALOG_LRDS
 *    some errors caused by function calls
 *
 * Side effects:
 *  1) volId : volume identifier if no error.
 */

#include <unistd.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "SM.h"
#include "LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four LRDS_Mount(
    Four handle,
    Four numDevices,            /* IN # of devices in the volume to be mounted */
    char **devNames,            /* IN devices' name in the volume to be mounted */
    Four *volId)                /* OUT mounted volume's volume id */
{
    Four e;			/* error number */
    Four volNo;                /* volume no. */
    Four v;			/* array index on the mount table */
    Four user_v;		/* array index on the user mount table */
    Four i;			/* temporary variable */
    Four j;			/* temporary variable */
    Four fixedStart;		/* starting offset of the first fixed-length column */
    Four tupHdrSize;            /* tuple header size */
    Four n;			/* temporary variable to access tuple buffer */
    Four catSmScanId;		/* SM level scan on the catalog table */
    Four sysOrn[LRDS_NUMOFCATALOGTABLES]; /* open relation numbers of catalog tables */
    char tuplebuf[100];		/* for access of tuples of catalog tables */
    KeyDesc kdesc;		/* a key descriptor */
    ObjectID oid;		/* ObjectID of catalog tables in LRDS_SYSTABLES */
    BoundCond lb;		/* a bound condition of range scan */
    FileID sysTablesDataFileId;	 /* FileID of LRDS_SYSTABLES */
    IndexID sysTablesRelNameIndex; /* index on relation name of LRDS_SYSTABLES */
    IndexID sysColumnsDataFileIdIndex; /* index on data file id of LRDS_SYSCOLUMNS */
    IndexID sysIndexesDataFileIdIndex; /* index on data file id of LRDS_SYSINDEXES */
    Two keyLen;			       /* should be 'Two' to store key length */
    IndexInfo *relTableEntry_ii;
    ColDesc *relTableEntry_cdesc;

    /* pointer for LRDS Data Structure of perThreadTable */
    LRDS_PerThreadDS_T *lrds_perThreadDSptr = LRDS_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LRDS, TR1, ("LRDS_Mount(numDevices=%lD, devNames=%P, volId=%P)", numDevices, devNames, volId));

    /* check parameters. */
    if (numDevices <= 0) ERR(handle, eBADPARAMETER);
    if (devNames == NULL) ERR(handle, eBADPARAMETER);
    if (volId == NULL) ERR(handle, eBADPARAMETER);

    /*
    ** serialize the mount/dismount operation
    */
    ERROR_PASS(handle, SHM_getLatch(handle, &LRDS_LATCH_OPENRELATION, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));

    /* Mount the volume. */
    e = SM_Mount(handle, numDevices, devNames, &volNo);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    /*
    ** look up the user mount table
    */
    for (user_v = 0; user_v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; user_v++)
	if (LRDS_USERMOUNTTABLE(handle)[user_v].volId == volNo) {
	    LRDS_USERMOUNTTABLE(handle)[user_v].nMount++;

	    ERROR_PASS(handle, SHM_releaseLatch(handle, &LRDS_LATCH_OPENRELATION, procIndex));

    	    /* Set the 'volId' */
	    *volId = volNo;

	    return(eNOERROR);	/* already mounted */
	}

    /* Find an empty entry in LRDS_USERMOUNTTABLE(handle). */
    for (user_v = 0; user_v < LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE; user_v++)
	if (LRDS_IS_UNUSED_ENTRY_OF_USERMOUNTTABLE(handle, user_v)) break; /* empty entry */

    if (user_v == LRDS_NUM_OF_ENTRIES_OF_USERMOUNTTABLE)
	ERRL1(handle, eTOOMANYVOLUMES, &LRDS_LATCH_OPENRELATION);

    /* Find an empty entries in LRDS_USEROPENRELTABLE(handle). */
    for (i = 0, j = 0; i < LRDS_NUM_OF_ENTRIES_OF_USEROPENRELTABLE && j < LRDS_NUMOFCATALOGTABLES; i++)
	if (LRDS_IS_UNUSED_ENTRY_OF_USEROPENRELTABLE(handle, i)) {
	    LRDS_USERMOUNTTABLE(handle)[user_v].catalogTableOrn[j] = i; j++;
	}

    if (j != LRDS_NUMOFCATALOGTABLES) ERRL1(handle, eTOOMANYOPENRELATIONS_LRDS, &LRDS_LATCH_OPENRELATION);


    /* Check whether the volume is already mounted. */
    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_MOUNTTABLE; v++)
	if (LRDS_MOUNTTABLE[v].volId == volNo) {
	    LRDS_MOUNTTABLE[v].nMount++;

	    for (j = 0; j < LRDS_NUMOFCATALOGTABLES; j++) {
		LRDS_RELTABLE[LRDS_MOUNTTABLE[v].catalogTableOrn[j]].count ++;

		LRDS_USEROPENRELTABLE(handle)[LRDS_USERMOUNTTABLE(handle)[user_v].catalogTableOrn[j]].sysOrn = LRDS_MOUNTTABLE[v].catalogTableOrn[j];
		LRDS_USEROPENRELTABLE(handle)[LRDS_USERMOUNTTABLE(handle)[user_v].catalogTableOrn[j]].count = 1;
		LRDS_USEROPENRELTABLE(handle)[LRDS_USERMOUNTTABLE(handle)[user_v].catalogTableOrn[j]].tmpRelationFlag = FALSE;
	    }
	    LRDS_USERMOUNTTABLE(handle)[user_v].volId = volNo;
	    LRDS_USERMOUNTTABLE(handle)[user_v].nMount = 1;

	    ERROR_PASS(handle, SHM_releaseLatch(handle, &LRDS_LATCH_OPENRELATION, procIndex));

    	    /* Set the 'volId' */
	    *volId = volNo;

	    return(eNOERROR);	/* already mounted */
	}

    /* Find an empty entry in lrdsMountTable. */
    for (v = 0; v < LRDS_NUM_OF_ENTRIES_OF_MOUNTTABLE; v++)
	if (LRDS_IS_UNUSED_ENTRY_OF_MOUNTTABLE(v)) break; /* empty entry */

    if (v == LRDS_NUM_OF_ENTRIES_OF_MOUNTTABLE)
	ERRL1(handle, eTOOMANYVOLUMES, &LRDS_LATCH_OPENRELATION);


    /* Find the empty entries of open relation tables for catalog tables. */
    for (i = 0, j = 0; i < LRDS_NUM_OF_ENTRIES_OF_RELTABLE && j < LRDS_NUMOFCATALOGTABLES; i++)
	if (LRDS_IS_UNUSED_ENTRY_OF_RELTABLE(i)) {
	    sysOrn[j] = i; j++;
	}

    if (j != LRDS_NUMOFCATALOGTABLES) ERRL1(handle, eTOOMANYOPENRELATIONS_LRDS, &LRDS_LATCH_OPENRELATION);


    /**************************************************************
     * Open LRDS_SYSTABLES, LRDS_SYSCOLUMNS, and LRDS_SYSINDEXES. *
     **************************************************************/

    /* At first, get the FileID's and IndexID's of the catalog tables. */
    /* LRDS_SYSTABLES */
    e = SM_GetMetaDictEntry(handle, volNo, "lrdsSysTablesDataFileId",
			    &sysTablesDataFileId, sizeof(FileID));
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    e = SM_GetMetaDictEntry(handle, volNo, "lrdsSysTablesRelNameIndex",
			    &sysTablesRelNameIndex, sizeof(IndexID));
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    /* LRDS_SYSCOLUMNS */
    e = SM_GetMetaDictEntry(handle, volNo, "lrdsSysColumnsDataFileIdIndex",
			    &sysColumnsDataFileIdIndex, sizeof(IndexID));
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    /* LRDS_SYSINDEXES */
    e = SM_GetMetaDictEntry(handle, volNo, "lrdsSysIndexesDataFileIdIndex",
			    &sysIndexesDataFileIdIndex, sizeof(IndexID));
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);


    /*
    ** Extract relation information from the LRDS_SYSTABLES.
    */
    /* start offset of data area */
    /* LRDS_SYSTABLES has 1 variable-length column. */
    tupHdrSize = TUPLE_HEADER_SIZE(LRDS_SYSTABLES_NUM_COLS, LRDS_SYSTABLES_NUM_VARCOLS);

    kdesc.flag = KEYFLAG_CLUSTERING|KEYFLAG_UNIQUE;
    kdesc.nparts = 1;
    kdesc.kpart[0].type = SM_VARSTRING;
    kdesc.kpart[0].length = MAXRELNAME;

    /*
    ** LRDS_SYSTABLES
    */
    lb.op = SM_EQ;
    keyLen = strlen(lrds_perThreadDSptr->catalogTable[LRDS_SYSTABLES]);
    lb.key.len = sizeof(Two) + keyLen;
    memcpy(&(lb.key.val[0]), &keyLen, sizeof(Two));
    memcpy(&(lb.key.val[sizeof(Two)]), lrds_perThreadDSptr->catalogTable[LRDS_SYSTABLES], keyLen);
    catSmScanId = SM_OpenIndexScan(handle, &sysTablesDataFileId,
				   &sysTablesRelNameIndex, &kdesc, &lb, &lb, NULL);
    if (catSmScanId < 0) ERRL1(handle, catSmScanId, &LRDS_LATCH_OPENRELATION);

    e = SM_NextObject(handle, catSmScanId, &oid, (ObjectHdr*)NULL, NULL, NULL, NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    if (e == EOS) ERRL1(handle, eCATALOGNOTFOUND_LRDS, &LRDS_LATCH_OPENRELATION);

    e = SM_FetchObject(handle, catSmScanId, (ObjectID*)NULL, tupHdrSize, REMAINDER, &(tuplebuf[0]), NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    LRDS_RELTABLE[sysOrn[0]].ri.catalogEntry = oid;
    n = 0;
    memcpy(&(LRDS_RELTABLE[sysOrn[0]].ri.fid), &(tuplebuf[n]), sizeof(FileID));
    n += sizeof(FileID);
    memcpy(&(LRDS_RELTABLE[sysOrn[0]].ri.nTuples), &(tuplebuf[n]), sizeof(Four));
    n += sizeof(Four);
    memcpy(&(LRDS_RELTABLE[sysOrn[0]].ri.maxTupleLen), &(tuplebuf[n]), sizeof(Four));
    n += sizeof(Four);
    memcpy(&(LRDS_RELTABLE[sysOrn[0]].ri.nColumns), &(tuplebuf[n]), sizeof(Two));
    n += sizeof(Two);
    memcpy(&(LRDS_RELTABLE[sysOrn[0]].ri.nIndexes), &(tuplebuf[n]), sizeof(Two));

    LRDS_RELTABLE[sysOrn[0]].ri.nVarColumns = LRDS_SYSTABLES_NUM_VARCOLS; /* # of variable-length columns */
    strcpy(LRDS_RELTABLE[sysOrn[0]].ri.relName, lrds_perThreadDSptr->catalogTable[LRDS_SYSTABLES]);

    e = SM_CloseScan(handle, catSmScanId);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);


    /*
    ** LRDS_SYSCOLUMNS
    */
    lb.op = SM_EQ;
    keyLen = strlen(lrds_perThreadDSptr->catalogTable[LRDS_SYSCOLUMNS]);
    lb.key.len = sizeof(Two) + keyLen;
    memcpy(&(lb.key.val[0]), &keyLen, sizeof(Two));
    memcpy(&(lb.key.val[sizeof(Two)]), lrds_perThreadDSptr->catalogTable[LRDS_SYSCOLUMNS], keyLen);
    catSmScanId = SM_OpenIndexScan(handle, &sysTablesDataFileId,
				   &sysTablesRelNameIndex, &kdesc, &lb, &lb, NULL);
    if (catSmScanId < 0) ERRL1(handle, catSmScanId, &LRDS_LATCH_OPENRELATION);

    e = SM_NextObject(handle, catSmScanId, &oid, (ObjectHdr*)NULL, NULL, NULL, NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    if (e == EOS) ERRL1(handle, eCATALOGNOTFOUND_LRDS, &LRDS_LATCH_OPENRELATION);

    e = SM_FetchObject(handle, catSmScanId, (ObjectID*)NULL, tupHdrSize, REMAINDER, &(tuplebuf[0]), NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    LRDS_RELTABLE[sysOrn[1]].ri.catalogEntry = oid;
    n = 0;
    memcpy(&(LRDS_RELTABLE[sysOrn[1]].ri.fid), &(tuplebuf[n]), sizeof(FileID));
    n += sizeof(FileID);
    memcpy(&(LRDS_RELTABLE[sysOrn[1]].ri.nTuples), &(tuplebuf[n]), sizeof(Four));
    n += sizeof(Four);
    memcpy(&(LRDS_RELTABLE[sysOrn[1]].ri.maxTupleLen), &(tuplebuf[n]), sizeof(Four));
    n += sizeof(Four);
    memcpy(&(LRDS_RELTABLE[sysOrn[1]].ri.nColumns), &(tuplebuf[n]), sizeof(Two));
    n += sizeof(Two);
    memcpy(&(LRDS_RELTABLE[sysOrn[1]].ri.nIndexes), &(tuplebuf[n]), sizeof(Two));

    LRDS_RELTABLE[sysOrn[1]].ri.nVarColumns = LRDS_SYSCOLUMNS_NUM_VARCOLS; /* # of variable-length columns */
    strcpy(LRDS_RELTABLE[sysOrn[1]].ri.relName, lrds_perThreadDSptr->catalogTable[LRDS_SYSCOLUMNS]);

    e = SM_CloseScan(handle, catSmScanId);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);


    /*
    ** LRDS_SYSINDEXES
    */
    lb.op = SM_EQ;
    keyLen = strlen(lrds_perThreadDSptr->catalogTable[LRDS_SYSINDEXES]);
    lb.key.len = sizeof(Two) + keyLen;
    memcpy(&(lb.key.val[0]), &keyLen, sizeof(Two));
    memcpy(&(lb.key.val[sizeof(Two)]), lrds_perThreadDSptr->catalogTable[LRDS_SYSINDEXES], keyLen);
    catSmScanId = SM_OpenIndexScan(handle, &sysTablesDataFileId,
				   &sysTablesRelNameIndex, &kdesc, &lb, &lb, NULL);
    if (catSmScanId < 0) ERR(handle, catSmScanId);

    e = SM_NextObject(handle, catSmScanId, &oid, (ObjectHdr*)NULL, NULL, NULL, NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    if (e == EOS) ERRL1(handle, eCATALOGNOTFOUND_LRDS, &LRDS_LATCH_OPENRELATION);

    e = SM_FetchObject(handle, catSmScanId, (ObjectID*)NULL, tupHdrSize, REMAINDER, &(tuplebuf[0]), NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);

    LRDS_RELTABLE[sysOrn[2]].ri.catalogEntry = oid;
    n = 0;
    memcpy(&(LRDS_RELTABLE[sysOrn[2]].ri.fid), &(tuplebuf[n]), sizeof(FileID));
    n += sizeof(FileID);
    memcpy(&(LRDS_RELTABLE[sysOrn[2]].ri.nTuples), &(tuplebuf[n]), sizeof(Four));
    n += sizeof(Four);
    memcpy(&(LRDS_RELTABLE[sysOrn[2]].ri.maxTupleLen), &(tuplebuf[n]), sizeof(Four));
    n += sizeof(Four);
    memcpy(&(LRDS_RELTABLE[sysOrn[2]].ri.nColumns), &(tuplebuf[n]), sizeof(Two));
    n += sizeof(Two);
    memcpy(&(LRDS_RELTABLE[sysOrn[2]].ri.nIndexes), &(tuplebuf[n]), sizeof(Two));

    LRDS_RELTABLE[sysOrn[2]].ri.nVarColumns = LRDS_SYSINDEXES_NUM_VARCOLS; /* # of variable-length columns  */
    strcpy(LRDS_RELTABLE[sysOrn[2]].ri.relName, lrds_perThreadDSptr->catalogTable[LRDS_SYSINDEXES]);

    e = SM_CloseScan(handle, catSmScanId);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);


    /*
    ** Construct 'cdesc' of catalog tables manually.
    */
    /*
    ** LRDS_SYSTABLES
    */
    /* fixedStart means starting offset of the first fixed-length column. */
    /* LRDS_SYSTABLES have one variable-length column and 6 columns. */
    fixedStart = 0;

    e = Util_getArrayFromHeap(handle, &LRDS_COLUMNTABLEHEAP, LRDS_SYSTABLES_NUM_COLS,
			      &relTableEntry_cdesc);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);
    LRDS_RELTABLE[sysOrn[0]].cdesc = LOGICAL_PTR(relTableEntry_cdesc);

    /* DATAFILEID */
    relTableEntry_cdesc[0].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[0].type = SM_FILEID;
    relTableEntry_cdesc[0].fixedColNo = 0;
    relTableEntry_cdesc[0].varColNo = NIL;
    relTableEntry_cdesc[0].offset = fixedStart;
    relTableEntry_cdesc[0].length = SM_FILEID_SIZE;
    relTableEntry_cdesc[0].auxInfo = LOGICAL_PTR(NULL);
    fixedStart += sizeof(FileID);

    /* NTUPLES */
    relTableEntry_cdesc[1].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[1].type = CONVERT_TO_SM_TYPE(Four);
    relTableEntry_cdesc[1].fixedColNo = 1;
    relTableEntry_cdesc[1].varColNo = NIL;
    relTableEntry_cdesc[1].offset = fixedStart;
    relTableEntry_cdesc[1].length = sizeof(Four);
    relTableEntry_cdesc[1].auxInfo = LOGICAL_PTR(NULL);
    fixedStart += sizeof(Four);

    /* MAXTUPLELEN */
    relTableEntry_cdesc[2].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[2].type = CONVERT_TO_SM_TYPE(Four);
    relTableEntry_cdesc[2].fixedColNo = 2;
    relTableEntry_cdesc[2].varColNo = NIL;
    relTableEntry_cdesc[2].offset = fixedStart;
    relTableEntry_cdesc[2].length = sizeof(Four);
    relTableEntry_cdesc[2].auxInfo = LOGICAL_PTR(NULL);
    fixedStart += sizeof(Four);

    /* NCOLUMNS */
    relTableEntry_cdesc[3].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[3].type = CONVERT_TO_SM_TYPE(Two);
    relTableEntry_cdesc[3].fixedColNo = 3;
    relTableEntry_cdesc[3].varColNo = NIL;
    relTableEntry_cdesc[3].offset = fixedStart;
    relTableEntry_cdesc[3].length = sizeof(Two);
    relTableEntry_cdesc[3].auxInfo = LOGICAL_PTR(NULL);
    fixedStart += sizeof(Two);

    /* NINDEXES */
    relTableEntry_cdesc[4].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[4].type = CONVERT_TO_SM_TYPE(Two);
    relTableEntry_cdesc[4].fixedColNo = 4;
    relTableEntry_cdesc[4].varColNo = NIL;
    relTableEntry_cdesc[4].offset = fixedStart;
    relTableEntry_cdesc[4].length = sizeof(Two);
    relTableEntry_cdesc[4].auxInfo = LOGICAL_PTR(NULL);

    /* RELNAME */
    relTableEntry_cdesc[5].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[5].type = SM_VARSTRING;
    relTableEntry_cdesc[5].fixedColNo = NIL;
    relTableEntry_cdesc[5].varColNo = 0;
    relTableEntry_cdesc[5].length = MAXRELNAME;
    relTableEntry_cdesc[5].auxInfo = LOGICAL_PTR(NULL);

    /*
    ** LRDS_SYSCOLUMNS
    */
    /* fixedStart means starting offset of the first fixed-length column. */
    /* LRDS_SYSCOLUMNS have no variable-length column and have 3 columns. */
    fixedStart = 0;

    e = Util_getArrayFromHeap(handle, &LRDS_COLUMNTABLEHEAP, LRDS_SYSCOLUMNS_NUM_COLS,
			      &relTableEntry_cdesc);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);
    LRDS_RELTABLE[sysOrn[1]].cdesc = LOGICAL_PTR(relTableEntry_cdesc);

    /* DATAFILEID */
    relTableEntry_cdesc[0].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[0].type = SM_FILEID;
    relTableEntry_cdesc[0].fixedColNo = 0;
    relTableEntry_cdesc[0].varColNo = NIL;
    relTableEntry_cdesc[0].offset = fixedStart;
    relTableEntry_cdesc[0].length = SM_FILEID_SIZE;
    relTableEntry_cdesc[0].auxInfo = LOGICAL_PTR(NULL);
    fixedStart += sizeof(FileID);

    /* COLUMNNO */
    relTableEntry_cdesc[1].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[1].type = CONVERT_TO_SM_TYPE(Two);
    relTableEntry_cdesc[1].fixedColNo = 1;
    relTableEntry_cdesc[1].varColNo = NIL;
    relTableEntry_cdesc[1].offset = fixedStart;
    relTableEntry_cdesc[1].length = sizeof(Two);
    relTableEntry_cdesc[1].auxInfo = LOGICAL_PTR(NULL);
    fixedStart += sizeof(Two);

    /* COLINFO */
    relTableEntry_cdesc[2].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[2].type = SM_STRING;
    relTableEntry_cdesc[2].fixedColNo = 2;
    relTableEntry_cdesc[2].varColNo = NIL;
    relTableEntry_cdesc[2].offset = fixedStart;
    relTableEntry_cdesc[2].length = sizeof(ColInfo);
    relTableEntry_cdesc[2].auxInfo = LOGICAL_PTR(NULL);

    /* ordered set */
    /* AUXCOLINFO */
    relTableEntry_cdesc[3].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[3].type = SM_VARSTRING;
    relTableEntry_cdesc[3].fixedColNo = NIL;
    relTableEntry_cdesc[3].varColNo = 0;
    relTableEntry_cdesc[3].length = sizeof(AuxColInfo_T);
    relTableEntry_cdesc[3].auxInfo = LOGICAL_PTR(NULL);

    /*
    ** LRDS_SYSINDEXES
    */
    /* fixedStart means starting offset of the first fixed-length column. */
    /* LRDS_SYSINDEXES have no variable-length column and have 3 columns. */
    fixedStart = 0;

    e = Util_getArrayFromHeap(handle, &LRDS_COLUMNTABLEHEAP, LRDS_SYSINDEXES_NUM_COLS,
			      &relTableEntry_cdesc);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);
    LRDS_RELTABLE[sysOrn[2]].cdesc = LOGICAL_PTR(relTableEntry_cdesc);

    /* DATAFILEID */
    relTableEntry_cdesc[0].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[0].type = SM_FILEID;
    relTableEntry_cdesc[0].fixedColNo = 0;
    relTableEntry_cdesc[0].varColNo = NIL;
    relTableEntry_cdesc[0].offset = fixedStart;
    relTableEntry_cdesc[0].length = SM_FILEID_SIZE;
    relTableEntry_cdesc[0].auxInfo = LOGICAL_PTR(NULL);
    fixedStart += sizeof(FileID);

    /* INDEXID */
    relTableEntry_cdesc[1].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[1].type = SM_INDEXID;
    relTableEntry_cdesc[1].fixedColNo = 1;
    relTableEntry_cdesc[1].varColNo = NIL;
    relTableEntry_cdesc[1].offset = fixedStart;
    relTableEntry_cdesc[1].length = SM_INDEXID_SIZE;
    relTableEntry_cdesc[1].auxInfo = LOGICAL_PTR(NULL);
    fixedStart += sizeof(IndexID);

    /* LRDS_IndexDesc */
    relTableEntry_cdesc[2].complexType = SM_COMPLEXTYPE_BASIC;
    relTableEntry_cdesc[2].type = SM_STRING;
    relTableEntry_cdesc[2].fixedColNo = 2;
    relTableEntry_cdesc[2].varColNo = NIL;
    relTableEntry_cdesc[2].offset = fixedStart;
    relTableEntry_cdesc[2].length = sizeof(LRDS_IndexDesc);
    relTableEntry_cdesc[2].auxInfo = LOGICAL_PTR(NULL);

    /*
    ** Fill IndexInfo Table for the catalog tables manually.
    */

    /*
    ** LRDS_SYSTABLES
    */
    e = Util_getArrayFromHeap(handle, &LRDS_INDEXTABLEHEAP, 1, &relTableEntry_ii);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);
    LRDS_RELTABLE[sysOrn[0]].ii = LOGICAL_PTR(relTableEntry_ii);

    relTableEntry_ii[0].indexType = SM_INDEXTYPE_BTREE;
    relTableEntry_ii[0].iid = sysTablesRelNameIndex;
    relTableEntry_ii[0].kdesc.btree.flag = KEYFLAG_CLUSTERING|KEYFLAG_UNIQUE;
    relTableEntry_ii[0].kdesc.btree.nparts = 1;
    relTableEntry_ii[0].kdesc.btree.kpart[0].type = SM_VARSTRING;
    relTableEntry_ii[0].kdesc.btree.kpart[0].length = MAXRELNAME;
    relTableEntry_ii[0].colNo[0] = LRDS_SYSTABLES_RELNAME_COLNO;

    /*
    ** LRDS_SYSCOLUMNS
    */
    e = Util_getArrayFromHeap(handle, &LRDS_INDEXTABLEHEAP, 1, &relTableEntry_ii);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);
    LRDS_RELTABLE[sysOrn[1]].ii = LOGICAL_PTR(relTableEntry_ii);

    relTableEntry_ii[0].indexType = SM_INDEXTYPE_BTREE;
    relTableEntry_ii[0].iid = sysColumnsDataFileIdIndex;
    relTableEntry_ii[0].kdesc.btree.flag = KEYFLAG_CLUSTERING;
    relTableEntry_ii[0].kdesc.btree.nparts = 1;
    relTableEntry_ii[0].kdesc.btree.kpart[0].type = SM_FILEID;
    relTableEntry_ii[0].kdesc.btree.kpart[0].length = SM_FILEID_SIZE;
    relTableEntry_ii[0].colNo[0] = LRDS_SYSCOLUMNS_DATAFILEID_COLNO;

    /*
    ** LRDS_SYSINDEXES
    */
    e = Util_getArrayFromHeap(handle, &LRDS_INDEXTABLEHEAP, 1, &relTableEntry_ii);
    if (e < eNOERROR) ERRL1(handle, e, &LRDS_LATCH_OPENRELATION);
    LRDS_RELTABLE[sysOrn[2]].ii = LOGICAL_PTR(relTableEntry_ii);

    relTableEntry_ii[0].indexType = SM_INDEXTYPE_BTREE;
    relTableEntry_ii[0].iid = sysIndexesDataFileIdIndex;
    relTableEntry_ii[0].kdesc.btree.flag = KEYFLAG_CLUSTERING;
    relTableEntry_ii[0].kdesc.btree.nparts = 1;
    relTableEntry_ii[0].kdesc.btree.kpart[0].type = SM_FILEID;
    relTableEntry_ii[0].kdesc.btree.kpart[0].length = SM_FILEID_SIZE;
    relTableEntry_ii[0].colNo[0] = LRDS_SYSINDEXES_DATAFILEID_COLNO;

    /* Set isCatalog Flag in LRDS Open Relation Table. */
    LRDS_RELTABLE[sysOrn[0]].isCatalog = TRUE;
    LRDS_RELTABLE[sysOrn[1]].isCatalog = TRUE;
    LRDS_RELTABLE[sysOrn[2]].isCatalog = TRUE;

    /* Set clusteringIndex in LRDS Open Relation Table. */
    LRDS_RELTABLE[sysOrn[0]].clusteringIndex = 0;
    LRDS_RELTABLE[sysOrn[1]].clusteringIndex = 0;
    LRDS_RELTABLE[sysOrn[2]].clusteringIndex = 0;

    /* Mark the entries in LRDS Open Relation Table. */
    LRDS_RELTABLE[sysOrn[0]].count = 1;
    LRDS_RELTABLE[sysOrn[1]].count = 1;
    LRDS_RELTABLE[sysOrn[2]].count = 1;


    /* Fill the LRDS mount table. */
    LRDS_MOUNTTABLE[v].volId = volNo;
    LRDS_MOUNTTABLE[v].catalogTableOrn[LRDS_SYSTABLES] = sysOrn[0];
    LRDS_MOUNTTABLE[v].catalogTableOrn[LRDS_SYSCOLUMNS] = sysOrn[1];
    LRDS_MOUNTTABLE[v].catalogTableOrn[LRDS_SYSINDEXES] = sysOrn[2];
    LRDS_MOUNTTABLE[v].nMount = 1;

    for (j = 0; j < LRDS_NUMOFCATALOGTABLES; j++) {
	LRDS_RELTABLE[LRDS_MOUNTTABLE[v].catalogTableOrn[j]].count ++;

	LRDS_USEROPENRELTABLE(handle)[LRDS_USERMOUNTTABLE(handle)[user_v].catalogTableOrn[j]].sysOrn = LRDS_MOUNTTABLE[v].catalogTableOrn[j];
	LRDS_USEROPENRELTABLE(handle)[LRDS_USERMOUNTTABLE(handle)[user_v].catalogTableOrn[j]].count = 1;
	LRDS_USEROPENRELTABLE(handle)[LRDS_USERMOUNTTABLE(handle)[user_v].catalogTableOrn[j]].tmpRelationFlag = FALSE;
    }
    LRDS_USERMOUNTTABLE(handle)[user_v].volId = volNo;
    LRDS_USERMOUNTTABLE(handle)[user_v].nMount = 1;

    ERROR_PASS(handle, SHM_releaseLatch(handle, &LRDS_LATCH_OPENRELATION, procIndex));

    /* Set the 'volId' */
    *volId = volNo;

    return(eNOERROR);

} /* LRDS_Mount( ) */
