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
 * Module: LRDS_ViewCatalog.c
 *
 * Description:
 *  Print the rows of the catalog tables of the LRDS.
 *
 * Usage:
 *  LRDS_ViewCatalog volume_name
 *
 */


#include <stdio.h>
#include <stdlib.h>

#include "common.h"
#include "error.h"
#include "SM_Internal.h"	
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

#define MAXDEVICESINVOLUME 20
#define ERROR(e) {printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__); SM_AbortTransaction(handle, &xactId); SM_Final(handle); exit(1);}

/* The procIndex variable is defined twice */
/* Four procIndex; */


/* Internal Function Prototypes */
static char *complexTypeName(Two);
static char *typeName(Two);


int main(int argc, char *argv[])
{
    Four e;			/* error code */
    Four i;			/* index variable */
    Four orn;			/* Open Relation Number */
    Four count;			/* number of tuples */
    Four scanId;		/* scan identifier */
    Four volId;			/* volume identifier */
    TupleID tid;		/* a tuple identifier */
    ColInfo cinfo;		/* column info */
    LRDS_IndexDesc idesc;	/* Index Descriptor */
    char   relName[MAXRELNAME];	/* relation name */
    ColListStruct clist[10];	/* column list */
    BoundCond startBound, stopBound; /* boundary condition of range scan */
    XactID xactId;		/* my transaction identifier */
    AuxColInfo_T auxColInfo;
    char *devNames[MAXDEVICESINVOLUME]; /* relation name */
    Four handle;		/* handle for each thread */


    if (argc < 2) {
	printf("%s [device path]+\n", argv[0]);
	exit(1);
    }
    

    /* get device names */
    for (i = 1; i < argc; i++) devNames[i-1] = argv[i];


    /* Initialzie the LRDS. */
    handle = 0;			
    e = SM_Init(handle);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	exit(1);
    }
    

    /* Mount the given volume. */
    e = LRDS_Mount(handle, argc-1, devNames, &volId);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	SM_Final(handle);
	exit(1);
    }
 
    e = SM_BeginTransaction(handle, &xactId, X_RR_RR); 
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	SM_Final(handle);
	exit(1);
    }

    /*
    ** LRDS_SYSTABLES
    */
    orn = LRDS_OpenRelation(handle, volId, LRDS_SYSTABLES_RELNAME);
    if ( orn < 0 ) ERROR(orn);
    
    scanId = LRDS_OpenSeqScan(handle, orn, FORWARD, 0, NULL, NULL);
    if (scanId < 0) ERROR(scanId);

    for (i = 0; i < LRDS_SYSTABLES_NUM_COLS; i++) {
	clist[i].colNo = i;
	clist[i].start = ALL_VALUE;
	clist[i].dataLength = MAXRELNAME; /* assume the maximum length */
    }
    clist[5].data.ptr = &relName[0];

    /* print header */
    printf("##################\n");
    printf("# LRDS_SYSTABLES #\n");
    printf("##################\n\n");
    printf("  NO  DATAFILEID     NTUPLES  MAXTUPLEN  NC   NI   RELATIONNAME\n");
    printf("------------------------------------------------------------------------\n");
    
    count = 0;
    while ((e = LRDS_NextTuple(handle, scanId, &tid, NULL)) != EOS) {
	if (e < 0) ERROR(e);

	e = LRDS_FetchTuple(handle, scanId, TRUE, NULL, LRDS_SYSTABLES_NUM_COLS, &clist[0]);
	if (e < 0) ERROR(e);

        printf("[%3ld]", ++count);
	
	/* DATAFILEID */
        printf(" (%3ld,%4ld) |", clist[0].data.pid.volNo, clist[0].data.pid.pageNo);

	/* NTUPLES */
        printf(" %7ld |", GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(clist[1], Four));

	/* MAXTUPLELEN */
        printf(" %7ld |", GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(clist[2], Four));
	
	/* NCOLUMNS */
        printf(" %2ld |", GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(clist[3], Two));
	
	/* NINDEXES */
        printf(" %2ld |", GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(clist[4], Two));

	/* RELNAME */
	printf(" ");
	for (i = 0; i < clist[5].retLength; i++) printf("%c", relName[i]);

        printf(" [TID:(%3ld,%4ld,%3ld,%8ld)]", tid.volNo, tid.pageNo, tid.slotNo, tid.unique);
	
	printf("\n");
    }
    
    e = LRDS_CloseScan(handle, scanId);
    if (e < 0) ERROR(e);


    startBound.op = SM_BOF;
    stopBound.op = SM_EOF;
    scanId = LRDS_OpenIndexScan(handle, orn, NULL, &startBound, &stopBound, 0, NULL, NULL);
    if (scanId < 0) ERROR(scanId);

    printf("<TupleIDs via B+ tree index>\n");
    while ((e = LRDS_NextTuple(handle, scanId, &tid, NULL)) != EOS) {
	if (e < 0) ERROR(e);

        printf("(%3ld, %4ld, %3ld, %8ld)\n", tid.volNo, tid.pageNo, tid.slotNo, tid.unique);
    }
    
    e = LRDS_CloseScan(handle, scanId);
    if (e < 0) ERROR(e);
    e = LRDS_CloseRelation(handle, orn);
    if (e < 0) ERROR(e);

    
    /*
    ** LRDS_SYSCOLUMNS
    */
    orn = LRDS_OpenRelation(handle, volId, LRDS_SYSCOLUMNS_RELNAME);
    if (orn < 0) ERROR(orn);

    scanId = LRDS_OpenSeqScan(handle, orn, FORWARD, 0, NULL, NULL);
    if (scanId < 0) ERROR(scanId);

    for (i = 0; i < LRDS_SYSCOLUMNS_NUM_COLS; i++) {
	clist[i].colNo = i;
	clist[i].start = ALL_VALUE;
    }
    clist[0].dataLength = SM_FILEID_SIZE;
    clist[1].dataLength = sizeof(Two);
    
    clist[2].dataLength = sizeof(ColInfo); /* assume maximum length */
    clist[2].data.ptr = &cinfo;
    
    clist[3].dataLength = sizeof(AuxColInfo_T); /* assume maximum length */
    clist[3].data.ptr = &auxColInfo;

    printf("\n###################\n");
    printf("# LRDS_SYSCOLUMNS #\n");
    printf("###################\n\n");
    printf("  NO  DATAFILEID    COLNO  COLINFO              AUXCOLINFO\n");
    printf("-----------------------------------------------------------------\n");
    
    count = 0;
    while ((e = LRDS_NextTuple(handle, scanId, &tid, NULL)) != EOS) {
	if (e < 0) ERROR(e);

	e = LRDS_FetchTuple(handle, scanId, TRUE, NULL, LRDS_SYSCOLUMNS_NUM_COLS, &clist[0]);
	if (e < 0) ERROR(e);

        printf("[%3ld]", ++count);
	
	/* DATAFILEID */
        printf(" (%3ld,%4ld) |", clist[0].data.pid.volNo, clist[0].data.pid.pageNo);

	/* COLUMNNO */
        printf(" %2ld |", GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(clist[1], Two));

	/* COLINFO */
        printf(" (%s,%s,%ld)", complexTypeName(cinfo.complexType), typeName(cinfo.type), cinfo.length);

        /* AUXCOLINFO */
        if (clist[3].nullFlag) {
            printf(" | NULL ");
        } else {
            printf(" | %P, (%0p,%2ld[", auxColInfo.orderedSet.nestedIndexFlag, auxColInfo.orderedSet.kdesc.flag, auxColInfo.orderedSet.kdesc.nparts);
            for (i = 0; i < auxColInfo.orderedSet.kdesc.nparts; i++)
                printf("%s%c", typeName(auxColInfo.orderedSet.kdesc.kpart[i].type & SM_TYPE_MASK), (i == auxColInfo.orderedSet.kdesc.nparts-1) ? ']':',');
            printf(") )");
        }
        
        printf(" [TID:(%3ld,%4ld,%3ld,%8ld)]", tid.volNo, tid.pageNo, tid.slotNo, tid.unique);

	printf("\n");
    }

    e = LRDS_CloseScan(handle, scanId);
    if (e < 0) ERROR(e);

    startBound.op = SM_BOF;
    stopBound.op = SM_EOF;
    scanId = LRDS_OpenIndexScan(handle, orn, NULL, &startBound, &stopBound, 0, NULL, NULL);
    if (scanId < 0) ERROR(scanId);

    printf("<TupleIDs via B+ tree index>\n");
    while ((e = LRDS_NextTuple(handle, scanId, &tid, NULL)) != EOS) {
	if (e < 0) ERROR(e);

        printf("(%3ld, %4ld, %3ld, %8ld)\n", tid.volNo, tid.pageNo, tid.slotNo, tid.unique);
    }
    
    e = LRDS_CloseScan(handle, scanId);
    if (e < 0) ERROR(e);

    e = LRDS_CloseRelation(handle, orn);
    if (e < 0) ERROR(e);
    
    /*
    ** LRDS_SYSINDEXES
    */
    orn = LRDS_OpenRelation(handle, volId, LRDS_SYSINDEXES_RELNAME);
    if (orn < 0) ERROR(orn);
    
    scanId = LRDS_OpenSeqScan(handle, orn, FORWARD, 0, NULL, NULL);
    if (scanId < 0) ERROR(scanId);

    for (i = 0; i < LRDS_SYSINDEXES_NUM_COLS; i++) {
	clist[i].colNo = i;
	clist[i].start = ALL_VALUE;
	clist[i].dataLength = sizeof(LRDS_IndexDesc); /* assume maximum length */
    }
    clist[2].data.ptr = &idesc;

    printf("\n###################\n");
    printf("# LRDS_SYSINDEXES #\n");
    printf("###################\n\n");
    printf("  NO  DATAFILEID       INDEXID     INDEXDESC\n");
    printf("------------------------------------------------------------------------\n");
    
    count = 0;
    while ((e = LRDS_NextTuple(handle, scanId, &tid, NULL)) != EOS) {
	if (e < 0) ERROR(e);

	e = LRDS_FetchTuple(handle, scanId, TRUE, NULL, LRDS_SYSINDEXES_NUM_COLS, &clist[0]);
	if (e < 0) ERROR(e);

        printf("[%3ld]", ++count);
	
	/* DATAFILEID */
        printf(" (%3ld,%4ld) |", clist[0].data.pid.volNo, clist[0].data.pid.pageNo);

	/* INDEXID */
        printf(" (%3ld,%4ld) |", clist[1].data.pid.volNo, clist[1].data.pid.pageNo);

	/* INDEXDESC */
	switch( idesc.indexType ) {
	  case SM_INDEXTYPE_BTREE:
	    printf(" (BTREEINDEX ");
            printf("(%0p,%2ld[", idesc.kinfo.btree.flag, idesc.kinfo.btree.nColumns);
	    for (i = 0; i < idesc.kinfo.btree.nColumns; i++)
                printf("%ld%c", idesc.kinfo.btree.columns[i].colNo, (i == idesc.kinfo.btree.nColumns-1) ? ']':','); 
	    printf(") )");
	    break;
	    
	  case SM_INDEXTYPE_MLGF:
	    printf(" (MLGFINDEX ");
            printf("(%0p,%2ld[", idesc.kinfo.mlgf.flag, idesc.kinfo.mlgf.nColumns);
	    for (i = 0; i < idesc.kinfo.mlgf.nColumns; i++)
                printf("%ld%c", idesc.kinfo.mlgf.colNo[i], (i == idesc.kinfo.mlgf.nColumns-1) ? ']':',');
            printf(",%ld) )", idesc.kinfo.mlgf.extraDataLen);
	    break;
	    
	  default:
	    printf(" (NOT KNOWN)");
	}

        printf(" [TID:(%3ld,%4ld,%3ld,%8ld)]", tid.volNo, tid.pageNo, tid.slotNo, tid.unique);

	printf("\n");
    }

    e = LRDS_CloseScan(handle, scanId);
    if (e < 0) ERROR(e);

    startBound.op = SM_BOF;
    stopBound.op = SM_EOF;
    scanId = LRDS_OpenIndexScan(handle, orn, NULL, &startBound, &stopBound, 0, NULL, NULL);
    if (scanId < 0) ERROR(scanId);

    printf("<TupleIDs via B+ tree index>\n");
    while ((e = LRDS_NextTuple(handle, scanId, &tid, NULL)) != EOS) {
	if (e < 0) ERROR(e);

        printf("(%3ld, %4ld, %3ld, %8ld)\n", tid.volNo, tid.pageNo, tid.slotNo, tid.unique);
    }
    
    e = LRDS_CloseScan(handle, scanId);
    if (e < 0) ERROR(e);
    
    e = LRDS_CloseRelation(handle, orn);
    if (e < 0) ERROR(e);

    e = SM_CommitTransaction(handle, &xactId);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	SM_Final(handle);
	exit(1);
    }

    /* Dismount the volume. */
    e = LRDS_Dismount(handle, volId);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	SM_Final(handle);
	exit(1);
    }
    
    /* Finalize the LRDS. */
    SM_Final(handle);

    return(0);

} /* main() */


/*
 * Function: static char *complexTypeName(Two)
 *
 * Description:
 *  Return type name corresponding to the given complex type number
 *
 * Returns:
 *  complex type name
 */
static char *complexTypeName(Two type)
{
    switch(type) {
      case SM_COMPLEXTYPE_BASIC:
	return("BASIC");
	
      case SM_COMPLEXTYPE_SET:
	return("SET");
	
      case SM_COMPLEXTYPE_ORDEREDSET:
	return("ORDEREDSET");
        
      case SM_COMPLEXTYPE_COLLECTIONSET:
	return("COLLECTIONSET");

      case SM_COMPLEXTYPE_COLLECTIONBAG:
	return("COLLECTIONBAG");

      case SM_COMPLEXTYPE_COLLECTIONLIST:
	return("COLLECTIONLIST");
    }

    return("NOT_KNOWN");
    
} /* complexTypeName() */



/*
 * Function: static char *typeName(Two)
 *
 * Description:
 *  Return type name corresponding to the given type number
 *
 * Returns:
 *  type name
 */
static char *typeName(Two type)
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
	
      case SM_INDEXID:
	return("SM_INDEXID");

      case SM_OID:
	return("SM_OID");
	
      case SM_TEXT:
	return("SM_TEXT");
	
      case SM_MBR:
	return("SM_MBR");
    }

    return("NOT_KNOWN");
    
} /* typeName() */
