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
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

#define MAXDEVICESINVOLUME 20
#define ERROR(e) {printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__); LRDS_AbortTransaction(handle, &xactId); LRDS_Final(handle); exit(1);}
#define ERROR2(e) {printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__); return(e);}

/* The procIndex variable is defined twice */
/* Four procIndex; */


/* Internal Function Prototypes */
Four getNumLargeObject(Four, XactID, Four, char*, Four*, Four*); 


int main(int argc, char *argv[])
{
    Four e;			/* error code */
    Four i;			/* index variable */
    Four orn;			/* Open Relation Number */
    Four count;			/* number of large object */
    Four count2;		/* number of large object which extreme one extent */
    Four totalCount = 0;	/* number of large object */
    Four totalCount2 = 0;	/* number of large object which extreme one extent */
    Four scanId;		/* scan identifier */
    TupleID tid;		/* a tuple identifier */
    Four volId;			/* volume identifier */
    XactID xactId;		/* my transaction identifier */
    char   relName[MAXRELNAME]; /* relation name */
    ColListStruct clist[10];    /* column list */
    char *devNames[MAXDEVICESINVOLUME]; /* relation name */
    Four handle;		/* handle for each thread */


    if (argc < 2) {
	printf("%s [device path]+ [relation name]\n", argv[0]);
	exit(1);
    }
    

    /* get device names */
    for (i = 1; i < argc; i++) devNames[i-1] = argv[i];


    /* Initialzie the LRDS. */
    handle = 0;			
    e = LRDS_Init(handle);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	exit(1);
    }
    

    /* Mount the given volume. */
    e = LRDS_Mount(handle, argc-1, devNames, &volId);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	LRDS_Final(handle);
	exit(1);
    }
 
    e = LRDS_BeginTransaction(handle, &xactId, X_RR_RR); 
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	LRDS_Final(handle);
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

    while ((e = LRDS_NextTuple(handle, scanId, &tid, NULL)) != EOS) {
        if (e < 0) ERROR(e);

        e = LRDS_FetchTuple(handle, scanId, TRUE, NULL, LRDS_SYSTABLES_NUM_COLS, &clist[0]);
        if (e < 0) ERROR(e);

        /* insert NULL into last of relName */
        relName[clist[5].retLength] = '\0';

        e = getNumLargeObject(handle, xactId, volId, relName, &count, &count2);
        if (e < 0) ERROR(e);

        printf("%s \t: \t%5ld(%5ld)\n", relName, count, count2);

        totalCount += count;
        totalCount2 += count2;
    }
    printf("\ntotal # of large object = %ld(%ld)\n", totalCount, totalCount2);

    e = LRDS_CloseScan(handle, scanId);
    if (e < 0) ERROR(e);

    e = LRDS_CloseRelation(handle, orn);
    if (e < 0) ERROR(e); 


    e = LRDS_CommitTransaction(handle, &xactId);
    if (e < 0) ERROR(e);

    /* Dismount the volume. */
    e = LRDS_Dismount(handle, volId);
    if (e < 0) ERROR(e);
    
    /* Finalize the LRDS. */
    LRDS_Final(handle);


    return(0);

} /* main() */


Four getNumLargeObject(
    Four handle,
    XactID xactId,		/* my transaction identifier */
    Four   volId,		/* volume identifier */
    char*  relName, 
    Four*  count, 
    Four*  count2)
{
    Four e;			/* error code */
    Four orn;			/* Open Relation Number */
    Four scanId;		/* scan identifier */
    Four smScanId;		/* scan identifier */
    TupleID tid;		/* a tuple identifier */
    ObjectHdr objHdr;

    orn = LRDS_OpenRelation(handle, volId, relName);
    if ( orn < 0 ) ERROR2(orn);
    
    scanId = LRDS_OpenSeqScan(handle, orn, FORWARD, 0, NULL, NULL);
    if (scanId < 0) ERROR2(scanId);

    /* get smScanId */
    smScanId = LRDS_SCANTABLE(handle)[scanId].smScanId;

    *count = 0;
    *count2 = 0;
    while ((e = LRDS_NextTuple(handle, scanId, &tid, NULL)) != EOS) {
	if (e < 0) ERROR2(e);

	e = SM_GetObjectHdr(handle, smScanId, &tid, &objHdr, NULL);
	if (e < 0) ERROR2(e);

        printf("(%ld, %ld)\n", tid.volNo, tid.pageNo);
	if (objHdr.properties & P_LRGOBJ) (*count)++;
	if (objHdr.length > 16*PAGESIZE) (*count2)++;
    }
    
    e = LRDS_CloseScan(handle, scanId);
    if (e < 0) ERROR2(e);

    e = LRDS_CloseRelation(handle, orn);
    if (e < 0) ERROR2(e);

    return eNOERROR;
}
