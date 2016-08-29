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
/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System       */
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
 * Module: COSMOS_InsertTuple.c
 *
 * Description:
 *   Insert a tuple into the relation.
 *
 * Exports:
 */


#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <stdio.h>
#include "cosmos_r.h"
#include "COSMOS_common.h"

/*@
 * Internal Function Prototypes
 */
void printUsage(char*);


int main(int argc, char *argv[])
{
    Four    e;                                    /* error code */
    Four    i;                                    /* index variable */
    Four    volId;                                /* volume identifier */
    Four    numDevices = 0;                       /* # of devices which consists formated volume */
    char    *devNames[MAX_DEVICES_IN_VOLUME];     /* device name */
	XactID  xactId;                               /* transaction identifier */
	char    relationName[MAX_RELATION_NAME];      /* relation name */
	Four    nColumns;                             /* the number of columns */
	Four    cType[MAX_N_COLUMNS];                 /* column type */
	ColListStruct clist[MAX_N_COLUMNS];           /* column value of the relation */ 
	Four    orn;                                  /* open relatioin number */

	char    columnTableName[MAX_RELATION_NAME+COLUMN_TABLE_PREFIX_LENGTH];/* column table name */
	ColListStruct columnTableClist[COLUMN_TABLE_N_COLUMNS];/* column value of newly inserted tuple */ 
	Four    columnTableOrn;                       /* open relation number of column table */
	Four    columnTableScanId;                    /* scan identifier fo column table */

	Four    intData;                              /* integer data */
	float   floatData;                            /* float data */
	char    stringData[MAX_N_COLUMNS][MAX_STRING];/* string data */ 
	Four    handle;                               /* system handle */

    
    /*
     * If there is no arguments
     */
    if(argc < 2) {
        printUsage(argv[0]);
        exit(1);
    }

    /*
     * Get arguments
     */

    /* for each argument */
    for (i = 1 ; i < argc; i++) {
        devNames[numDevices] = argv[i];
        numDevices++;
        if (numDevices > MAX_DEVICES_IN_VOLUME) {
             printf("Too Many devices in volume!!\n");
             exit(1);
        }
    }

    /* 
     *  Initialize the storage system 
     */
	/* Initialize COSMOS */
    e = LRDS_Init();
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        exit(1);
    }

    /* Allocate handle */
    e = LRDS_AllocHandle(&handle);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        LRDS_Final();
        exit(1);
    }
    
    /*  Mount volume */
    e = LRDS_Mount(handle, numDevices, devNames, &volId);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

	/* Begin Transaction */
    e = LRDS_BeginTransaction(handle, &xactId, X_RR_RR);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

	/*
	 * Read column information
	 */
	printf("Please type the relation name: ");
	scanf("%s", relationName);
	printf("\n");

	/* make column table name */
	strcpy(columnTableName, COLUMN_TABLE_PREFIX);
	strcat(columnTableName, relationName);

	/* open column table */
	columnTableOrn = LRDS_OpenRelation(handle, volId, columnTableName);
    if (columnTableOrn < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,columnTableOrn));
		LRDS_AbortTransaction(handle, &xactId);
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

	/* open column table scan */
	columnTableScanId = LRDS_OpenSeqScan(handle,columnTableOrn, FORWARD, 0, NULL , NULL);
    if (columnTableScanId < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,columnTableScanId));
		LRDS_AbortTransaction(handle, &xactId);
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

    /* prepare for fetching tuples from  column table */
	nColumns = 0;
	columnTableClist[0].colNo = 0;
	columnTableClist[0].start = ALL_VALUE;

	columnTableClist[1].colNo = 1;
	columnTableClist[1].start = ALL_VALUE;

    /* Fetch tuples from column table */
	while((e = LRDS_NextTuple(handle, columnTableScanId, NULL, NULL)) !=EOS ){
        if (e < eNOERROR) {
            printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
	        LRDS_AbortTransaction(handle, &xactId);
            LRDS_FreeHandle(handle);
            LRDS_Final();
            exit(1);
        }

		e = LRDS_FetchTuple(handle, columnTableScanId, SM_TRUE, NULL, COLUMN_TABLE_N_COLUMNS, 
		                    columnTableClist);
        if (e < eNOERROR) {
            printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
	        LRDS_AbortTransaction(handle, &xactId);
            LRDS_FreeHandle(handle);
            LRDS_Final();
            exit(1);
        }

	    /* column table has two columns 
	     * 1st column : column number
	     * 2nd column : column type 
		 */
		i = columnTableClist[0].data.i;
		cType[i] = columnTableClist[1].data.i;

		if( cType[i]!=SM_INT && cType[i]!=SM_FLOAT && cType[i] !=SM_VARSTRING){
            printf("FILE:%s LINE:%ld :: Internal Error\n", __FILE__, __LINE__);
   	        LRDS_AbortTransaction(handle, &xactId);
            LRDS_FreeHandle(handle);
            LRDS_Final();
            exit(1);
		}

		nColumns ++;

	}

    /* close column table scan */
	e = LRDS_CloseScan(handle, columnTableScanId);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
		LRDS_AbortTransaction(handle, &xactId);
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

    /* close column table */
	e = LRDS_CloseRelation(handle, columnTableOrn);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
		LRDS_AbortTransaction(handle, &xactId);
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

    /* 
	 * Get the value of new tuple 
	 */
	for(i = 0; i < nColumns ; i++){
	    clist[i].colNo = i;
		clist[i].nullFlag = SM_FALSE;
		clist[i].start = ALL_VALUE;


	    switch( cType[i] ){

		case SM_INT:
            printf("Please type the value of %dth column (integer): ", i+1);
            scanf("%d", &intData);

		    clist[i].dataLength = sizeof(Four);
			clist[i].data.i = intData;
		break;

		case SM_FLOAT:
            printf("Please type the value of %dth column (float): ", i+1);
            scanf("%f", &floatData);

		    clist[i].dataLength = sizeof(float);
			clist[i].data.f = floatData;
		break;

		case SM_VARSTRING:
            printf("Please type the value of %dth column (string(%d)): ", i+1, MAX_STRING);
            scanf("%s", &(stringData[i]));
		    clist[i].dataLength = strlen(stringData[i]);
		    clist[i].data.ptr = stringData[i];
		break;

		default:
            printf("FILE:%s LINE:%ld :: Internal Error\n", __FILE__, __LINE__);
		    LRDS_AbortTransaction(handle, &xactId);
            LRDS_FreeHandle(handle);
            LRDS_Final();
            exit(1);
		}
	}

	/*
	 * Insert Tuple
	 */
    /* Open relation */
    orn = LRDS_OpenRelation(handle, volId, relationName);
    if (orn < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
		LRDS_AbortTransaction(handle, &xactId);
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

    /* Insert tuple */
	e = LRDS_CreateTuple(handle, orn, SM_FALSE, nColumns, clist, NULL);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
      	LRDS_AbortTransaction(handle, &xactId);
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
	}


    /* Open relation */
    e = LRDS_CloseRelation(handle, orn);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
		LRDS_AbortTransaction(handle, &xactId);
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

    /* 
     *  Finalize the storage system. 
     */
	/* Commit Transaction */
    e = LRDS_CommitTransaction(handle, &xactId);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

	/* Dismount volume */
    e = LRDS_Dismount(handle, volId);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

    /* Free handle */
    e = LRDS_FreeHandle(handle);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        LRDS_Final();
        exit(1);
    }

    /* Finalize COSMOS */
    e = LRDS_Final();
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        exit(1);
    }


    return(0);
    
} /* main() */


void printUsage(
    char *cmdName)              /* IN command name */
{
    fprintf(stderr, "Usage: %s [<device path>]+\n", cmdName);
} /* printUsage() */
