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
 * Module: COSMOS_CreateRelation.c
 *
 * Description:
 *   Create a relation in a volume.
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
	ColInfo cinfo[MAX_N_COLUMNS];                 /* column information */

	char    columnTableName[MAX_RELATION_NAME+COLUMN_TABLE_PREFIX_LENGTH];/* column table name */
	ColInfo columnTableCinfo[COLUMN_TABLE_N_COLUMNS];/* column information of column table*/
	ColListStruct columnTableClist[COLUMN_TABLE_N_COLUMNS];/* column value of newly inserted tuple */ 
	Four    columnTableOrn;                       /* open relation number of columnTable */

	Four    columnType;                           /* column type */
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
	 * Get information of relation 
	 */
	/* Get the name of new relation */
	printf("Please type the relation name: ");
	scanf("%s", relationName);
	printf("\n");


	/* Get the number of columns */
    nColumns = 0;
	while(nColumns == 0){
        printf("Please type the number of columns: ");
        scanf("%d", &nColumns);
        printf("\n");

        if(nColumns < 1 || nColumns > MAX_N_COLUMNS){
            nColumns = 0;
            printf("the number of columns should be larger than 0 and less than %d\n\n", MAX_N_COLUMNS+1);
        }
    }

	/* Get the type of each column */
	for(i = 0; i < nColumns ; i++){
	    columnType = 0;
		while(columnType == 0){
            printf("Please type the type of %dth column (1:integer, 2:float, 3:string(%d)): ", i+1, MAX_STRING);
            scanf("%d", &columnType);

            cinfo[i].complexType = SM_COMPLEXTYPE_BASIC;
			switch ( columnType ){
			case 1:
				cinfo[i].type = SM_INT;
				cinfo[i].length = sizeof(Four);
			break;

			case 2:
				cinfo[i].type = SM_FLOAT;
				cinfo[i].length = sizeof(float);
			break;

			case 3:
				cinfo[i].type = SM_VARSTRING;
				cinfo[i].length = MAX_STRING;
			break;

			default:
                columnType = 0;
                printf("the number of columns should be larger than 0 and less than 4\n\n");
			}
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
	 * Create new relation
	 */
    e = LRDS_CreateRelation(handle, volId, relationName, NULL, nColumns, cinfo, SM_FALSE);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
		LRDS_AbortTransaction(handle, &xactId);
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

	/*
	 * Create column table of the new relation
	 * Column table store column information of the new relation
	 */

	/* make column table name */
	strcpy(columnTableName, COLUMN_TABLE_PREFIX);
	strcat(columnTableName, relationName);

    /* column table has two columns 
	 * 1st column : column number
	 * 2nd column : column type
	 */
	columnTableCinfo[0].complexType = SM_COMPLEXTYPE_BASIC;
	columnTableCinfo[0].type = SM_INT;
	columnTableCinfo[0].length = sizeof(Four);

	columnTableCinfo[1].complexType = SM_COMPLEXTYPE_BASIC;
	columnTableCinfo[1].type = SM_INT;
	columnTableCinfo[1].length = sizeof(Four);

    /* create column table */
    e = LRDS_CreateRelation(handle, volId, columnTableName, NULL, COLUMN_TABLE_N_COLUMNS, 
	                        columnTableCinfo, SM_FALSE);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
		LRDS_AbortTransaction(handle, &xactId);
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

	/* open column table */
	columnTableOrn = LRDS_OpenRelation(handle, volId, columnTableName);
    if (columnTableOrn < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,columnTableOrn));
		LRDS_AbortTransaction(handle, &xactId);
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }

    /* insert each column information into column table */
	for(i = 0 ; i < nColumns ; i++){

	    columnTableClist[0].colNo = 0;
		columnTableClist[0].nullFlag = SM_FALSE;
		columnTableClist[0].start = ALL_VALUE;
		columnTableClist[0].dataLength = sizeof(Four);
		columnTableClist[0].data.i = i;

	    columnTableClist[1].colNo = 1;
		columnTableClist[1].nullFlag = SM_FALSE;
		columnTableClist[1].start = ALL_VALUE;
		columnTableClist[1].dataLength = sizeof(Four);
		columnTableClist[1].data.i = cinfo[i].type;

		e = LRDS_CreateTuple(handle, columnTableOrn, SM_FALSE, COLUMN_TABLE_N_COLUMNS, 
		                     &(columnTableClist[0]), NULL);
        if (e < eNOERROR) {
            printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
        	LRDS_AbortTransaction(handle, &xactId);
            LRDS_FreeHandle(handle);
            LRDS_Final();
            exit(1);
        }
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
