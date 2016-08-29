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
 * Module: SM_Format.c
 *
 * Description:
 *  Format a file or a raw device so that it can be used as the volume of our
 *  storage system. This format is for Scan Manager level.
 */


#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <stdio.h>
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "SM_Internal.h"
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

#define MAXDEVICESINVOLUME 20


/*
 * Argument List
 *  0 : volume title
 *  1 : volume ID
 *  2 : extent size
 *  3 : device information(device path, device size)
 */
#define NUM_ARGS 4


/*@
 * Internal Function Prototypes
 */
void printUsage(char* );


int main(int argc, char *argv[])
{
    Four        e;                                    /* error code */
    Four        i;                                    /* index variable */
    Boolean     argFlag[NUM_ARGS];                    /* flag which indicates each argument is set */
    Boolean     err = FALSE;                          /* TRUE if there is any error. */
    char        *title;                               /* volume title */
    Four        volId;                                /* volume identifier */
    Two         extSize;                              /* size of an extent */
    Four        numDevices = 0;                       /* # of devices which consists formated volume */
    char        *devNames[MAXDEVICESINVOLUME];        /* device name */
    Four        numPagesInDevices[MAXDEVICESINVOLUME];/* # of pages in the each devices */
    Four        totalNumPages = 0;                    /* total # of pages in formated volume */
    Boolean     dataVolFlag = TRUE;                   /* TRUE if we are formatting data volume */
    Four	handle;				      /* handle for each thread */

    
    /*
     * If there is no arguments
     */
    if(argc < 2) {
        printUsage(argv[0]);
        exit(1);
    }


    /*
     * check option
     */
    if(strcmp(argv[1], "-l") == 0) dataVolFlag = FALSE;

    /*
     * Get argument
     */

    /* initialize 'argFlag' */
    for (i = 0; i < NUM_ARGS; i++) argFlag[i] = FALSE;

    /* for each argument */
    for (i = dataVolFlag ? 1 : 2 ; i < argc; i++) {

        /* get 'volumeTitle' */
        if (strcmp(argv[i], "-volumeTitle") == 0)
        {
            i++;
            if (!argFlag[0] && i < argc)
                title = argv[i];
            else {
                printUsage(argv[0]);
                exit(1);
            }

            argFlag[0] = TRUE;
        }
        /* get 'volumeID' */
        else if (strcmp(argv[i], "-volumeID") == 0)
        {
            i++;
            if (!argFlag[1] && i < argc)
                volId = atol(argv[i]);
            else {
                printUsage(argv[0]);
                exit(1);
            }

            argFlag[1] = TRUE;
        }
        /* get 'extentSize' */
        else if (strcmp(argv[i], "-extentSize") == 0)
        {
            i++;
            if (!argFlag[2] && i < argc)
                extSize = atol(argv[i]);
            else {
                printUsage(argv[0]);
                exit(1);
            }

            argFlag[2] = TRUE;
        }
        /* get 'devNames[]' & 'numPagesInDevices[]' */
        else if (strcmp(argv[i], "-device") == 0)
        {
            i++;
            if (i < argc)
                devNames[numDevices] = argv[i];
            else {
                printUsage(argv[0]);
                exit(1);
            }

            i++;
            if (i < argc)
                numPagesInDevices[numDevices] = atol(argv[i]);
            else {
                printUsage(argv[0]);
                exit(1);
            }

            argFlag[3] = TRUE;

            numDevices++;
            if (numDevices > MAXDEVICESINVOLUME) {
                printf("Too Many devices in volume!!\n");
                exit(1);
            }
        }
        /* invalid arguments */
        else {
            printUsage(argv[0]);
            exit(1);
        }
    }


    /* 
     *  error check 
     */

    if (numDevices <= 0) {
        printUsage(argv[0]);
	err = TRUE;
    }

    for (i = 0; i < NUM_ARGS; i++) {
        if (!argFlag[i]) {
            printUsage(argv[0]);
	    err = TRUE;
        }
    }
    
    if (volId < 0 || volId > MAX_VOLUME_NUMBER) {
        fprintf(stderr, "Legal volume ID range: 0 ~ %ld\n", MAX_VOLUME_NUMBER);
	err = TRUE;
    }

    if (extSize < 1) {
        fprintf(stderr, "Valid extent size range: 1 ~ %ld\n", LONG_MAX);
	err = TRUE;
    }

    for (i = 0; i < numDevices; i++) {
        totalNumPages += numPagesInDevices[i];
        if (numPagesInDevices[i] < extSize) {
            fprintf(stderr, "Valid number of pages range: %ld ~ %ld\n", extSize, LONG_MAX);
	    err = TRUE;
        }
    }

    if (err) exit(1);


    /* 
     *  Initialize the storage system 
     */
    handle = 0;		
    e = SM_Init(handle);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	exit(1);
    }

    
    /* 
     *  Format volume
     */
    if (dataVolFlag)
        e = SM_FormatDataVolume(handle, numDevices, devNames, title, volId, extSize, numPagesInDevices, 0);
    else
        e = SM_FormatLogVolume(handle, numDevices, devNames, title, volId, extSize, numPagesInDevices);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, SM_Err(handle, e));
        SM_Final(handle);
        exit(1);
    }
    

    /* 
     *  Finalize the storage system. 
     */
    e = SM_Final(handle);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        exit(1);
    }

    return(0);
    
} /* main() */


void printUsage(
    char *cmdName)              /* IN command name */
{
    fprintf(stderr, "Usage: %s [-volumeTitle <volume title>] [-volumeID <volume ID>] [-extentSize <extent size>] [-device <device path> <number of page>]+\n", cmdName);
    fprintf(stderr, "               OR\n");
    fprintf(stderr, "       %s -l [-volumeTitle <volume title>] [-volumeID <volume ID>] [-extentSize <extent size>] [-device <device path> <number of page>]+\n", cmdName);
} /* printUsage() */
