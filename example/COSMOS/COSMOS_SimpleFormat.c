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
 * Module: COSMOS_SimpleFormat.c
 *
 * Description:
 *  Format a file or a raw device so that it can be used as the volume of our
 *  storage system.
 *
 * Exports:
 */


#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <stdio.h>
#include "cosmos_r.h"
#include "COSMOS_common.h"

/*
 * Argument List
 *  0 : volume title
 *  1 : volume ID
 *  2 : device information(device path, device size)
 */
#define NUM_ARGS 3


/*@
 * Internal Function Prototypes
 */
void printUsage(char*);


int main(int argc, char *argv[])
{
    Four    e;                                    /* error code */
    Four    i;                                    /* index variable */
    Boolean argFlag[NUM_ARGS];                    /* flag which indicates each argument is set */
    Boolean err = SM_FALSE;                       /* TRUE if there is any error. */
    char    *title;                               /* volume title */
    Four    volId;                                /* volume identifier */
    Two     extSize;                              /* size of an extent */
    Four    segmentSize;                          /* size of a segment */
    Four    numDevices = 0;                       /* # of devices which consists formated volume */
    char    *devNames[MAX_DEVICES_IN_VOLUME];        /* device name */
    Four    numPagesInDevices[MAX_DEVICES_IN_VOLUME];/* # of pages in the each devices */
    Four    totalNumPages = 0;                    /* total # of pages in formated volume */
	Four    handle;                               /* system handle */

    
    extSize = 16;
    segmentSize = 16;

    /*
     * If there is no arguments
     */
    if(argc < 2) {
        printUsage(argv[0]);
        exit(1);
    }


    /*
     * Get argument
     */

    /* initialize 'argFlag' */
    for (i = 0; i < NUM_ARGS; i++) argFlag[i] = SM_FALSE;

    /* for each argument */
    for (i = 1 ; i < argc; i++) {

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

            argFlag[0] = SM_TRUE;
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

            argFlag[1] = SM_TRUE;
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

            argFlag[2] = SM_TRUE;

            numDevices++;
            if (numDevices > MAX_DEVICES_IN_VOLUME) {
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
        err = SM_TRUE;
    }

    for (i = 0; i < NUM_ARGS; i++) {
        if (!argFlag[i]) {
            printUsage(argv[0]);
            err = SM_TRUE;
        }
    }
    
    if (volId < 0) {
        fprintf(stderr, "Legal volume ID range: 0 ~ %ld\n", 32767);
        err = SM_TRUE;
    }


    for (i = 0; i < numDevices; i++) {
        totalNumPages += numPagesInDevices[i];
        if (numPagesInDevices[i] < extSize) {
            fprintf(stderr, "Valid number of pages range: %ld ~ %ld\n", extSize, LONG_MAX);
            err = SM_TRUE;
        }
    }

    if (err) exit(1);


    /* 
     *  Initialize the storage system 
     */
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

    
    /* 
     *  Format volume
     */
    e = LRDS_FormatDataVolume(handle, numDevices, devNames, title, volId, extSize, numPagesInDevices, segmentSize);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, LRDS_Err(handle,e));
        LRDS_FreeHandle(handle);
        LRDS_Final();
        exit(1);
    }
    

    /* 
     *  Finalize the storage system. 
     */
    /* Free handle */
    e = LRDS_FreeHandle(handle);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        LRDS_Final();
        exit(1);
    }

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
    fprintf(stderr, "Usage: %s [-volumeTitle <volume title>] [-volumeID <volume ID>] [-device <device path> <number of page>]+\n", cmdName);
} /* printUsage() */
