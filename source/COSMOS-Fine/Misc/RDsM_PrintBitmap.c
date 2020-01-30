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

#include "common.h"
#include "SHM.h"
#include "TM.h"
#include "OM.h"
#include "SM.h"
#include "RDsM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


#define MAXDEVICESINVOLUME 20 
#define ERROR(_e) printf("<FILE:%s LINE:%ld> %ld::%s\n", __FILE__, __LINE__, _e, SM_Err(handle, _e))

#define ERROR_AND_EXIT(_e) \
	BEGIN_MACRO \
	ERROR(_e); \
	(Four) SM_AbortTransaction(handle, &xactId); \
	(Four) SM_Final( ); \
	exit(1); \
	END_MACRO

Four procIndex;


int main(int argc, char *argv[])
{
    Four e;			/* error code */
    Four volId;			/* volume identifier */
    XactID xactId;		/* transaction Id */
    Four i;
    Four extentSize, numPages, numUsedPages;
    char *devNames[MAXDEVICESINVOLUME]; 

    Four handle = 0;

    if (argc < 2) {
	printf("%s [device_name]+\n", argv[0]);
	exit(1);
    }


    /* get device names */
    for (i = 1; i < argc; i++) devNames[i-1] = argv[i];

    /* Initialzie the storage system. */
    e = SM_Init( );
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
        SM_FreeHandle(handle);
        (Four) SM_Final( );
        exit(1);
    }


    e = SM_BeginTransaction(handle, &xactId, X_CS_BROWSE); 
    if (e < eNOERROR) {
        ERROR(e);
        SM_FreeHandle(handle);
        (Four) SM_Final( );
        exit(1);
    }


    e = RDsM_GetStatistics(handle, volId, &extentSize, &numPages, &numUsedPages, TRUE);
    if (e < eNOERROR) ERROR_AND_EXIT(e);

    printf("\n");
    printf("extentSize = %ld\n", extentSize);
    printf("numPages = %ld\n", numPages);
    printf("numUsedPages = %ld\n", numUsedPages);
    printf("\n");

    /* Commit the transaction. */
    e = SM_CommitTransaction(handle, &xactId);
    if (e < eNOERROR) ERROR_AND_EXIT(e);


    /* Dismount the volume. */
    e = SM_Dismount(handle, volId);
    if (e < eNOERROR) {
        ERROR(e);
        (Four) SM_FreeHandle(handle);
        (Four) SM_Final( );
        exit(1);
    }


    /* Finalize the LRDS. */
    e = SM_FreeHandle(handle);
    if (e < eNOERROR) {
        ERROR(e);
        exit(1);
    }

    e = SM_Final( );
    if (e < eNOERROR) {
        ERROR(e);
        exit(1);
    }

    return(0);

} /* main() */


