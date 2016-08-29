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
 * Module: SM_GetVolumeStatistics.c
 *
 * Description:
 *
 * Exports:
 */


#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "trace.h"
#include "BtM.h"
#include "SM_Internal.h"	
#include "Util.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

#define MAXDEVICESINVOLUME 20

#define ERROR(e) {printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__); TM_transAbort(&xactId); end_process(procIndex); exit(1);}


int main(int argc, char *argv[])
{
    Four                e;                      /* error number */
    Four                i;                      /* a temporary variable */
    Four                volId;                  /* volume identifier */
    Two                 extentSize;
    Four                nTotalExtents;
    Four                nUsedExtents;
    sm_NumPages         pageInfo;
    Four                nUsedPages;
    Four                nFreePages;
    char                *devNames[MAXDEVICESINVOLUME]; /* relation name */    
    Four		handle;			       /* handel for each thread */


    if (argc < 2) {
        printf("%s [device path]+\n", argv[0]);
        exit(1);
    }

    /* get device names */
    for (i = 1; i < argc; i++) devNames[i-1] = argv[i]; 


    /* Initialize the KAOSS */
    handle = 0;		
    e = SM_Init(handle);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	exit(1);
    }
    
    /* Mount the given volume. */
    e = SM_Mount(handle, argc-1, devNames, &volId);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        _SM_Final(handle);
	exit(1);
    }

    e = _SM_GetStatistics_numExtents(handle, volId, &extentSize, &nTotalExtents, &nUsedExtents);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        _SM_Final(handle);
	exit(1);
    }
        
    e = _SM_GetStatistics_numPages(handle, volId, &pageInfo, FALSE, FALSE); 
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        _SM_Final(handle);
        exit(1);
    }

    /* Dismount the volume. */
    e = SM_Dismount(handle, volId);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        _SM_Final(handle);
	exit(1);
    }

    /* Finalize KAOSS. */
    e = SM_Final(handle);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
	exit(1);
    }

    printf("\n");
    printf("extent size = %ld\n", extentSize);
    printf("\n");
    printf("# of total extents = %ld\n", nTotalExtents);
    printf("# of used extents = %ld\n", nUsedExtents);
    printf("# of free extents = %ld\n", nTotalExtents - nUsedExtents);
    printf("\n");
    nUsedPages = pageInfo.numTotalPages; 
    nFreePages = nTotalExtents*extentSize - nUsedPages;
    printf("# of total pages = %ld\n", nTotalExtents*extentSize);
    printf("# of used pages = %ld\n", nUsedPages);
    printf("# of free pages = %ld\n", nFreePages);
    printf("\n");
/*
    printf("# of numSlottedPage = %ld\n", pageInfo.numSlottedPage);
    printf("# of numLOT_I_Node = %ld\n", pageInfo.numLOT_I_Node);
    printf("# of numLOT_L_Node = %ld\n", pageInfo.numLOT_L_Node);
    printf("# of numBtree_I_Node = %ld\n", pageInfo.numBtree_I_Node);
    printf("# of numBtree_L_Node = %ld\n", pageInfo.numBtree_L_Node);
    printf("# of numBtree_O_Node = %ld\n", pageInfo.numBtree_O_Node);
    printf("# of numExtEntryPage = %ld\n", pageInfo.numExtEntryPage);
    printf("# of numBitMapPage = %ld\n", pageInfo.numBitMapPage);
    printf("# of numVolInfoPage = %ld\n", pageInfo.numVolInfoPage);
    printf("# of numMetaDicPage = %ld\n", pageInfo.numMetaDicPage);
    printf("# of numUniqueNumPage = %ld\n", pageInfo.numUniqueNumPage);
    printf("\n");
*/


    return(0);
    
} /* main() */
