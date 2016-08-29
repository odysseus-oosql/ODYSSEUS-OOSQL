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
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <string.h>

#include "common.h"
#include "error.h"
#include "trace.h"
#include "SM.h"
#include "OM_Internal.h"
#include "BfM.h"
#include "RDsM.h"
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


int main(int argc, char *argv[])
{
    Four        e;                      /* error code */
    char        *devNames[20];
    Four        volId;
    Four        i;
    XactID      xactId;
    

    if (argc < 2) {
        printf("%s [device path]+\n", argv[0]);
        exit(1);
    }

    /* get device names */
    for (i = 1; i < argc; i++) devNames[i-1] = argv[i]; 

    /*@
     * initialization
     */
    /* Initialize the COSMOS */
    e = SM_Init(handle);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, SM_Err(handle, e));
        exit(1);
    }

    e = SM_Mount(handle, argc-1, devNames, &volId);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, SM_Err(handle, e));
	exit(1);
    }

    /* start Format transaction */
    e = SM_BeginTransaction(handle, &xactId, X_RR_RR); 
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        SM_Final(handle);
        exit(1);
    }

    e = RDsM_P_UsedPageMap(handle, volId);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        SM_Final(handle);
        exit(1);
    }

    /* end Format transaction */
    e = SM_CommitTransaction(handle, &xactId);
    if (e < 0) {
        printf("FILE:%s LINE:%ld\n", __FILE__, __LINE__);
        SM_Final(handle);
        exit(1);
    }

    e = SM_Dismount(handle, volId);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, SM_Err(handle, e));
	exit(1);
    }

    e = SM_Final(handle);
    if (e < eNOERROR) {
        printf("FILE:%s LINE:%ld :: %s\n", __FILE__, __LINE__, SM_Err(handle, e));
        exit(1);
    }
    
    return(0);

} /* main() */
