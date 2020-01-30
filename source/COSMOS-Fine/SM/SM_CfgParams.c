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
 * Module: SM_CfgParams.c
 *
 * Description:
 *  Manages the configuration parameters.
 *
 * Exports:
 *  Four SM_SetCfgParam(Four, char*, char*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



extern CfgParams_T sm_cfgParams;


/*@================================
 * SM_SetCfgParam( )
 *================================*/
Four SM_SetCfgParam(
    Four handle,
    char *name,                 /* IN configuration parameter name */
    char *value)                /* IN configuration parameter value */
{


    TR_PRINT(handle, TR_SM, TR1, ("SM_SetCfgParam(name=%P, value=%P)", name, value));


    /*@
     * Check parameters
     */
    if (name == NULL || value == NULL) ERR(handle, eBADPARAMETER);

    if (strcmp(name, "LOG_VOLUME_DEVICE_LIST") == 0) {

        strcpy(sm_cfgParams.logVolumeDeviceList, value);

    } else if (strcmp(name, "COHERENCY_VOLUME_DEVICE") == 0) {

        /* needless in multi-user version */
        printf("COHERENCY_VOLUME_DEVICE option is needless in multi-user version.\n");

    } else if (strcmp(name, "USE_DEADLOCK_AVOIDANCE") == 0) {

        /* needless in multi-user version */
        printf("USE_DEADLOCK_AVOIDANCE option is needless in multi-user version.\n");

    } else if (strcmp(name, "USE_BULKFLUSH") == 0) {

        /* needless in multi-user version */
        printf("USE_BULKFLUSH option is needless in multi-user version.\n");

    } else {

        ERR(handle, eINVALIDCFGPARAM_SM);
    }

    return(eNOERROR);

} /* SM_SetCfgParam() */


/*@================================
 * SM_GetCfgParam()
 *================================*/
char* SM_GetCfgParam(
    Four handle,
    char *name)                 /* IN configuration parameter name */
{
    char* value;

    TR_PRINT(handle, TR_SM, TR1, ("SM_GetCfgParam(name=%P)", name));

    /*@
     * Check parameters
     */
    if (name == (char*)NULL) ERR(handle, eBADPARAMETER);

    if (strcmp(name, "LOG_VOLUME_DEVICE_LIST") == 0) {

        value = sm_cfgParams.logVolumeDeviceList;

    }
    else {

        value = NULL;
    }

    return value;

} /* SM_GetCfgParam() */

