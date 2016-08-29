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
 * Module: RDsM_P_VolTable.c
 *
 * Description:
 *  Prints the content of the volume table.
 *
 * Exports:
 *  Four RDsM_P_VolTable(Two)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * RDsM_P_VolTable()
 *================================*/
/*
 * Function: Four RDsM_P_VolTable(Two)
 *
 * Description:
 *  print a specific entry in the volume table
 *
 * Returns:
 *  error code
 */
Four	RDsM_P_VolTable(
    Four 		handle,
    Two			i)		/* IN index of the volume table entry */
{
    TR_PRINT(TR_RDSM, TR1, ("RDsM_P_VolTable(handle, i=%lD)", i));


    printf("\n\n /* %ldth volume table entry is as follows */\n\n", i);
    printf("1. DevName: %s\n", RDSM_PER_THREAD_DS(handle).volTable[i].DevName);
    printf("2. Title: %s\n", RDSM_PER_THREAD_DS(handle).volTable[i].Title);
    printf("3. DevAddr: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].DevAddr);
    printf("4. VolNo: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].VolNo);
    printf("5. SizeOfExt: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].SizeOfExt);
    printf("6. NumOfExts: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].NumOfExts);
    printf("7. NumOfFreeExts: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].NumOfFreeExts);
    printf("8. FirstFreeExt: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].FirstFreeExt);
    printf("9. VolInfoSize: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].VolInfoSize);
    printf("10. UniqNumSize: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].UniqNumSize);
    printf("11. MetaDicSize: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].MetaDicSize);
    printf("12.BitMapSize: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].BitMapSize);
    printf("13.ExtEntryArraySize: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].ExtEntryArraySize);
    printf("14.TotalSysPages: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].TotalSysPages);
    printf("15.TotalSysExts: %ld\n\n", RDSM_PER_THREAD_DS(handle).volTable[i].TotalSysExts);
    printf("16.VolInfoPageId: 1) VolNo: %ld, ", RDSM_PER_THREAD_DS(handle).volTable[i].VolInfoPageId.volNo);
    printf("2) PageNo: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].VolInfoPageId.pageNo);
    printf("17.UniqNumPageId: 1) VolNo: %ld, ", RDSM_PER_THREAD_DS(handle).volTable[i].UniqNumPageId.volNo);
    printf("2) PageNo: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].UniqNumPageId.pageNo);
    printf("18.MetaDicPageId: 1) VolNo: %ld, ", RDSM_PER_THREAD_DS(handle).volTable[i].MetaDicPageId.volNo);
    printf("2) PageNo: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].MetaDicPageId.pageNo);
    printf("19.BitMapPageId: 1) VolNo: %ld, ", RDSM_PER_THREAD_DS(handle).volTable[i].BitMapPageId.volNo);
    printf("2) PageNo: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].BitMapPageId.pageNo);
    printf("20.ExtEntryArrayPageId: 1) VolNo: %ld, ", RDSM_PER_THREAD_DS(handle).volTable[i].ExtEntryArrayPageId.volNo);
    printf("2) PageNo: %ld\n", RDSM_PER_THREAD_DS(handle).volTable[i].ExtEntryArrayPageId.pageNo);

    return(eNOERROR);

} /* RDsM_P_VolTable() */
